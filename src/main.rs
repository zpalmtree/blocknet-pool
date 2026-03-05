use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use blocknet_pool_rs::api::{run_api, ApiState};
use blocknet_pool_rs::config::{generate_default_env, Config};
use blocknet_pool_rs::engine::{JobRepository, NodeApi, PoolEngine, ShareStore};
use blocknet_pool_rs::jobs::JobManager;
use blocknet_pool_rs::logging::init_logging;
use blocknet_pool_rs::node::NodeClient;
use blocknet_pool_rs::payout::PayoutProcessor;
use blocknet_pool_rs::pow::Argon2PowHasher;
use blocknet_pool_rs::stats::PoolStats;
use blocknet_pool_rs::store::PoolStore;
use blocknet_pool_rs::stratum::StratumServer;
use blocknet_pool_rs::validation::ValidationEngine;
use parking_lot::Mutex;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let mut args = env::args().skip(1);
    let first = args.next();

    if matches!(first.as_deref(), Some("--help") | Some("-h")) {
        println!("usage: blocknet-pool-rs [flags]");
        println!();
        println!("flags:");
        println!("  -c, --config  path to config file (default: config.json)");
        return Ok(());
    }

    let mut config_path = PathBuf::from("config.json");
    if let Some(flag) = first {
        if flag == "--config" || flag == "-c" {
            if let Some(path) = args.next() {
                config_path = PathBuf::from(path);
            }
        }
    }

    ensure_runtime_files(&config_path)?;
    load_dotenv(&config_path);
    let cfg = Config::load(&config_path)?;
    info!(
        "vardiff init={} min={} max={} target_shares={} retarget={}",
        cfg.initial_share_difficulty,
        cfg.min_share_difficulty,
        cfg.max_share_difficulty,
        cfg.vardiff_target_shares,
        cfg.vardiff_retarget_interval
    );
    if cfg.api_key.trim().is_empty() {
        warn!(
            host = %cfg.api_host,
            port = cfg.api_port,
            "api_key is empty; only /api/stats and /api/miner/:address are public (other /api routes disabled)"
        );
    }

    let cfg_for_store = cfg.clone();
    let store = tokio::task::spawn_blocking(move || PoolStore::open_from_config(&cfg_for_store))
        .await
        .context("join store initialization task")??;
    let daemon_api = cfg.daemon_api.clone();
    let daemon_token = cfg.daemon_token.clone();
    let daemon_data_dir = cfg.daemon_data_dir.clone();
    let daemon_cookie_path = cfg.daemon_cookie_path.clone();
    let node = tokio::task::spawn_blocking(move || {
        NodeClient::new_with_daemon_auth(
            &daemon_api,
            &daemon_token,
            &daemon_data_dir,
            &daemon_cookie_path,
        )
    })
    .await
    .context("join node client init task")??;
    let node = Arc::new(node);

    let node_for_probe = Arc::clone(&node);
    if let Err(err) = tokio::task::spawn_blocking(move || node_for_probe.get_status())
        .await
        .context("join node startup probe task")?
    {
        warn!(error = %err, "cannot reach daemon on startup; continuing");
    }

    let jobs = JobManager::new(Arc::clone(&node), cfg.clone());
    jobs.start();

    let validation = Arc::new(ValidationEngine::new(
        cfg.clone(),
        Arc::new(Argon2PowHasher::default()),
    ));

    let engine = Arc::new(PoolEngine::new(
        cfg.clone(),
        Arc::clone(&validation),
        Arc::clone(&jobs) as Arc<dyn JobRepository>,
        Arc::clone(&store) as Arc<dyn ShareStore>,
        Arc::clone(&node) as Arc<dyn NodeApi>,
    ));
    start_found_block_recovery(Arc::clone(&engine));

    let stats = Arc::new(PoolStats::new());

    let stratum_addr = SocketAddr::from(([0, 0, 0, 0], cfg.stratum_port));
    let stratum = StratumServer::new(
        stratum_addr,
        Arc::clone(&engine),
        Arc::clone(&jobs),
        Arc::clone(&stats),
    );

    let payout = PayoutProcessor::new(cfg.clone(), Arc::clone(&store), Arc::clone(&node));
    payout.start();
    start_seen_share_gc(cfg.clone(), Arc::clone(&store));
    start_stat_snapshots(Arc::clone(&stats), Arc::clone(&store));

    let api_addr: SocketAddr = format!("{}:{}", cfg.api_host, cfg.api_port)
        .parse()
        .with_context(|| {
            format!(
                "invalid api listen address {}:{}",
                cfg.api_host, cfg.api_port
            )
        })?;

    let api_state = ApiState {
        store: Arc::clone(&store),
        stats: Arc::clone(&stats),
        jobs: Arc::clone(&jobs),
        node: Arc::clone(&node),
        validation: Arc::clone(&validation),
        db_totals_cache: Arc::new(Mutex::new(blocknet_pool_rs::api::DbTotalsCache::default())),
        daemon_health_cache: Arc::new(Mutex::new(
            blocknet_pool_rs::api::DaemonHealthCache::default(),
        )),
        network_hashrate_cache: Arc::new(Mutex::new(
            blocknet_pool_rs::api::NetworkHashrateCache::default(),
        )),
        insights_cache: Arc::new(Mutex::new(blocknet_pool_rs::api::InsightsCache::default())),
        status_history: Arc::new(Mutex::new(blocknet_pool_rs::api::StatusHistory::default())),
        api_key: cfg.api_key.clone(),
        pool_name: cfg.pool_name.clone(),
        pool_url: cfg.pool_url.clone(),
        stratum_port: cfg.stratum_port,
        pool_fee_pct: cfg.pool_fee_pct,
        pool_fee_flat: cfg.pool_fee_flat,
        min_payout_amount: cfg.min_payout_amount,
        blocks_before_payout: cfg.blocks_before_payout,
        payout_scheme: cfg.payout_scheme.clone(),
        started_at: std::time::Instant::now(),
        started_at_system: std::time::SystemTime::now(),
    };

    start_status_sampler(api_state.clone());

    info!(pool = %cfg.pool_name, "pool runtime started");

    tokio::select! {
        result = Arc::clone(&stratum).run() => {
            if let Err(err) = result {
                return Err(anyhow!("stratum server exited: {err}"));
            }
        }
        result = run_api(api_addr, api_state) => {
            if let Err(err) = result {
                return Err(anyhow!("api server exited: {err}"));
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("received shutdown signal");
        }
    }

    Ok(())
}

fn start_seen_share_gc(cfg: Config, store: Arc<PoolStore>) {
    tokio::spawn(async move {
        let interval = cfg
            .seen_share_gc_interval_duration()
            .max(Duration::from_secs(30));
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let store = Arc::clone(&store);
            match tokio::task::spawn_blocking(move || store.clean_expired_seen_shares()).await {
                Ok(Ok(removed)) if removed > 0 => {
                    tracing::debug!(removed, "cleaned expired seen-share entries");
                }
                Ok(Ok(_)) => {}
                Ok(Err(err)) => tracing::warn!(error = %err, "seen-share cleanup failed"),
                Err(err) => tracing::warn!(error = %err, "seen-share cleanup task join failed"),
            }
        }
    });
}

fn start_stat_snapshots(stats: Arc<PoolStats>, store: Arc<PoolStore>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(5 * 60));
        let retain = Duration::from_secs(60 * 24 * 60 * 60); // 60 days
        let hr_window = Duration::from_secs(60 * 60);
        loop {
            ticker.tick().await;
            let snap = stats.snapshot();
            let store = Arc::clone(&store);
            match tokio::task::spawn_blocking(move || {
                let hashrate = db_pool_hashrate(&store, hr_window);
                store.add_stat_snapshot(
                    std::time::SystemTime::now(),
                    hashrate,
                    snap.connected_miners as i32,
                    snap.connected_workers as i32,
                )?;
                store.clean_old_snapshots(retain)?;
                Ok::<_, anyhow::Error>(())
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => tracing::warn!(error = %err, "stat snapshot failed"),
                Err(err) => tracing::warn!(error = %err, "stat snapshot task join failed"),
            }
        }
    });
}

fn db_pool_hashrate(store: &PoolStore, window: Duration) -> f64 {
    let since = std::time::SystemTime::now()
        .checked_sub(window)
        .unwrap_or(std::time::UNIX_EPOCH);
    let Ok((total_diff, count, oldest, newest)) = store.hashrate_stats_pool(since) else {
        return 0.0;
    };
    if count < 2 {
        return 0.0;
    }
    let (Some(oldest), Some(newest)) = (oldest, newest) else {
        return 0.0;
    };
    let Ok(w) = newest.duration_since(oldest) else {
        return 0.0;
    };
    if w.as_secs_f64() < 1.0 {
        return 0.0;
    }
    total_diff as f64 / w.as_secs_f64()
}

fn start_found_block_recovery(engine: Arc<PoolEngine>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        loop {
            ticker.tick().await;
            let engine = Arc::clone(&engine);
            match tokio::task::spawn_blocking(move || engine.recover_found_block_outbox()).await {
                Ok(()) => {}
                Err(err) => {
                    tracing::warn!(error = %err, "found-block recovery task join failed")
                }
            }
        }
    });
}

fn start_status_sampler(api_state: ApiState) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        loop {
            ticker.tick().await;
            api_state.sample_status().await;
        }
    });
}

fn ensure_runtime_files(config_path: &Path) -> Result<()> {
    if !config_path.exists() {
        if let Some(parent) = config_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create config dir {}", parent.display()))?;
            }
        }
        Config::write_default(config_path)?;
        info!(path = %config_path.display(), "created default config");
    }

    let env_path = config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".env");
    if generate_default_env(&env_path)? {
        warn!(
            path = %env_path.display(),
            "created .env template; set BLOCKNET_WALLET_PASSWORD"
        );
    }
    Ok(())
}

fn load_dotenv(config_path: &Path) {
    let candidates = [
        config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(".env"),
        PathBuf::from(".env"),
    ];

    for candidate in candidates {
        if dotenvy::from_path(&candidate).is_ok() {
            info!(path = %candidate.display(), "loaded environment");
            return;
        }
    }
}
