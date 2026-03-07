use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use tracing::{info, warn};

use crate::api::{
    load_persisted_status_history, ApiState, DaemonHealthCache, DbTotalsCache, InsightsCache,
    NetworkHashrateCache, StatusHistory, DEFAULT_MAX_SSE_SUBSCRIBERS,
};
use crate::config::{generate_default_env, Config};
use crate::engine::{JobRepository, NodeApi, PoolEngine, ShareStore};
use crate::jobs::JobManager;
use crate::node::NodeClient;
use crate::payout::PayoutProcessor;
use crate::pow::Argon2PowHasher;
use crate::service_state::{PersistedRuntimeSnapshot, LIVE_RUNTIME_SNAPSHOT_META_KEY};
use crate::stats::PoolStats;
use crate::store::PoolStore;
use crate::stratum::StratumServer;
use crate::validation::{ValidationEngine, ValidationStateStore};

const HASHRATE_WARMUP_WINDOW: Duration = Duration::from_secs(5 * 60);

pub struct SharedRuntime {
    pub cfg: Config,
    pub store: Arc<PoolStore>,
    pub node: Arc<NodeClient>,
    pub jobs: Arc<JobManager>,
    pub validation: Arc<ValidationEngine>,
    pub stats: Arc<PoolStats>,
}

pub async fn bootstrap_shared_runtime(config_path: &Path) -> Result<SharedRuntime> {
    ensure_runtime_files(config_path)?;
    load_dotenv(config_path);
    let cfg = Config::load(config_path)?;
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
            "api_key is empty; protected routes (/api/miners, /api/payouts, /api/fees, /api/admin/blocks/:height/reward-breakdown, /api/health, /api/daemon/logs/stream) are disabled while public routes remain available (/api/info, /api/stats, /api/stats/history, /api/stats/insights, /api/luck, /api/status, /api/events, /api/blocks, /api/payouts/recent, /api/miner/:address, /api/miner/:address/balance, /api/miner/:address/hashrate)"
        );
    }
    if !cfg.api_tls_cert_path.trim().is_empty() ^ !cfg.api_tls_key_path.trim().is_empty() {
        warn!(
            cert_path = %cfg.api_tls_cert_path,
            key_path = %cfg.api_tls_key_path,
            "api tls is partially configured; set both api_tls_cert_path and api_tls_key_path to enable tls"
        );
    }
    if !is_local_bind_host(&cfg.stratum_host) {
        warn!(
            host = %cfg.stratum_host,
            port = cfg.stratum_port,
            "stratum is bound on a non-local host and transport is plaintext; place stratum behind a tls terminator when exposed publicly"
        );
    }
    warn_on_validation_visibility_config(&cfg);

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

    let validation = {
        let cfg = cfg.clone();
        let store = Arc::clone(&store) as Arc<dyn ValidationStateStore>;
        tokio::task::spawn_blocking(move || {
            Arc::new(ValidationEngine::new_with_state_store(
                cfg,
                Arc::new(Argon2PowHasher::default()),
                store,
            ))
        })
        .await
        .context("join validation engine init task")?
    };

    Ok(SharedRuntime {
        cfg,
        store,
        node,
        jobs,
        validation,
        stats: Arc::new(PoolStats::new()),
    })
}

pub async fn build_api_state(shared: &SharedRuntime) -> Result<ApiState> {
    let persisted_status_history = {
        let store = Arc::clone(&shared.store);
        match tokio::task::spawn_blocking(move || load_persisted_status_history(store.as_ref()))
            .await
            .context("join status history load task")?
        {
            Ok(value) => value,
            Err(err) => {
                warn!(error = %err, "failed loading persisted status history; starting fresh");
                StatusHistory::default()
            }
        }
    };

    Ok(ApiState {
        config: shared.cfg.clone(),
        store: Arc::clone(&shared.store),
        stats: Arc::clone(&shared.stats),
        jobs: Arc::clone(&shared.jobs),
        node: Arc::clone(&shared.node),
        validation: Arc::clone(&shared.validation),
        db_totals_cache: Arc::new(Mutex::new(DbTotalsCache::default())),
        daemon_health_cache: Arc::new(Mutex::new(DaemonHealthCache::default())),
        network_hashrate_cache: Arc::new(Mutex::new(NetworkHashrateCache::default())),
        insights_cache: Arc::new(Mutex::new(InsightsCache::default())),
        miner_pending_estimate_cache: Arc::new(Mutex::new(std::collections::HashMap::new())),
        live_runtime_snapshot_cache: Arc::new(Mutex::new(
            crate::api::LiveRuntimeSnapshotCache::default(),
        )),
        status_history: Arc::new(Mutex::new(persisted_status_history)),
        sse_subscriber_limiter: Arc::new(tokio::sync::Semaphore::new(DEFAULT_MAX_SSE_SUBSCRIBERS)),
        api_key: shared.cfg.api_key.clone(),
        pool_name: shared.cfg.pool_name.clone(),
        pool_url: shared.cfg.pool_url.clone(),
        stratum_port: shared.cfg.stratum_port,
        pool_fee_pct: shared.cfg.pool_fee_pct,
        pool_fee_flat: shared.cfg.pool_fee_flat,
        min_payout_amount: shared.cfg.min_payout_amount,
        blocks_before_payout: shared.cfg.blocks_before_payout,
        payout_scheme: shared.cfg.payout_scheme.clone(),
        started_at: Instant::now(),
        started_at_system: SystemTime::now(),
    })
}

pub fn build_engine(shared: &SharedRuntime) -> Arc<PoolEngine> {
    Arc::new(PoolEngine::new(
        shared.cfg.clone(),
        Arc::clone(&shared.validation),
        Arc::clone(&shared.jobs) as Arc<dyn JobRepository>,
        Arc::clone(&shared.store) as Arc<dyn ShareStore>,
        Arc::clone(&shared.node) as Arc<dyn NodeApi>,
    ))
}

pub fn build_stratum_server(
    shared: &SharedRuntime,
    engine: Arc<PoolEngine>,
) -> Result<Arc<StratumServer>> {
    let addr = stratum_listen_addr(&shared.cfg)?;
    Ok(StratumServer::new(
        addr,
        engine,
        Arc::clone(&shared.jobs),
        Arc::clone(&shared.stats),
        shared.cfg.stratum_idle_timeout_duration(),
        shared.cfg.stratum_submit_rate_limit_window_duration(),
        shared.cfg.stratum_submit_rate_limit_max.max(1) as usize,
    ))
}

pub fn api_listen_addr(cfg: &Config) -> Result<SocketAddr> {
    format!("{}:{}", cfg.api_host, cfg.api_port)
        .parse()
        .with_context(|| {
            format!(
                "invalid api listen address {}:{}",
                cfg.api_host, cfg.api_port
            )
        })
}

pub fn stratum_listen_addr(cfg: &Config) -> Result<SocketAddr> {
    format!("{}:{}", cfg.stratum_host, cfg.stratum_port)
        .parse()
        .with_context(|| {
            format!(
                "invalid stratum listen address {}:{}",
                cfg.stratum_host, cfg.stratum_port
            )
        })
}

pub fn start_api_background_tasks(api_state: ApiState) {
    start_status_sampler(api_state);
}

pub fn start_stratum_background_tasks(shared: &SharedRuntime, engine: Arc<PoolEngine>) {
    start_found_block_recovery(engine);
    let payout = PayoutProcessor::new(
        shared.cfg.clone(),
        Arc::clone(&shared.store),
        Arc::clone(&shared.node),
    );
    payout.start();
    start_seen_share_gc(shared.cfg.clone(), Arc::clone(&shared.store));
    start_stat_snapshots(Arc::clone(&shared.stats), Arc::clone(&shared.store));
    start_retention_maintenance(shared.cfg.clone(), Arc::clone(&shared.store));
    start_live_runtime_snapshot_persist(
        Arc::clone(&shared.jobs),
        Arc::clone(&shared.stats),
        Arc::clone(&shared.validation),
        Arc::clone(&shared.store),
    );
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
        let retain = Duration::from_secs(60 * 24 * 60 * 60);
        let hr_window = Duration::from_secs(60 * 60);
        loop {
            ticker.tick().await;
            let snap = stats.snapshot();
            let store = Arc::clone(&store);
            match tokio::task::spawn_blocking(move || {
                let hashrate = db_pool_hashrate(&store, hr_window);
                store.add_stat_snapshot(
                    SystemTime::now(),
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

fn start_retention_maintenance(cfg: Config, store: Arc<PoolStore>) {
    tokio::spawn(async move {
        let interval = cfg
            .retention_interval_duration()
            .max(Duration::from_secs(5 * 60));
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let now = SystemTime::now();
            let shares_before = cfg
                .shares_retention_duration()
                .and_then(|age| now.checked_sub(age));
            let payouts_before = cfg
                .payouts_retention_duration()
                .and_then(|age| now.checked_sub(age));
            if shares_before.is_none() && payouts_before.is_none() {
                continue;
            }

            let store = Arc::clone(&store);
            match tokio::task::spawn_blocking(move || {
                store.rollup_and_prune_retention(shares_before, payouts_before)
            })
            .await
            {
                Ok(Ok(report)) => {
                    if report.shares_pruned > 0 || report.payouts_pruned > 0 {
                        tracing::info!(
                            shares_pruned = report.shares_pruned,
                            payouts_pruned = report.payouts_pruned,
                            "completed retention rollup/prune cycle"
                        );
                    }
                }
                Ok(Err(err)) => tracing::warn!(error = %err, "retention rollup/prune failed"),
                Err(err) => tracing::warn!(error = %err, "retention task join failed"),
            }
        }
    });
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

fn start_live_runtime_snapshot_persist(
    jobs: Arc<JobManager>,
    stats: Arc<PoolStats>,
    validation: Arc<ValidationEngine>,
    store: Arc<PoolStore>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(5));
        loop {
            ticker.tick().await;
            let payload = PersistedRuntimeSnapshot::from_live(
                stats.snapshot(),
                validation.snapshot(),
                jobs.runtime_snapshot(),
            );
            let store = Arc::clone(&store);
            match tokio::task::spawn_blocking(move || -> Result<()> {
                let bytes = serde_json::to_vec(&payload)?;
                store.set_meta(LIVE_RUNTIME_SNAPSHOT_META_KEY, &bytes)?;
                Ok(())
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::warn!(error = %err, "failed persisting live runtime snapshot");
                }
                Err(err) => {
                    tracing::warn!(error = %err, "live runtime snapshot task join failed");
                }
            }
        }
    });
}

fn db_pool_hashrate(store: &PoolStore, window: Duration) -> f64 {
    let since = SystemTime::now()
        .checked_sub(window)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let Ok((total_diff, count, oldest, newest)) = store.hashrate_stats_pool(since) else {
        return 0.0;
    };
    hashrate_from_stats_with_warmup(
        total_diff,
        count,
        oldest,
        newest,
        window,
        HASHRATE_WARMUP_WINDOW,
    )
}

fn hashrate_from_stats_with_warmup(
    total_diff: u64,
    count: u64,
    oldest: Option<SystemTime>,
    newest: Option<SystemTime>,
    smoothing_window: Duration,
    warmup_window: Duration,
) -> f64 {
    if total_diff == 0 {
        return 0.0;
    }

    let smoothing_secs = smoothing_window.as_secs_f64().max(1.0);
    let warmup_secs = warmup_window.as_secs_f64().clamp(1.0, smoothing_secs);
    let observed_secs = if count < 2 {
        0.0
    } else {
        let (Some(oldest), Some(newest)) = (oldest, newest) else {
            return total_diff as f64 / warmup_secs;
        };
        let Ok(window) = newest.duration_since(oldest) else {
            return total_diff as f64 / warmup_secs;
        };
        window.as_secs_f64()
    };

    let denominator = if observed_secs >= 1.0 {
        observed_secs.clamp(warmup_secs, smoothing_secs)
    } else {
        warmup_secs
    };
    total_diff as f64 / denominator
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
        std::path::PathBuf::from(".env"),
    ];

    for candidate in candidates {
        if dotenvy::from_path(&candidate).is_ok() {
            info!(path = %candidate.display(), "loaded environment");
            return;
        }
    }
}

fn warn_on_validation_visibility_config(cfg: &Config) {
    if cfg.validation_mode.trim().eq_ignore_ascii_case("full") {
        return;
    }

    let periodic_floor = if cfg.min_sample_every > 0 {
        1.0 / cfg.min_sample_every as f64
    } else {
        0.0
    };
    let typical_verified_ratio = cfg.sample_rate.max(periodic_floor).clamp(0.0, 1.0);

    if cfg.payout_min_verified_ratio >= typical_verified_ratio - f64::EPSILON {
        warn!(
            sample_rate = cfg.sample_rate,
            min_sample_every = cfg.min_sample_every,
            warmup_shares = cfg.warmup_shares,
            payout_min_verified_ratio = cfg.payout_min_verified_ratio,
            typical_verified_ratio,
            "payout_min_verified_ratio is at or above the sampler's typical verified-share coverage; honest miners can miss payout eligibility due to sampling variance and vardiff"
        );
    } else if cfg.payout_min_verified_ratio > 0.0
        && typical_verified_ratio - cfg.payout_min_verified_ratio < 0.01
    {
        warn!(
            sample_rate = cfg.sample_rate,
            min_sample_every = cfg.min_sample_every,
            warmup_shares = cfg.warmup_shares,
            payout_min_verified_ratio = cfg.payout_min_verified_ratio,
            typical_verified_ratio,
            "payout_min_verified_ratio is very close to the sampler's typical verified-share coverage; honest miners may flap around the cutoff"
        );
    }

    if cfg.payout_provisional_cap_multiplier > 0.0 {
        let full_credit_verified_ratio =
            1.0 / (1.0 + cfg.payout_provisional_cap_multiplier.max(0.0));
        if full_credit_verified_ratio > typical_verified_ratio + f64::EPSILON {
            warn!(
                sample_rate = cfg.sample_rate,
                min_sample_every = cfg.min_sample_every,
                warmup_shares = cfg.warmup_shares,
                payout_provisional_cap_multiplier = cfg.payout_provisional_cap_multiplier,
                full_credit_verified_ratio,
                typical_verified_ratio,
                "sampler coverage is below the provisional cap's full-credit target; honest miners may see reduced payout weight until more shares are fully verified"
            );
        }
    }
}

fn is_local_bind_host(host: &str) -> bool {
    let trimmed = host.trim().to_ascii_lowercase();
    matches!(trimmed.as_str(), "127.0.0.1" | "::1" | "localhost")
}
