use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use blocknet_pool_rs::api::{run_api, ApiState};
use blocknet_pool_rs::config::{generate_default_env, Config};
use blocknet_pool_rs::db::SqliteStore;
use blocknet_pool_rs::engine::{JobRepository, NodeApi, PoolEngine, ShareStore};
use blocknet_pool_rs::jobs::JobManager;
use blocknet_pool_rs::node::NodeClient;
use blocknet_pool_rs::payout::PayoutProcessor;
use blocknet_pool_rs::pow::Argon2PowHasher;
use blocknet_pool_rs::stats::PoolStats;
use blocknet_pool_rs::stratum::StratumServer;
use blocknet_pool_rs::validation::ValidationEngine;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".parse().expect("valid filter")),
        )
        .init();

    let mut args = env::args().skip(1);
    let first = args.next();

    match first.as_deref() {
        Some("init") => {
            let config_path = PathBuf::from("config.json");
            Config::write_default(&config_path)?;
            let env_path = config_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(".env");
            let created = generate_default_env(&env_path)?;
            println!("wrote {}", config_path.display());
            if created {
                println!("wrote {}", env_path.display());
            } else {
                println!("kept existing {}", env_path.display());
            }
            println!(
                "IMPORTANT: edit .env and set BLOCKNET_WALLET_PASSWORD before running the pool."
            );
            return Ok(());
        }
        Some("--help") | Some("-h") => {
            println!("usage: blocknet-pool-rs [command] [flags]");
            println!();
            println!("commands:");
            println!("  init          generate default config.json and .env");
            println!();
            println!("flags:");
            println!("  -c, --config  path to config file (default: config.json)");
            return Ok(());
        }
        _ => {}
    }

    let mut config_path = PathBuf::from("config.json");
    if let Some(flag) = first {
        if flag == "--config" || flag == "-c" {
            if let Some(path) = args.next() {
                config_path = PathBuf::from(path);
            }
        }
    }

    load_dotenv(&config_path);
    let cfg = Config::load(&config_path)?;

    let store = SqliteStore::open(&cfg.database_path)?;
    let node = Arc::new(NodeClient::new(&cfg.daemon_api, &cfg.daemon_token)?);

    if let Err(err) = node.get_status() {
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
        validation: Arc::clone(&validation),
    };

    info!(pool = %cfg.pool_name, "pool runtime started");

    tokio::select! {
        result = Arc::clone(&stratum).run() => {
            result?;
        }
        result = run_api(api_addr, api_state) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            info!("received shutdown signal");
        }
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
