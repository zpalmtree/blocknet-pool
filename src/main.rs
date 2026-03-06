use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use blocknet_pool_rs::api::run_api;
use blocknet_pool_rs::logging::init_logging;
use blocknet_pool_rs::runtime::{
    api_listen_addr, bootstrap_shared_runtime, build_api_state, build_engine,
    build_stratum_server, start_api_background_tasks, start_stratum_background_tasks,
};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let config_path = parse_config_path(env::args().skip(1))?;
    let shared = bootstrap_shared_runtime(&config_path).await?;
    let engine = build_engine(&shared);
    let stratum = build_stratum_server(&shared, Arc::clone(&engine))?;
    start_stratum_background_tasks(&shared, engine);

    let api_addr = api_listen_addr(&shared.cfg)?;
    let api_state = build_api_state(&shared).await?;
    start_api_background_tasks(api_state.clone());

    info!(pool = %shared.cfg.pool_name, "pool runtime started");

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

fn parse_config_path(mut args: impl Iterator<Item = String>) -> Result<PathBuf> {
    let first = args.next();
    if matches!(first.as_deref(), Some("--help") | Some("-h")) {
        println!("usage: blocknet-pool-rs [flags]");
        println!();
        println!("flags:");
        println!("  -c, --config  path to config file (default: config.json)");
        std::process::exit(0);
    }

    let mut config_path = PathBuf::from("config.json");
    if let Some(flag) = first {
        if flag == "--config" || flag == "-c" {
            if let Some(path) = args.next() {
                config_path = PathBuf::from(path);
            }
        }
    }
    Ok(config_path)
}
