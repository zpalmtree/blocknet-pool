use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use blocknet_pool_api_app::api::run_api;
use blocknet_pool_api_app::runtime::{
    api_listen_addr, bootstrap_api_runtime, build_api_state, start_api_background_tasks,
};
use pool_common::logging::init_logging;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let config_path = parse_config_path(env::args().skip(1))?;
    let (cfg, shared) = bootstrap_api_runtime(&config_path).await?;
    let api_addr = api_listen_addr(&cfg)?;
    let api_state = build_api_state(&cfg, &shared).await?;
    start_api_background_tasks(api_state.clone());

    info!(pool = %cfg.pool_name, "api runtime started");

    tokio::select! {
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
        println!("usage: blocknet-pool-api [flags]");
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
