use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use pool_common::logging::init_logging;
use pool_runtime::runtime::{
    bootstrap_shared_runtime, build_engine, build_stratum_server, start_stratum_background_tasks,
};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let config_path = parse_config_path(env::args().skip(1))?;
    let shared = bootstrap_shared_runtime(&config_path).await?;
    let engine = build_engine(&shared).await?;
    let stratum = build_stratum_server(&shared, Arc::clone(&engine))?;
    start_stratum_background_tasks(&shared, engine);

    info!(pool = %shared.cfg.pool_name, "stratum runtime started");

    tokio::select! {
        result = Arc::clone(&stratum).run() => {
            if let Err(err) = result {
                return Err(anyhow!("stratum server exited: {err}"));
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
        println!("usage: blocknet-pool-stratum [flags]");
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
