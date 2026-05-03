use std::sync::Arc;

use anyhow::{anyhow, Result};
use pool_common::{cli::parse_config_path, logging::init_logging};
use pool_runtime::runtime::{
    bootstrap_shared_runtime, build_engine, build_stratum_server, start_stratum_background_tasks,
};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let config_path = parse_config_path("blocknet-pool-stratum", std::env::args().skip(1));
    let shared = bootstrap_shared_runtime(&config_path).await?;
    let engine = build_engine(&shared).await?;
    let stratum = build_stratum_server(&shared, Arc::clone(&engine))?;
    start_stratum_background_tasks(&shared, engine, Arc::clone(&stratum));

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
