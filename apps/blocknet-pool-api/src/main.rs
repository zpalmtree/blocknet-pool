use anyhow::{anyhow, Result};
use pool_common::{cli::parse_config_path, logging::init_logging};
use tracing::info;

mod api;
mod config;
mod runtime;

use api::run_api;
use runtime::{api_listen_addr, bootstrap_api_runtime, build_api_state};

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let config_path = parse_config_path("blocknet-pool-api", std::env::args().skip(1));
    let (cfg, runtime) = bootstrap_api_runtime(&config_path).await?;
    let api_addr = api_listen_addr(&cfg)?;
    let api_state = build_api_state(&cfg, &runtime);

    info!(pool = %cfg.runtime.pool_name, "api runtime started");

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
