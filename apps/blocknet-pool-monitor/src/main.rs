use anyhow::Result;
use pool_common::{cli::parse_config_path, logging::init_logging};

mod config;
mod monitor;

use monitor::run_monitor;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let config_path = parse_config_path("blocknet-pool-monitor", std::env::args().skip(1));
    run_monitor(&config_path).await
}
