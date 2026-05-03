use anyhow::Result;
use pool_common::{cli::parse_config_path, logging::init_logging};
use pool_recovery::{Config, RecoveryAgent};

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let config_path = parse_config_path("blocknet-pool-recoveryd", std::env::args().skip(1));
    let cfg = Config::load(&config_path)?;
    let agent = RecoveryAgent::new(cfg).await?;
    agent.run().await
}
