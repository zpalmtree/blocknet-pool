use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};

use crate::api::ApiState;
use crate::config::Config;
use pool_runtime::runtime::ApiRuntime;

pub(crate) async fn bootstrap_api_runtime(config_path: &Path) -> Result<(Config, ApiRuntime)> {
    pool_runtime::runtime::load_dotenv(config_path);
    let cfg = Config::load(config_path)?;
    validate_api_config(&cfg)?;
    let runtime =
        pool_runtime::runtime::bootstrap_api_runtime_from_config(cfg.runtime.clone()).await?;
    Ok((cfg, runtime))
}

pub(crate) fn build_api_state(cfg: &Config, runtime: &ApiRuntime) -> ApiState {
    ApiState::new(
        cfg.clone(),
        Arc::clone(&runtime.store),
        Arc::clone(&runtime.jobs),
        Arc::clone(&runtime.node),
    )
}

pub(crate) fn api_listen_addr(cfg: &Config) -> Result<SocketAddr> {
    format!("{}:{}", cfg.api_host, cfg.api_port)
        .parse()
        .with_context(|| {
            format!(
                "invalid api listen address {}:{}",
                cfg.api_host, cfg.api_port
            )
        })
}

fn validate_api_config(cfg: &Config) -> Result<()> {
    if cfg.api_key.trim().is_empty() {
        bail!("api_key must be set for protected admin routes");
    }
    if cfg.monitor_ingest_secret.trim().is_empty() {
        bail!("monitor_ingest_secret must be set for signed Cloudflare monitor ingest");
    }
    Ok(())
}
