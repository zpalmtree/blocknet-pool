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

/// Placeholder shipped in `config.example.json`.
const EXAMPLE_SECRET_PLACEHOLDER: &str = "replace-with-a-long-random-secret";
const MIN_API_KEY_LEN: usize = 32;

fn validate_api_config(cfg: &Config) -> Result<()> {
    let api_key = cfg.api_key.trim();
    if api_key.is_empty() {
        bail!("api_key must be set for protected admin routes");
    }
    if api_key == EXAMPLE_SECRET_PLACEHOLDER {
        bail!("api_key is still the config.example.json placeholder; generate a random key (e.g. openssl rand -hex 32)");
    }
    if api_key.chars().count() < MIN_API_KEY_LEN {
        bail!("api_key must be at least {MIN_API_KEY_LEN} characters (e.g. openssl rand -hex 32)");
    }
    let ingest_secret = cfg.monitor_ingest_secret.trim();
    if ingest_secret.is_empty() {
        bail!("monitor_ingest_secret must be set for signed Cloudflare monitor ingest");
    }
    if ingest_secret == EXAMPLE_SECRET_PLACEHOLDER {
        bail!("monitor_ingest_secret is still the config.example.json placeholder");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{validate_api_config, EXAMPLE_SECRET_PLACEHOLDER};
    use crate::config::Config;

    fn config(api_key: &str, ingest: &str) -> Config {
        Config {
            api_key: api_key.to_string(),
            monitor_ingest_secret: ingest.to_string(),
            ..Config::default()
        }
    }

    #[test]
    fn api_key_must_be_long_and_not_the_example_placeholder() {
        let good = "0123456789abcdef".repeat(4);
        assert!(validate_api_config(&config(&good, "ingest")).is_ok());
        assert!(validate_api_config(&config(&good[..32], "ingest")).is_ok());
        for bad in ["", "short-key", &good[..31], EXAMPLE_SECRET_PLACEHOLDER] {
            let err = validate_api_config(&config(bad, "ingest")).unwrap_err();
            assert!(err.to_string().contains("api_key"), "{bad}: {err}");
        }
        assert!(validate_api_config(&config(&good, EXAMPLE_SECRET_PLACEHOLDER)).is_err());
        assert!(validate_api_config(&config(&good, " ")).is_err());
    }
}
