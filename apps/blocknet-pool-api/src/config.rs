use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use pool_recovery::RecoveryConfig;
use pool_runtime::config::Config as RuntimeConfig;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(default)]
pub(crate) struct Config {
    #[serde(flatten)]
    pub(crate) runtime: RuntimeConfig,
    pub(crate) pool_url: String,
    pub(crate) checkpoints_path: String,
    pub(crate) api_port: u16,
    pub(crate) api_host: String,
    pub(crate) api_key: String,
    pub(crate) monitor_ingest_secret: String,
    /// Latest released miner version, surfaced to the UI so it can nudge
    /// workers reporting an older (or no) version. Bumped per seine release.
    pub(crate) latest_miner_version: String,
    pub(crate) recovery: RecoveryConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            runtime: RuntimeConfig::default(),
            pool_url: "http://localhost:24783".to_string(),
            checkpoints_path: String::new(),
            api_port: 24783,
            api_host: "127.0.0.1".to_string(),
            api_key: String::new(),
            monitor_ingest_secret: String::new(),
            latest_miner_version: "0.2.15".to_string(),
            recovery: RecoveryConfig::default(),
        }
    }
}

impl Config {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let data = fs::read(path).with_context(|| format!("read config {}", path.display()))?;
        let cfg: Config = serde_json::from_slice(&data)
            .with_context(|| format!("parse config {}", path.display()))?;
        cfg.runtime.validate()?;
        cfg.recovery.validate()?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.pool_url.trim().is_empty() {
            anyhow::bail!("pool_url must not be empty");
        }
        if self.api_host.trim().is_empty() {
            anyhow::bail!("api_host must not be empty");
        }
        if self.api_port == 0 {
            anyhow::bail!("api_port must be non-zero");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn api_config_accepts_flat_runtime_fields() {
        let cfg: Config =
            serde_json::from_str(r#"{"pool_name":"flat","stratum_port":4444,"api_port":1234}"#)
                .expect("parse config");

        assert_eq!(cfg.runtime.pool_name, "flat");
        assert_eq!(cfg.runtime.stratum_port, 4444);
        assert_eq!(cfg.api_port, 1234);
    }

    #[test]
    fn api_config_load_validates_flat_runtime_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, r#"{"payout_interval":"not-a-duration"}"#).unwrap();

        let err = match Config::load(&path) {
            Ok(_) => panic!("invalid runtime duration should fail API config load"),
            Err(err) => err,
        };
        assert!(format!("{err:#}").contains("payout_interval"));
    }

    #[test]
    fn api_config_load_rejects_empty_app_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");

        for (json, expected) in [
            (r#"{"pool_url":""}"#, "pool_url"),
            (r#"{"api_host":"   "}"#, "api_host"),
            (r#"{"api_port":0}"#, "api_port"),
            (r#"{"recovery":{"socket_path":""}}"#, "recovery.socket_path"),
        ] {
            std::fs::write(&path, json).unwrap();
            let err = match Config::load(&path) {
                Ok(_) => panic!("{expected} should fail API config load"),
                Err(err) => err,
            };
            assert!(format!("{err:#}").contains(expected));
        }
    }
}
