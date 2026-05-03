use std::fs;
use std::path::Path;

use anyhow::{bail, Context, Result};
use pool_runtime::config::Config as RuntimeConfig;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(default)]
pub(crate) struct Config {
    #[serde(flatten)]
    pub(crate) runtime: RuntimeConfig,
    pub(crate) api_host: String,
    pub(crate) api_port: u16,
    pub(crate) api_key: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            runtime: RuntimeConfig::default(),
            api_host: "127.0.0.1".to_string(),
            api_port: 24783,
            api_key: String::new(),
        }
    }
}

impl Config {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let data = fs::read(path).with_context(|| format!("read config {}", path.display()))?;
        let mut cfg: Config = serde_json::from_slice(&data)
            .with_context(|| format!("parse config {}", path.display()))?;
        cfg.normalize();
        if cfg.api_key.trim().is_empty() {
            bail!("api_key must be set for monitor access to protected API telemetry");
        }
        cfg.api_key = cfg.api_key.trim().to_string();
        Ok(cfg)
    }

    pub(crate) fn normalize(&mut self) {
        self.runtime.normalize();
        if self.api_host.trim().is_empty() {
            self.api_host = "127.0.0.1".to_string();
        }
        if self.api_port == 0 {
            self.api_port = 24783;
        }
    }
}
