use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub api_host: String,
    pub api_port: u16,
    pub api_key: String,
    pub api_tls_cert_path: String,
    pub api_tls_key_path: String,
    pub daemon_data_dir: String,
    pub daemon_api: String,
    pub daemon_token: String,
    pub daemon_cookie_path: String,
    pub stratum_host: String,
    pub stratum_port: u16,
    pub database_url: String,
    pub database_pool_size: i32,
    pub max_validation_queue: i32,
    pub monitor_host: String,
    pub monitor_port: u16,
    pub monitor_interval: String,
    pub monitor_spool_path: String,
    pub seed_status_urls: Vec<String>,
    pub pool_status_url: String,
    pub explorer_status_url: String,
    pub height_divergence_threshold: u64,
    pub payout_interval: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_host: "127.0.0.1".to_string(),
            api_port: 24783,
            api_key: String::new(),
            api_tls_cert_path: String::new(),
            api_tls_key_path: String::new(),
            daemon_data_dir: "data".to_string(),
            daemon_api: "http://127.0.0.1:8332".to_string(),
            daemon_token: String::new(),
            daemon_cookie_path: "/etc/blocknet/pool/daemon-active.api.cookie".to_string(),
            stratum_host: "127.0.0.1".to_string(),
            stratum_port: 3333,
            database_url: String::new(),
            database_pool_size: 4,
            max_validation_queue: 2048,
            monitor_host: "127.0.0.1".to_string(),
            monitor_port: 24784,
            monitor_interval: "10s".to_string(),
            monitor_spool_path: "/var/lib/blocknet-pool/monitor-spool.jsonl".to_string(),
            seed_status_urls: default_seed_status_urls(),
            pool_status_url: "https://bntpool.com/api/status".to_string(),
            explorer_status_url: "https://explorer.blocknetcrypto.com/status.json".to_string(),
            height_divergence_threshold: 2,
            payout_interval: "1h".to_string(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let data = fs::read(path).with_context(|| format!("read config {}", path.display()))?;
        let mut cfg: Config = serde_json::from_slice(&data)
            .with_context(|| format!("parse config {}", path.display()))?;
        cfg.normalize();
        Ok(cfg)
    }

    pub fn normalize(&mut self) {
        if self.api_host.trim().is_empty() {
            self.api_host = "127.0.0.1".to_string();
        }
        if self.api_port == 0 {
            self.api_port = 24783;
        }
        if self.api_key.trim().is_empty() {
            self.api_key.clear();
        }
        if self.daemon_cookie_path.trim().is_empty() {
            self.daemon_cookie_path = "/etc/blocknet/pool/daemon-active.api.cookie".to_string();
        }
        if self.monitor_host.trim().is_empty() {
            self.monitor_host = "127.0.0.1".to_string();
        }
        if self.monitor_port == 0 {
            self.monitor_port = 24784;
        }
        if self.monitor_interval.trim().is_empty() {
            self.monitor_interval = "10s".to_string();
        }
        if self.monitor_spool_path.trim().is_empty() {
            self.monitor_spool_path = "/var/lib/blocknet-pool/monitor-spool.jsonl".to_string();
        }
        self.seed_status_urls = self
            .seed_status_urls
            .iter()
            .map(|item| item.trim())
            .filter(|item| !item.is_empty())
            .map(str::to_string)
            .collect();
        if self.seed_status_urls.is_empty() {
            self.seed_status_urls = default_seed_status_urls();
        }
        if self.pool_status_url.trim().is_empty() {
            self.pool_status_url = "https://bntpool.com/api/status".to_string();
        } else {
            self.pool_status_url = self.pool_status_url.trim().to_string();
        }
        if self.explorer_status_url.trim().is_empty() {
            self.explorer_status_url =
                "https://explorer.blocknetcrypto.com/status.json".to_string();
        } else {
            self.explorer_status_url = self.explorer_status_url.trim().to_string();
        }
        if self.payout_interval.trim().is_empty() {
            self.payout_interval = "1h".to_string();
        }
        if self.database_pool_size < 1 {
            self.database_pool_size = 1;
        }
        if self.max_validation_queue < 1 {
            self.max_validation_queue = 2048;
        }
    }

    pub fn monitor_interval_duration(&self) -> Duration {
        parse_duration_or(&self.monitor_interval, Duration::from_secs(10))
            .clamp(Duration::from_secs(5), Duration::from_secs(5 * 60))
    }

    pub fn payout_interval_duration(&self) -> Duration {
        parse_duration_or(&self.payout_interval, Duration::from_secs(60 * 60))
            .clamp(Duration::from_secs(60), Duration::from_secs(24 * 60 * 60))
    }
}

fn parse_duration_or(value: &str, fallback: Duration) -> Duration {
    humantime::parse_duration(value).unwrap_or(fallback)
}

fn default_seed_status_urls() -> Vec<String> {
    [
        "bnt-0.blocknetcrypto.com",
        "bnt-1.blocknetcrypto.com",
        "bnt-2.blocknetcrypto.com",
        "bnt-3.blocknetcrypto.com",
        "bnt-4.blocknetcrypto.com",
    ]
    .into_iter()
    .map(|host| format!("http://{host}:28081/status"))
    .collect()
}
