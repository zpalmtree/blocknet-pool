use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use pool_recovery::RecoveryConfig;
use pool_runtime::config::Config as RuntimeConfig;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(default)]
pub(crate) struct Config {
    pub(crate) hosted_wallet: Option<crate::hosted_wallet::HostedWalletConfig>,
    #[serde(flatten)]
    pub(crate) runtime: RuntimeConfig,
    pub(crate) pool_url: String,
    pub(crate) checkpoints_path: String,
    pub(crate) api_port: u16,
    pub(crate) api_host: String,
    pub(crate) api_key: String,
    pub(crate) monitor_ingest_secret: String,
    /// Header carrying the client address set by the trusted reverse proxy
    /// (our nginx sets `X-Real-IP $remote_addr`). Empty disables it, leaving
    /// only the rightmost `X-Forwarded-For` hop.
    pub(crate) api_client_ip_header: String,
    /// Extra proxy addresses (besides loopback) whose forwarding headers are
    /// trusted. Requests from any other peer are keyed by the peer address.
    pub(crate) api_trusted_proxies: Vec<String>,
    /// `statement_timeout` applied to database reads made on behalf of public,
    /// unauthenticated requests. 0 disables. Admin and background work keep
    /// the server default.
    pub(crate) api_public_db_statement_timeout_ms: u64,
    /// Optional startup fallback until GitHub supplies the latest stable seine
    /// release. Empty hides update notices until the first successful check.
    pub(crate) latest_miner_version: String,
    pub(crate) recovery: RecoveryConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            hosted_wallet: None,
            runtime: RuntimeConfig::default(),
            pool_url: "http://localhost:24783".to_string(),
            checkpoints_path: String::new(),
            api_port: 24783,
            api_host: "127.0.0.1".to_string(),
            api_key: String::new(),
            monitor_ingest_secret: String::new(),
            api_client_ip_header: crate::protect::DEFAULT_CLIENT_IP_HEADER.to_string(),
            api_trusted_proxies: Vec::new(),
            api_public_db_statement_timeout_ms: 15_000,
            latest_miner_version: String::new(),
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

    pub(crate) fn active_hosted_wallet(&self) -> Option<&crate::hosted_wallet::HostedWalletConfig> {
        self.hosted_wallet.as_ref().filter(|wallet| wallet.enabled)
    }

    fn validate(&self) -> Result<()> {
        if let Some(wallet) = self.active_hosted_wallet() {
            wallet.validate()?;
        }
        if self.pool_url.trim().is_empty() {
            anyhow::bail!("pool_url must not be empty");
        }
        if self.api_host.trim().is_empty() {
            anyhow::bail!("api_host must not be empty");
        }
        if self.api_port == 0 {
            anyhow::bail!("api_port must be non-zero");
        }
        let ip_header = self.api_client_ip_header.trim();
        if !ip_header.is_empty()
            && axum::http::HeaderName::from_bytes(ip_header.to_ascii_lowercase().as_bytes())
                .is_err()
        {
            anyhow::bail!("api_client_ip_header is not a valid header name: {ip_header}");
        }
        for proxy in &self.api_trusted_proxies {
            if proxy.trim().parse::<std::net::IpAddr>().is_err() {
                anyhow::bail!("api_trusted_proxies entry is not an IP address: {proxy}");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn hosted_wallet_requires_explicit_enablement() {
        for json in [
            r#"{}"#,
            r#"{"hosted_wallet":null}"#,
            r#"{"hosted_wallet":{"enabled":false}}"#,
            r#"{"hosted_wallet":{"backend_url":"http://127.0.0.1:8335","gateway_token_file":"/missing/token"}}"#,
        ] {
            let cfg: Config = serde_json::from_str(json).unwrap();
            assert!(cfg.active_hosted_wallet().is_none());
            cfg.validate().unwrap();
        }
        let cfg: Config = serde_json::from_str(r#"{"hosted_wallet":{"enabled":true}}"#).unwrap();
        assert!(cfg.validate().is_err());
        let cfg: Config = serde_json::from_str(r#"{"hosted_wallet":{"enabled":true,"backend_url":"http://127.0.0.1:8335","gateway_token_file":"/missing/token"}}"#).unwrap();
        assert!(cfg.active_hosted_wallet().is_some());
        cfg.validate().unwrap();
        assert!(serde_json::from_str::<Config>(r#"{"hosted_wallet":{"enabled":"true"}}"#).is_err());
    }

    #[test]
    fn api_config_accepts_flat_runtime_fields() {
        let cfg: Config =
            serde_json::from_str(r#"{"pool_name":"flat","stratum_port":4444,"api_port":1234}"#)
                .expect("parse config");

        assert_eq!(cfg.runtime.pool_name, "flat");
        assert_eq!(cfg.runtime.stratum_port, 4444);
        assert_eq!(cfg.api_port, 1234);
        assert!(cfg.latest_miner_version.is_empty());
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
            (
                r#"{"api_client_ip_header":"bad header"}"#,
                "api_client_ip_header",
            ),
            (
                r#"{"api_trusted_proxies":["nginx"]}"#,
                "api_trusted_proxies",
            ),
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
