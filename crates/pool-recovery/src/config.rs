use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::recovery::RecoveryInstanceId;

const DEFAULT_PAYOUT_PAUSE_FILE: &str = "/etc/blocknet/pool/payouts.pause";

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub payout_pause_file: String,
    pub recovery: RecoveryConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            payout_pause_file: DEFAULT_PAYOUT_PAUSE_FILE.to_string(),
            recovery: RecoveryConfig::default(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let data = fs::read(path).with_context(|| format!("read config {}", path.display()))?;
        let mut cfg: Config = serde_json::from_slice(&data)
            .with_context(|| format!("parse config {}", path.display()))?;
        cfg.normalize_and_validate()?;
        Ok(cfg)
    }

    pub fn normalize_and_validate(&mut self) -> Result<()> {
        self.normalize();
        self.validate()
    }

    pub fn normalize(&mut self) {
        self.recovery.normalize();
    }

    pub fn validate(&self) -> Result<()> {
        ensure_nonempty("payout_pause_file", &self.payout_pause_file)?;
        self.recovery.validate()
    }
}

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct RecoveryConfig {
    pub enabled: bool,
    pub socket_path: String,
    pub state_path: String,
    pub secret_path: String,
    pub proxy_include_path: String,
    pub active_cookie_path: String,
    pub primary: RecoveryDaemonInstanceConfig,
    pub standby: RecoveryDaemonInstanceConfig,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            socket_path: "/run/blocknet-recoveryd.sock".to_string(),
            state_path: "/var/lib/blocknet-recovery/state.json".to_string(),
            secret_path: "/etc/blocknet/recovery/pool-wallet.json".to_string(),
            proxy_include_path: "/etc/nginx/blocknet-daemon-active-upstream.inc".to_string(),
            active_cookie_path: "/etc/blocknet/pool/daemon-active.api.cookie".to_string(),
            primary: RecoveryDaemonInstanceConfig {
                service: "blocknetd@primary.service".to_string(),
                api: "http://127.0.0.1:18331".to_string(),
                wallet_path: "/var/lib/blocknet/wallet.dat".to_string(),
                data_dir: "/var/lib/blocknet/data".to_string(),
                cookie_path: "/var/lib/blocknet/data/api.cookie".to_string(),
            },
            standby: RecoveryDaemonInstanceConfig {
                service: "blocknetd@standby.service".to_string(),
                api: "http://127.0.0.1:18332".to_string(),
                wallet_path: "/var/lib/blocknet-standby/wallet.dat".to_string(),
                data_dir: "/var/lib/blocknet-standby/data".to_string(),
                cookie_path: "/var/lib/blocknet-standby/data/api.cookie".to_string(),
            },
        }
    }
}

impl RecoveryConfig {
    pub fn normalize(&mut self) {
        self.primary.normalize(RecoveryInstanceId::Primary);
        self.standby.normalize(RecoveryInstanceId::Standby);
    }

    pub fn validate(&self) -> Result<()> {
        ensure_nonempty("recovery.socket_path", &self.socket_path)?;
        ensure_nonempty("recovery.state_path", &self.state_path)?;
        ensure_nonempty("recovery.secret_path", &self.secret_path)?;
        ensure_nonempty("recovery.proxy_include_path", &self.proxy_include_path)?;
        ensure_nonempty("recovery.active_cookie_path", &self.active_cookie_path)?;
        self.primary.validate("recovery.primary")?;
        self.standby.validate("recovery.standby")
    }

    pub fn instance(&self, id: RecoveryInstanceId) -> &RecoveryDaemonInstanceConfig {
        match id {
            RecoveryInstanceId::Primary => &self.primary,
            RecoveryInstanceId::Standby => &self.standby,
        }
    }

    pub(crate) fn detect_proxy_target(&self) -> Option<RecoveryInstanceId> {
        let raw = fs::read_to_string(self.proxy_include_path.trim()).ok()?;
        let primary_api = self.primary.api.trim();
        let standby_api = self.standby.api.trim();
        if !primary_api.is_empty() && raw.contains(primary_api) {
            Some(RecoveryInstanceId::Primary)
        } else if !standby_api.is_empty() && raw.contains(standby_api) {
            Some(RecoveryInstanceId::Standby)
        } else {
            None
        }
    }

    pub(crate) fn detect_active_cookie_target(&self) -> Option<RecoveryInstanceId> {
        let target = fs::read_link(Path::new(self.active_cookie_path.trim())).ok()?;
        if path_matches(&target, Path::new(self.primary.cookie_path.trim())) {
            Some(RecoveryInstanceId::Primary)
        } else if path_matches(&target, Path::new(self.standby.cookie_path.trim())) {
            Some(RecoveryInstanceId::Standby)
        } else {
            None
        }
    }

    pub fn effective_active_instance(&self) -> Option<RecoveryInstanceId> {
        Self::effective_active_instance_from_targets(
            self.detect_proxy_target(),
            self.detect_active_cookie_target(),
        )
    }

    pub(crate) fn effective_active_instance_from_targets(
        proxy_target: Option<RecoveryInstanceId>,
        active_cookie_target: Option<RecoveryInstanceId>,
    ) -> Option<RecoveryInstanceId> {
        match (proxy_target, active_cookie_target) {
            (Some(a), Some(b)) if a == b => Some(a),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            _ => None,
        }
    }
}

#[derive(Clone, Default, Deserialize)]
#[serde(default)]
pub struct RecoveryDaemonInstanceConfig {
    pub service: String,
    pub api: String,
    pub wallet_path: String,
    pub data_dir: String,
    pub cookie_path: String,
}

impl RecoveryDaemonInstanceConfig {
    fn normalize(&mut self, id: RecoveryInstanceId) {
        let defaults = RecoveryConfig::default();
        let default = defaults.instance(id);
        *self = RecoveryDaemonInstanceConfig {
            service: non_empty_or(self.service.as_str(), &default.service),
            api: non_empty_or(self.api.as_str(), &default.api),
            wallet_path: non_empty_or(self.wallet_path.as_str(), &default.wallet_path),
            data_dir: non_empty_or(self.data_dir.as_str(), &default.data_dir),
            cookie_path: non_empty_or(self.cookie_path.as_str(), &default.cookie_path),
        };
    }

    fn validate(&self, prefix: &str) -> Result<()> {
        ensure_nonempty(format!("{prefix}.service"), &self.service)?;
        ensure_nonempty(format!("{prefix}.api"), &self.api)?;
        ensure_nonempty(format!("{prefix}.wallet_path"), &self.wallet_path)?;
        ensure_nonempty(format!("{prefix}.data_dir"), &self.data_dir)?;
        ensure_nonempty(format!("{prefix}.cookie_path"), &self.cookie_path)
    }
}

fn non_empty_or(value: &str, default: &str) -> String {
    if value.trim().is_empty() {
        default.to_string()
    } else {
        value.to_string()
    }
}

fn ensure_nonempty(field: impl AsRef<str>, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        anyhow::bail!("{} must not be empty", field.as_ref());
    }
    Ok(())
}

fn path_matches(actual: &Path, expected: &Path) -> bool {
    if actual == expected {
        return true;
    }
    match (fs::canonicalize(actual), fs::canonicalize(expected)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, RecoveryConfig};

    #[test]
    fn load_rejects_empty_top_level_paths() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, r#"{"payout_pause_file":""}"#).unwrap();

        let err = match Config::load(&path) {
            Ok(_) => panic!("empty payout pause path should fail recovery config load"),
            Err(err) => err,
        };
        assert!(format!("{err:#}").contains("payout_pause_file"));
    }

    #[test]
    fn validate_rejects_empty_recovery_paths() {
        let cfg = RecoveryConfig {
            socket_path: String::new(),
            ..RecoveryConfig::default()
        };

        let err = match cfg.validate() {
            Ok(_) => panic!("empty recovery socket path should fail validation"),
            Err(err) => err,
        };
        assert!(format!("{err:#}").contains("recovery.socket_path"));
    }

    #[test]
    fn load_uses_defaults_when_recovery_config_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, "{}").unwrap();

        let cfg = Config::load(&path).unwrap();
        assert_eq!(
            cfg.recovery.socket_path,
            RecoveryConfig::default().socket_path
        );
    }
}
