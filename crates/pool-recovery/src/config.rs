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
        cfg.normalize();
        Ok(cfg)
    }

    pub fn normalize(&mut self) {
        if self.payout_pause_file.trim().is_empty() {
            self.payout_pause_file = DEFAULT_PAYOUT_PAUSE_FILE.to_string();
        }
        self.recovery.normalize();
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
        if self.socket_path.trim().is_empty() {
            self.socket_path = Self::default().socket_path;
        }
        if self.state_path.trim().is_empty() {
            self.state_path = Self::default().state_path;
        }
        if self.secret_path.trim().is_empty() {
            self.secret_path = Self::default().secret_path;
        }
        if self.proxy_include_path.trim().is_empty() {
            self.proxy_include_path = Self::default().proxy_include_path;
        }
        if self.active_cookie_path.trim().is_empty() {
            self.active_cookie_path = Self::default().active_cookie_path;
        }
        self.primary.normalize(RecoveryInstanceId::Primary);
        self.standby.normalize(RecoveryInstanceId::Standby);
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
        if self.service.trim().is_empty() {
            self.service = default.service.clone();
        }
        if self.api.trim().is_empty() {
            self.api = default.api.clone();
        }
        if self.wallet_path.trim().is_empty() {
            self.wallet_path = default.wallet_path.clone();
        }
        if self.data_dir.trim().is_empty() {
            self.data_dir = default.data_dir.clone();
        }
        if self.cookie_path.trim().is_empty() {
            self.cookie_path = default.cookie_path.clone();
        }
    }
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
