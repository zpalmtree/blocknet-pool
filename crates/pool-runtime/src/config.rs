use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use pool_common::protocol::{address_network, AddressNetwork};
use serde::Deserialize;

pub(crate) const CANDIDATE_SUBMIT_QUEUE_SIZE: usize = 64;
pub const CANDIDATE_VALIDATION_QUEUE_SIZE: usize = 64;

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub pool_name: String,

    pub stratum_host: String,
    pub stratum_port: u16,

    pub daemon_data_dir: String,
    pub daemon_api: String,
    pub daemon_cookie_path: String,
    pub pool_fee_wallet_address: String,

    pub initial_share_difficulty: u64,
    pub refresh_on_same_height: bool,
    pub job_timeout: String,
    pub stale_submit_grace: String,
    pub validation_mode: String,
    pub regular_submit_queue: i32,
    pub regular_validation_queue: i32,
    pub audit_validation_queue: i32,
    pub regular_verifiers: i32,
    pub audit_verifiers: i32,
    pub sample_rate: f64,
    pub warmup_shares: i32,
    pub min_sample_every: i32,
    pub invalid_sample_threshold: f64,
    pub invalid_sample_min: i32,
    pub invalid_escalation_window_duration: String,
    pub forced_verify_duration: String,
    pub quarantine_duration: String,
    pub max_quarantine_duration: String,
    pub invalid_escalation_quarantine_strikes: i32,
    pub provisional_share_delay: String,
    pub enable_vardiff: bool,
    pub vardiff_target_shares: i32,
    pub vardiff_window: String,
    pub vardiff_retarget_interval: String,
    pub vardiff_tolerance: f64,
    pub min_share_difficulty: u64,
    pub max_share_difficulty: u64,

    pub pool_fee_pct: f64,

    pub pplns_window_duration: String,
    pub blocks_before_payout: i32,
    pub min_payout_amount: f64,
    pub payout_min_verified_shares: i32,
    pub payout_provisional_cap_multiplier: f64,
    pub payouts_enabled: bool,
    pub payout_max_recipients_per_tick: i32,
    pub payout_max_total_per_tick: f64,
    pub payout_max_per_recipient: f64,
    pub payout_pause_file: String,
    pub payout_interval: String,

    pub database_url: String,
    pub database_pool_size: i32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            pool_name: "blocknet pool".to_string(),
            stratum_host: "127.0.0.1".to_string(),
            stratum_port: 3333,
            daemon_data_dir: "data".to_string(),
            daemon_api: "http://127.0.0.1:8332".to_string(),
            daemon_cookie_path: "/etc/blocknet/pool/daemon-active.api.cookie".to_string(),
            pool_fee_wallet_address: String::new(),
            initial_share_difficulty: 60,
            refresh_on_same_height: false,
            job_timeout: "5m".to_string(),
            stale_submit_grace: "8s".to_string(),
            validation_mode: "probabilistic".to_string(),
            regular_submit_queue: 512,
            regular_validation_queue: 512,
            audit_validation_queue: 128,
            regular_verifiers: 2,
            audit_verifiers: 1,
            sample_rate: 0.10,
            warmup_shares: 20,
            min_sample_every: 10,
            invalid_sample_threshold: 0.10,
            invalid_sample_min: 50,
            invalid_escalation_window_duration: "24h".to_string(),
            forced_verify_duration: "1h".to_string(),
            quarantine_duration: "1h".to_string(),
            max_quarantine_duration: "168h".to_string(),
            invalid_escalation_quarantine_strikes: 1,
            provisional_share_delay: "15m".to_string(),
            enable_vardiff: true,
            vardiff_target_shares: 10,
            vardiff_window: "5m".to_string(),
            vardiff_retarget_interval: "5s".to_string(),
            vardiff_tolerance: 0.25,
            min_share_difficulty: 1,
            max_share_difficulty: 1_000_000_000,
            pool_fee_pct: 0.0,
            pplns_window_duration: "6h".to_string(),
            blocks_before_payout: 60,
            min_payout_amount: 0.1,
            payout_min_verified_shares: 1,
            payout_provisional_cap_multiplier: 19.0,
            payouts_enabled: true,
            payout_max_recipients_per_tick: 500,
            payout_max_total_per_tick: 0.0,
            payout_max_per_recipient: 0.0,
            payout_pause_file: "/etc/blocknet/pool/payouts.pause".to_string(),
            payout_interval: "1h".to_string(),
            database_url: String::new(),
            database_pool_size: 4,
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let data = fs::read(path).with_context(|| format!("read config {}", path.display()))?;
        let mut cfg: Config = serde_json::from_slice(&data)
            .with_context(|| format!("parse config {}", path.display()))?;
        cfg.normalize();
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn normalize(&mut self) {
        self.validation_mode = self.validation_mode.trim().to_ascii_lowercase();

        clamp_i32_min(&mut self.regular_submit_queue, 1);
        clamp_i32_min(&mut self.regular_validation_queue, 1);
        clamp_i32_min(&mut self.audit_validation_queue, 1);
        clamp_i32_min(&mut self.audit_verifiers, 1);
        clamp_i32_min(&mut self.regular_verifiers, 0);
        self.sample_rate = self.sample_rate.clamp(0.0, 1.0);
        clamp_i32_min(&mut self.warmup_shares, 0);
        clamp_i32_min(&mut self.min_sample_every, 0);
        clamp_i32_min(&mut self.invalid_sample_min, 1);
        if !(0.0 < self.invalid_sample_threshold && self.invalid_sample_threshold <= 1.0) {
            self.invalid_sample_threshold = 0.10;
        }
        clamp_i32_min(&mut self.invalid_escalation_quarantine_strikes, 0);
        self.initial_share_difficulty = self.initial_share_difficulty.max(1);
        self.min_share_difficulty = self.min_share_difficulty.max(1);
        if self.max_share_difficulty < self.min_share_difficulty {
            self.max_share_difficulty = self.min_share_difficulty;
        }
        self.initial_share_difficulty = self
            .initial_share_difficulty
            .clamp(self.min_share_difficulty, self.max_share_difficulty);
        clamp_i32_min(&mut self.vardiff_target_shares, 1);
        self.vardiff_tolerance = self.vardiff_tolerance.clamp(0.01, 0.95);
        clamp_i32_min(&mut self.payout_min_verified_shares, 0);
        clamp_f64_min(&mut self.payout_provisional_cap_multiplier, 0.0);
        clamp_i32_min(&mut self.payout_max_recipients_per_tick, 0);
        clamp_f64_min(&mut self.payout_max_total_per_tick, 0.0);
        clamp_f64_min(&mut self.payout_max_per_recipient, 0.0);
        clamp_i32_min(&mut self.database_pool_size, 1);
        let max_atomic_amount = (u64::MAX as f64) / 100_000_000.0;
        if !self.min_payout_amount.is_finite() || self.min_payout_amount < 0.0 {
            self.min_payout_amount = 0.1;
        } else {
            self.min_payout_amount = self.min_payout_amount.clamp(0.0, max_atomic_amount);
        }
    }

    pub(crate) fn job_timeout_duration(&self) -> Duration {
        config_duration("job_timeout", &self.job_timeout)
    }

    pub(crate) fn stale_submit_grace_duration(&self) -> Duration {
        config_duration("stale_submit_grace", &self.stale_submit_grace)
    }

    pub(crate) fn regular_submit_queue_size(&self) -> usize {
        self.regular_submit_queue.max(1) as usize
    }

    pub fn regular_validation_queue_size(&self) -> usize {
        self.regular_validation_queue.max(1) as usize
    }

    pub fn audit_validation_queue_size(&self) -> usize {
        self.audit_validation_queue.max(1) as usize
    }

    pub(crate) fn regular_verifier_count(&self) -> usize {
        self.regular_verifiers.max(1) as usize
    }

    pub(crate) fn audit_verifier_count(&self) -> usize {
        self.audit_verifiers.max(1) as usize
    }

    pub(crate) fn forced_verify_duration(&self) -> Duration {
        config_duration("forced_verify_duration", &self.forced_verify_duration)
    }

    pub(crate) fn invalid_escalation_window_duration(&self) -> Duration {
        config_duration(
            "invalid_escalation_window_duration",
            &self.invalid_escalation_window_duration,
        )
    }

    pub fn provisional_share_delay_duration(&self) -> Duration {
        config_duration("provisional_share_delay", &self.provisional_share_delay)
    }

    pub(crate) fn quarantine_duration(&self) -> Duration {
        config_duration("quarantine_duration", &self.quarantine_duration)
    }

    pub(crate) fn max_quarantine_duration(&self) -> Duration {
        config_duration("max_quarantine_duration", &self.max_quarantine_duration)
    }

    pub fn payout_interval_duration(&self) -> Duration {
        config_duration("payout_interval", &self.payout_interval)
    }

    pub(crate) fn vardiff_window_duration(&self) -> Duration {
        config_duration("vardiff_window", &self.vardiff_window)
    }

    pub(crate) fn vardiff_retarget_interval_duration(&self) -> Duration {
        config_duration("vardiff_retarget_interval", &self.vardiff_retarget_interval)
    }

    pub fn pplns_window_duration(&self) -> Duration {
        let duration = config_duration("pplns_window_duration", &self.pplns_window_duration);
        assert!(
            !duration.is_zero(),
            "pplns_window_duration must be greater than 0"
        );
        duration
    }

    pub fn pool_fee(&self, reward: u64) -> u64 {
        if self.pool_fee_pct <= 0.0 {
            return 0;
        }
        ((reward as f64) * self.pool_fee_pct / 100.0).clamp(0.0, reward as f64) as u64
    }

    pub(crate) fn pool_fee_wallet_address_network(&self) -> Result<Option<AddressNetwork>, String> {
        let configured = self.pool_fee_wallet_address.trim();
        if configured.is_empty() {
            return Ok(None);
        }
        address_network(configured)
    }

    fn validate_duration_fields(&self) -> Result<()> {
        config_duration_result("job_timeout", &self.job_timeout)?;
        config_duration_result("stale_submit_grace", &self.stale_submit_grace)?;
        config_duration_result(
            "invalid_escalation_window_duration",
            &self.invalid_escalation_window_duration,
        )?;
        config_duration_result("forced_verify_duration", &self.forced_verify_duration)?;
        config_duration_result("quarantine_duration", &self.quarantine_duration)?;
        config_duration_result("max_quarantine_duration", &self.max_quarantine_duration)?;
        config_duration_result("provisional_share_delay", &self.provisional_share_delay)?;
        config_duration_result("vardiff_window", &self.vardiff_window)?;
        config_duration_result("vardiff_retarget_interval", &self.vardiff_retarget_interval)?;
        ensure_nonzero_duration("pplns_window_duration", &self.pplns_window_duration)?;
        config_duration_result("payout_interval", &self.payout_interval)?;
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        match self.validation_mode.as_str() {
            "full" | "probabilistic" => {}
            _ => anyhow::bail!("validation_mode must be either \"full\" or \"probabilistic\""),
        }
        ensure_nonempty("daemon_cookie_path", &self.daemon_cookie_path)?;
        ensure_nonempty("payout_pause_file", &self.payout_pause_file)?;
        self.validate_duration_fields()
    }
}

fn clamp_i32_min(value: &mut i32, minimum: i32) {
    if *value < minimum {
        *value = minimum;
    }
}

fn clamp_f64_min(value: &mut f64, minimum: f64) {
    if !value.is_finite() || *value < minimum {
        *value = minimum;
    }
}

fn config_duration(field: &str, value: &str) -> Duration {
    config_duration_result(field, value).unwrap_or_else(|err| panic!("{err:#}"))
}

fn config_duration_result(field: &str, value: &str) -> Result<Duration> {
    humantime::parse_duration(value.trim())
        .with_context(|| format!("invalid duration for {field}: {value:?}"))
}

fn ensure_nonzero_duration(field: &str, value: &str) -> Result<Duration> {
    let duration = config_duration_result(field, value)?;
    if duration.is_zero() {
        anyhow::bail!("{field} must be greater than 0");
    }
    Ok(duration)
}

fn ensure_nonempty(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        anyhow::bail!("{field} must not be empty");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_clamps_values() {
        let mut cfg = Config {
            validation_mode: " FULL ".to_string(),
            regular_submit_queue: 0,
            regular_validation_queue: 0,
            audit_validation_queue: 0,
            regular_verifiers: -1,
            audit_verifiers: 0,
            sample_rate: 2.0,
            warmup_shares: -5,
            min_sample_every: -1,
            invalid_sample_min: 0,
            invalid_sample_threshold: 2.0,
            invalid_escalation_quarantine_strikes: -2,
            initial_share_difficulty: 0,
            min_share_difficulty: 10,
            max_share_difficulty: 5,
            vardiff_target_shares: 0,
            vardiff_tolerance: 2.0,
            payout_min_verified_shares: -3,
            payout_provisional_cap_multiplier: f64::NAN,
            payout_max_recipients_per_tick: -2,
            payout_max_total_per_tick: -10.0,
            payout_max_per_recipient: f64::NAN,
            min_payout_amount: -1.0,
            database_pool_size: 0,
            ..Config::default()
        };
        cfg.normalize();

        assert_eq!(cfg.validation_mode, "full");
        assert_eq!(cfg.regular_submit_queue, 1);
        assert_eq!(cfg.regular_validation_queue, 1);
        assert_eq!(cfg.audit_validation_queue, 1);
        assert_eq!(cfg.regular_verifiers, 0);
        assert_eq!(cfg.audit_verifiers, 1);
        assert_eq!(cfg.sample_rate, 1.0);
        assert_eq!(cfg.warmup_shares, 0);
        assert_eq!(cfg.min_sample_every, 0);
        assert_eq!(cfg.invalid_sample_min, 1);
        assert_eq!(cfg.invalid_sample_threshold, 0.10);
        assert_eq!(cfg.invalid_escalation_quarantine_strikes, 0);
        assert_eq!(cfg.min_share_difficulty, 10);
        assert_eq!(cfg.max_share_difficulty, 10);
        assert_eq!(cfg.initial_share_difficulty, 10);
        assert_eq!(cfg.vardiff_target_shares, 1);
        assert_eq!(cfg.vardiff_tolerance, 0.95);
        assert_eq!(cfg.payout_min_verified_shares, 0);
        assert_eq!(cfg.payout_provisional_cap_multiplier, 0.0);
        assert_eq!(cfg.payout_max_recipients_per_tick, 0);
        assert_eq!(cfg.payout_max_total_per_tick, 0.0);
        assert_eq!(cfg.payout_max_per_recipient, 0.0);
        assert_eq!(cfg.min_payout_amount, 0.1);
        assert_eq!(cfg.database_pool_size, 1);
    }

    #[test]
    fn fee_applies_pct() {
        let cfg = Config {
            pool_fee_pct: 10.0,
            ..Config::default()
        };
        let reward = 10_000_000_000u64;
        assert_eq!(cfg.pool_fee(reward), 1_000_000_000);
    }

    #[test]
    fn load_rejects_invalid_duration() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, r#"{"payout_interval":"not-a-duration"}"#).unwrap();

        let err = match Config::load(&path) {
            Ok(_) => panic!("invalid duration should fail config load"),
            Err(err) => err,
        };
        assert!(format!("{err:#}").contains("payout_interval"));
    }

    #[test]
    fn load_rejects_invalid_validation_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, r#"{"validation_mode":"partial"}"#).unwrap();

        let err = match Config::load(&path) {
            Ok(_) => panic!("invalid validation mode should fail config load"),
            Err(err) => err,
        };
        assert!(format!("{err:#}").contains("validation_mode"));
    }

    #[test]
    fn load_rejects_empty_runtime_paths() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, r#"{"daemon_cookie_path":"   "}"#).unwrap();

        let err = match Config::load(&path) {
            Ok(_) => panic!("empty daemon cookie path should fail config load"),
            Err(err) => err,
        };
        assert!(format!("{err:#}").contains("daemon_cookie_path"));

        std::fs::write(&path, r#"{"payout_pause_file":""}"#).unwrap();
        let err = match Config::load(&path) {
            Ok(_) => panic!("empty payout pause path should fail config load"),
            Err(err) => err,
        };
        assert!(format!("{err:#}").contains("payout_pause_file"));
    }

    #[test]
    fn load_rejects_zero_pplns_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, r#"{"pplns_window_duration":"0s"}"#).unwrap();

        let err = match Config::load(&path) {
            Ok(_) => panic!("zero PPLNS window should fail config load"),
            Err(err) => err,
        };
        assert!(format!("{err:#}").contains("pplns_window_duration"));
    }

    #[test]
    fn load_uses_defaults_for_missing_duration_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, "{}").unwrap();

        let cfg = Config::load(&path).unwrap();
        assert_eq!(
            cfg.invalid_escalation_window_duration(),
            Duration::from_secs(24 * 60 * 60)
        );
    }
}
