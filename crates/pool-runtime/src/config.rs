use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

const CANDIDATE_SUBMIT_QUEUE_SIZE: usize = 64;
const CANDIDATE_VALIDATION_QUEUE_SIZE: usize = 64;

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
        Ok(cfg)
    }

    pub fn normalize(&mut self) {
        let mode = self.validation_mode.trim().to_ascii_lowercase();
        self.validation_mode = match mode.as_str() {
            "full" | "probabilistic" => mode,
            _ => "probabilistic".to_string(),
        };

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
        if self.pplns_window_duration.trim().is_empty() {
            self.pplns_window_duration = "6h".to_string();
        }
        clamp_i32_min(&mut self.payout_min_verified_shares, 0);
        clamp_f64_min(&mut self.payout_provisional_cap_multiplier, 0.0);
        clamp_i32_min(&mut self.payout_max_recipients_per_tick, 0);
        clamp_f64_min(&mut self.payout_max_total_per_tick, 0.0);
        clamp_f64_min(&mut self.payout_max_per_recipient, 0.0);
        clamp_i32_min(&mut self.database_pool_size, 1);
        if self.daemon_cookie_path.trim().is_empty() {
            self.daemon_cookie_path = "/etc/blocknet/pool/daemon-active.api.cookie".to_string();
        }
        if self.payout_pause_file.trim().is_empty() {
            self.payout_pause_file = "/etc/blocknet/pool/payouts.pause".to_string();
        }
        let max_atomic_amount = (u64::MAX as f64) / 100_000_000.0;
        if !self.min_payout_amount.is_finite() || self.min_payout_amount < 0.0 {
            self.min_payout_amount = 0.1;
        } else {
            self.min_payout_amount = self.min_payout_amount.clamp(0.0, max_atomic_amount);
        }
    }

    pub(crate) fn job_timeout_duration(&self) -> Duration {
        parse_duration_or(&self.job_timeout, Duration::from_secs(5 * 60))
    }

    pub(crate) fn stale_submit_grace_duration(&self) -> Duration {
        parse_duration_or(&self.stale_submit_grace, Duration::from_secs(8))
    }

    pub(crate) fn candidate_submit_queue_size(&self) -> usize {
        CANDIDATE_SUBMIT_QUEUE_SIZE
    }

    pub(crate) fn regular_submit_queue_size(&self) -> usize {
        self.regular_submit_queue.max(1) as usize
    }

    pub fn candidate_validation_queue_size(&self) -> usize {
        CANDIDATE_VALIDATION_QUEUE_SIZE
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
        parse_duration_or(
            &self.forced_verify_duration,
            Duration::from_secs(2 * 60 * 60),
        )
    }

    pub(crate) fn invalid_escalation_window_duration(&self) -> Duration {
        parse_duration_or(
            &self.invalid_escalation_window_duration,
            Duration::from_secs(6 * 60 * 60),
        )
    }

    pub fn provisional_share_delay_duration(&self) -> Duration {
        parse_duration_or(&self.provisional_share_delay, Duration::from_secs(15 * 60))
    }

    pub(crate) fn quarantine_duration(&self) -> Duration {
        parse_duration_or(&self.quarantine_duration, Duration::from_secs(15 * 60))
    }

    pub(crate) fn max_quarantine_duration(&self) -> Duration {
        parse_duration_or(
            &self.max_quarantine_duration,
            Duration::from_secs(2 * 60 * 60),
        )
    }

    pub fn payout_interval_duration(&self) -> Duration {
        parse_duration_or(&self.payout_interval, Duration::from_secs(60 * 60))
    }

    pub(crate) fn vardiff_window_duration(&self) -> Duration {
        parse_duration_or(&self.vardiff_window, Duration::from_secs(5 * 60))
    }

    pub(crate) fn vardiff_retarget_interval_duration(&self) -> Duration {
        parse_duration_or(&self.vardiff_retarget_interval, Duration::from_secs(30))
    }

    pub fn pplns_window_duration(&self) -> Duration {
        let duration = parse_duration_or(
            &self.pplns_window_duration,
            Duration::from_secs(6 * 60 * 60),
        );
        if duration.is_zero() {
            Duration::from_secs(6 * 60 * 60)
        } else {
            duration
        }
    }

    pub fn pool_fee(&self, reward: u64) -> u64 {
        if self.pool_fee_pct <= 0.0 {
            return 0;
        }
        ((reward as f64) * self.pool_fee_pct / 100.0).clamp(0.0, reward as f64) as u64
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

fn parse_duration_or(value: &str, fallback: Duration) -> Duration {
    humantime::parse_duration(value).unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_clamps_values() {
        let mut cfg = Config {
            validation_mode: "invalid".to_string(),
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

        assert_eq!(cfg.validation_mode, "probabilistic");
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
}
