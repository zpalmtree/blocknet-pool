use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub pool_name: String,
    pub pool_url: String,

    pub stratum_host: String,
    pub stratum_port: u16,
    pub api_port: u16,
    pub api_host: String,
    pub api_tls_cert_path: String,
    pub api_tls_key_path: String,

    pub daemon_binary: String,
    pub daemon_data_dir: String,
    pub daemon_api: String,
    pub daemon_token: String,
    pub daemon_cookie_path: String,
    pub pool_wallet_address: String,

    pub initial_share_difficulty: u64,
    pub block_poll_interval: String,
    pub sse_enabled: bool,
    pub refresh_on_same_height: bool,
    pub job_timeout: String,
    pub stale_submit_grace: String,
    pub validation_mode: String,
    pub max_verifiers: i32,
    pub max_validation_queue: i32,
    pub sample_rate: f64,
    pub warmup_shares: i32,
    pub min_sample_every: i32,
    pub invalid_sample_threshold: f64,
    pub invalid_sample_min: i32,
    pub forced_verify_duration: String,
    pub quarantine_duration: String,
    pub max_quarantine_duration: String,
    pub invalid_escalation_quarantine_strikes: i32,
    pub provisional_share_delay: String,
    pub max_provisional_shares: i32,
    pub stratum_submit_v2_required: bool,
    pub stratum_idle_timeout: String,
    pub stratum_submit_rate_limit_window: String,
    pub stratum_submit_rate_limit_max: i32,
    pub enable_vardiff: bool,
    pub vardiff_target_shares: i32,
    pub vardiff_window: String,
    pub vardiff_retarget_interval: String,
    pub vardiff_tolerance: f64,
    pub min_share_difficulty: u64,
    pub max_share_difficulty: u64,

    pub pool_fee_flat: f64,
    pub pool_fee_pct: f64,

    pub payout_scheme: String,
    pub pplns_window: i32,
    pub pplns_window_duration: String,
    pub blocks_before_payout: i32,
    pub min_payout_amount: f64,
    pub block_finder_bonus: bool,
    pub block_finder_bonus_pct: f64,
    pub payout_min_verified_shares: i32,
    pub payout_min_verified_ratio: f64,
    pub payout_provisional_cap_multiplier: f64,
    pub payouts_enabled: bool,
    pub payout_max_recipients_per_tick: i32,
    pub payout_max_total_per_tick: f64,
    pub payout_max_per_recipient: f64,
    pub payout_wait_priority_threshold: String,
    pub payout_pause_file: String,
    pub payout_interval: String,
    pub shares_retention: String,
    pub payouts_retention: String,
    pub retention_interval: String,

    pub database_path: String,
    pub database_url: String,
    pub database_pool_size: i32,
    pub api_key: String,
    pub seen_share_gc_interval: String,

    #[serde(skip)]
    pub log_path: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            pool_name: "blocknet pool".to_string(),
            pool_url: "http://localhost:24783".to_string(),
            stratum_host: "127.0.0.1".to_string(),
            stratum_port: 3333,
            api_port: 24783,
            api_host: "127.0.0.1".to_string(),
            api_tls_cert_path: String::new(),
            api_tls_key_path: String::new(),
            daemon_binary: "./blocknet".to_string(),
            daemon_data_dir: "data".to_string(),
            daemon_api: "http://127.0.0.1:8332".to_string(),
            daemon_token: String::new(),
            daemon_cookie_path: String::new(),
            pool_wallet_address: String::new(),
            initial_share_difficulty: 60,
            block_poll_interval: "2s".to_string(),
            sse_enabled: true,
            refresh_on_same_height: false,
            job_timeout: "5m".to_string(),
            stale_submit_grace: "5s".to_string(),
            validation_mode: "probabilistic".to_string(),
            max_verifiers: 2,
            max_validation_queue: 2048,
            sample_rate: 0.05,
            warmup_shares: 50,
            min_sample_every: 20,
            invalid_sample_threshold: 0.01,
            invalid_sample_min: 50,
            forced_verify_duration: "24h".to_string(),
            quarantine_duration: "1h".to_string(),
            max_quarantine_duration: "168h".to_string(),
            invalid_escalation_quarantine_strikes: 3,
            provisional_share_delay: "15m".to_string(),
            max_provisional_shares: 200,
            stratum_submit_v2_required: true,
            stratum_idle_timeout: "15m".to_string(),
            stratum_submit_rate_limit_window: "10s".to_string(),
            stratum_submit_rate_limit_max: 120,
            enable_vardiff: true,
            vardiff_target_shares: 10,
            vardiff_window: "5m".to_string(),
            vardiff_retarget_interval: "5s".to_string(),
            vardiff_tolerance: 0.25,
            min_share_difficulty: 1,
            max_share_difficulty: 1_000_000_000,
            pool_fee_flat: 0.0,
            pool_fee_pct: 0.0,
            payout_scheme: "pplns".to_string(),
            pplns_window: 1000,
            pplns_window_duration: "6h".to_string(),
            blocks_before_payout: 60,
            min_payout_amount: 0.1,
            block_finder_bonus: false,
            block_finder_bonus_pct: 5.0,
            payout_min_verified_shares: 1,
            payout_min_verified_ratio: 0.05,
            payout_provisional_cap_multiplier: 0.0,
            payouts_enabled: true,
            payout_max_recipients_per_tick: 500,
            payout_max_total_per_tick: 0.0,
            payout_max_per_recipient: 0.0,
            payout_wait_priority_threshold: "6h".to_string(),
            payout_pause_file: "payouts.pause".to_string(),
            payout_interval: "1h".to_string(),
            shares_retention: "90d".to_string(),
            payouts_retention: "365d".to_string(),
            retention_interval: "1h".to_string(),
            database_path: "pool.db".to_string(),
            database_url: String::new(),
            database_pool_size: 4,
            api_key: String::new(),
            seen_share_gc_interval: "10m".to_string(),
            log_path: String::new(),
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

    pub fn write_default(path: &Path) -> Result<()> {
        let cfg = Config::default();
        let data = serde_json::to_vec_pretty(&cfg).context("serialize default config")?;
        fs::write(path, data).with_context(|| format!("write config {}", path.display()))
    }

    pub fn normalize(&mut self) {
        let mode = self.validation_mode.trim().to_ascii_lowercase();
        self.validation_mode = match mode.as_str() {
            "full" | "probabilistic" => mode,
            _ => "probabilistic".to_string(),
        };

        if self.max_verifiers < 0 {
            self.max_verifiers = 0;
        }
        if self.max_validation_queue < 1 {
            self.max_validation_queue = 2048;
        }
        self.sample_rate = self.sample_rate.clamp(0.0, 1.0);
        if self.warmup_shares < 0 {
            self.warmup_shares = 0;
        }
        if self.min_sample_every < 0 {
            self.min_sample_every = 0;
        }
        if self.invalid_sample_min < 1 {
            self.invalid_sample_min = 1;
        }
        if !(0.0 < self.invalid_sample_threshold && self.invalid_sample_threshold <= 1.0) {
            self.invalid_sample_threshold = 0.01;
        }
        if self.max_provisional_shares < 0 {
            self.max_provisional_shares = 0;
        }
        if self.stratum_submit_rate_limit_max < 1 {
            self.stratum_submit_rate_limit_max = 1;
        }
        if self.invalid_escalation_quarantine_strikes < 0 {
            self.invalid_escalation_quarantine_strikes = 0;
        }
        if self.initial_share_difficulty < 1 {
            self.initial_share_difficulty = 1;
        }
        if self.min_share_difficulty < 1 {
            self.min_share_difficulty = 1;
        }
        if self.max_share_difficulty < self.min_share_difficulty {
            self.max_share_difficulty = self.min_share_difficulty;
        }
        self.initial_share_difficulty = self
            .initial_share_difficulty
            .clamp(self.min_share_difficulty, self.max_share_difficulty);
        if self.vardiff_target_shares < 1 {
            self.vardiff_target_shares = 1;
        }
        self.vardiff_tolerance = self.vardiff_tolerance.clamp(0.01, 0.95);
        if !self.block_finder_bonus_pct.is_finite() {
            self.block_finder_bonus_pct = 0.0;
        } else {
            self.block_finder_bonus_pct = self.block_finder_bonus_pct.clamp(0.0, 100.0);
        }
        if self.payout_min_verified_shares < 0 {
            self.payout_min_verified_shares = 0;
        }
        if !self.payout_min_verified_ratio.is_finite() {
            self.payout_min_verified_ratio = 0.0;
        } else {
            self.payout_min_verified_ratio = self.payout_min_verified_ratio.clamp(0.0, 1.0);
        }
        if !self.payout_provisional_cap_multiplier.is_finite()
            || self.payout_provisional_cap_multiplier < 0.0
        {
            self.payout_provisional_cap_multiplier = 0.0;
        }
        if self.payout_max_recipients_per_tick < 0 {
            self.payout_max_recipients_per_tick = 0;
        }
        if !self.payout_max_total_per_tick.is_finite() || self.payout_max_total_per_tick < 0.0 {
            self.payout_max_total_per_tick = 0.0;
        }
        if !self.payout_max_per_recipient.is_finite() || self.payout_max_per_recipient < 0.0 {
            self.payout_max_per_recipient = 0.0;
        }
        if self.database_pool_size < 1 {
            self.database_pool_size = 1;
        }
        let max_atomic_amount = (u64::MAX as f64) / 100_000_000.0;
        if !self.min_payout_amount.is_finite() || self.min_payout_amount < 0.0 {
            self.min_payout_amount = 0.1;
        } else {
            self.min_payout_amount = self.min_payout_amount.clamp(0.0, max_atomic_amount);
        }
    }

    pub fn block_poll_duration(&self) -> Duration {
        parse_duration_or(&self.block_poll_interval, Duration::from_secs(2))
    }

    pub fn job_timeout_duration(&self) -> Duration {
        parse_duration_or(&self.job_timeout, Duration::from_secs(5 * 60))
    }

    pub fn stale_submit_grace_duration(&self) -> Duration {
        parse_duration_or(&self.stale_submit_grace, Duration::from_secs(5))
    }

    pub fn stratum_idle_timeout_duration(&self) -> Duration {
        parse_duration_or(&self.stratum_idle_timeout, Duration::from_secs(15 * 60))
            .clamp(Duration::from_secs(30), Duration::from_secs(24 * 60 * 60))
    }

    pub fn stratum_submit_rate_limit_window_duration(&self) -> Duration {
        parse_duration_or(
            &self.stratum_submit_rate_limit_window,
            Duration::from_secs(10),
        )
        .clamp(Duration::from_secs(1), Duration::from_secs(5 * 60))
    }

    pub fn forced_verify_duration(&self) -> Duration {
        parse_duration_or(
            &self.forced_verify_duration,
            Duration::from_secs(24 * 60 * 60),
        )
    }

    pub fn provisional_share_delay_duration(&self) -> Duration {
        parse_duration_or(&self.provisional_share_delay, Duration::from_secs(15 * 60))
    }

    pub fn quarantine_duration_duration(&self) -> Duration {
        parse_duration_or(&self.quarantine_duration, Duration::from_secs(60 * 60))
    }

    pub fn max_quarantine_duration_duration(&self) -> Duration {
        parse_duration_or(
            &self.max_quarantine_duration,
            Duration::from_secs(168 * 60 * 60),
        )
    }

    pub fn payout_interval_duration(&self) -> Duration {
        parse_duration_or(&self.payout_interval, Duration::from_secs(60 * 60))
    }

    pub fn payout_wait_priority_threshold_duration(&self) -> Duration {
        parse_duration_or(
            &self.payout_wait_priority_threshold,
            Duration::from_secs(6 * 60 * 60),
        )
    }

    pub fn shares_retention_duration(&self) -> Option<Duration> {
        parse_optional_duration(&self.shares_retention)
    }

    pub fn payouts_retention_duration(&self) -> Option<Duration> {
        parse_optional_duration(&self.payouts_retention)
    }

    pub fn retention_interval_duration(&self) -> Duration {
        parse_duration_or(&self.retention_interval, Duration::from_secs(60 * 60))
    }

    pub fn has_api_tls(&self) -> bool {
        !self.api_tls_cert_path.trim().is_empty() && !self.api_tls_key_path.trim().is_empty()
    }

    pub fn vardiff_window_duration(&self) -> Duration {
        parse_duration_or(&self.vardiff_window, Duration::from_secs(5 * 60))
    }

    pub fn vardiff_retarget_interval_duration(&self) -> Duration {
        parse_duration_or(&self.vardiff_retarget_interval, Duration::from_secs(30))
    }

    pub fn seen_share_gc_interval_duration(&self) -> Duration {
        parse_duration_or(&self.seen_share_gc_interval, Duration::from_secs(10 * 60))
    }

    pub fn pplns_window_duration_duration(&self) -> Duration {
        if self.pplns_window_duration.trim().is_empty() {
            return Duration::from_secs(0);
        }
        parse_duration_or(
            &self.pplns_window_duration,
            Duration::from_secs(6 * 60 * 60),
        )
    }

    pub fn pool_fee(&self, reward: u64) -> u64 {
        let mut fee = 0u64;

        if self.pool_fee_flat > 0.0 {
            let flat = (self.pool_fee_flat * 100_000_000.0) as u64;
            if flat > reward {
                return reward;
            }
            fee = fee.saturating_add(flat);
        }

        if self.pool_fee_pct > 0.0 {
            let remainder = reward.saturating_sub(fee);
            let pct = ((remainder as f64) * self.pool_fee_pct / 100.0) as u64;
            fee = fee.saturating_add(pct);
        }

        fee.min(reward)
    }
}

fn parse_duration_or(value: &str, fallback: Duration) -> Duration {
    humantime::parse_duration(value).unwrap_or(fallback)
}

fn parse_optional_duration(value: &str) -> Option<Duration> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    humantime::parse_duration(trimmed).ok()
}

pub fn generate_default_env(path: &Path) -> Result<bool> {
    if path.exists() {
        return Ok(false);
    }
    let template = "# Blocknet pool runtime secrets\n# REQUIRED for wallet unlock/load operations\nBLOCKNET_WALLET_PASSWORD=\n";
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    let mut file = options
        .open(path)
        .with_context(|| format!("write {}", path.display()))?;
    file.write_all(template.as_bytes())
        .with_context(|| format!("write {}", path.display()))?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_clamps_values() {
        let mut cfg = Config {
            validation_mode: "invalid".to_string(),
            max_verifiers: -1,
            max_validation_queue: 0,
            sample_rate: 2.0,
            warmup_shares: -5,
            min_sample_every: -1,
            invalid_sample_min: 0,
            invalid_sample_threshold: 2.0,
            invalid_escalation_quarantine_strikes: -2,
            max_provisional_shares: -1,
            stratum_submit_v2_required: false,
            stratum_submit_rate_limit_max: 0,
            initial_share_difficulty: 0,
            min_share_difficulty: 10,
            max_share_difficulty: 5,
            vardiff_target_shares: 0,
            vardiff_tolerance: 2.0,
            block_finder_bonus_pct: 250.0,
            payout_min_verified_shares: -3,
            payout_min_verified_ratio: 2.0,
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
        assert_eq!(cfg.max_verifiers, 0);
        assert_eq!(cfg.max_validation_queue, 2048);
        assert_eq!(cfg.sample_rate, 1.0);
        assert_eq!(cfg.warmup_shares, 0);
        assert_eq!(cfg.min_sample_every, 0);
        assert_eq!(cfg.invalid_sample_min, 1);
        assert_eq!(cfg.invalid_sample_threshold, 0.01);
        assert_eq!(cfg.invalid_escalation_quarantine_strikes, 0);
        assert_eq!(cfg.max_provisional_shares, 0);
        assert!(!cfg.stratum_submit_v2_required);
        assert_eq!(cfg.stratum_submit_rate_limit_max, 1);
        assert_eq!(cfg.min_share_difficulty, 10);
        assert_eq!(cfg.max_share_difficulty, 10);
        assert_eq!(cfg.initial_share_difficulty, 10);
        assert_eq!(cfg.vardiff_target_shares, 1);
        assert_eq!(cfg.vardiff_tolerance, 0.95);
        assert_eq!(cfg.block_finder_bonus_pct, 100.0);
        assert_eq!(cfg.payout_min_verified_shares, 0);
        assert_eq!(cfg.payout_min_verified_ratio, 1.0);
        assert_eq!(cfg.payout_provisional_cap_multiplier, 0.0);
        assert_eq!(cfg.payout_max_recipients_per_tick, 0);
        assert_eq!(cfg.payout_max_total_per_tick, 0.0);
        assert_eq!(cfg.payout_max_per_recipient, 0.0);
        assert_eq!(cfg.min_payout_amount, 0.1);
        assert_eq!(cfg.database_pool_size, 1);
    }

    #[test]
    fn fee_applies_flat_then_pct() {
        let cfg = Config {
            pool_fee_flat: 1.0,
            pool_fee_pct: 10.0,
            ..Config::default()
        };
        let reward = 10_000_000_000u64;
        let fee = cfg.pool_fee(reward);
        assert!(fee > 100_000_000);
        assert!(fee < reward);
    }

    #[test]
    fn payout_wait_priority_threshold_defaults_to_six_hours() {
        let cfg = Config::default();
        assert_eq!(
            cfg.payout_wait_priority_threshold_duration(),
            Duration::from_secs(6 * 60 * 60)
        );
    }

    #[test]
    fn writes_env_template_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let env_path = dir.path().join(".env");

        assert!(generate_default_env(&env_path).expect("create env"));
        assert!(!generate_default_env(&env_path).expect("second create should be false"));
    }
}
