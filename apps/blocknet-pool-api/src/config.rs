use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use pool_recovery::RecoveryConfig;
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
    pub candidate_submit_queue: i32,
    pub regular_submit_queue: i32,
    pub candidate_submit_workers: i32,
    pub regular_submit_workers: i32,
    pub candidate_validation_queue: i32,
    pub regular_validation_queue: i32,
    pub audit_validation_queue: i32,
    pub candidate_verifiers: i32,
    pub regular_verifiers: i32,
    pub audit_verifiers: i32,
    pub validation_wait_timeout: String,
    pub overload_shed_queue_pct: f64,
    pub overload_emergency_queue_pct: f64,
    pub overload_clear_queue_pct: f64,
    pub overload_shed_oldest_age: String,
    pub overload_emergency_oldest_age: String,
    pub overload_clear_oldest_age: String,
    pub overload_clear_hold: String,
    pub overload_sample_rate_floor: f64,
    pub audit_max_addresses_per_tick: i32,
    pub audit_max_shares_per_address: i32,
    pub candidate_claim_window: String,
    pub candidate_claim_max_per_window: i32,
    pub candidate_claim_max_inflight: i32,
    pub sample_rate: f64,
    pub warmup_shares: i32,
    pub min_sample_every: i32,
    pub invalid_sample_threshold: f64,
    pub invalid_sample_min: i32,
    pub invalid_sample_count_threshold: i32,
    pub forced_validation_quarantine_threshold: f64,
    pub invalid_escalation_window_duration: String,
    pub forced_verify_duration: String,
    pub quarantine_duration: String,
    pub max_quarantine_duration: String,
    pub suspected_fraud_force_verify_duration: String,
    pub suspected_fraud_window_duration: String,
    pub suspected_fraud_quarantine_duration: String,
    pub suspected_fraud_max_quarantine_duration: String,
    pub suspected_fraud_quarantine_strikes: i32,
    pub invalid_escalation_quarantine_strikes: i32,
    pub provisional_share_delay: String,
    pub max_provisional_shares: i32,
    pub max_provisional_recent_verified_multiplier: f64,
    pub stratum_submit_v2_required: bool,
    pub stratum_idle_timeout: String,
    pub stratum_submit_rate_limit_window: String,
    pub stratum_submit_rate_limit_max: i32,
    pub enable_vardiff: bool,
    pub vardiff_target_shares: i32,
    pub vardiff_window: String,
    pub vardiff_retarget_interval: String,
    pub vardiff_decrease_retarget_interval: String,
    pub vardiff_tolerance: f64,
    pub vardiff_min_change_pct: f64,
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

    pub database_url: String,
    pub database_pool_size: i32,
    pub api_key: String,
    pub seen_share_gc_interval: String,
    pub monitor_interval: String,
    pub monitor_ingest_secret: String,
    pub recovery: RecoveryConfig,

    #[serde(skip)]
    pub log_path: String,
}

impl Default for Config {
    fn default() -> Self {
        let runtime = pool_runtime::config::Config::default();
        Self {
            pool_name: runtime.pool_name,
            pool_url: "http://localhost:24783".to_string(),
            stratum_host: runtime.stratum_host,
            stratum_port: runtime.stratum_port,
            api_port: 24783,
            api_host: "127.0.0.1".to_string(),
            api_tls_cert_path: String::new(),
            api_tls_key_path: String::new(),
            daemon_data_dir: runtime.daemon_data_dir,
            daemon_api: runtime.daemon_api,
            daemon_token: runtime.daemon_token,
            daemon_cookie_path: runtime.daemon_cookie_path,
            pool_wallet_address: runtime.pool_wallet_address,
            initial_share_difficulty: runtime.initial_share_difficulty,
            block_poll_interval: runtime.block_poll_interval,
            sse_enabled: runtime.sse_enabled,
            refresh_on_same_height: runtime.refresh_on_same_height,
            job_timeout: runtime.job_timeout,
            stale_submit_grace: runtime.stale_submit_grace,
            validation_mode: runtime.validation_mode,
            max_verifiers: runtime.max_verifiers,
            max_validation_queue: runtime.max_validation_queue,
            candidate_submit_queue: runtime.candidate_submit_queue,
            regular_submit_queue: runtime.regular_submit_queue,
            candidate_submit_workers: runtime.candidate_submit_workers,
            regular_submit_workers: runtime.regular_submit_workers,
            candidate_validation_queue: runtime.candidate_validation_queue,
            regular_validation_queue: runtime.regular_validation_queue,
            audit_validation_queue: runtime.audit_validation_queue,
            candidate_verifiers: runtime.candidate_verifiers,
            regular_verifiers: runtime.regular_verifiers,
            audit_verifiers: runtime.audit_verifiers,
            validation_wait_timeout: runtime.validation_wait_timeout,
            overload_shed_queue_pct: runtime.overload_shed_queue_pct,
            overload_emergency_queue_pct: runtime.overload_emergency_queue_pct,
            overload_clear_queue_pct: runtime.overload_clear_queue_pct,
            overload_shed_oldest_age: runtime.overload_shed_oldest_age,
            overload_emergency_oldest_age: runtime.overload_emergency_oldest_age,
            overload_clear_oldest_age: runtime.overload_clear_oldest_age,
            overload_clear_hold: runtime.overload_clear_hold,
            overload_sample_rate_floor: runtime.overload_sample_rate_floor,
            audit_max_addresses_per_tick: runtime.audit_max_addresses_per_tick,
            audit_max_shares_per_address: runtime.audit_max_shares_per_address,
            candidate_claim_window: runtime.candidate_claim_window,
            candidate_claim_max_per_window: runtime.candidate_claim_max_per_window,
            candidate_claim_max_inflight: runtime.candidate_claim_max_inflight,
            sample_rate: runtime.sample_rate,
            warmup_shares: runtime.warmup_shares,
            min_sample_every: runtime.min_sample_every,
            invalid_sample_threshold: runtime.invalid_sample_threshold,
            invalid_sample_min: runtime.invalid_sample_min,
            invalid_sample_count_threshold: runtime.invalid_sample_count_threshold,
            forced_validation_quarantine_threshold: runtime.forced_validation_quarantine_threshold,
            invalid_escalation_window_duration: runtime.invalid_escalation_window_duration,
            forced_verify_duration: runtime.forced_verify_duration,
            quarantine_duration: runtime.quarantine_duration,
            max_quarantine_duration: runtime.max_quarantine_duration,
            suspected_fraud_force_verify_duration: runtime.suspected_fraud_force_verify_duration,
            suspected_fraud_window_duration: runtime.suspected_fraud_window_duration,
            suspected_fraud_quarantine_duration: runtime.suspected_fraud_quarantine_duration,
            suspected_fraud_max_quarantine_duration: runtime
                .suspected_fraud_max_quarantine_duration,
            suspected_fraud_quarantine_strikes: runtime.suspected_fraud_quarantine_strikes,
            invalid_escalation_quarantine_strikes: runtime.invalid_escalation_quarantine_strikes,
            provisional_share_delay: runtime.provisional_share_delay,
            max_provisional_shares: runtime.max_provisional_shares,
            max_provisional_recent_verified_multiplier: runtime
                .max_provisional_recent_verified_multiplier,
            stratum_submit_v2_required: runtime.stratum_submit_v2_required,
            stratum_idle_timeout: runtime.stratum_idle_timeout,
            stratum_submit_rate_limit_window: runtime.stratum_submit_rate_limit_window,
            stratum_submit_rate_limit_max: runtime.stratum_submit_rate_limit_max,
            enable_vardiff: runtime.enable_vardiff,
            vardiff_target_shares: runtime.vardiff_target_shares,
            vardiff_window: runtime.vardiff_window,
            vardiff_retarget_interval: runtime.vardiff_retarget_interval,
            vardiff_decrease_retarget_interval: runtime.vardiff_decrease_retarget_interval,
            vardiff_tolerance: runtime.vardiff_tolerance,
            vardiff_min_change_pct: runtime.vardiff_min_change_pct,
            min_share_difficulty: runtime.min_share_difficulty,
            max_share_difficulty: runtime.max_share_difficulty,
            pool_fee_flat: runtime.pool_fee_flat,
            pool_fee_pct: runtime.pool_fee_pct,
            payout_scheme: runtime.payout_scheme,
            pplns_window: runtime.pplns_window,
            pplns_window_duration: runtime.pplns_window_duration,
            blocks_before_payout: runtime.blocks_before_payout,
            min_payout_amount: runtime.min_payout_amount,
            block_finder_bonus: runtime.block_finder_bonus,
            block_finder_bonus_pct: runtime.block_finder_bonus_pct,
            payout_min_verified_shares: runtime.payout_min_verified_shares,
            payout_min_verified_ratio: runtime.payout_min_verified_ratio,
            payout_provisional_cap_multiplier: runtime.payout_provisional_cap_multiplier,
            payouts_enabled: runtime.payouts_enabled,
            payout_max_recipients_per_tick: runtime.payout_max_recipients_per_tick,
            payout_max_total_per_tick: runtime.payout_max_total_per_tick,
            payout_max_per_recipient: runtime.payout_max_per_recipient,
            payout_wait_priority_threshold: runtime.payout_wait_priority_threshold,
            payout_pause_file: runtime.payout_pause_file,
            payout_interval: runtime.payout_interval,
            shares_retention: runtime.shares_retention,
            payouts_retention: runtime.payouts_retention,
            retention_interval: runtime.retention_interval,
            database_url: runtime.database_url,
            database_pool_size: runtime.database_pool_size,
            api_key: String::new(),
            seen_share_gc_interval: runtime.seen_share_gc_interval,
            monitor_interval: "10s".to_string(),
            monitor_ingest_secret: String::new(),
            recovery: RecoveryConfig::default(),
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

    pub fn normalize(&mut self) {
        let runtime = self.to_runtime_config_normalized();
        self.apply_runtime_config(runtime);
        self.recovery.normalize();
        if self.pool_url.trim().is_empty() {
            self.pool_url = "http://localhost:24783".to_string();
        }
        if self.api_host.trim().is_empty() {
            self.api_host = "127.0.0.1".to_string();
        }
        if self.api_port == 0 {
            self.api_port = 24783;
        }
        if self.monitor_interval.trim().is_empty() {
            self.monitor_interval = "10s".to_string();
        }
    }

    pub fn to_runtime_config(&self) -> pool_runtime::config::Config {
        pool_runtime::config::Config {
            pool_name: self.pool_name.clone(),
            stratum_host: self.stratum_host.clone(),
            stratum_port: self.stratum_port,
            daemon_data_dir: self.daemon_data_dir.clone(),
            daemon_api: self.daemon_api.clone(),
            daemon_token: self.daemon_token.clone(),
            daemon_cookie_path: self.daemon_cookie_path.clone(),
            pool_wallet_address: self.pool_wallet_address.clone(),
            initial_share_difficulty: self.initial_share_difficulty,
            block_poll_interval: self.block_poll_interval.clone(),
            sse_enabled: self.sse_enabled,
            refresh_on_same_height: self.refresh_on_same_height,
            job_timeout: self.job_timeout.clone(),
            stale_submit_grace: self.stale_submit_grace.clone(),
            validation_mode: self.validation_mode.clone(),
            max_verifiers: self.max_verifiers,
            max_validation_queue: self.max_validation_queue,
            candidate_submit_queue: self.candidate_submit_queue,
            regular_submit_queue: self.regular_submit_queue,
            candidate_submit_workers: self.candidate_submit_workers,
            regular_submit_workers: self.regular_submit_workers,
            candidate_validation_queue: self.candidate_validation_queue,
            regular_validation_queue: self.regular_validation_queue,
            audit_validation_queue: self.audit_validation_queue,
            candidate_verifiers: self.candidate_verifiers,
            regular_verifiers: self.regular_verifiers,
            audit_verifiers: self.audit_verifiers,
            validation_wait_timeout: self.validation_wait_timeout.clone(),
            overload_shed_queue_pct: self.overload_shed_queue_pct,
            overload_emergency_queue_pct: self.overload_emergency_queue_pct,
            overload_clear_queue_pct: self.overload_clear_queue_pct,
            overload_shed_oldest_age: self.overload_shed_oldest_age.clone(),
            overload_emergency_oldest_age: self.overload_emergency_oldest_age.clone(),
            overload_clear_oldest_age: self.overload_clear_oldest_age.clone(),
            overload_clear_hold: self.overload_clear_hold.clone(),
            overload_sample_rate_floor: self.overload_sample_rate_floor,
            audit_max_addresses_per_tick: self.audit_max_addresses_per_tick,
            audit_max_shares_per_address: self.audit_max_shares_per_address,
            candidate_claim_window: self.candidate_claim_window.clone(),
            candidate_claim_max_per_window: self.candidate_claim_max_per_window,
            candidate_claim_max_inflight: self.candidate_claim_max_inflight,
            sample_rate: self.sample_rate,
            warmup_shares: self.warmup_shares,
            min_sample_every: self.min_sample_every,
            invalid_sample_threshold: self.invalid_sample_threshold,
            invalid_sample_min: self.invalid_sample_min,
            invalid_sample_count_threshold: self.invalid_sample_count_threshold,
            forced_validation_quarantine_threshold: self.forced_validation_quarantine_threshold,
            invalid_escalation_window_duration: self.invalid_escalation_window_duration.clone(),
            forced_verify_duration: self.forced_verify_duration.clone(),
            quarantine_duration: self.quarantine_duration.clone(),
            max_quarantine_duration: self.max_quarantine_duration.clone(),
            suspected_fraud_force_verify_duration: self
                .suspected_fraud_force_verify_duration
                .clone(),
            suspected_fraud_window_duration: self.suspected_fraud_window_duration.clone(),
            suspected_fraud_quarantine_duration: self.suspected_fraud_quarantine_duration.clone(),
            suspected_fraud_max_quarantine_duration: self
                .suspected_fraud_max_quarantine_duration
                .clone(),
            suspected_fraud_quarantine_strikes: self.suspected_fraud_quarantine_strikes,
            invalid_escalation_quarantine_strikes: self.invalid_escalation_quarantine_strikes,
            provisional_share_delay: self.provisional_share_delay.clone(),
            max_provisional_shares: self.max_provisional_shares,
            max_provisional_recent_verified_multiplier: self
                .max_provisional_recent_verified_multiplier,
            stratum_submit_v2_required: self.stratum_submit_v2_required,
            stratum_idle_timeout: self.stratum_idle_timeout.clone(),
            stratum_submit_rate_limit_window: self.stratum_submit_rate_limit_window.clone(),
            stratum_submit_rate_limit_max: self.stratum_submit_rate_limit_max,
            enable_vardiff: self.enable_vardiff,
            vardiff_target_shares: self.vardiff_target_shares,
            vardiff_window: self.vardiff_window.clone(),
            vardiff_retarget_interval: self.vardiff_retarget_interval.clone(),
            vardiff_decrease_retarget_interval: self.vardiff_decrease_retarget_interval.clone(),
            vardiff_tolerance: self.vardiff_tolerance,
            vardiff_min_change_pct: self.vardiff_min_change_pct,
            min_share_difficulty: self.min_share_difficulty,
            max_share_difficulty: self.max_share_difficulty,
            pool_fee_flat: self.pool_fee_flat,
            pool_fee_pct: self.pool_fee_pct,
            payout_scheme: self.payout_scheme.clone(),
            pplns_window: self.pplns_window,
            pplns_window_duration: self.pplns_window_duration.clone(),
            blocks_before_payout: self.blocks_before_payout,
            min_payout_amount: self.min_payout_amount,
            block_finder_bonus: self.block_finder_bonus,
            block_finder_bonus_pct: self.block_finder_bonus_pct,
            payout_min_verified_shares: self.payout_min_verified_shares,
            payout_min_verified_ratio: self.payout_min_verified_ratio,
            payout_provisional_cap_multiplier: self.payout_provisional_cap_multiplier,
            payouts_enabled: self.payouts_enabled,
            payout_max_recipients_per_tick: self.payout_max_recipients_per_tick,
            payout_max_total_per_tick: self.payout_max_total_per_tick,
            payout_max_per_recipient: self.payout_max_per_recipient,
            payout_wait_priority_threshold: self.payout_wait_priority_threshold.clone(),
            payout_pause_file: self.payout_pause_file.clone(),
            payout_interval: self.payout_interval.clone(),
            shares_retention: self.shares_retention.clone(),
            payouts_retention: self.payouts_retention.clone(),
            retention_interval: self.retention_interval.clone(),
            database_url: self.database_url.clone(),
            database_pool_size: self.database_pool_size,
            seen_share_gc_interval: self.seen_share_gc_interval.clone(),
            log_path: self.log_path.clone(),
        }
    }

    fn to_runtime_config_normalized(&self) -> pool_runtime::config::Config {
        let mut runtime = self.to_runtime_config();
        runtime.normalize();
        runtime
    }

    fn apply_runtime_config(&mut self, runtime: pool_runtime::config::Config) {
        self.pool_name = runtime.pool_name;
        self.stratum_host = runtime.stratum_host;
        self.stratum_port = runtime.stratum_port;
        self.daemon_data_dir = runtime.daemon_data_dir;
        self.daemon_api = runtime.daemon_api;
        self.daemon_token = runtime.daemon_token;
        self.daemon_cookie_path = runtime.daemon_cookie_path;
        self.pool_wallet_address = runtime.pool_wallet_address;
        self.initial_share_difficulty = runtime.initial_share_difficulty;
        self.block_poll_interval = runtime.block_poll_interval;
        self.sse_enabled = runtime.sse_enabled;
        self.refresh_on_same_height = runtime.refresh_on_same_height;
        self.job_timeout = runtime.job_timeout;
        self.stale_submit_grace = runtime.stale_submit_grace;
        self.validation_mode = runtime.validation_mode;
        self.max_verifiers = runtime.max_verifiers;
        self.max_validation_queue = runtime.max_validation_queue;
        self.candidate_submit_queue = runtime.candidate_submit_queue;
        self.regular_submit_queue = runtime.regular_submit_queue;
        self.candidate_submit_workers = runtime.candidate_submit_workers;
        self.regular_submit_workers = runtime.regular_submit_workers;
        self.candidate_validation_queue = runtime.candidate_validation_queue;
        self.regular_validation_queue = runtime.regular_validation_queue;
        self.audit_validation_queue = runtime.audit_validation_queue;
        self.candidate_verifiers = runtime.candidate_verifiers;
        self.regular_verifiers = runtime.regular_verifiers;
        self.audit_verifiers = runtime.audit_verifiers;
        self.validation_wait_timeout = runtime.validation_wait_timeout;
        self.overload_shed_queue_pct = runtime.overload_shed_queue_pct;
        self.overload_emergency_queue_pct = runtime.overload_emergency_queue_pct;
        self.overload_clear_queue_pct = runtime.overload_clear_queue_pct;
        self.overload_shed_oldest_age = runtime.overload_shed_oldest_age;
        self.overload_emergency_oldest_age = runtime.overload_emergency_oldest_age;
        self.overload_clear_oldest_age = runtime.overload_clear_oldest_age;
        self.overload_clear_hold = runtime.overload_clear_hold;
        self.overload_sample_rate_floor = runtime.overload_sample_rate_floor;
        self.audit_max_addresses_per_tick = runtime.audit_max_addresses_per_tick;
        self.audit_max_shares_per_address = runtime.audit_max_shares_per_address;
        self.candidate_claim_window = runtime.candidate_claim_window;
        self.candidate_claim_max_per_window = runtime.candidate_claim_max_per_window;
        self.candidate_claim_max_inflight = runtime.candidate_claim_max_inflight;
        self.sample_rate = runtime.sample_rate;
        self.warmup_shares = runtime.warmup_shares;
        self.min_sample_every = runtime.min_sample_every;
        self.invalid_sample_threshold = runtime.invalid_sample_threshold;
        self.invalid_sample_min = runtime.invalid_sample_min;
        self.invalid_sample_count_threshold = runtime.invalid_sample_count_threshold;
        self.forced_validation_quarantine_threshold =
            runtime.forced_validation_quarantine_threshold;
        self.invalid_escalation_window_duration = runtime.invalid_escalation_window_duration;
        self.forced_verify_duration = runtime.forced_verify_duration;
        self.quarantine_duration = runtime.quarantine_duration;
        self.max_quarantine_duration = runtime.max_quarantine_duration;
        self.suspected_fraud_force_verify_duration = runtime.suspected_fraud_force_verify_duration;
        self.suspected_fraud_window_duration = runtime.suspected_fraud_window_duration;
        self.suspected_fraud_quarantine_duration = runtime.suspected_fraud_quarantine_duration;
        self.suspected_fraud_max_quarantine_duration =
            runtime.suspected_fraud_max_quarantine_duration;
        self.suspected_fraud_quarantine_strikes = runtime.suspected_fraud_quarantine_strikes;
        self.invalid_escalation_quarantine_strikes = runtime.invalid_escalation_quarantine_strikes;
        self.provisional_share_delay = runtime.provisional_share_delay;
        self.max_provisional_shares = runtime.max_provisional_shares;
        self.max_provisional_recent_verified_multiplier =
            runtime.max_provisional_recent_verified_multiplier;
        self.stratum_submit_v2_required = runtime.stratum_submit_v2_required;
        self.stratum_idle_timeout = runtime.stratum_idle_timeout;
        self.stratum_submit_rate_limit_window = runtime.stratum_submit_rate_limit_window;
        self.stratum_submit_rate_limit_max = runtime.stratum_submit_rate_limit_max;
        self.enable_vardiff = runtime.enable_vardiff;
        self.vardiff_target_shares = runtime.vardiff_target_shares;
        self.vardiff_window = runtime.vardiff_window;
        self.vardiff_retarget_interval = runtime.vardiff_retarget_interval;
        self.vardiff_decrease_retarget_interval = runtime.vardiff_decrease_retarget_interval;
        self.vardiff_tolerance = runtime.vardiff_tolerance;
        self.vardiff_min_change_pct = runtime.vardiff_min_change_pct;
        self.min_share_difficulty = runtime.min_share_difficulty;
        self.max_share_difficulty = runtime.max_share_difficulty;
        self.pool_fee_flat = runtime.pool_fee_flat;
        self.pool_fee_pct = runtime.pool_fee_pct;
        self.payout_scheme = runtime.payout_scheme;
        self.pplns_window = runtime.pplns_window;
        self.pplns_window_duration = runtime.pplns_window_duration;
        self.blocks_before_payout = runtime.blocks_before_payout;
        self.min_payout_amount = runtime.min_payout_amount;
        self.block_finder_bonus = runtime.block_finder_bonus;
        self.block_finder_bonus_pct = runtime.block_finder_bonus_pct;
        self.payout_min_verified_shares = runtime.payout_min_verified_shares;
        self.payout_min_verified_ratio = runtime.payout_min_verified_ratio;
        self.payout_provisional_cap_multiplier = runtime.payout_provisional_cap_multiplier;
        self.payouts_enabled = runtime.payouts_enabled;
        self.payout_max_recipients_per_tick = runtime.payout_max_recipients_per_tick;
        self.payout_max_total_per_tick = runtime.payout_max_total_per_tick;
        self.payout_max_per_recipient = runtime.payout_max_per_recipient;
        self.payout_wait_priority_threshold = runtime.payout_wait_priority_threshold;
        self.payout_pause_file = runtime.payout_pause_file;
        self.payout_interval = runtime.payout_interval;
        self.shares_retention = runtime.shares_retention;
        self.payouts_retention = runtime.payouts_retention;
        self.retention_interval = runtime.retention_interval;
        self.database_url = runtime.database_url;
        self.database_pool_size = runtime.database_pool_size;
        self.seen_share_gc_interval = runtime.seen_share_gc_interval;
        self.log_path = runtime.log_path;
    }

    pub fn has_api_tls(&self) -> bool {
        !self.api_tls_cert_path.trim().is_empty() && !self.api_tls_key_path.trim().is_empty()
    }

    pub fn monitor_interval_duration(&self) -> Duration {
        parse_duration_or(&self.monitor_interval, Duration::from_secs(10))
            .clamp(Duration::from_secs(5), Duration::from_secs(5 * 60))
    }

    pub fn provisional_share_delay_duration(&self) -> Duration {
        parse_duration_or(&self.provisional_share_delay, Duration::from_secs(15 * 60))
    }

    pub fn max_provisional_recent_verified_multiplier(&self) -> f64 {
        self.max_provisional_recent_verified_multiplier.max(0.0)
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
