use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;

use crate::jobs::JobRuntimeSnapshot;
use crate::payout::PayoutRuntimeSnapshot;
use crate::stats::PoolSnapshot;
use crate::stratum::SubmitRuntimeSnapshot;
use crate::telemetry::TimedOperationSummary;
use crate::validation::ValidationSnapshot;

pub const LIVE_RUNTIME_SNAPSHOT_META_KEY: &str = "live_runtime_snapshot_v1";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistedValidationSummary {
    pub in_flight: i64,
    pub candidate_queue_depth: usize,
    pub regular_queue_depth: usize,
    #[serde(default)]
    pub audit_queue_depth: usize,
    #[serde(default)]
    pub candidate_oldest_age_millis: Option<u64>,
    #[serde(default)]
    pub regular_oldest_age_millis: Option<u64>,
    #[serde(default)]
    pub audit_oldest_age_millis: Option<u64>,
    #[serde(default)]
    pub candidate_wait: crate::telemetry::PercentileSummary,
    #[serde(default)]
    pub regular_wait: crate::telemetry::PercentileSummary,
    #[serde(default)]
    pub audit_wait: crate::telemetry::PercentileSummary,
    #[serde(default)]
    pub validation_duration: crate::telemetry::PercentileSummary,
    #[serde(default)]
    pub audit_duration: crate::telemetry::PercentileSummary,
    pub tracked_addresses: usize,
    pub forced_verify_addresses: usize,
    pub total_shares: u64,
    pub sampled_shares: u64,
    pub invalid_samples: u64,
    pub pending_provisional: u64,
    pub fraud_detections: u64,
    #[serde(default)]
    pub candidate_false_claims: u64,
    #[serde(default)]
    pub hot_accepts: u64,
    #[serde(default)]
    pub sync_full_verifies: u64,
    #[serde(default)]
    pub audit_enqueued: u64,
    #[serde(default)]
    pub audit_verified: u64,
    #[serde(default)]
    pub audit_rejected: u64,
    #[serde(default)]
    pub audit_deferred: u64,
    #[serde(default)]
    pub overload_mode: crate::validation::OverloadMode,
    #[serde(default)]
    pub effective_sample_rate: f64,
}

impl From<ValidationSnapshot> for PersistedValidationSummary {
    fn from(value: ValidationSnapshot) -> Self {
        Self {
            in_flight: value.in_flight,
            candidate_queue_depth: value.candidate_queue_depth,
            regular_queue_depth: value.regular_queue_depth,
            audit_queue_depth: value.audit_queue_depth,
            candidate_oldest_age_millis: value.candidate_oldest_age_millis,
            regular_oldest_age_millis: value.regular_oldest_age_millis,
            audit_oldest_age_millis: value.audit_oldest_age_millis,
            candidate_wait: value.candidate_wait,
            regular_wait: value.regular_wait,
            audit_wait: value.audit_wait,
            validation_duration: value.validation_duration,
            audit_duration: value.audit_duration,
            tracked_addresses: value.tracked_addresses,
            forced_verify_addresses: value.forced_verify_addresses,
            total_shares: value.total_shares,
            sampled_shares: value.sampled_shares,
            invalid_samples: value.invalid_samples,
            pending_provisional: value.pending_provisional,
            fraud_detections: value.fraud_detections,
            candidate_false_claims: value.candidate_false_claims,
            hot_accepts: value.hot_accepts,
            sync_full_verifies: value.sync_full_verifies,
            audit_enqueued: value.audit_enqueued,
            audit_verified: value.audit_verified,
            audit_rejected: value.audit_rejected,
            audit_deferred: value.audit_deferred,
            overload_mode: value.overload_mode,
            effective_sample_rate: value.effective_sample_rate,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistedSubmitSummary {
    #[serde(default)]
    pub candidate_queue_depth: usize,
    #[serde(default)]
    pub regular_queue_depth: usize,
    #[serde(default)]
    pub candidate_oldest_age_millis: Option<u64>,
    #[serde(default)]
    pub regular_oldest_age_millis: Option<u64>,
    #[serde(default)]
    pub candidate_wait: crate::telemetry::PercentileSummary,
    #[serde(default)]
    pub regular_wait: crate::telemetry::PercentileSummary,
}

impl From<SubmitRuntimeSnapshot> for PersistedSubmitSummary {
    fn from(value: SubmitRuntimeSnapshot) -> Self {
        Self {
            candidate_queue_depth: value.candidate_queue_depth,
            regular_queue_depth: value.regular_queue_depth,
            candidate_oldest_age_millis: value.candidate_oldest_age_millis,
            regular_oldest_age_millis: value.regular_oldest_age_millis,
            candidate_wait: value.candidate_wait,
            regular_wait: value.regular_wait,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistedPayoutRuntime {
    #[serde(default)]
    pub payout_interval_seconds: u64,
    #[serde(default)]
    pub maintenance_interval_seconds: u64,
    #[serde(default)]
    pub next_sweep_at: Option<SystemTime>,
    #[serde(default)]
    pub last_tick_at: Option<SystemTime>,
    #[serde(default)]
    pub reserve_target_amount: u64,
    #[serde(default)]
    pub safe_spend_budget: u64,
    #[serde(default)]
    pub spendable_output_count: usize,
    #[serde(default)]
    pub small_output_count: usize,
    #[serde(default)]
    pub medium_output_count: usize,
    #[serde(default)]
    pub large_output_count: usize,
    #[serde(default)]
    pub planned_batch_count: usize,
    #[serde(default)]
    pub planned_recipient_count: usize,
    #[serde(default)]
    pub rebalance_required: bool,
    #[serde(default)]
    pub rebalance_active: bool,
    #[serde(default)]
    pub inventory_health: String,
}

impl From<PayoutRuntimeSnapshot> for PersistedPayoutRuntime {
    fn from(value: PayoutRuntimeSnapshot) -> Self {
        Self {
            payout_interval_seconds: value.payout_interval_seconds,
            maintenance_interval_seconds: value.maintenance_interval_seconds,
            next_sweep_at: value.next_sweep_at,
            last_tick_at: value.last_tick_at,
            reserve_target_amount: value.reserve_target_amount,
            safe_spend_budget: value.safe_spend_budget,
            spendable_output_count: value.spendable_output_count,
            small_output_count: value.small_output_count,
            medium_output_count: value.medium_output_count,
            large_output_count: value.large_output_count,
            planned_batch_count: value.planned_batch_count,
            planned_recipient_count: value.planned_recipient_count,
            rebalance_required: value.rebalance_required,
            rebalance_active: value.rebalance_active,
            inventory_health: value.inventory_health,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedRuntimeSnapshot {
    pub sampled_at: SystemTime,
    #[serde(default)]
    pub total_shares_accepted: u64,
    pub connected_miners: usize,
    pub connected_workers: usize,
    pub estimated_hashrate: f64,
    #[serde(default)]
    pub last_share_at: Option<SystemTime>,
    #[serde(default)]
    pub jobs: JobRuntimeSnapshot,
    #[serde(default)]
    pub payouts: PersistedPayoutRuntime,
    #[serde(default)]
    pub submit: PersistedSubmitSummary,
    pub validation: PersistedValidationSummary,
    #[serde(default)]
    pub runtime_tasks: BTreeMap<String, TimedOperationSummary>,
}

impl PersistedRuntimeSnapshot {
    pub fn from_live(
        pool: PoolSnapshot,
        submit: SubmitRuntimeSnapshot,
        validation: ValidationSnapshot,
        jobs: JobRuntimeSnapshot,
        payouts: PayoutRuntimeSnapshot,
        runtime_tasks: BTreeMap<String, TimedOperationSummary>,
    ) -> Self {
        Self {
            sampled_at: SystemTime::now(),
            total_shares_accepted: pool.total_shares_accepted,
            connected_miners: pool.connected_miners,
            connected_workers: pool.connected_workers,
            estimated_hashrate: pool.estimated_hashrate,
            last_share_at: pool.last_share_at,
            jobs,
            payouts: payouts.into(),
            submit: submit.into(),
            validation: validation.into(),
            runtime_tasks,
        }
    }
}
