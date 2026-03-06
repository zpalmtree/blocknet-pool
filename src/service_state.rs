use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::jobs::JobRuntimeSnapshot;
use crate::stats::PoolSnapshot;
use crate::validation::ValidationSnapshot;

pub const LIVE_RUNTIME_SNAPSHOT_META_KEY: &str = "live_runtime_snapshot_v1";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistedValidationSummary {
    pub in_flight: i64,
    pub candidate_queue_depth: usize,
    pub regular_queue_depth: usize,
    pub tracked_addresses: usize,
    pub forced_verify_addresses: usize,
    pub total_shares: u64,
    pub sampled_shares: u64,
    pub invalid_samples: u64,
    pub pending_provisional: u64,
    pub fraud_detections: u64,
}

impl From<ValidationSnapshot> for PersistedValidationSummary {
    fn from(value: ValidationSnapshot) -> Self {
        Self {
            in_flight: value.in_flight,
            candidate_queue_depth: value.candidate_queue_depth,
            regular_queue_depth: value.regular_queue_depth,
            tracked_addresses: value.tracked_addresses,
            forced_verify_addresses: value.forced_verify_addresses,
            total_shares: value.total_shares,
            sampled_shares: value.sampled_shares,
            invalid_samples: value.invalid_samples,
            pending_provisional: value.pending_provisional,
            fraud_detections: value.fraud_detections,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedRuntimeSnapshot {
    pub sampled_at: SystemTime,
    pub connected_miners: usize,
    pub connected_workers: usize,
    pub estimated_hashrate: f64,
    #[serde(default)]
    pub jobs: JobRuntimeSnapshot,
    pub validation: PersistedValidationSummary,
}

impl PersistedRuntimeSnapshot {
    pub fn from_live(
        pool: PoolSnapshot,
        validation: ValidationSnapshot,
        jobs: JobRuntimeSnapshot,
    ) -> Self {
        Self {
            sampled_at: SystemTime::now(),
            connected_miners: pool.connected_miners,
            connected_workers: pool.connected_workers,
            estimated_hashrate: pool.estimated_hashrate,
            jobs,
            validation: validation.into(),
        }
    }
}
