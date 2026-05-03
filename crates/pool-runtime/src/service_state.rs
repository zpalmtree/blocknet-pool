use std::collections::BTreeMap;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::jobs::JobRuntimeSnapshot;
use crate::payout::PayoutRuntimeSnapshot;
use crate::stats::PoolSnapshot;
use crate::telemetry::TimedOperationSummary;

pub use crate::stratum::SubmitRuntimeSnapshot;
pub use crate::validation::ValidationSnapshot;

pub const LIVE_RUNTIME_SNAPSHOT_META_KEY: &str = "live_runtime_snapshot_v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedRuntimeSnapshot {
    pub sampled_at: SystemTime,
    pub connected_miners: usize,
    pub connected_workers: usize,
    pub estimated_hashrate: f64,
    pub last_share_at: Option<SystemTime>,
    #[serde(default)]
    pub jobs: JobRuntimeSnapshot,
    #[serde(default)]
    pub payouts: PayoutRuntimeSnapshot,
    #[serde(default)]
    pub submit: SubmitRuntimeSnapshot,
    pub validation: ValidationSnapshot,
    #[serde(default)]
    pub runtime_tasks: BTreeMap<String, TimedOperationSummary>,
}

impl PersistedRuntimeSnapshot {
    pub(crate) fn from_live(
        pool: PoolSnapshot,
        submit: SubmitRuntimeSnapshot,
        validation: ValidationSnapshot,
        jobs: JobRuntimeSnapshot,
        payouts: PayoutRuntimeSnapshot,
        runtime_tasks: BTreeMap<String, TimedOperationSummary>,
    ) -> Self {
        Self {
            sampled_at: SystemTime::now(),
            connected_miners: pool.connected_miners,
            connected_workers: pool.connected_workers,
            estimated_hashrate: pool.estimated_hashrate,
            last_share_at: pool.last_share_at,
            jobs,
            payouts,
            submit,
            validation,
            runtime_tasks,
        }
    }
}
