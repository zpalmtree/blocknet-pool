use std::collections::BTreeMap;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::jobs::JobRuntimeSnapshot;
use crate::payout::PayoutRuntimeSnapshot;
use crate::stats::PoolSnapshot;
use crate::telemetry::{LatencyHistogramSnapshot, TimedOperationSummary};

pub use crate::stratum::{ConnectionSnapshot, SubmitRuntimeSnapshot};
pub use crate::validation::ValidationSnapshot;

pub const LIVE_RUNTIME_SNAPSHOT_META_KEY: &str = "live_runtime_snapshot_v1";

/// Whether this Stratum process owns pool-wide singleton work: payout ticks,
/// the live runtime snapshot, stat snapshots, retention and seen-share GC.
/// A process serving miners directly is always primary. Behind the gateway
/// (STRATUM_GATEWAY.md) the gateway promotes the newest engine and demotes
/// the one it replaces, so two engines never both run singleton work for
/// longer than an in-flight tick.
#[derive(Debug)]
pub struct EngineRole {
    primary: std::sync::atomic::AtomicBool,
    changed: tokio::sync::Notify,
}

impl EngineRole {
    pub fn new(primary: bool) -> Self {
        Self {
            primary: std::sync::atomic::AtomicBool::new(primary),
            changed: tokio::sync::Notify::new(),
        }
    }

    pub fn is_primary(&self) -> bool {
        self.primary.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn set_primary(&self, primary: bool) {
        let previous = self
            .primary
            .swap(primary, std::sync::atomic::Ordering::SeqCst);
        if previous != primary {
            tracing::info!(primary, "engine role changed");
            self.changed.notify_waiters();
        }
    }

    /// Resolves once this process is primary.
    pub async fn wait_primary(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_primary() {
                return;
            }
            changed.await;
        }
    }
}

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
    /// Cumulative latency histograms by metric family (e.g. `submit_ack`),
    /// then lane.
    #[serde(default)]
    pub latency: BTreeMap<String, BTreeMap<String, LatencyHistogramSnapshot>>,
}

impl PersistedRuntimeSnapshot {
    pub(crate) fn from_live(
        pool: PoolSnapshot,
        submit: SubmitRuntimeSnapshot,
        validation: ValidationSnapshot,
        jobs: JobRuntimeSnapshot,
        payouts: PayoutRuntimeSnapshot,
        runtime_tasks: BTreeMap<String, TimedOperationSummary>,
        latency: BTreeMap<String, BTreeMap<String, LatencyHistogramSnapshot>>,
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
            latency,
        }
    }
}
