use std::time::{Duration, SystemTime};

use crate::service_state::PersistedRuntimeSnapshot;

pub const POOL_ACTIVITY_LOSS_RECENT_AFTER: Duration = Duration::from_secs(10 * 60);
pub const POOL_ACTIVITY_SNAPSHOT_STALE_AFTER: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct PoolActivityAssessment {
    pub state: &'static str,
    pub detail: String,
    pub connected_miners: u64,
    pub connected_workers: u64,
    pub estimated_hashrate: f64,
    pub snapshot_age_seconds: Option<u64>,
    pub last_share_age_seconds: Option<u64>,
}

pub fn assess_pool_activity(
    now: SystemTime,
    snapshot: Option<&PersistedRuntimeSnapshot>,
    snapshot_stale_after: Duration,
) -> PoolActivityAssessment {
    let Some(snapshot) = snapshot else {
        return PoolActivityAssessment {
            state: "unknown",
            detail: "runtime snapshot unavailable".to_string(),
            connected_miners: 0,
            connected_workers: 0,
            estimated_hashrate: 0.0,
            snapshot_age_seconds: None,
            last_share_age_seconds: None,
        };
    };

    let connected_miners = snapshot.connected_miners as u64;
    let connected_workers = snapshot.connected_workers as u64;
    let estimated_hashrate = snapshot.estimated_hashrate.max(0.0);
    let snapshot_age = now.duration_since(snapshot.sampled_at).ok();
    let last_share_age = snapshot
        .last_share_at
        .and_then(|last| now.duration_since(last).ok());
    let all_zero = connected_miners == 0 && connected_workers == 0 && estimated_hashrate <= 0.0;

    let (state, detail) = if snapshot_age.is_some_and(|age| age > snapshot_stale_after) {
        (
            "stale",
            format!(
                "runtime snapshot stale (age_seconds={} miners={} workers={} hashrate={:.2})",
                snapshot_age.map(|age| age.as_secs()).unwrap_or_default(),
                connected_miners,
                connected_workers,
                estimated_hashrate,
            ),
        )
    } else if all_zero && last_share_age.is_some_and(|age| age <= POOL_ACTIVITY_LOSS_RECENT_AFTER) {
        (
            "collapsed",
            format!(
                "recently active pool dropped to zero miners/workers/hashrate (miners={} workers={} hashrate={:.2} last_share_age_seconds={})",
                connected_miners,
                connected_workers,
                estimated_hashrate,
                last_share_age.map(|age| age.as_secs()).unwrap_or_default(),
            ),
        )
    } else if all_zero {
        let age = last_share_age.map(|value| value.as_secs());
        (
            "idle",
            match age {
                Some(age) => format!(
                    "pool idle with no miners/workers/hashrate (last_share_age_seconds={age})"
                ),
                None => "pool idle with no accepted share history yet".to_string(),
            },
        )
    } else {
        (
            "active",
            format!(
                "pool activity present (miners={} workers={} hashrate={:.2} last_share_age_seconds={})",
                connected_miners,
                connected_workers,
                estimated_hashrate,
                last_share_age.map(|age| age.as_secs()).unwrap_or_default(),
            ),
        )
    };

    PoolActivityAssessment {
        state,
        detail,
        connected_miners,
        connected_workers,
        estimated_hashrate,
        snapshot_age_seconds: snapshot_age.map(|age| age.as_secs()),
        last_share_age_seconds: last_share_age.map(|age| age.as_secs()),
    }
}
