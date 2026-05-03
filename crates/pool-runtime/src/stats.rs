use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;
use serde::Serialize;

const MAX_RECENT_SHARES: usize = 200_000;
const HASHRATE_WINDOW: Duration = Duration::from_secs(60 * 60);
const ACCEPTED_EVENTS_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Debug, Clone)]
struct ShareRecord {
    difficulty: u64,
    timestamp: SystemTime,
}

#[derive(Debug, Clone)]
struct ConnectedWorker {
    address: String,
    worker: String,
}

#[derive(Debug, Clone)]
pub(crate) struct PoolSnapshot {
    pub connected_miners: usize,
    pub connected_workers: usize,
    pub estimated_hashrate: f64,
    pub last_share_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RejectionReasonCount {
    pub reason: String,
    pub count: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RejectionAnalyticsSnapshot {
    pub accepted: u64,
    pub rejected: u64,
    pub by_reason: Vec<RejectionReasonCount>,
    pub totals_by_reason: Vec<RejectionReasonCount>,
}

#[derive(Debug, Default)]
pub(crate) struct PoolStats {
    connected_miners: RwLock<HashMap<String, ConnectedWorker>>, // conn_id -> active miner/worker
    recent_shares: RwLock<VecDeque<ShareRecord>>,
}

impl PoolStats {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn add_miner(&self, conn_id: &str, address: &str, worker: &str) {
        self.connected_miners.write().insert(
            conn_id.to_string(),
            ConnectedWorker {
                address: address.to_string(),
                worker: worker.to_string(),
            },
        );
    }

    pub(crate) fn remove_miner(&self, conn_id: &str) {
        self.connected_miners.write().remove(conn_id);
    }

    pub(crate) fn record_accepted_share(&self, difficulty: u64) {
        let now = SystemTime::now();

        let mut recent = self.recent_shares.write();
        recent.push_back(ShareRecord {
            difficulty,
            timestamp: now,
        });

        let cutoff = now
            .checked_sub(ACCEPTED_EVENTS_RETENTION)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        while recent
            .front()
            .is_some_and(|share| share.timestamp <= cutoff)
        {
            recent.pop_front();
        }
        while recent.len() > MAX_RECENT_SHARES {
            recent.pop_front();
        }
    }

    fn estimate_hashrate(&self) -> f64 {
        let recent = self.recent_shares.read();
        let cutoff = SystemTime::now()
            .checked_sub(HASHRATE_WINDOW)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let mut oldest = None;
        let mut newest = None;
        let mut total_diff = 0u64;
        let mut count = 0usize;

        for share in recent.iter().filter(|share| share.timestamp >= cutoff) {
            oldest.get_or_insert(share.timestamp);
            newest = Some(share.timestamp);
            total_diff = total_diff.saturating_add(share.difficulty);
            count += 1;
        }

        if count < 2 {
            return 0.0;
        }

        let oldest = oldest.unwrap_or(SystemTime::UNIX_EPOCH);
        let newest = newest.unwrap_or(SystemTime::UNIX_EPOCH);
        let Ok(window) = newest.duration_since(oldest) else {
            return 0.0;
        };
        if window.as_secs_f64() < 1.0 {
            return 0.0;
        }
        total_diff as f64 / window.as_secs_f64()
    }

    pub(crate) fn snapshot(&self) -> PoolSnapshot {
        let connected = self.connected_miners.read();
        let connected_miners = connected
            .values()
            .map(|entry| entry.address.as_str())
            .collect::<HashSet<_>>()
            .len();
        let connected_workers = connected
            .values()
            .map(|entry| (entry.address.as_str(), entry.worker.as_str()))
            .collect::<HashSet<_>>()
            .len();
        drop(connected);

        PoolSnapshot {
            connected_miners,
            connected_workers,
            estimated_hashrate: self.estimate_hashrate(),
            last_share_at: self
                .recent_shares
                .read()
                .back()
                .map(|share| share.timestamp),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PoolStats, MAX_RECENT_SHARES};

    #[test]
    fn records_shares_and_connected_counts() {
        let stats = PoolStats::new();
        stats.add_miner("c1", "addr1", "rig1");
        stats.record_accepted_share(10);
        stats.record_accepted_share(20);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.connected_miners, 1);
        assert_eq!(snapshot.connected_workers, 1);
        assert_eq!(stats.recent_shares.read().len(), 2);
    }

    #[test]
    fn connected_counts_track_active_connections() {
        let stats = PoolStats::new();
        stats.add_miner("c1", "addr1", "rig1");
        stats.add_miner("c2", "addr1", "rig1");
        stats.add_miner("c3", "addr2", "rig9");

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.connected_miners, 2);
        assert_eq!(snapshot.connected_workers, 2);

        stats.remove_miner("c1");
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.connected_miners, 2);
        assert_eq!(snapshot.connected_workers, 2);

        stats.remove_miner("c2");
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.connected_miners, 1);
        assert_eq!(snapshot.connected_workers, 1);

        stats.remove_miner("c3");
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.connected_miners, 0);
        assert_eq!(snapshot.connected_workers, 0);
    }

    #[test]
    fn accepted_share_history_is_count_bounded() {
        let stats = PoolStats::new();
        stats.add_miner("c1", "addr1", "rig1");

        for _ in 0..(MAX_RECENT_SHARES + 1_000) {
            stats.record_accepted_share(1);
        }

        assert!(stats.recent_shares.read().len() <= MAX_RECENT_SHARES);
    }
}
