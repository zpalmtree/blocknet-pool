use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use parking_lot::RwLock;
use serde::Serialize;

const MINER_STATS_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_TRACKED_MINERS: usize = 100_000;
const MAX_RECENT_SHARES: usize = 200_000;
const MAX_RECENT_REJECTIONS: usize = 200_000;
const HASHRATE_WINDOW: Duration = Duration::from_secs(60 * 60);
const ACCEPTED_EVENTS_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const SHARE_EVENTS_PER_PRUNE_SWEEP: u64 = 1024;
const MIN_PRUNE_SWEEP_INTERVAL: Duration = Duration::from_secs(30);
const REJECTION_EVENTS_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Debug, Clone, Serialize)]
pub struct MinerStats {
    pub address: String,
    pub workers: HashSet<String>,
    pub shares_accepted: u64,
    pub shares_rejected: u64,
    pub blocks_found: u64,
    pub last_share_at: Option<SystemTime>,
}

#[derive(Debug, Clone)]
struct ShareRecord {
    miner: String,
    difficulty: u64,
    timestamp: SystemTime,
}

#[derive(Debug, Clone)]
struct RejectionRecord {
    reason: String,
    timestamp: SystemTime,
}

#[derive(Debug, Clone)]
struct ConnectedWorker {
    address: String,
    worker: String,
}

#[derive(Debug, Clone)]
pub struct PoolSnapshot {
    pub total_shares_accepted: u64,
    pub total_shares_rejected: u64,
    pub total_blocks_found: u64,
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

#[derive(Debug, Clone, Serialize)]
pub struct RejectionAnalyticsSnapshot {
    pub window_seconds: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub rejection_rate_pct: f64,
    pub by_reason: Vec<RejectionReasonCount>,
    pub totals_by_reason: Vec<RejectionReasonCount>,
    pub total_rejected: u64,
}

#[derive(Debug, Default)]
pub struct PoolStats {
    total_shares_accepted: AtomicU64,
    total_shares_rejected: AtomicU64,
    total_blocks_found: AtomicU64,

    miner_stats: RwLock<HashMap<String, MinerStats>>,
    connected_miners: RwLock<HashMap<String, ConnectedWorker>>, // conn_id -> active miner/worker
    recent_shares: RwLock<VecDeque<ShareRecord>>,
    rejection_reason_totals: RwLock<HashMap<String, u64>>,
    recent_rejections: RwLock<VecDeque<RejectionRecord>>,
    share_events_since_prune: AtomicU64,
    last_prune_sweep_at: parking_lot::Mutex<Option<Instant>>,
}

impl PoolStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_miner(&self, conn_id: &str, address: &str, worker: &str) {
        self.connected_miners.write().insert(
            conn_id.to_string(),
            ConnectedWorker {
                address: address.to_string(),
                worker: worker.to_string(),
            },
        );

        let mut miners = self.miner_stats.write();
        let entry = miners
            .entry(address.to_string())
            .or_insert_with(|| MinerStats {
                address: address.to_string(),
                workers: HashSet::new(),
                shares_accepted: 0,
                shares_rejected: 0,
                blocks_found: 0,
                last_share_at: None,
            });
        entry.workers.insert(worker.to_string());
        drop(miners);
        self.prune_miner_stats();
    }

    pub fn remove_miner(&self, conn_id: &str) {
        self.connected_miners.write().remove(conn_id);
        self.prune_miner_stats();
    }

    pub fn record_accepted_share(&self, address: &str, difficulty: u64) {
        self.total_shares_accepted.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now();

        {
            let mut miners = self.miner_stats.write();
            let entry = miners
                .entry(address.to_string())
                .or_insert_with(|| MinerStats {
                    address: address.to_string(),
                    workers: HashSet::new(),
                    shares_accepted: 0,
                    shares_rejected: 0,
                    blocks_found: 0,
                    last_share_at: None,
                });
            entry.shares_accepted = entry.shares_accepted.saturating_add(1);
            entry.last_share_at = Some(now);
        }

        let mut recent = self.recent_shares.write();
        recent.push_back(ShareRecord {
            miner: address.to_string(),
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
        drop(recent);
        self.maybe_prune_after_share_event();
    }

    pub fn record_rejected_share(&self, address: &str, reason: &str) {
        self.total_shares_rejected.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now();
        let normalized_reason = {
            let trimmed = reason.trim();
            if trimmed.is_empty() {
                "other".to_string()
            } else {
                trimmed.to_ascii_lowercase()
            }
        };

        {
            if let Some(entry) = self.miner_stats.write().get_mut(address) {
                entry.shares_rejected = entry.shares_rejected.saturating_add(1);
            }
        }

        {
            let mut totals = self.rejection_reason_totals.write();
            let entry = totals.entry(normalized_reason.clone()).or_insert(0);
            *entry = entry.saturating_add(1);
        }

        {
            let mut recent = self.recent_rejections.write();
            recent.push_back(RejectionRecord {
                reason: normalized_reason,
                timestamp: now,
            });
            let cutoff = now
                .checked_sub(REJECTION_EVENTS_RETENTION)
                .unwrap_or(SystemTime::UNIX_EPOCH);
            while recent
                .front()
                .is_some_and(|record| record.timestamp <= cutoff)
            {
                recent.pop_front();
            }
            while recent.len() > MAX_RECENT_REJECTIONS {
                recent.pop_front();
            }
        }

        self.maybe_prune_after_share_event();
    }

    pub fn record_block_found(&self, address: &str) {
        self.total_blocks_found.fetch_add(1, Ordering::Relaxed);
        if let Some(entry) = self.miner_stats.write().get_mut(address) {
            entry.blocks_found = entry.blocks_found.saturating_add(1);
        }
    }

    fn prune_miner_stats(&self) {
        let mut active_workers_by_address = HashMap::<String, HashSet<String>>::new();
        {
            let connected = self.connected_miners.read();
            for entry in connected.values() {
                active_workers_by_address
                    .entry(entry.address.clone())
                    .or_default()
                    .insert(entry.worker.clone());
            }
        }

        let now = SystemTime::now();
        let cutoff = now
            .checked_sub(MINER_STATS_RETENTION)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let mut miners = self.miner_stats.write();
        miners.retain(|address, stats| {
            let active_workers = active_workers_by_address
                .remove(address)
                .unwrap_or_default();
            let is_active = !active_workers.is_empty();
            stats.workers = active_workers;
            if is_active {
                return true;
            }
            stats.last_share_at.is_some_and(|ts| ts > cutoff)
        });

        if miners.len() <= MAX_TRACKED_MINERS {
            return;
        }

        let mut removable = miners
            .iter()
            .filter_map(|(address, stats)| {
                if !stats.workers.is_empty() {
                    return None;
                }
                Some((
                    address.clone(),
                    stats.last_share_at.unwrap_or(SystemTime::UNIX_EPOCH),
                ))
            })
            .collect::<Vec<(String, SystemTime)>>();
        removable.sort_by_key(|(_, last_seen)| *last_seen);

        let excess = miners.len().saturating_sub(MAX_TRACKED_MINERS);
        for (address, _) in removable.into_iter().take(excess) {
            miners.remove(&address);
        }
    }

    fn maybe_prune_after_share_event(&self) {
        let events = self
            .share_events_since_prune
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if events < SHARE_EVENTS_PER_PRUNE_SWEEP {
            return;
        }

        let now = Instant::now();
        {
            let mut guard = self.last_prune_sweep_at.lock();
            if guard.is_some_and(|last| now.duration_since(last) < MIN_PRUNE_SWEEP_INTERVAL) {
                return;
            }
            *guard = Some(now);
        }

        self.share_events_since_prune.store(0, Ordering::Relaxed);
        self.prune_miner_stats();
    }

    pub fn estimate_hashrate(&self) -> f64 {
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

    pub fn estimate_all_miner_hashrates(&self) -> HashMap<String, f64> {
        #[derive(Clone, Copy)]
        struct Aggregate {
            total_diff: u64,
            first: SystemTime,
            last: SystemTime,
            count: u64,
        }

        let mut aggregates = HashMap::<String, Aggregate>::new();
        let recent = self.recent_shares.read();
        let cutoff = SystemTime::now()
            .checked_sub(HASHRATE_WINDOW)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        for share in recent.iter().filter(|share| share.timestamp >= cutoff) {
            let entry = aggregates.entry(share.miner.clone()).or_insert(Aggregate {
                total_diff: 0,
                first: share.timestamp,
                last: share.timestamp,
                count: 0,
            });
            entry.total_diff = entry.total_diff.saturating_add(share.difficulty);
            entry.last = share.timestamp;
            entry.count = entry.count.saturating_add(1);
        }

        aggregates
            .into_iter()
            .map(|(miner, agg)| {
                let hashrate = if agg.count < 2 {
                    0.0
                } else {
                    agg.last
                        .duration_since(agg.first)
                        .ok()
                        .map(|window| {
                            if window.as_secs_f64() < 1.0 {
                                0.0
                            } else {
                                agg.total_diff as f64 / window.as_secs_f64()
                            }
                        })
                        .unwrap_or(0.0)
                };
                (miner, hashrate)
            })
            .collect()
    }

    pub fn connected_miner_count(&self) -> usize {
        self.connected_miners
            .read()
            .values()
            .map(|entry| entry.address.as_str())
            .collect::<HashSet<_>>()
            .len()
    }

    pub fn connected_worker_count(&self) -> usize {
        self.connected_miners
            .read()
            .values()
            .map(|entry| (entry.address.as_str(), entry.worker.as_str()))
            .collect::<HashSet<_>>()
            .len()
    }

    pub fn all_miner_stats(&self) -> HashMap<String, MinerStats> {
        self.miner_stats.read().clone()
    }

    pub fn snapshot(&self) -> PoolSnapshot {
        PoolSnapshot {
            total_shares_accepted: self.total_shares_accepted.load(Ordering::Relaxed),
            total_shares_rejected: self.total_shares_rejected.load(Ordering::Relaxed),
            total_blocks_found: self.total_blocks_found.load(Ordering::Relaxed),
            connected_miners: self.connected_miner_count(),
            connected_workers: self.connected_worker_count(),
            estimated_hashrate: self.estimate_hashrate(),
            last_share_at: self
                .recent_shares
                .read()
                .back()
                .map(|share| share.timestamp),
        }
    }

    pub fn rejection_analytics(&self, window: Duration) -> RejectionAnalyticsSnapshot {
        let cutoff = SystemTime::now()
            .checked_sub(window)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let accepted = {
            let recent = self.recent_shares.read();
            recent
                .iter()
                .filter(|share| share.timestamp >= cutoff)
                .count() as u64
        };

        let (rejected, by_reason) = {
            let recent = self.recent_rejections.read();
            let mut counts: HashMap<String, u64> = HashMap::new();
            let mut total = 0u64;
            for record in recent.iter().filter(|record| record.timestamp >= cutoff) {
                total = total.saturating_add(1);
                let entry = counts.entry(record.reason.clone()).or_insert(0);
                *entry = entry.saturating_add(1);
            }
            (total, sort_reason_counts(counts))
        };

        let totals_by_reason = {
            let totals = self.rejection_reason_totals.read();
            sort_reason_counts(totals.clone())
        };

        let denom = accepted.saturating_add(rejected);
        let rejection_rate_pct = if denom == 0 {
            0.0
        } else {
            (rejected as f64 / denom as f64) * 100.0
        };

        RejectionAnalyticsSnapshot {
            window_seconds: window.as_secs(),
            accepted,
            rejected,
            rejection_rate_pct,
            by_reason,
            totals_by_reason,
            total_rejected: self.total_shares_rejected.load(Ordering::Relaxed),
        }
    }
}

fn sort_reason_counts(counts: HashMap<String, u64>) -> Vec<RejectionReasonCount> {
    let mut items = counts
        .into_iter()
        .map(|(reason, count)| RejectionReasonCount { reason, count })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.reason.cmp(&b.reason)));
    items
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{PoolStats, MAX_RECENT_SHARES};

    #[test]
    fn records_shares_and_estimates_hashrate() {
        let stats = PoolStats::new();
        stats.add_miner("c1", "addr1", "rig1");
        stats.record_accepted_share("addr1", 10);
        stats.record_accepted_share("addr1", 20);

        assert_eq!(stats.connected_miner_count(), 1);
        assert_eq!(stats.connected_worker_count(), 1);
        assert!(stats.snapshot().total_shares_accepted >= 2);
    }

    #[test]
    fn connected_counts_track_active_connections() {
        let stats = PoolStats::new();
        stats.add_miner("c1", "addr1", "rig1");
        stats.add_miner("c2", "addr1", "rig1");
        stats.add_miner("c3", "addr2", "rig9");

        assert_eq!(stats.connected_miner_count(), 2);
        assert_eq!(stats.connected_worker_count(), 2);

        stats.remove_miner("c1");
        assert_eq!(stats.connected_miner_count(), 2);
        assert_eq!(stats.connected_worker_count(), 2);

        stats.remove_miner("c2");
        assert_eq!(stats.connected_miner_count(), 1);
        assert_eq!(stats.connected_worker_count(), 1);

        stats.remove_miner("c3");
        assert_eq!(stats.connected_miner_count(), 0);
        assert_eq!(stats.connected_worker_count(), 0);
    }

    #[test]
    fn all_miner_stats_only_reports_currently_connected_workers() {
        let stats = PoolStats::new();
        stats.add_miner("c1", "addr1", "rig-old");
        stats.add_miner("c2", "addr1", "rig-new");

        let mut miner_stats = stats.all_miner_stats();
        let workers = miner_stats.remove("addr1").expect("addr1 stats").workers;
        assert_eq!(
            workers,
            HashSet::from(["rig-old".to_string(), "rig-new".to_string(),])
        );

        stats.remove_miner("c1");

        let mut miner_stats = stats.all_miner_stats();
        let workers = miner_stats.remove("addr1").expect("addr1 stats").workers;
        assert_eq!(workers, HashSet::from(["rig-new".to_string()]));
    }

    #[test]
    fn accepted_share_history_is_count_bounded() {
        let stats = PoolStats::new();
        stats.add_miner("c1", "addr1", "rig1");

        for _ in 0..(MAX_RECENT_SHARES + 1_000) {
            stats.record_accepted_share("addr1", 1);
        }

        assert!(stats.recent_shares.read().len() <= MAX_RECENT_SHARES);
    }
}
