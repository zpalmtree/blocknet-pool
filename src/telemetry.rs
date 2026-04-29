use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

const DEFAULT_SAMPLE_LIMIT: usize = 512;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct PercentileSummary {
    #[serde(default)]
    pub samples: usize,
    #[serde(default)]
    pub p50_millis: Option<u64>,
    #[serde(default)]
    pub p95_millis: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TimedOperationSummary {
    #[serde(default)]
    pub count: u64,
    #[serde(default)]
    pub error_count: u64,
    #[serde(default)]
    pub slow_count: u64,
    #[serde(default)]
    pub total_millis: u64,
    #[serde(default)]
    pub max_millis: u64,
    #[serde(default)]
    pub duration: PercentileSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CacheCounterSummary {
    #[serde(default)]
    pub hits: u64,
    #[serde(default)]
    pub misses: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ApiPerformanceSnapshot {
    pub sampled_at: Option<SystemTime>,
    #[serde(default)]
    pub routes: BTreeMap<String, TimedOperationSummary>,
    #[serde(default)]
    pub operations: BTreeMap<String, TimedOperationSummary>,
    #[serde(default)]
    pub tasks: BTreeMap<String, TimedOperationSummary>,
    #[serde(default)]
    pub caches: BTreeMap<String, CacheCounterSummary>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct QueuePressureSnapshot {
    #[serde(default)]
    pub depth: usize,
    #[serde(default)]
    pub oldest_age_millis: Option<u64>,
    #[serde(default)]
    pub wait: PercentileSummary,
}

#[derive(Debug, Default)]
pub struct LatencyWindow {
    limit: usize,
    samples: VecDeque<u64>,
}

impl LatencyWindow {
    pub fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            samples: VecDeque::with_capacity(limit.max(1)),
        }
    }

    pub fn record(&mut self, duration: Duration) {
        let millis = duration.as_millis().min(u64::MAX as u128) as u64;
        self.samples.push_back(millis);
        while self.samples.len() > self.limit {
            self.samples.pop_front();
        }
    }

    pub fn snapshot(&self) -> PercentileSummary {
        percentile_summary(&self.samples)
    }
}

#[derive(Debug, Default)]
pub struct TimedOperationWindow {
    count: u64,
    error_count: u64,
    slow_count: u64,
    total_millis: u64,
    max_millis: u64,
    durations: LatencyWindow,
}

impl TimedOperationWindow {
    pub fn new(sample_limit: usize) -> Self {
        Self {
            durations: LatencyWindow::new(sample_limit),
            ..Self::default()
        }
    }

    pub fn record(&mut self, duration: Duration, failed: bool, slow: bool) {
        let millis = duration.as_millis().min(u64::MAX as u128) as u64;
        self.count = self.count.saturating_add(1);
        if failed {
            self.error_count = self.error_count.saturating_add(1);
        }
        if slow {
            self.slow_count = self.slow_count.saturating_add(1);
        }
        self.total_millis = self.total_millis.saturating_add(millis);
        self.max_millis = self.max_millis.max(millis);
        self.durations.record(duration);
    }

    pub fn snapshot(&self) -> TimedOperationSummary {
        TimedOperationSummary {
            count: self.count,
            error_count: self.error_count,
            slow_count: self.slow_count,
            total_millis: self.total_millis,
            max_millis: self.max_millis,
            duration: self.durations.snapshot(),
        }
    }
}

#[derive(Debug, Default)]
struct CacheCounterWindow {
    hits: u64,
    misses: u64,
}

#[derive(Debug, Default)]
pub struct NamedTimedOperationTracker {
    entries: Mutex<HashMap<String, TimedOperationWindow>>,
}

impl NamedTimedOperationTracker {
    pub fn record(&self, name: &str, duration: Duration, failed: bool, slow: bool) {
        let mut entries = self.entries.lock();
        let entry = entries
            .entry(name.to_string())
            .or_insert_with(default_timed_operation_window);
        entry.record(duration, failed, slow);
    }

    pub fn snapshot(&self) -> BTreeMap<String, TimedOperationSummary> {
        self.entries
            .lock()
            .iter()
            .map(|(name, entry)| (name.clone(), entry.snapshot()))
            .collect()
    }
}

#[derive(Debug, Default)]
pub struct NamedCacheCounterTracker {
    entries: Mutex<HashMap<String, CacheCounterWindow>>,
}

impl NamedCacheCounterTracker {
    pub fn record_hit(&self, name: &str) {
        let mut entries = self.entries.lock();
        let entry = entries
            .entry(name.to_string())
            .or_insert_with(CacheCounterWindow::default);
        entry.hits = entry.hits.saturating_add(1);
    }

    pub fn record_miss(&self, name: &str) {
        let mut entries = self.entries.lock();
        let entry = entries
            .entry(name.to_string())
            .or_insert_with(CacheCounterWindow::default);
        entry.misses = entry.misses.saturating_add(1);
    }

    pub fn snapshot(&self) -> BTreeMap<String, CacheCounterSummary> {
        self.entries
            .lock()
            .iter()
            .map(|(name, entry)| {
                (
                    name.clone(),
                    CacheCounterSummary {
                        hits: entry.hits,
                        misses: entry.misses,
                    },
                )
            })
            .collect()
    }
}

#[derive(Debug, Default)]
pub struct QueueTracker {
    next_id: AtomicU64,
    queued_at: Mutex<VecDeque<(u64, Instant)>>,
    waits: Mutex<LatencyWindow>,
}

impl QueueTracker {
    pub fn new(sample_limit: usize) -> Self {
        Self {
            next_id: AtomicU64::new(1),
            queued_at: Mutex::new(VecDeque::new()),
            waits: Mutex::new(LatencyWindow::new(sample_limit.max(1))),
        }
    }

    pub fn push(&self, queued_at: Instant) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.queued_at.lock().push_back((id, queued_at));
        id
    }

    pub fn remove(&self, id: u64) {
        let mut queued_at = self.queued_at.lock();
        let Some(position) = queued_at.iter().position(|(queued_id, _)| *queued_id == id) else {
            return;
        };
        queued_at.remove(position);
    }

    pub fn pop_and_record_wait(&self, started_at: Instant) {
        let queued_at = self.queued_at.lock().pop_front();
        if let Some((_, queued_at)) = queued_at {
            self.waits
                .lock()
                .record(started_at.saturating_duration_since(queued_at));
        }
    }

    pub fn snapshot(&self, now: Instant) -> QueuePressureSnapshot {
        let queued_at = self.queued_at.lock();
        let oldest_age_millis = queued_at
            .front()
            .map(|(_, queued_at)| now.saturating_duration_since(*queued_at).as_millis())
            .map(|millis| millis.min(u64::MAX as u128) as u64);
        QueuePressureSnapshot {
            depth: queued_at.len(),
            oldest_age_millis,
            wait: self.waits.lock().snapshot(),
        }
    }
}

pub fn default_latency_window() -> LatencyWindow {
    LatencyWindow::new(DEFAULT_SAMPLE_LIMIT)
}

fn default_timed_operation_window() -> TimedOperationWindow {
    TimedOperationWindow::new(DEFAULT_SAMPLE_LIMIT)
}

fn percentile_summary(samples: &VecDeque<u64>) -> PercentileSummary {
    let count = samples.len();
    if count == 0 {
        return PercentileSummary::default();
    }

    let mut sorted = samples.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    let p50_idx = percentile_index(count, 50);
    let p95_idx = percentile_index(count, 95);
    PercentileSummary {
        samples: count,
        p50_millis: sorted.get(p50_idx).copied(),
        p95_millis: sorted.get(p95_idx).copied(),
    }
}

fn percentile_index(count: usize, percentile: usize) -> usize {
    if count <= 1 {
        return 0;
    }
    ((count - 1) * percentile) / 100
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timed_operation_window_tracks_counts_and_latency() {
        let mut window = TimedOperationWindow::new(8);
        window.record(Duration::from_millis(10), false, false);
        window.record(Duration::from_millis(25), true, true);

        let snapshot = window.snapshot();
        assert_eq!(snapshot.count, 2);
        assert_eq!(snapshot.error_count, 1);
        assert_eq!(snapshot.slow_count, 1);
        assert_eq!(snapshot.total_millis, 35);
        assert_eq!(snapshot.max_millis, 25);
        assert_eq!(snapshot.duration.samples, 2);
        assert_eq!(snapshot.duration.p50_millis, Some(10));
        assert_eq!(snapshot.duration.p95_millis, Some(10));
    }

    #[test]
    fn named_cache_counter_tracker_tracks_hits_and_misses() {
        let tracker = NamedCacheCounterTracker::default();
        tracker.record_hit("stats");
        tracker.record_hit("stats");
        tracker.record_miss("stats");

        let snapshot = tracker.snapshot();
        let stats = snapshot.get("stats").expect("stats cache summary");
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
    }
}
