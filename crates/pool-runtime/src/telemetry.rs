use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

const DEFAULT_SAMPLE_LIMIT: usize = 512;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct PercentileSummary {
    pub p50_millis: Option<u64>,
    pub p95_millis: Option<u64>,
    #[serde(default)]
    pub p99_millis: Option<u64>,
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
    #[serde(default)]
    pub success_duration: PercentileSummary,
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
    #[serde(default)]
    pub database_reads: BTreeMap<String, TimedOperationSummary>,
}

pub use pool_common::latency::LATENCY_BUCKET_BOUNDS_MILLIS;

/// Process-lifetime latency histogram. Unlike the rolling percentile windows,
/// its counts only grow, so Prometheus can compute quantiles over any range
/// and compare before/after a change.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct LatencyHistogramSnapshot {
    pub bounds_millis: Vec<u64>,
    /// Per-bucket (not cumulative) counts; the extra last entry is `+Inf`.
    pub counts: Vec<u64>,
    pub sum_micros: u64,
}

impl LatencyHistogramSnapshot {
    pub fn count(&self) -> u64 {
        self.counts.iter().sum()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LatencyHistogram {
    counts: [u64; LATENCY_BUCKET_BOUNDS_MILLIS.len() + 1],
    sum_micros: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            counts: [0; LATENCY_BUCKET_BOUNDS_MILLIS.len() + 1],
            sum_micros: 0,
        }
    }
}

impl LatencyHistogram {
    pub(crate) fn record(&mut self, duration: Duration) {
        let micros = duration.as_micros().min(u64::MAX as u128) as u64;
        let idx = LATENCY_BUCKET_BOUNDS_MILLIS
            .partition_point(|bound| bound.saturating_mul(1_000) < micros);
        self.counts[idx] = self.counts[idx].saturating_add(1);
        self.sum_micros = self.sum_micros.saturating_add(micros);
    }

    pub(crate) fn snapshot(&self) -> LatencyHistogramSnapshot {
        LatencyHistogramSnapshot {
            bounds_millis: LATENCY_BUCKET_BOUNDS_MILLIS.to_vec(),
            counts: self.counts.to_vec(),
            sum_micros: self.sum_micros,
        }
    }
}

/// Latency histograms keyed by lane, e.g. `candidate` or `regular_sync`.
#[derive(Debug, Default)]
pub struct NamedLatencyHistograms {
    entries: Mutex<BTreeMap<&'static str, LatencyHistogram>>,
}

impl NamedLatencyHistograms {
    pub fn record(&self, lane: &'static str, duration: Duration) {
        self.entries
            .lock()
            .entry(lane)
            .or_default()
            .record(duration);
    }

    pub fn snapshot(&self) -> BTreeMap<String, LatencyHistogramSnapshot> {
        self.entries
            .lock()
            .iter()
            .map(|(lane, histogram)| (lane.to_string(), histogram.snapshot()))
            .collect()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub(crate) struct QueuePressureSnapshot {
    #[serde(default)]
    pub depth: usize,
    pub oldest_age_millis: Option<u64>,
    #[serde(default)]
    pub wait: PercentileSummary,
}

#[derive(Debug, Default)]
pub(crate) struct LatencyWindow {
    limit: usize,
    samples: VecDeque<u64>,
}

impl LatencyWindow {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            samples: VecDeque::with_capacity(limit.max(1)),
        }
    }

    pub(crate) fn record(&mut self, duration: Duration) {
        let millis = duration.as_millis().min(u64::MAX as u128) as u64;
        self.samples.push_back(millis);
        while self.samples.len() > self.limit {
            self.samples.pop_front();
        }
    }

    pub(crate) fn snapshot(&self) -> PercentileSummary {
        percentile_summary(&self.samples)
    }
}

#[derive(Debug, Default)]
struct TimedOperationWindow {
    count: u64,
    error_count: u64,
    slow_count: u64,
    total_millis: u64,
    max_millis: u64,
    durations: LatencyWindow,
    success_durations: LatencyWindow,
}

impl TimedOperationWindow {
    fn new(sample_limit: usize) -> Self {
        Self {
            durations: LatencyWindow::new(sample_limit),
            success_durations: LatencyWindow::new(sample_limit),
            ..Self::default()
        }
    }

    fn record(&mut self, duration: Duration, failed: bool, slow: bool) {
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
        if !failed {
            self.success_durations.record(duration);
        }
    }

    fn snapshot(&self) -> TimedOperationSummary {
        TimedOperationSummary {
            count: self.count,
            error_count: self.error_count,
            slow_count: self.slow_count,
            total_millis: self.total_millis,
            max_millis: self.max_millis,
            duration: self.durations.snapshot(),
            success_duration: self.success_durations.snapshot(),
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
            .or_insert_with(|| TimedOperationWindow::new(DEFAULT_SAMPLE_LIMIT));
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
        let entry = entries.entry(name.to_string()).or_default();
        entry.hits = entry.hits.saturating_add(1);
    }

    pub fn record_miss(&self, name: &str) {
        let mut entries = self.entries.lock();
        let entry = entries.entry(name.to_string()).or_default();
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
pub(crate) struct QueueTracker {
    next_id: AtomicU64,
    queued_at: Mutex<VecDeque<(u64, Instant)>>,
    waits: Mutex<LatencyWindow>,
    wait_histogram: Mutex<LatencyHistogram>,
}

impl QueueTracker {
    pub(crate) fn new(sample_limit: usize) -> Self {
        Self {
            next_id: AtomicU64::new(1),
            queued_at: Mutex::new(VecDeque::new()),
            waits: Mutex::new(LatencyWindow::new(sample_limit.max(1))),
            wait_histogram: Mutex::new(LatencyHistogram::default()),
        }
    }

    pub(crate) fn push(&self, queued_at: Instant) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.queued_at.lock().push_back((id, queued_at));
        id
    }

    pub(crate) fn remove(&self, id: u64) {
        let mut queued_at = self.queued_at.lock();
        let Some(position) = queued_at.iter().position(|(queued_id, _)| *queued_id == id) else {
            return;
        };
        queued_at.remove(position);
    }

    pub(crate) fn pop_and_record_wait(&self, started_at: Instant) {
        let queued_at = self.queued_at.lock().pop_front();
        if let Some((_, queued_at)) = queued_at {
            let wait = started_at.saturating_duration_since(queued_at);
            self.waits.lock().record(wait);
            self.wait_histogram.lock().record(wait);
        }
    }

    pub(crate) fn wait_histogram(&self) -> LatencyHistogramSnapshot {
        self.wait_histogram.lock().snapshot()
    }

    pub(crate) fn snapshot(&self, now: Instant) -> QueuePressureSnapshot {
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

pub(crate) fn default_latency_window() -> LatencyWindow {
    LatencyWindow::new(DEFAULT_SAMPLE_LIMIT)
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
        p50_millis: sorted.get(p50_idx).copied(),
        p95_millis: sorted.get(p95_idx).copied(),
        p99_millis: sorted.get(percentile_index(count, 99)).copied(),
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
    fn latency_histogram_buckets_are_inclusive_upper_bounds() {
        let mut histogram = LatencyHistogram::default();
        histogram.record(Duration::from_micros(0));
        histogram.record(Duration::from_millis(1));
        histogram.record(Duration::from_micros(1_001));
        histogram.record(Duration::from_millis(1_900));
        histogram.record(Duration::from_secs(600));
        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.counts.len(), snapshot.bounds_millis.len() + 1);
        assert_eq!(snapshot.counts[0], 2, "0 ms and exactly 1 ms land in le=1");
        assert_eq!(snapshot.counts[1], 1, "1.001 ms lands in le=2");
        let le_2000 = snapshot
            .bounds_millis
            .iter()
            .position(|bound| *bound == 2_000)
            .expect("2 s bound");
        assert_eq!(snapshot.counts[le_2000], 1);
        assert_eq!(*snapshot.counts.last().expect("+Inf"), 1);
        assert_eq!(snapshot.count(), 5);
        assert_eq!(snapshot.sum_micros, 1_000 + 1_001 + 1_900_000 + 600_000_000);
    }

    #[test]
    fn success_percentiles_exclude_fast_rejections_and_keep_tail_latency() {
        let mut window = TimedOperationWindow::new(512);
        for millis in 1..=100 {
            window.record(Duration::from_millis(millis), false, false);
        }
        for _ in 0..512 {
            window.record(Duration::ZERO, true, false);
        }
        let snapshot = window.snapshot();
        assert_eq!(snapshot.duration.p99_millis, Some(0));
        assert_eq!(snapshot.success_duration.p99_millis, Some(99));
        assert_eq!(snapshot.error_count, 512);
        let old: TimedOperationSummary =
            serde_json::from_str(r#"{"duration":{"p50_millis":1,"p95_millis":2}}"#).unwrap();
        assert_eq!(old.duration.p99_millis, None);
        assert_eq!(old.success_duration.p99_millis, None);
    }

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
