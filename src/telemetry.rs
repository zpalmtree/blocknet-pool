use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

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
