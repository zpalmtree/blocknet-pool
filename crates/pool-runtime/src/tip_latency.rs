//! Timing for the path from a new chain tip to the next job being published
//! to miners: how the pool learned of the tip (its own accepted block, the
//! daemon's `new_block` event, or a status poll), the template fetch, and the
//! install. Recorded as the `tip_refresh` latency family.

use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::telemetry::{LatencyHistogramSnapshot, NamedLatencyHistograms};

/// Tip refreshes slower than this are logged with their fetch history.
const SLOW_TIP_REFRESH: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TipTrigger {
    /// The daemon accepted a block this pool submitted.
    OwnBlock,
    /// The daemon's `new_block` SSE event.
    Sse,
    /// The periodic daemon status poll saw a different tip.
    Poll,
}

impl TipTrigger {
    fn fetch_start_lane(self) -> &'static str {
        match self {
            Self::OwnBlock => "own_block_to_fetch_start",
            Self::Sse => "sse_to_fetch_start",
            Self::Poll => "poll_to_fetch_start",
        }
    }

    fn published_lane(self) -> &'static str {
        match self {
            Self::OwnBlock => "own_block_to_job_published",
            Self::Sse => "sse_to_job_published",
            Self::Poll => "poll_to_job_published",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct TipObservation {
    trigger: TipTrigger,
    /// Height of the new tip block.
    height: Option<u64>,
    observed_at: Instant,
    /// Daemon wall-clock acceptance time, when the daemon reported it.
    daemon_accepted_at: Option<SystemTime>,
    fetch_started: bool,
    /// Template fetches since the observation, and how many of them came
    /// back below the new tip (the daemon was not serving it yet).
    fetches: u32,
    stale_fetches: u32,
    /// When the daemon's SSE event for this tip arrived, if it was not the
    /// first observation (e.g. after our own block's submit returned).
    sse_after: Option<Duration>,
    /// Whether a refresh was requested for the tip, and whether that request
    /// found another refresh in flight and was folded into it.
    refresh_requested: bool,
    refresh_absorbed: bool,
}

#[derive(Debug, Default)]
struct TipLatencyState {
    pending: Option<TipObservation>,
    /// Height of the last published job; a tip at or above this height-1 has
    /// already been served and must not start a new measurement.
    last_published_job_height: u64,
}

#[derive(Debug, Default)]
pub(crate) struct TipLatency {
    state: Mutex<TipLatencyState>,
    histograms: NamedLatencyHistograms,
}

impl TipLatency {
    /// Notes that a new tip at `height` was seen. The earliest observation of
    /// a tip wins, so an own-block acceptance is not overwritten by the SSE
    /// event the same block produces moments later (which only fills in the
    /// daemon's acceptance time).
    pub(crate) fn observe(
        &self,
        trigger: TipTrigger,
        height: Option<u64>,
        daemon_accepted_at: Option<SystemTime>,
        now: Instant,
    ) {
        let mut state = self.state.lock();
        if let Some(height) = height {
            if height.saturating_add(1) <= state.last_published_job_height {
                return;
            }
        }
        if let Some(pending) = state.pending.as_mut() {
            let newer = match (pending.height, height) {
                (Some(pending_height), Some(height)) => height > pending_height,
                _ => false,
            };
            if !newer {
                if pending.daemon_accepted_at.is_none() {
                    pending.daemon_accepted_at = daemon_accepted_at;
                }
                if trigger == TipTrigger::Sse && pending.sse_after.is_none() {
                    pending.sse_after = Some(now.saturating_duration_since(pending.observed_at));
                }
                return;
            }
        }
        state.pending = Some(TipObservation {
            trigger,
            height,
            observed_at: now,
            daemon_accepted_at,
            fetch_started: false,
            fetches: 0,
            stale_fetches: 0,
            sse_after: None,
            refresh_requested: false,
            refresh_absorbed: false,
        });
    }

    /// Daemon acceptance → event read by the pool. Both clocks are wall
    /// clocks, normally on the same host; a negative skew is dropped.
    pub(crate) fn record_sse_delivery(&self, daemon_accepted_at: SystemTime, now: SystemTime) {
        if let Ok(elapsed) = now.duration_since(daemon_accepted_at) {
            self.histograms.record("sse_delivery", elapsed);
        }
    }

    /// A template fetch is starting; the first one after a tip observation
    /// measures how long the refresh waited (e.g. behind one in flight).
    pub(crate) fn fetch_started(&self, now: Instant) {
        let mut state = self.state.lock();
        if let Some(pending) = state.pending.as_mut() {
            if !pending.fetch_started {
                pending.fetch_started = true;
                let lane = pending.trigger.fetch_start_lane();
                let waited = now.saturating_duration_since(pending.observed_at);
                drop(state);
                self.histograms.record(lane, waited);
            }
        }
    }

    /// A template refresh was requested for the pending tip.
    pub(crate) fn refresh_requested(&self) {
        if let Some(pending) = self.state.lock().pending.as_mut() {
            pending.refresh_requested = true;
        }
    }

    /// A refresh request found another refresh in flight and was folded
    /// into it.
    pub(crate) fn refresh_absorbed(&self) {
        if let Some(pending) = self.state.lock().pending.as_mut() {
            pending.refresh_absorbed = true;
        }
    }

    /// A template fetch finished with a job at `job_height` (None: failed).
    /// Counts fetches toward the pending tip, and those still below it.
    pub(crate) fn fetch_finished(&self, job_height: Option<u64>) {
        let mut state = self.state.lock();
        if let Some(pending) = state.pending.as_mut() {
            pending.fetches = pending.fetches.saturating_add(1);
            let stale = match (job_height, pending.height) {
                (Some(job_height), Some(tip)) => job_height <= tip,
                (None, _) => true,
                _ => false,
            };
            if stale {
                pending.stale_fetches = pending.stale_fetches.saturating_add(1);
            }
        }
    }

    pub(crate) fn record_template_fetch(&self, elapsed: Duration, success: bool) {
        let lane = if success {
            "template_fetch"
        } else {
            "template_fetch_failed"
        };
        self.histograms.record(lane, elapsed);
    }

    pub(crate) fn record_template_install(&self, elapsed: Duration) {
        self.histograms.record("template_install", elapsed);
    }

    /// A job for `job_height` was handed to the Stratum fan-out. Completes the
    /// pending tip measurement when this job builds on that tip.
    pub(crate) fn job_published(&self, job_height: u64, now: Instant, wall_now: SystemTime) {
        let mut state = self.state.lock();
        state.last_published_job_height = state.last_published_job_height.max(job_height);
        let completes = state
            .pending
            .is_some_and(|pending| pending.height.is_none_or(|height| job_height > height));
        if !completes {
            return;
        }
        let Some(pending) = state.pending.take() else {
            return;
        };
        drop(state);
        let elapsed = now.saturating_duration_since(pending.observed_at);
        self.histograms
            .record(pending.trigger.published_lane(), elapsed);
        if elapsed >= SLOW_TIP_REFRESH {
            tracing::info!(
                trigger = pending.trigger.published_lane(),
                tip_height = pending.height,
                job_height,
                elapsed_ms = elapsed.as_millis() as u64,
                fetch_started = pending.fetch_started,
                fetches = pending.fetches,
                stale_fetches = pending.stale_fetches,
                sse_after_ms = pending.sse_after.map(|after| after.as_millis() as u64),
                refresh_requested = pending.refresh_requested,
                refresh_absorbed = pending.refresh_absorbed,
                "slow tip refresh"
            );
        }
        if let Some(accepted_at) = pending.daemon_accepted_at {
            if let Ok(elapsed) = wall_now.duration_since(accepted_at) {
                self.histograms
                    .record("daemon_accept_to_job_published", elapsed);
            }
        }
    }

    pub(crate) fn snapshot(&self) -> BTreeMap<String, LatencyHistogramSnapshot> {
        self.histograms.snapshot()
    }
}

/// Converts the daemon's `*_unix_us` SSE fields to a wall-clock time.
pub(crate) fn system_time_from_unix_micros(micros: u64) -> Option<SystemTime> {
    UNIX_EPOCH.checked_add(Duration::from_micros(micros))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(latency: &TipLatency, lane: &str) -> u64 {
        latency
            .snapshot()
            .get(lane)
            .map(LatencyHistogramSnapshot::count)
            .unwrap_or(0)
    }

    #[test]
    fn fetches_below_the_new_tip_are_counted_as_stale() {
        let latency = TipLatency::default();
        let start = Instant::now();
        latency.observe(TipTrigger::Sse, Some(100), None, start);
        latency.fetch_finished(Some(100));
        latency.fetch_finished(None);
        latency.fetch_finished(Some(101));
        {
            let state = latency.state.lock();
            let pending = state.pending.expect("pending tip");
            assert_eq!((pending.fetches, pending.stale_fetches), (3, 2));
        }
        latency.job_published(101, start + Duration::from_secs(1), SystemTime::now());
        assert_eq!(count(&latency, "sse_to_job_published"), 1);
        assert!(latency.state.lock().pending.is_none());
    }

    #[test]
    fn own_block_observation_is_kept_when_the_sse_event_follows() {
        let latency = TipLatency::default();
        let start = Instant::now();
        let accepted = UNIX_EPOCH + Duration::from_secs(1_000);
        latency.observe(TipTrigger::OwnBlock, Some(100), None, start);
        latency.observe(
            TipTrigger::Sse,
            Some(100),
            Some(accepted),
            start + Duration::from_millis(5),
        );
        latency.fetch_started(start + Duration::from_millis(6));
        latency.job_published(
            101,
            start + Duration::from_millis(40),
            accepted + Duration::from_millis(38),
        );

        assert_eq!(count(&latency, "own_block_to_fetch_start"), 1);
        assert_eq!(count(&latency, "own_block_to_job_published"), 1);
        assert_eq!(count(&latency, "sse_to_job_published"), 0);
        assert_eq!(count(&latency, "daemon_accept_to_job_published"), 1);
    }

    #[test]
    fn same_tip_rotation_does_not_complete_or_restart_a_measurement() {
        let latency = TipLatency::default();
        let start = Instant::now();
        latency.observe(TipTrigger::Sse, Some(100), None, start);
        // A mempool rotation for the old tip's job height does not complete it.
        latency.job_published(100, start + Duration::from_millis(1), SystemTime::now());
        assert_eq!(count(&latency, "sse_to_job_published"), 0);
        latency.job_published(101, start + Duration::from_millis(9), SystemTime::now());
        assert_eq!(count(&latency, "sse_to_job_published"), 1);

        // A duplicate event for the served tip must not open a new measurement.
        latency.observe(
            TipTrigger::Poll,
            Some(100),
            None,
            start + Duration::from_secs(1),
        );
        latency.job_published(101, start + Duration::from_secs(2), SystemTime::now());
        assert_eq!(count(&latency, "poll_to_job_published"), 0);
    }

    #[test]
    fn a_newer_tip_replaces_the_pending_observation() {
        let latency = TipLatency::default();
        let start = Instant::now();
        latency.observe(TipTrigger::Poll, Some(100), None, start);
        latency.observe(
            TipTrigger::Sse,
            Some(101),
            None,
            start + Duration::from_millis(3),
        );
        latency.job_published(102, start + Duration::from_millis(10), SystemTime::now());
        assert_eq!(count(&latency, "poll_to_job_published"), 0);
        assert_eq!(count(&latency, "sse_to_job_published"), 1);
    }

    #[test]
    fn sse_delivery_drops_negative_clock_skew() {
        let latency = TipLatency::default();
        let accepted = UNIX_EPOCH + Duration::from_secs(1_000);
        latency.record_sse_delivery(accepted, accepted + Duration::from_micros(400));
        latency.record_sse_delivery(accepted, accepted - Duration::from_millis(1));
        assert_eq!(count(&latency, "sse_delivery"), 1);
    }
}
