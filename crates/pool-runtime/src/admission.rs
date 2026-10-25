//! Submit admission control shared by the Stratum dispatcher and the engine.
//!
//! A submit the pool could not process (queue full, shed, rate limited,
//! verification capacity exhausted) is answered "busy/retry" without being
//! judged. Those answers are counted here by reason, and the dispatcher marks
//! submit pressure so vardiff never mistakes the pool's own overload for a
//! miner that stopped finding shares.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// How long after the last sign of regular-lane saturation vardiff keeps
/// refusing to lower anyone's difficulty.
pub(crate) const SUBMIT_PRESSURE_HOLD: Duration = Duration::from_secs(30);

/// Why a submit was answered without being judged on its proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnprocessedReason {
    /// The regular submit queue was full.
    SubmitQueueFull,
    /// The connection exceeded the per-connection submit rate limit.
    RateLimited,
    /// A share that must be verified before credit found the validation
    /// queue full.
    ValidationQueueFull,
    /// A synchronous verification did not finish in time.
    ValidationTimeout,
}

impl UnprocessedReason {
    /// The dispatcher itself is saturated (as opposed to one session or one
    /// verification path).
    pub(crate) fn is_submit_pressure(self) -> bool {
        matches!(self, Self::SubmitQueueFull)
    }
}

/// Persisted per-reason totals of unprocessed submits.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct UnprocessedSubmitSnapshot {
    #[serde(default)]
    pub submit_queue_full_total: u64,
    #[serde(default)]
    pub fair_shed_total: u64,
    #[serde(default)]
    pub rate_limited_total: u64,
    #[serde(default)]
    pub validation_queue_full_total: u64,
    #[serde(default)]
    pub sync_verify_busy_total: u64,
    #[serde(default)]
    pub validation_timeout_total: u64,
    /// Upward vardiff steps taken on unprocessed-submit evidence alone.
    #[serde(default)]
    pub vardiff_busy_raises_total: u64,
    /// Downward vardiff steps refused because of submit pressure.
    #[serde(default)]
    pub vardiff_downward_held_total: u64,
}

impl UnprocessedSubmitSnapshot {
    /// (label, total) pairs for every reason, for metrics export.
    pub fn by_reason(&self) -> [(&'static str, u64); 6] {
        [
            ("submit_queue_full", self.submit_queue_full_total),
            ("fair_shed", self.fair_shed_total),
            ("rate_limited", self.rate_limited_total),
            ("validation_queue_full", self.validation_queue_full_total),
            ("sync_verify_busy", self.sync_verify_busy_total),
            ("validation_timeout", self.validation_timeout_total),
        ]
    }
}

#[derive(Debug)]
pub(crate) struct AdmissionState {
    epoch: Instant,
    /// Millis since `epoch` of the last pressure mark, plus one (0 = never).
    last_pressure: AtomicU64,
    submit_queue_full: AtomicU64,
    fair_shed: AtomicU64,
    rate_limited: AtomicU64,
    validation_queue_full: AtomicU64,
    sync_verify_busy: AtomicU64,
    validation_timeout: AtomicU64,
    vardiff_busy_raises: AtomicU64,
    vardiff_downward_held: AtomicU64,
}

impl AdmissionState {
    pub(crate) fn new() -> Self {
        Self {
            epoch: Instant::now(),
            last_pressure: AtomicU64::new(0),
            submit_queue_full: AtomicU64::new(0),
            fair_shed: AtomicU64::new(0),
            rate_limited: AtomicU64::new(0),
            validation_queue_full: AtomicU64::new(0),
            sync_verify_busy: AtomicU64::new(0),
            validation_timeout: AtomicU64::new(0),
            vardiff_busy_raises: AtomicU64::new(0),
            vardiff_downward_held: AtomicU64::new(0),
        }
    }

    pub(crate) fn record(&self, reason: UnprocessedReason, now: Instant) {
        let counter = match reason {
            UnprocessedReason::SubmitQueueFull => &self.submit_queue_full,
            UnprocessedReason::RateLimited => &self.rate_limited,
            UnprocessedReason::ValidationQueueFull => &self.validation_queue_full,
            UnprocessedReason::ValidationTimeout => &self.validation_timeout,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if reason.is_submit_pressure() {
            self.mark_pressure(now);
        }
    }

    /// Notes that the regular submit lane is saturated or close to it.
    pub(crate) fn mark_pressure(&self, now: Instant) {
        let millis = now.saturating_duration_since(self.epoch).as_millis() as u64;
        self.last_pressure
            .fetch_max(millis.saturating_add(1), Ordering::Relaxed);
    }

    /// Whether the regular lane was saturated within [`SUBMIT_PRESSURE_HOLD`].
    pub(crate) fn pressure_active(&self, now: Instant) -> bool {
        let marked = self.last_pressure.load(Ordering::Relaxed);
        if marked == 0 {
            return false;
        }
        let at = self.epoch + Duration::from_millis(marked - 1);
        now.saturating_duration_since(at) < SUBMIT_PRESSURE_HOLD
    }

    pub(crate) fn note_vardiff_busy_raise(&self) {
        self.vardiff_busy_raises.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_vardiff_downward_held(&self) {
        self.vardiff_downward_held.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> UnprocessedSubmitSnapshot {
        UnprocessedSubmitSnapshot {
            submit_queue_full_total: self.submit_queue_full.load(Ordering::Relaxed),
            fair_shed_total: self.fair_shed.load(Ordering::Relaxed),
            rate_limited_total: self.rate_limited.load(Ordering::Relaxed),
            validation_queue_full_total: self.validation_queue_full.load(Ordering::Relaxed),
            sync_verify_busy_total: self.sync_verify_busy.load(Ordering::Relaxed),
            validation_timeout_total: self.validation_timeout.load(Ordering::Relaxed),
            vardiff_busy_raises_total: self.vardiff_busy_raises.load(Ordering::Relaxed),
            vardiff_downward_held_total: self.vardiff_downward_held.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_holds_for_a_while_after_the_last_mark() {
        let state = AdmissionState::new();
        let now = Instant::now();
        assert!(!state.pressure_active(now));
        state.record(UnprocessedReason::RateLimited, now);
        assert!(
            !state.pressure_active(now),
            "rate limits are per connection"
        );
        state.record(UnprocessedReason::SubmitQueueFull, now);
        assert!(state.pressure_active(now));
        assert!(state.pressure_active(now + SUBMIT_PRESSURE_HOLD - Duration::from_millis(5)));
        assert!(!state.pressure_active(now + SUBMIT_PRESSURE_HOLD + Duration::from_millis(5)));
        let snapshot = state.snapshot();
        assert_eq!(snapshot.submit_queue_full_total, 1);
        assert_eq!(snapshot.rate_limited_total, 1);
    }
}
