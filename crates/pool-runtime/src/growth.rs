//! Growth mode: when the sustained share rate nears what the verifiers can
//! audit at the configured coverage, Stratum asks miners for fewer, larger
//! shares (`growth_vardiff_target_shares`) and samples at
//! `growth_sample_rate`, so verification keeps up as the pool grows. Approved
//! as the pool's load-growth plan on 2026-10-27; see VERIFY_PERFORMANCE.md.

use std::time::{Duration, Instant};

use crate::config::Config;

/// Smoothing of the measured share rate.
const RATE_TIME_CONSTANT: Duration = Duration::from_secs(5 * 60);
/// After a switch, vardiff needs minutes to move miners to the new share
/// size; decisions wait until it has.
const SETTLE: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Default)]
pub(crate) struct GrowthController {
    active: bool,
    /// Share rate at the normal share size, smoothed.
    rate: Option<f64>,
    last: Option<(Instant, u64)>,
    settle_until: Option<Instant>,
}

impl GrowthController {
    pub(crate) fn new(active: bool, now: Instant) -> Self {
        Self {
            active,
            rate: None,
            last: None,
            // A restart resumes the persisted mode; give the share rate time
            // to be measured before reconsidering it.
            settle_until: Some(now + SETTLE / 2),
        }
    }

    #[cfg(test)]
    pub(crate) fn active(&self) -> bool {
        self.active
    }

    /// Smoothed share rate at the normal share size, once measured.
    pub(crate) fn normal_size_rate(&self) -> Option<f64> {
        self.rate
    }

    /// Feeds the cumulative submit count; returns the new mode on a switch.
    pub(crate) fn observe(
        &mut self,
        now: Instant,
        total_submits: u64,
        cfg: &Config,
    ) -> Option<bool> {
        let previous = self.last.replace((now, total_submits));
        let (then, before) = previous?;
        let elapsed = now.saturating_duration_since(then).as_secs_f64();
        if elapsed <= 0.0 || total_submits < before {
            // Counter reset (new process) or no time passed: start over.
            return None;
        }
        let observed = (total_submits - before) as f64 / elapsed;
        // In growth mode each share is `normal / growth` times larger, so the
        // same hashrate would send that many times more normal-size shares.
        let normal_size = if self.active {
            observed * cfg.vardiff_target_shares.max(1) as f64
                / cfg.growth_vardiff_target_shares.max(1) as f64
        } else {
            observed
        };
        let weight = 1.0 - (-elapsed / RATE_TIME_CONSTANT.as_secs_f64()).exp();
        let rate = match self.rate {
            Some(rate) => rate + weight * (normal_size - rate),
            None => normal_size,
        };
        self.rate = Some(rate);

        if self.settle_until.is_some_and(|until| now < until) {
            return None;
        }
        let enabled = cfg.growth_submit_rate > 0.0;
        let next = if !self.active {
            enabled && rate > cfg.growth_submit_rate
        } else {
            enabled && rate >= cfg.growth_exit_submit_rate
        };
        if next == self.active {
            return None;
        }
        self.active = next;
        self.settle_until = Some(now + SETTLE);
        Some(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Config {
        Config {
            vardiff_target_shares: 10,
            growth_vardiff_target_shares: 5,
            growth_submit_rate: 55.0,
            growth_exit_submit_rate: 40.0,
            ..Config::default()
        }
    }

    /// Drives the controller with `rate` shares/s for `minutes`, observing
    /// every 30 s; returns the switches it made.
    fn drive(
        c: &mut GrowthController,
        now: &mut Instant,
        total: &mut u64,
        rate: f64,
        minutes: u64,
    ) -> Vec<bool> {
        let cfg = cfg();
        let mut switches = Vec::new();
        for _ in 0..minutes * 2 {
            *now += Duration::from_secs(30);
            *total += (rate * 30.0) as u64;
            if let Some(active) = c.observe(*now, *total, &cfg) {
                switches.push(active);
            }
        }
        switches
    }

    #[test]
    fn enters_above_the_threshold_and_leaves_below_the_exit() {
        let mut now = Instant::now();
        let mut c = GrowthController::default();
        let mut total = 0;
        assert!(drive(&mut c, &mut now, &mut total, 48.0, 60).is_empty());
        assert_eq!(drive(&mut c, &mut now, &mut total, 70.0, 30), vec![true]);
        // Miners now send half as many shares; at the normal size that is
        // still 70/s, so growth mode holds.
        assert!(drive(&mut c, &mut now, &mut total, 35.0, 60).is_empty());
        assert!(c.active());
        // Load falls to 30/s at the normal size (15/s in growth mode).
        assert_eq!(drive(&mut c, &mut now, &mut total, 15.0, 60), vec![false]);
    }

    #[test]
    fn noise_between_the_thresholds_does_not_flap() {
        let mut now = Instant::now();
        let mut c = GrowthController::default();
        let mut total = 0;
        for minute in 0..240u64 {
            let rate = if minute % 2 == 0 { 42.0 } else { 54.0 };
            assert!(drive(&mut c, &mut now, &mut total, rate, 1).is_empty());
        }
    }

    #[test]
    fn a_restart_resumes_the_persisted_mode_without_switching() {
        let mut now = Instant::now();
        let mut c = GrowthController::new(true, now);
        let mut total = 0;
        // Persisted vardiff hints keep miners on growth-size shares.
        assert!(drive(&mut c, &mut now, &mut total, 35.0, 60).is_empty());
        assert!(c.active());
        assert!((c.normal_size_rate().unwrap() - 70.0).abs() < 1.0);
    }

    #[test]
    fn disabling_it_leaves_growth_mode() {
        let mut now = Instant::now();
        let mut c = GrowthController::new(true, now);
        let mut total = 0;
        let disabled = Config {
            growth_submit_rate: 0.0,
            ..cfg()
        };
        let mut switched = None;
        for _ in 0..40 {
            now += Duration::from_secs(30);
            total += 35 * 30;
            if let Some(active) = c.observe(now, total, &disabled) {
                switched = Some(active);
            }
        }
        assert_eq!(switched, Some(false));
    }
}
