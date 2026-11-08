//! Growth mode: when the sustained share rate nears what the verifiers can
//! audit at the configured coverage, Stratum asks miners for fewer, larger
//! shares and samples at `growth_sample_rate`, so verification keeps up as
//! the pool grows. Level 1 uses `growth_vardiff_target_shares`; level 2, above
//! `growth2_submit_rate`, uses `growth2_vardiff_target_shares`; level 3, above
//! `growth3_submit_rate`, uses `growth3_vardiff_target_shares`. Approved as
//! the pool's load-growth plan on 2026-10-27 (level 2 on 2026-10-29, level 3
//! on 2026-11-05); see VERIFY_PERFORMANCE.md.

use std::time::{Duration, Instant};

use crate::config::Config;

/// Smoothing of the measured share rate.
const RATE_TIME_CONSTANT: Duration = Duration::from_secs(5 * 60);
/// After a switch, vardiff needs minutes to move miners to the new share
/// size; decisions wait until it has.
const SETTLE: Duration = Duration::from_secs(10 * 60);
/// A step down needs the rate below the level's exit for this long without a
/// break. Outages and mass reconnects cut the share rate for a while even
/// though the miners are all still there, and stepping down then floods the
/// pool with small shares when they recover. Stepping up stays immediate.
const STEP_DOWN_HOLD: Duration = Duration::from_secs(2 * 60 * 60);

#[derive(Debug, Default)]
pub(crate) struct GrowthController {
    level: u8,
    /// Share rate at the normal share size, smoothed.
    rate: Option<f64>,
    last: Option<(Instant, u64)>,
    settle_until: Option<Instant>,
    /// Since when the rate has been below the current level's exit.
    below_exit_since: Option<Instant>,
}

/// Shares per vardiff window that miners are asked for at `level`.
fn target_shares(cfg: &Config, level: u8) -> f64 {
    cfg.with_growth_level(level).vardiff_target_shares.max(1) as f64
}

impl GrowthController {
    pub(crate) fn new(level: u8, now: Instant) -> Self {
        Self {
            level,
            rate: None,
            last: None,
            // A restart resumes the persisted level; give the share rate time
            // to be measured before reconsidering it.
            settle_until: Some(now + SETTLE / 2),
            below_exit_since: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn level(&self) -> u8 {
        self.level
    }

    /// Smoothed share rate at the normal share size, once measured.
    pub(crate) fn normal_size_rate(&self) -> Option<f64> {
        self.rate
    }

    /// Feeds the cumulative submit count; returns the new level on a switch.
    /// Levels change one step at a time.
    pub(crate) fn observe(&mut self, now: Instant, total_submits: u64, cfg: &Config) -> Option<u8> {
        let previous = self.last.replace((now, total_submits));
        let (then, before) = previous?;
        let elapsed = now.saturating_duration_since(then).as_secs_f64();
        if elapsed <= 0.0 || total_submits < before {
            // Counter reset (new process) or no time passed: start over.
            return None;
        }
        let observed = (total_submits - before) as f64 / elapsed;
        // At a growth level each share is `normal / level` times larger, so
        // the same hashrate would send that many times more normal-size shares.
        let normal_size = observed * target_shares(cfg, 0) / target_shares(cfg, self.level);
        let weight = 1.0 - (-elapsed / RATE_TIME_CONSTANT.as_secs_f64()).exp();
        let rate = match self.rate {
            Some(rate) => rate + weight * (normal_size - rate),
            None => normal_size,
        };
        self.rate = Some(rate);

        if self.settle_until.is_some_and(|until| now < until) {
            return None;
        }
        let max = cfg.max_growth_level();
        let next = if self.level > max {
            max
        } else {
            let up = match self.level {
                0 => max >= 1 && rate > cfg.growth_submit_rate,
                1 => max >= 2 && rate > cfg.growth2_submit_rate,
                2 => max >= 3 && rate > cfg.growth3_submit_rate,
                _ => false,
            };
            let below_exit = match self.level {
                1 => rate < cfg.growth_exit_submit_rate,
                2 => rate < cfg.growth2_exit_submit_rate,
                3 => rate < cfg.growth3_exit_submit_rate,
                _ => false,
            };
            if up {
                self.level + 1
            } else if below_exit {
                let since = *self.below_exit_since.get_or_insert(now);
                if now.saturating_duration_since(since) >= STEP_DOWN_HOLD {
                    self.level - 1
                } else {
                    self.level
                }
            } else {
                self.below_exit_since = None;
                self.level
            }
        };
        if next == self.level {
            return None;
        }
        self.level = next;
        self.below_exit_since = None;
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
            growth2_vardiff_target_shares: 3,
            growth2_submit_rate: 110.0,
            growth2_exit_submit_rate: 80.0,
            growth3_vardiff_target_shares: 2,
            growth3_submit_rate: 150.0,
            growth3_exit_submit_rate: 120.0,
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
    ) -> Vec<u8> {
        drive_with(c, now, total, rate, minutes, &cfg())
    }

    fn drive_with(
        c: &mut GrowthController,
        now: &mut Instant,
        total: &mut u64,
        rate: f64,
        minutes: u64,
        cfg: &Config,
    ) -> Vec<u8> {
        let mut switches = Vec::new();
        for _ in 0..minutes * 2 {
            *now += Duration::from_secs(30);
            *total += (rate * 30.0) as u64;
            if let Some(level) = c.observe(*now, *total, cfg) {
                switches.push(level);
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
        // Ten minutes: the switch's settle time keeps the controller at level 1.
        assert_eq!(drive(&mut c, &mut now, &mut total, 70.0, 10), vec![1]);
        // Miners now send half as many shares; at the normal size that is
        // still 70/s, so growth mode holds.
        assert!(drive(&mut c, &mut now, &mut total, 35.0, 60).is_empty());
        assert_eq!(c.level(), 1);
        // Load falls to 30/s at the normal size (15/s in growth mode).
        // It must stay there for two hours before growth mode is left.
        assert!(drive(&mut c, &mut now, &mut total, 15.0, 60).is_empty());
        assert_eq!(drive(&mut c, &mut now, &mut total, 15.0, 70), vec![0]);
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
        let mut c = GrowthController::new(1, now);
        let mut total = 0;
        // Persisted vardiff hints keep miners on growth-size shares.
        assert!(drive(&mut c, &mut now, &mut total, 35.0, 60).is_empty());
        assert_eq!(c.level(), 1);
        assert!((c.normal_size_rate().unwrap() - 70.0).abs() < 1.0);
    }

    #[test]
    fn disabling_it_leaves_growth_mode() {
        let mut now = Instant::now();
        let mut c = GrowthController::new(2, now);
        let mut total = 0;
        let disabled = Config {
            growth_submit_rate: 0.0,
            ..cfg()
        };
        let mut switched = None;
        for _ in 0..40 {
            now += Duration::from_secs(30);
            total += 35 * 30;
            if let Some(level) = c.observe(now, total, &disabled) {
                switched = Some(level);
            }
        }
        assert_eq!(switched, Some(0));
    }

    #[test]
    fn climbs_to_the_second_level_one_step_at_a_time_and_back() {
        let mut now = Instant::now();
        let mut c = GrowthController::default();
        let mut total = 0;
        // Level 3 off: 150/s is its trigger, and the old share size driven
        // for a while after a switch reads as a higher normal-size rate.
        let two_levels = Config {
            growth3_submit_rate: 0.0,
            ..cfg()
        };
        // 150/s at the normal size: level 1 first, level 2 after it settles.
        assert_eq!(
            drive_with(&mut c, &mut now, &mut total, 150.0, 5, &two_levels),
            vec![1]
        );
        assert_eq!(
            drive_with(&mut c, &mut now, &mut total, 75.0, 30, &two_levels),
            vec![2]
        );
        // Level 2 asks for 3 shares instead of 10: 45/s is still 150/s.
        assert!(drive_with(&mut c, &mut now, &mut total, 45.0, 60, &two_levels).is_empty());
        assert!((c.normal_size_rate().unwrap() - 150.0).abs() < 2.0);
        // 60/s at the normal size: down to level 1, then held there.
        assert_eq!(
            drive_with(&mut c, &mut now, &mut total, 18.0, 135, &two_levels),
            vec![1]
        );
        assert!(drive_with(&mut c, &mut now, &mut total, 30.0, 60, &two_levels).is_empty());
        assert_eq!(c.level(), 1);
    }

    #[test]
    fn disabling_the_second_level_steps_down_to_the_first() {
        let mut now = Instant::now();
        let mut c = GrowthController::new(2, now);
        let mut total = 0;
        let one_level = Config {
            growth2_submit_rate: 0.0,
            ..cfg()
        };
        let mut switched = None;
        for _ in 0..40 {
            now += Duration::from_secs(30);
            total += 45 * 30;
            if let Some(level) = c.observe(now, total, &one_level) {
                switched = Some(level);
            }
        }
        assert_eq!(switched, Some(1));
    }

    #[test]
    fn climbs_to_the_third_level_and_back() {
        let mut now = Instant::now();
        let mut c = GrowthController::new(2, now);
        let mut total = 0;
        // 180/s at the normal size is 54/s at level 2 (3 shares instead of 10).
        assert_eq!(drive(&mut c, &mut now, &mut total, 54.0, 15), vec![3]);
        // Level 3 asks for 2 shares: 36/s is still 180/s.
        assert!(drive(&mut c, &mut now, &mut total, 36.0, 60).is_empty());
        assert!((c.normal_size_rate().unwrap() - 180.0).abs() < 2.0);
        // 130/s sits between level 3's exit and its trigger: no change.
        assert!(drive(&mut c, &mut now, &mut total, 26.0, 60).is_empty());
        assert_eq!(c.level(), 3);
        // 100/s at the normal size: down to level 2, then held there.
        assert_eq!(drive(&mut c, &mut now, &mut total, 20.0, 135), vec![2]);
        assert!(drive(&mut c, &mut now, &mut total, 30.0, 60).is_empty());
        assert_eq!(c.level(), 2);
    }

    #[test]
    fn a_dip_shorter_than_the_hold_keeps_the_level() {
        let mut now = Instant::now();
        let mut c = GrowthController::new(3, now);
        let mut total = 0;
        // 180/s at the normal size (36/s at level 3), then an hour-long
        // outage at 50/s, recovery, and another hour-long dip: never two
        // hours below the exit in a row.
        assert!(drive(&mut c, &mut now, &mut total, 36.0, 30).is_empty());
        assert!(drive(&mut c, &mut now, &mut total, 10.0, 100).is_empty());
        assert!(drive(&mut c, &mut now, &mut total, 36.0, 20).is_empty());
        assert!(drive(&mut c, &mut now, &mut total, 10.0, 100).is_empty());
        assert_eq!(c.level(), 3);
    }

    #[test]
    fn disabling_the_third_level_steps_down_to_the_second() {
        let mut now = Instant::now();
        let mut c = GrowthController::new(3, now);
        let mut total = 0;
        let two_levels = Config {
            growth3_submit_rate: 0.0,
            ..cfg()
        };
        let mut switched = None;
        for _ in 0..40 {
            now += Duration::from_secs(30);
            total += 36 * 30;
            if let Some(level) = c.observe(now, total, &two_levels) {
                switched = Some(level);
            }
        }
        assert_eq!(switched, Some(2));
    }
}
