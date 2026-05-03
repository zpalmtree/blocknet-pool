use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, Copy)]
pub struct HashrateStatsInput {
    pub total_diff: u64,
    pub count: u64,
    pub oldest: Option<SystemTime>,
    pub newest: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy)]
pub struct MinerHashrateRamp {
    pub smoothing_window: Duration,
    pub warmup_window: Duration,
    pub brand_new_min_window: Duration,
    pub now: SystemTime,
}

fn from_stats(
    total_diff: u64,
    count: u64,
    oldest: Option<SystemTime>,
    newest: Option<SystemTime>,
) -> f64 {
    if count < 2 {
        return 0.0;
    }
    let (Some(oldest), Some(newest)) = (oldest, newest) else {
        return 0.0;
    };
    let Ok(window) = newest.duration_since(oldest) else {
        return 0.0;
    };
    if window.as_secs_f64() < 1.0 {
        return 0.0;
    }
    total_diff as f64 / window.as_secs_f64()
}

pub fn from_stats_or_window_floor(
    total_diff: u64,
    count: u64,
    oldest: Option<SystemTime>,
    newest: Option<SystemTime>,
    floor_window: Duration,
) -> f64 {
    let from_stats = from_stats(total_diff, count, oldest, newest);
    if from_stats > 0.0 {
        return from_stats;
    }
    if total_diff == 0 {
        return 0.0;
    }
    let secs = floor_window.as_secs_f64().max(1.0);
    total_diff as f64 / secs
}

pub fn from_stats_with_warmup(
    total_diff: u64,
    count: u64,
    oldest: Option<SystemTime>,
    newest: Option<SystemTime>,
    smoothing_window: Duration,
    warmup_window: Duration,
) -> f64 {
    if total_diff == 0 {
        return 0.0;
    }

    let smoothing_secs = smoothing_window.as_secs_f64().max(1.0);
    let warmup_secs = warmup_window.as_secs_f64().clamp(1.0, smoothing_secs);
    let observed_secs = if count < 2 {
        0.0
    } else {
        let (Some(oldest), Some(newest)) = (oldest, newest) else {
            return total_diff as f64 / warmup_secs;
        };
        let Ok(window) = newest.duration_since(oldest) else {
            return total_diff as f64 / warmup_secs;
        };
        window.as_secs_f64()
    };

    let denominator = if observed_secs >= 1.0 {
        observed_secs.clamp(warmup_secs, smoothing_secs)
    } else {
        warmup_secs
    };
    total_diff as f64 / denominator
}

pub fn from_stats_with_miner_ramp(stats: HashrateStatsInput, ramp: MinerHashrateRamp) -> f64 {
    if stats.total_diff == 0 {
        return 0.0;
    }

    let smoothing_secs = ramp.smoothing_window.as_secs_f64().max(1.0);
    let warmup_secs = ramp.warmup_window.as_secs_f64().clamp(1.0, smoothing_secs);
    let brand_new_min_secs = ramp
        .brand_new_min_window
        .as_secs_f64()
        .clamp(1.0, warmup_secs);

    let span_with_idle_secs = match (stats.oldest, stats.newest) {
        (Some(oldest), Some(newest)) => {
            let newest_age_secs = ramp
                .now
                .duration_since(newest)
                .ok()
                .map(|age| age.as_secs_f64())
                .unwrap_or(0.0);
            let observed_secs = if stats.count < 2 {
                0.0
            } else {
                newest
                    .duration_since(oldest)
                    .ok()
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0)
            };
            if observed_secs >= 1.0 {
                observed_secs + newest_age_secs
            } else {
                newest_age_secs
            }
        }
        _ => 0.0,
    };

    let is_brand_new = stats
        .oldest
        .and_then(|first| ramp.now.duration_since(first).ok())
        .is_some_and(|age| age.as_secs_f64() <= warmup_secs);

    let min_denominator_secs = if is_brand_new {
        brand_new_min_secs
    } else {
        warmup_secs
    };
    let denominator = if span_with_idle_secs >= 1.0 {
        span_with_idle_secs.clamp(min_denominator_secs, smoothing_secs)
    } else {
        min_denominator_secs
    };
    stats.total_diff as f64 / denominator
}
