/// Inclusive upper bounds of the cumulative latency histogram buckets. Dense
/// between 1 s and 4 s so a few-percent shift in Argon2 verify time is visible
/// in `histogram_quantile`, and wide enough to keep audit-backlog waits,
/// which run past a minute under overload. The blocknet daemon's
/// `/api/perf/latency` and the Stratum gateway use the same bounds.
pub const LATENCY_BUCKET_BOUNDS_MILLIS: [u64; 29] = [
    1, 2, 5, 10, 25, 50, 100, 250, 500, 750, 1_000, 1_250, 1_500, 1_750, 2_000, 2_250, 2_500,
    2_750, 3_000, 3_500, 4_000, 5_000, 7_500, 10_000, 15_000, 30_000, 60_000, 120_000, 300_000,
];
