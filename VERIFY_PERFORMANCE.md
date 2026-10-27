# Verifier performance log

One line per experiment, newest first: hypothesis, tier (T0 parity, T1 kernel
micro-bench, T2 replay, T3 production), host, effect, verdict. Raw results live
under `perf-results/<date>/pool-verify/` in the workspace. Production numbers
come from `scripts/verify_perf_report.py --host bntpool`.

## Production baseline, 2026-10-27 (master ea29cf0, stratum up since 2026-10-26 03:58 UTC)

From the persisted runtime snapshot and Prometheus, before latency histograms existed:

- Load: ~51 submits/s from ~3,090 workers; 13 addresses under forced review.
- Overload: Emergency 95.5%, Shed 3.3%, Normal 1.2% of 16.9 h (stratum log transitions).
- Regular validation queue: avg 408/512 over 6 h; live-audit wait p50 90 s, p99 91 s.
- Throughput: ~5.5 audits/s (514k verified in ~26 h) with 8 regular verifiers.
- Hash time (audit lane, 8-10 concurrent): p50 1.43 s, p95 1.61 s. Sync verify p50 1.56 s.
- Drops: 803k live audits deferred (queue full) vs 514k enqueued.
- Miner-facing: submit-queue waits 0 ms p50/p99 (hot accepts answer immediately).

## Experiments
