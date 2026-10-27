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

- 2026-10-27 · T0+T1 · Cherry Ryzen 7 9700X (Zen 5 desktop, 2×DDR5-5600, 2 MiB HugeTLB arenas) · seine v0.2.39 `avx512-256-stream` vs pool's April pin (seine 8f9b5a9, `avx512`): +34% verifies/s and -26% latency at 8 concurrent (ABBA 7.46/7.46 vs 10.07/9.99 /s; 1,072 vs ~797 ms mean); +34-36% at 4-16 concurrent. Full-size parity vs RustCrypto argon2 passed; identical output digests. **Landed.**
- 2026-10-27 · T1 · same host · attribution: the new kernel crate forced to `avx512` = 7.40 /s, so the gain is the streaming kernel, not the crate bump.
- 2026-10-27 · T1 · same host · concurrency curve: throughput flat from ~6 concurrent hashes (stream 9.90 → 10.37 /s at 6 → 16) while latency grows linearly; more verifier threads add latency, not throughput. **Rejected** as a lever.
- 2026-10-27 · T1 · same host · arena retention 4 vs 8 at 8 concurrent: 10.11 vs 10.07 /s, no effect at steady load (production churn is ~0.4% of hashes). **Dropped.**
- 2026-10-27 · T1 · same host · THP-hinted vs explicit HugeTLB arenas: -1.2% (stream), -0.5% (avx512) on a fresh host. Low value; enlarging the bntpool hugepage pool stays optional.
- 2026-10-27 · unit test · sync verifies served ahead of queued live audits (they previously queued behind ~77 s of audits and hit the 10 s validation timeout). **Landed**; T2 replay not run (the load generator's claimed hashes fail real Argon2 audits), so production confirmation comes from the `sync` lane of `validation_queue_wait`.

Raw results: `perf-results/2026-10-27/pool-verify/pv-9700x5/results/` (runs.jsonl, host.txt, parity-fullsize.txt).

