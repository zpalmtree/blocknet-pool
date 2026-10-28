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

- 2026-10-27 · T2 · replay of the production share mix (279 addresses, 47.9 shares/s, 1 h) through the real validation engine (`replay_production_share_mix`, 5x time compression, hasher modelling the bandwidth ceiling: current 5.7/s, kernel 7.7/s). Calibration: current capacity reproduces production (97% Emergency, audit wait p50 150 s, 8.9 audit drops/s, coverage 8.9%).
- 2026-10-27 · T2 · kernel alone at 1x: still 55% Emergency / 42% Shed, coverage 10.8%. Holds that audit every share of a held farm flood the lane in bursts. Not sufficient alone.
- 2026-10-27 · T2 · kernel + `hold_audit_rate` 0.25 at 1x: 100% Normal in 2/2 runs, 0 drops, audit wait p50 5 ms / p99 1.25 s, coverage 11.6-11.7%. At 2x arrivals with half the shares (vardiff_target_shares 10 → 5): 100% Normal in 2/2, coverage 11.6%. **Landed** (default 0.25). Hold rate 0.5 was bistable (5% Normal in 3 of 5 runs). Hold change without the kernel: still 94% overloaded.
- 2026-10-27 · T2 · `sample_rate` 0.08 instead of 0.05 with hold 0.25: also 100% Normal, coverage 12.4-12.6%. Not proposed; small coverage gain for less headroom.
- 2026-10-27 · T2 · surges above capacity with kernel + hold 0.25: 2x for 5 simulated minutes → 63% Normal over 40 min; 3x with half shares → 81% Normal; 0 audit drops and ~11% coverage in both. Recovery is stretched by the fixed 30 s clear-hold (2.5 simulated minutes at 5x). Open lead: overload clear logic for an async audit queue.

- 2026-10-27 · T3 · production, batch at 51c978c (kernel, hold 0.25, sync lane) deployed 08:56 UTC: 100% Normal, live-audit wait p50 1 ms, 0 drops, live-audit hash mean 1,396 → 567 ms. Regression: hot-ack p99 50 → ~215 ms, tracking 2 GiB arena discards (~20/min, r = 0.55) once verifier concurrency started fluctuating.
- 2026-10-27 · T3 · production, Stratum gateway + arena retention = verifier threads (4e9ba96) migrated 21:33 UTC (swap 0.4 s; 2,345 workers back within ~40 s): 30-min window hot-ack p50/p99 9/25 ms (baseline 22/50), 0 arena discards (was 613/30 min), 100% Normal, 0 transitions, live-audit wait p99 1 ms, 6.9 verifies/s, 0 busy replies. **Landed.**

- 2026-10-27 · T0+T1 · Cherry Ryzen 7 7700X (Zen 4 desktop like bntpool's 7900X, 2×DDR5-4800, HugeTLB): `avx512-256-stream` vs the old pin's `avx512`, ABBA at 8 concurrent: 11.81/11.86 vs 7.35/7.40 verifies/s (**+61%**), mean latency 1,084 → 676 ms. Full-size parity passed; identical digests for every kernel. Variant screen: r8d2 11.92 and avx2-stream 11.79 (within ~1%, noise), avx512-256 7.83, r64d3/d4/d6 10.6-10.7. No kernel lead above 5% remains.
- 2026-10-27 · T2 · replay with block candidates (0.05/s real, 126 per run) through the candidate lane: production config at 1x 100% Normal, 0 candidates refused, candidate p99 1.2 s. With `vardiff_target_shares` 5 and `sample_rate` 0.08 (hot-reloadable): 2x arrivals and a 2x restart storm (every address back in probation) 100% Normal in 3/3 runs, 0 transitions, 0 drops, 0 candidates refused, candidate p99 1.3-1.7 s, coverage 12.7-12.9%. `sample_rate` 0.10 shed briefly in 1 of 2 2x storms.
- 2026-10-27 · T3 · live engine handover on bntpool (same build 4e9ba96, forced): blocknet-pool-stratum@b started 22:40:27, 2,440 miners migrated by 22:40:43 with 0 replay failures and 0 busy replies, @a retired 22:41:10 (exit success). Gateway connections stayed 2,439-2,443; 30 s share buckets stayed at 846-954 with 0 rejected shares through the handover.
- 2026-10-27 · steady state after the batch and gateway (30 min, 21:34-22:04 UTC): regular validation queue avg 0 / max 0, live-audit wait p99 1 ms, hot-ack p99 25 ms.

Raw results: `perf-results/2026-10-27/pool-verify/pv-9700x5/results/` (runs.jsonl, host.txt, parity-fullsize.txt) and `perf-results/2026-10-27/pool-verify/replay/*.json`.

## Growth mode (automatic; approved 2026-10-27)

The owner first approved the load-growth config as a manual plan, then asked
for it to switch automatically. Stratum's primary engine measures the share
rate (normalised to the normal share size, 5-min EWMA, every 30 s). Above
`growth_submit_rate` (55/s) it runs with `growth_vardiff_target_shares` (5)
and `growth_sample_rate` (0.08); below `growth_exit_submit_rate` (40/s) it
returns to the configured values. Switches wait 10 minutes to settle, the mode
is persisted (meta `growth_mode_v1`) so restarts and handovers resume it, and
setting `growth_submit_rate` to 0 (by reload) turns it off. Alerts:
`BlocknetPoolGrowthModeActive`, `BlocknetPoolBeyondGrowthCapacity`.

- 2026-10-27 · T2 · load ramping 1x → 2x over 30 simulated minutes, then
  held at 2x, with the real controller in the loop and miners' share size
  following its target with a 2-minute lag: 100% Normal in 2/2 runs, 0
  transitions, 0 drops, 182/182 block candidates answered (p99 1.8 s),
  coverage 12.3-12.4%; growth mode switched on at minute 13.5 (~1.3x load).
  Control without the controller: 29% Normal, 50% Emergency, 3 drops/s.
  2x restart storm resuming growth mode: 100% Normal, 126/126 candidates.

- 2026-10-28 · T3 · growth-mode rollout (deploy-growth-20261027 = 4e9ba96 + growth mode, built without other sessions' unreleased payout/API changes) by handover 03:44 UTC: 2,544 of ~2,549 miners moved to @a in ~15 s with 0 replay failures and 0 busy replies; one miner (rejected first login, retried with a new ID) was never tracked, so @b did not retire and the deploy stopped after 10 min with both engines serving. With the owner's approval @b was stopped at 04:04; that one miner was held then reconnected. Growth mode is off at ~31 shares/s (normal size) as intended. Fixed in the gateway (master 8afeda7: pending logins tracked by ID; 3-minute drain deadline); live after the next gateway restart.

## Goal "Done" criteria and evidence (BNTPOOL_VERIFY_GOAL.md, 2026-10-27)

1. *Approved-ready batch with T2 evidence at 2x peak arrival and in a restart
   storm: zero candidate delays, zero busy/shed/timeout rejects, no Shed or
   Emergency, share-ack p99 no worse at 1x.* The batch is the deployed code
   (kernel, hold 0.25, sync lane, arena retention, gateway, hot reload) plus
   automatic growth mode (above). T2 with the deployed defaults: a 1x → 2x
   ramp and a 2x restart storm are 100% Normal with 0 transitions, 0 drops
   and 0 refused or delayed block candidates (growth-mode section). Without
   growth mode this hardware cannot hold 2x: ~11.6% coverage of ~96
   shares/s needs ~11 verifies/s against ~7.7 capacity. At 1x the current config holds (100%
   Normal in 2/2 runs incl. a restart storm, candidates p99 1.2 s).
   Share-ack p99 at 1x in production: 25 ms vs 50 ms before.
2. *Steady-state regular queue near empty.* Production 21:34-22:04 UTC:
   depth avg 0 / max 0, live-audit wait p99 1 ms (was ~75 s).
3. *Cheat-detection bounds unchanged or better.* Time to forced review is
   5 / (cheater share rate x coverage). At 1x: coverage 8.9% -> 11.6%+
   (~1.3x faster detection). At 2x with the growth config: ~78/r vs ~111/r
   for the old code at the same load (~1.4x faster).
4. *No remaining lead with a plausible gain above 5% in verifies/s.* Zen 4
   variant screen: best alternative within ~1%; more verifier threads flat
   (bandwidth-bound); arena retention and THP vs HugeTLB at most ~1.2%.

Not part of these criteria, recorded for completeness: five large farms
(old seine 0.2.21/0.2.36) left around the 08:56 Stratum restart and did not
return. Their decline started before it (0.2.21 work 968 -> 685 at 08:40;
7eseCPTm gone at 08:47), their audited shares were valid, and they made no
login attempts afterwards. The cause is on their side and is not observable
from the pool. The live gateway handover since then disconnected no one.

