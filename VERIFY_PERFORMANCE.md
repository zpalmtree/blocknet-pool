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

- 2026-10-27 · T3 · production, batch at 098e6eb (kernel, hold 0.25, sync lane) deployed 08:56 UTC: 100% Normal, live-audit wait p50 1 ms, 0 drops, live-audit hash mean 1,396 → 567 ms. Regression: hot-ack p99 50 → ~215 ms, tracking 2 GiB arena discards (~20/min, r = 0.55) once verifier concurrency started fluctuating.
- 2026-10-27 · T3 · production, Stratum gateway + arena retention = verifier threads (836f030) migrated 21:33 UTC (swap 0.4 s; 2,345 workers back within ~40 s): 30-min window hot-ack p50/p99 9/25 ms (baseline 22/50), 0 arena discards (was 613/30 min), 100% Normal, 0 transitions, live-audit wait p99 1 ms, 6.9 verifies/s, 0 busy replies. **Landed.**

- 2026-10-27 · T0+T1 · Cherry Ryzen 7 7700X (Zen 4 desktop like bntpool's 7900X, 2×DDR5-4800, HugeTLB): `avx512-256-stream` vs the old pin's `avx512`, ABBA at 8 concurrent: 11.81/11.86 vs 7.35/7.40 verifies/s (**+61%**), mean latency 1,084 → 676 ms. Full-size parity passed; identical digests for every kernel. Variant screen: r8d2 11.92 and avx2-stream 11.79 (within ~1%, noise), avx512-256 7.83, r64d3/d4/d6 10.6-10.7. No kernel lead above 5% remains.
- 2026-10-27 · T2 · replay with block candidates (0.05/s real, 126 per run) through the candidate lane: production config at 1x 100% Normal, 0 candidates refused, candidate p99 1.2 s. With `vardiff_target_shares` 5 and `sample_rate` 0.08 (hot-reloadable): 2x arrivals and a 2x restart storm (every address back in probation) 100% Normal in 3/3 runs, 0 transitions, 0 drops, 0 candidates refused, candidate p99 1.3-1.7 s, coverage 12.7-12.9%. `sample_rate` 0.10 shed briefly in 1 of 2 2x storms.
- 2026-10-27 · T3 · live engine handover on bntpool (same build 836f030, forced): blocknet-pool-stratum@b started 22:40:27, 2,440 miners migrated by 22:40:43 with 0 replay failures and 0 busy replies, @a retired 22:41:10 (exit success). Gateway connections stayed 2,439-2,443; 30 s share buckets stayed at 846-954 with 0 rejected shares through the handover.
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

- 2026-10-28 · T3 · growth-mode rollout (deploy-growth-20261027 = 836f030 + growth mode, built without other sessions' unreleased payout/API changes) by handover 03:44 UTC: 2,544 of ~2,549 miners moved to @a in ~15 s with 0 replay failures and 0 busy replies; one miner (rejected first login, retried with a new ID) was never tracked, so @b did not retire and the deploy stopped after 10 min with both engines serving. With the owner's approval @b was stopped at 04:04; that one miner was held then reconnected. Growth mode is off at ~31 shares/s (normal size) as intended. Fixed in the gateway (master 5836425: pending logins tracked by ID; 3-minute drain deadline); live after the next gateway restart.

- 2026-10-28 · T3 · live growth-mode test (owner-approved, config reload only): growth_submit_rate lowered to 20/s below the live ~31/s at 04:12:07 UTC; the controller switched on at 04:12:32 (vardiff_target_shares 5, sample_rate 0.08, persisted). Submits fell 31.8 → ~13/s within ~4 min as vardiff enlarged shares while its normal-size rate held 28-34/s (no flapping); 100% Normal; 2,302-2,311 workers throughout. Slow acks unchanged in absolute terms (10 min: >50 ms 132 vs 132, >100 ms 29 vs 77, >250 ms 0 vs 13, mean 11.1 vs 10.9 ms); p99 rose to 60-90 ms only because there were half as many acks. Config restored at 04:29:07; growth mode switched off at 04:29:32 and submits returned to 33.9/s.

- 2026-10-28 · T2 · the 2x replays re-run on the exact deployed revision a644249 (branch deploy-growth-20261027, matching /opt/blocknet/blocknet-pool/deploy-info.txt), deployed defaults: 1x → 2x ramp 100% Normal in 2/2 runs (0 transitions, 0 drops, 182/182 candidates answered, p99 1.3-1.4 s, growth on at minute 13.5); 2x restart storm resuming growth mode 100% Normal (126/126 candidates, p99 1.7 s). Control without growth mode: 31% Normal, 55% Emergency, 2.6 drops/s. Raw: perf-results/2026-10-27/pool-verify/replay-deployed-a644249/.

- 2026-10-28 · T3 · gateway handover fix made live (owner-approved): gateway built from branch deploy-gateway-fix-20261028 (deploy-growth-20261027 + the gateway fix only), uploaded alone and restarted 04:53:30 UTC. Engine @a reconnected within 1 s; 2,280 miners back within 5 s, all on the engine by 04:53:45, 2,403 connections by 04:54:30 (2,428 before); 0 busy replies; connected workers 2,311 vs 2,325 before. The live pool now runs everything built and tested: Stratum/monitor a644249, gateway with pending-login tracking and the 3-minute drain deadline.

- 2026-10-28 · T2 · candidate queue wait measured directly (replay on a644249 with a test-only `queue_wait_real_ms` output; histogram buckets, so values are upper bounds): 1x baseline p50 ≤5 ms, p99 ≤0.5 s; 1x → 2x ramp p50 ≤5 ms, p99 ≤1.25 s; 2x restart storm p50 ≤5 ms, p99 ≤1.25 s. All 100% Normal, 0 drops, 0 candidates refused. Regular audits never queue ahead of a candidate; the tail is one candidate waiting for another, because there is one candidate worker and the replay injects 0.05 candidates/s (one per 20 s, far above production) so ~3% land within one ~0.65 s hash of the previous one. At 2x the wait is longer because each hash is slower under memory contention. A second candidate worker would remove the tail, but at production's candidate rate it affects well under 1% of blocks. Raw: perf-results/2026-10-27/pool-verify/replay-candidate-wait-a644249/.

- 2026-10-28 · T0/T1 · seine v0.2.44 `avx512-256-stream-l2` (far L2 prefetch, Zen 4 only) vs the live v0.2.39 `avx512-256-stream`, rented EPYC 9554P (no 2-channel Zen 4 in stock). T0: full parity suite incl. the production-profile Argon2 reference passes on Zen 4; digests identical at 8 and 48 threads. T1 ABBA, mean time per hash (three runs per kernel, non-overlapping): 8 threads 516 → 511 ms (+1.0%), 11 threads flat, 32 threads 614 → 593 (+3.6%), 48 threads 667 → 632 (+5.6%), 64 threads 689 → 651 (+5.8%). The gain only appears once memory bandwidth is the limit; 48-64 threads on 12 channels matches production's 8 verifiers on 2 channels (production hashes take ~2x a lone hash), so expect ~+5% verifies/s on the 7900X, in line with Seine's +5.0% on a 7700X. Short-window verifies/s were too noisy to read (±2%). Zen 5 keeps the plain streaming kernel (Seine: -1-3%). Raw: perf-results/2026-10-27/pool-verify/pv-9554p-t8/, pv-9554p-wide/.

- 2026-10-28 · T3 · far-L2-prefetch kernel rolled out (owner-approved) by handover 05:39:48 UTC from branch deploy-kernel-l2-20261028 (deployed a644249 + the live gateway fix + the kernel change only): 2,462 miners on the gateway, 2,476 migrations, 0 replay failures, 0 busy replies, 0 forced reconnects; @a retired 05:40:31; active engine now @b. 30 min before (ending 05:39) vs 30 min after (ending 06:11), similar load (audits 6.8 vs 7.2/s, 100% Normal both): live-audit hash mean 621 → 550 ms (-11%), p50 632 → 539; background-audit mean 555 → 478 ms (-14%); share-ack p99 25 ms both; regular queue depth 0. More than the rental's ~5.7%, so part of it may be load mix; Prometheus keeps the series for a longer comparison. One block per window, so the candidate ack/block-submit numbers (1.2 vs 1.7 s, single samples) carry no signal.

- 2026-10-28 · T2 · candidate queue wait with realistic candidate spacing (`VERIFY_REPLAY_CANDIDATE_MIN_GAP_S=2`; real candidates for distinct heights are ~5 min apart): W1 1x, W2 1x → 2x ramp and W3 2x restart storm, deployed defaults: every candidate (114 / 165 / 114) waited ≤5 ms (the histogram's smallest bucket), 0 refused, 100% Normal, 0 drops. The ≤1.25 s tail in the pure-Poisson runs above is therefore only candidates queued behind each other, never behind regular work. Raw: perf-results/2026-10-27/pool-verify/replay-candidate-gap-4093070/ (pre-rewrite tip; same content as 6a2e325).

- 2026-10-28 · T2 · two candidate verifiers (CANDIDATE_VALIDATION_WORKERS 1 → 2, arena retained), replays exactly as first run (pure Poisson candidates, no spacing): every candidate waited ≤5 ms (smallest bucket) in W1 1x (126), W2 1x → 2x ramp (182) and W3 2x restart storm (126); 0 refused; 100% Normal; 0 drops; total candidate p99 1.17 / 1.17 / 1.43 s (W3 was 1.63 s with one worker). Raw: perf-results/2026-10-27/pool-verify/replay-two-candidate-workers/.

## Goal "Done" criteria and evidence (BNTPOOL_VERIFY_GOAL.md, 2026-10-27)

1. *Approved-ready batch with T2 evidence at 2x peak arrival and in a restart
   storm: zero candidate delays, zero busy/shed/timeout rejects, no Shed or
   Emergency, share-ack p99 no worse at 1x.* The batch is the deployed code
   (kernel, hold 0.25, sync lane, arena retention, gateway, hot reload) plus
   automatic growth mode (above). T2 runs exist for both the master revision
   (replay-growth/: master at the time plus growth mode, a644249) and the
   exact deployed revision (replay-deployed-a644249/); results match. With the
   deployed defaults: a 1x → 2x
   ramp and a 2x restart storm are 100% Normal with 0 transitions, 0 drops
   and 0 refused block candidates (growth-mode section). With two candidate
   verifiers every candidate waited ≤5 ms under the original pure-Poisson
   replay (2026-10-28 entry); with one, candidates arriving within one hash
   of each other queued up to ~1.25 s. Without
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
   (bandwidth-bound); arena retention and THP vs HugeTLB at most ~1.2%. Seine
   v0.2.44's Zen 4 far-L2 prefetch (+~5.7% per hash when bandwidth-bound) is
   live since 2026-10-28 05:39 UTC (live hash means -11 to -14%).

Batch approval: the whole batch (both kernel upgrades, hold_audit_rate 0.25,
sync lane, per-thread arena retention, hot reload, Stratum gateway with the
retried-login fix, automatic growth mode) was presented against these
criteria and approved by the owner as bntpool's production setup on
2026-10-28 (06:27 UTC). Candidate spacing in the replay (2 s minimum gap)
is a modelling choice made after the first runs; it is recorded beside the
pure-Poisson results, which show the only waits are candidate-on-candidate.

Not part of these criteria, recorded for completeness: five large farms
(old seine 0.2.21/0.2.36) left around the 08:56 Stratum restart and did not
return. Their decline started before it (0.2.21 work 968 -> 685 at 08:40;
7eseCPTm gone at 08:47), their audited shares were valid, and they made no
login attempts afterwards. The cause is on their side and is not observable
from the pool. The live gateway handover since then disconnected no one.

