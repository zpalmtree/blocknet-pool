# Block path latency

The path from a miner finding a block to the daemon accepting it, and from
any new tip to every miner hashing the next height. Goal and leads:
`BLOCK_PATH_LATENCY_GOAL.md` in the workspace root. Raw data:
`perf-results/<date>/block-path/`.

## Baseline (bntpool, 2026-10-28)

Eleven pool blocks between 06:47 and 19:25 EDT.

| Stage | Today |
|---|---|
| Miner found → share sent | p50 8 ms, p90 20 ms |
| Pool PoW verify (candidate) | median 556 ms |
| Daemon submitblock | median 1089 ms, almost all PoW |
| Share received → daemon accepted | median 1633 ms |
| Daemon accepted → job published | p50 38 ms |
| Job fanout, template → written | p50 37 ms, p99 234 ms (retarget p99 150 ms) |
| Seine job switch | p50 410 ms, p90 750 ms |
| Peer block, daemon process | p50 1076 ms (PoW 983 ms) |

The pool verifies about 8 hashes a second (0.54-0.67 s each), so about five
2 GiB hashes are always running beside the daemon's.

## Experiments

One line each: hypothesis, stage, tier and host, result, verdict.

- **Daemon PoW on hugepages + current kernel** (core `c110b32`). The daemon's
  2 GiB arena sat on 4 KiB pages (THP is `madvise` on bntpool) and used the
  April kernel. T1, EPYC 9124P, THP madvise: verify 658 → 514 ms idle,
  803 → 576 ms with 8 other hashes, 955 → 645 ms with 15; first hash after
  start 1.8 s → 0.66 s. THP off (kernel bump alone): -3.5 to -6%. Full 2 GiB
  parity against the reference on Zen 4 and Zen 5. **Win.**
- **T2 harness.** One EPYC 9124P runs a private testnet chain: the
  daemon in `--seed` mode with public testnet seeds blocked, the pool
  stratum on a fresh Postgres database, and seine v0.2.45 with 2 threads
  through the pool, for 6 minutes per variant. About 22 pool blocks per
  run. Scripts and per-run data are in `perf-results/2026-10-28/block-path/`.
- **T2 a → b, new daemon only.** Daemon PoW p50 698 → 543 ms. Share received
  → daemon accepted p50 1256 → 1100 ms, p99 2695 → 1248 ms. **Win.**
- **Block claims to the daemon in parallel with the pool verify** (pool
  `c184ad2`). T2 c: early-submitted blocks went from received to accepted in
  557-597 ms, against 1081-1257 ms for the run's serial blocks. The pool
  verify (545-596 ms) and the daemon (549-586 ms) now fully overlap. The
  4-per-10-minute bound held the rest of T2's ~3.5 blocks/min to the serial
  path, as designed; prod finds ~15 a day. Fake-claim cost: at most 4 extra
  daemon hashes per 10 minutes, and each fake strikes its source.
  **Win.**
- **Reuse mempool tx checks in submitblock.** Prod tx validation is 1.7 ms
  p50 and only reaches 210 ms on rare large blocks. Skipping a consensus
  check is not worth that at p50. **Dropped.**
- **Send the job before writing the vardiff hint** (pool `d413306`). Every
  job tick ran the connection's retarget on the blocking pool, with a
  Postgres hint upsert ahead of the job, which was due on nearly every new
  block. T2 with 2,000 idle connections (vardiff on), base → candidate:
  template → written p50 42 → 19 ms, p99 408 → 98 ms; retarget p99
  246 → 7.5 ms. **Win.** What remains is `template_to_conn` (p99 97 ms):
  per-connection job building, partly under the JobState write lock.
- **Seine drops stale hashes on a new height** (seine `82830a7`). The CPU
  cancel waited for every thread's in-flight 2 GiB hash. The kernel now
  checks a stop hook at slice boundaries and every 32Ki blocks. T2 d vs
  a-c: job switch p50 240-350 → 7 ms, p90 ~465 → 9 ms. Hashrate A/B on the
  EPYC 9124P at 16 threads: avx512-256-stream-l2 +0.09% (3 pairs),
  avx2-stream -0.1% (2), avx2 +1.4% (1); all neutral. Same-height refreshes
  still finish their hashes, because the pool credits them within
  `stale_submit_grace`. **Win**, ships with the next batched release.
- **Upstream took the daemon PoW change.** blocknetprivacy/core merged #62
  (reuse the 2 GiB buffer, THP) and #63 (shared kernel, same Zen 4/5
  selection) on 2026-10-29. Core `pool` was rebuilt on them as `26d76f6`.
  That dropped core `07fbe40` and kept two pool patches: the startup warm-up
  and the new `<source>_lock_wait` timer. Under 0.23.0 (`a45b3ff`), peer
  blocks averaged sync_total 1.57 s: 0.90 s of preverify (PoW) plus ~0.6 s
  that no timer covered, presumably the chain-lock wait the new timer
  records.
- **Deployed 2026-10-29 (EDT).** Stratum `c364b5d` at 01:08; blocknet-91's
  `b16a385` (adds Faraday) took over at 01:14. Rolling daemon deploy of core
  `26d76f6` finished at 02:43, with payouts restored. T3 starts from those
  times.
- **T3, first 10 peer blocks on `26d76f6` (02:43-03:12 EDT).** Preverify
  (PoW) 902 → 533 ms mean. The chain-lock wait is ~0 ms, so that is not
  the gap. sync_total is 1062 ms, which leaves ~517 ms outside the timed
  stages. It is the precheck before preverify: `passesCheapPrecheck` reads
  61 ancestor headers, and `decodeBlockHeader` fully decodes any block
  stored before 0.23.0 (legacy JSON, below ~70153). The window holds only
  binary blocks once the tip passes ~70214, so the gap should close on its
  own. If it doesn't, decode legacy headers without the full block.
- **Slow new-tip refreshes are on the pool side.** A live watch on the
  primary (`/api/events`, then template fetches) showed the daemon's
  template at the new height within 21 ms of every new_block, three blocks
  in a row. Yet every slow pool refresh logged `fetches=2 stale_fetches=1`:
  the first fetch came back at the old height, and the job waited for the
  2 s status poll. SSE delivery was ≤1 ms (34/34), nginx has
  `proxy_buffering off`, and no fetch or install took over 250 ms. Own
  blocks are a separate symptom: the first fetch starts ~1.2 s after the
  submit returns, with stale_fetches=0. Pool `1d8ec39` refetches every
  100 ms while the template is below the announced tip; `a75cfc3` traces
  SSE arrival and absorbed refreshes. Both deployed as
  `deploy-tip-refresh-20261029` (live b16a385 + cherry-picks) at 04:24 EDT.
- **Checkpoint save before notify.** Block 70200 spent 173 ms saving
  checkpoints before the broadcast and SSE. That happens on every 100th
  block only. **Not worth it** at p50.
- **T3, 2026-10-29 04:25-12:46 EDT** (engine `8a0ddfe`; daemon `26d76f6` since
  02:43). Share received → daemon accepted p50 ~650 ms (n=10, baseline
  1633); the pool verify p50 571 ms runs inside it. Peer-block daemon
  sync_total p50 639 ms, preverify p50 551 ms (n=113, baseline 1076/983).
  Daemon accept → job published p50 30 ms, p99 427 ms (n=87). Before, 17%
  of tips took 0.5-7.5 s. Fanout template → written p50 61 ms, p99 244 ms,
  of which retarget is now p99 1.5 ms; the rest is template install
  (p99 215 ms), which pool `8bddb1b` moves after the broadcast (pushed;
  deploy held by the owner). Seine job switch p50 729 ms. That is fleet
  telemetry, unchanged until the batched seine release ships `82830a7`.
- **T3, peer blocks over the last 6 h (after the legacy window cleared at
  ~70214).** Daemon sync_total p50 584 ms, p90 761 ms; preverify p50
  551 ms (n=67). This meets the ≤600 ms target. Submit total p50 625 ms
  (n=7).
- **Fanout gate (one-permit async semaphore around retarget + build).**
  T2 on a Ryzen 7700X with 4,000 idle connections, 2 interleaved pairs:
  template → written base p50 15-16 / p99 81-86 ms, gate 24 / 96 ms. The
  gate cuts wakeup (template_to_conn p99 80 → 5 ms) but serializes ~24 µs
  of task handoff per connection. A profile shows the build itself is
  negligible CPU (PoW verify ~51%, socket writes ~11%), so the tail is task
  scheduling for ~2 tasks per connection, not lock work. **Rejected.**
  Next: one fanout task builds all assignments in chunked lock passes,
  splices per-connection fields into the template's pre-serialized job,
  and queues bytes straight to each writer.
- **Pipelined batch fanout** (pool `5c42fe3`). One task fans each template
  out: a few threads take 256-connection chunks, and each chunk is
  retargeted, built in one job-state lock pass and queued straight into
  the connections' latest-job slots. T2 on a Ryzen 7700X, template →
  written p50/p99: 4,000 idle connections base 15-16/49-71 ms → 7-8/26-40
  ms; 12,000 base 50/239 → 27/50 ms. A first version (all retargets, then
  all builds, then all queues) improved p99 but worsened p50 at 12k
  (61/99 ms). **Win.**
- **Prod template fetch regression.** Since ~13:00 EDT, template_fetch p50
  went 9 → 77 ms. The daemon's template is 2.6 MB (mempool of 167 txs,
  55 MB). The pool parsed it into a JSON tree and deep-copied it per job.
  Pool `f517826` keeps the block as raw JSON and reads only the header:
  10-20 → 7.5 ms on a desktop CPU. Next lead: a compact template from the
  daemon, with the full block fetched off the critical path for the
  fallback submit.
- **T3, pipelined fanout in prod** (engine `56fd15f`, 2026-10-29 15:55-16:36
  EDT, ~5,000 connections, overlapping the template-cap daemon deploy).
  Template → written p50 73 → 21.6 ms, p90 161 → 46 ms, p99 248 → 186 ms.
  The p99 was build_chunk (p99 148 ms): chunks waited behind the
  post-template assignment cleanup under the same write lock. Template
  fetch p50 32 ms (the cap shrank templates to ~0.6 MB).
- **Batched assignment cleanup** (pool `3f89eef`, deployed as `8c51e3d`
  at 16:57 EDT). T3 over 9 tips: template → written p50 19.8 ms, p99
  89.9 ms; build_chunk p99 148 → 47 ms; daemon accept → job published p50
  34 ms, p99 50 ms. **Win.** The p99 is within ~10 ms of the 80 ms target.
  What remains is lock time per chunk, at roughly twice T2's cost on the
  busier prod host.
- **T3, engine `052f409` (17:44-19:00 EDT; includes the pipelined fanout and
  batched cleanup).** Template → written p50 19.6 ms, p90 42 ms, p99 76.2 ms
  (n=369k writes); build_chunk p99 40 ms. Daemon accept → job published
  p50 35 ms, p99 50 ms (n=7); template fetch p50 29 ms, p99 61 ms. The
  fanout stage now meets 80 ms at p99. Accept → written end to end is
  roughly 90-125 ms at p99, and the next lever is the template fetch: a
  compact template from the daemon, with the full block fetched off the
  critical path.
