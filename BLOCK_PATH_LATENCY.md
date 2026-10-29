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
