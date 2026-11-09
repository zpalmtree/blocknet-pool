# Connection scaling

How many miner connections one gateway and one Stratum engine can serve, and
what grows with the count. Share verification is left out on purpose: its
cost follows the share rate, not the connection count, and has its own replay
harness (VERIFY_PERFORMANCE.md).

## Method

`scripts/conn_loadtest.sh` runs one step on a single host: `mock_daemon.py`
(a new block every 30 s), the gateway, one engine and Postgres, then
`pool_conn_load.py` shards that open N mostly idle miner connections from 64
loopback source addresses (322 real payout addresses, 50 workers each),
submitting 0.003 shares/s per connection (production's rate on 2026-11-08:
~40 shares/s over ~14.5k connections). Each step ramps for 2-4 minutes and
measures for 5 minutes (10 blocks). `scripts/conn_loadtest_report.py`
tabulates the steps.

The engine is built with `--features loadtest-pow`, which replaces share
proof hashing with a constant hash so synthetic shares pass verification,
probation and audits. Production builds never enable the feature.

Delivery times are measured two ways: client-side, the spread between the
first and last connection to receive a block's job (includes the load
generator's own reading time, ~4-6k connections per Python process), and
engine-side, the `job_fanout` histograms from the persisted runtime snapshot.

## Results (2026-11-08)

GCP `c2d-highcpu-32` (32 vCPU AMD EPYC 7B13, 64 GB), pool master 069a02e plus
the load-test tooling. Evidence: `perf-results/2026-11-08/conn-loadtest/` in
the workspace.

| Connections | Logged in | Last connection gets job | Engine template→written mean | Submit ack p99 | Gateway CPU | Engine CPU | Engine RSS |
|---|---|---|---|---|---|---|---|
| 15,000 | all | 0.19 s | - | 7 ms | 4% | 9% | 0.6 GB |
| 30,000 | all | 0.28 s | 0.09 s | 30 ms | 8% | 18% | 1.1 GB |
| 50,000 | all | 0.48 s | 0.18 s | 118 ms | 10% | 24% | 1.8 GB |
| 65,000 | all | 0.65 s | 0.25 s | 161 ms | 14% | 31% | 2.5 GB |
| 100,000 | all | 1.28 s | 0.48 s | 384 ms | 21% | 49% | 3.6 GB |

CPU is percent of one core, averaged over the measured window. Every job
reached every connection; one share in ~234,000 was rejected (stale).

## Findings

- **Nothing breaks up to 100k connections on one host.** Logins, job delivery
  and share acceptance all hold; memory is ~37 KB per connection.
- **Job fanout was serialized by the job-state write lock.** Writing to the
  gateway takes ~0.2 ms; nearly all the time was `build_miner_jobs`, whose 16
  threads each installed 256 assignments under `JobManager.state.write()`, so
  installs ran one at a time at ~3-5 µs each (rising with table size). Fixed
  below by sharding the assignments.
- **The cost of slow fanout is small.** With blocks minutes apart, even 1 s
  of fanout wastes ~0.25% of hashing on the previous job.
- **The client-side columns are bounded by the load generator.** Each Python
  shard reads ~5,500 connections; during a fanout it reads that many job
  lines before the next share reply, so "Last connection gets job" and
  "Submit ack p99" mostly measure the generator. The engine's own histograms
  (below) are the pool's numbers. An earlier version of this file attributed
  the client ack tail to the lock; the engine-side ack p99 at 100k was
  ≤250 ms, not the 384 ms measured by the client.
- **The gateway is not close to a limit** (21% of a core at 100k after the
  per-event gauge fix, 91086d2; ≤0.6 ms queue wait).

## Sharded assignments (2026-11-08, later the same day)

Assignments and their indexes moved out of the job-state lock into 64
shards keyed by connection (assignment IDs carry their shard); job builds
take the job-state read lock and one shard's write lock at a time. Then the
fanout's per-connection vardiff step was batched under one sessions lock
per chunk (`Engine::job_tick`). Same host type and method; evidence in
`perf-results/2026-11-08/conn-loadtest-sharded/`.

Engine-side, from the runtime snapshot histograms (means; p99 is the
histogram bucket bound):

| Connections | Build | Template→written mean | Build per 256-conn chunk | Retarget per chunk | Engine submit ack p99 |
|---|---|---|---|---|---|
| 30,000 | single lock | 91 ms | 17.2 ms | 2.2 ms | ≤10 ms |
| 30,000 | sharded | 62 ms | 1.6 ms | 6.1 ms | ≤10 ms |
| 65,000 | single lock | 252 ms | 28.0 ms | 1.5 ms | ≤100 ms |
| 65,000 | sharded | 114 ms | 2.4 ms | 6.5 ms | ≤25 ms |
| 65,000 | sharded + batched retarget | 109 ms | 2.9 ms | 1.1 ms | ≤25 ms |
| 100,000 | single lock | 480 ms | 35.9 ms | 1.2 ms | ≤250 ms |
| 100,000 | sharded | 174 ms | 2.8 ms | 6.2 ms | ≤25 ms |
| 100,000 | sharded + batched retarget | 167 ms | 3.6 ms | 1.2 ms | ≤50 ms |

Every step: all connections logged in and received every job, no shares
rejected, no disconnects. Gateway and engine CPU and memory were unchanged.

- Fanout is 2-3x faster at 65k-100k and no longer grows with lock
  contention; submit acks no longer queue behind it (engine ack p99 ≤25-50 ms
  at 100k, from ≤250 ms).
- With the lock gone, the per-connection sessions-lock retarget showed up
  (6 ms per chunk); batching it cut that to ~1 ms, a small end-to-end gain.
  The rest of each chunk is building and queueing the job lines, which
  already runs in parallel.
- In-process, `fanout_bench` (jobs.rs, ignored) shows the same: at 100k
  connections and 16 threads, a submit lookup during fanout waited 4-7 ms
  (p50) and up to 300 ms before, 3 µs (p50) and ~1-2 ms (p99) after.

## Next levers, if needed

1. Raise `stratum_max_connections` / `gateway_max_miners` past 65,536 if
   real growth calls for it; one host handled 100k with fanout under 0.2 s
   mean in the engine.
2. Several engines behind the gateway would split the remaining per-engine
   work, but the numbers above say it is not needed below ~100k connections.
3. A better load generator (compiled, or more processes) is needed before
   client-side delivery numbers mean much above ~30k connections.
