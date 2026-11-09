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
- **Job fanout is serialized by the job-state write lock.** Writing to the
  gateway takes ~0.2 ms; nearly all the time is `build_miner_jobs`, whose 16
  threads each install 256 assignments under `JobManager.state.write()`, so
  installs run one at a time at ~3-5 µs each (rising with table size:
  3.0 µs at 30k, 4.8 µs at 100k).
- **The cost of slow fanout is small.** With blocks minutes apart, even 1 s
  of fanout wastes ~0.25% of hashing on the previous job. The visible effect
  is submit acks waiting behind the lock during each fanout (p99 7 ms at 15k,
  384 ms at 100k). Miners lose nothing from a slow ack.
- **The gateway is not close to a limit** (21% of a core at 100k after the
  per-event gauge fix, 91086d2).

## Next levers, if needed

1. Build assignments outside the write lock (generate IDs, ranges and targets
   per chunk without the lock, then insert the batch under it), or shard the
   assignment table by connection, so fanout uses the 16 threads it already
   has. This should cut fanout and the ack tail several-fold.
2. Raise `stratum_max_connections` / `gateway_max_miners` past 65,536 only
   with (1) in place; the ack tail grows faster than linearly.
3. Several engines behind the gateway would split the lock entirely, but the
   numbers above say it is not needed below ~100k connections.
