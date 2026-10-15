# API read behavior and verification

Miner detail reads admit two database loads at a time. A cold request waits up
to 50 ms for a slot, then returns HTTP 503 with `Retry-After: 5`. Cache hits do
not consume a slot. Cancellation retains the slot until blocking work finishes.
Per-IP telemetry rate limits remain enforced; HTTP 429 also carries Retry-After.
Browser reads coalesce overlapping requests and honor these cooldowns.

`GET /api/admin/blocks/:height/reward-breakdown` runs one background computation
at a time. A result ready within 150 ms returns the existing HTTP 200 JSON shape.
Otherwise it returns HTTP 202, `Retry-After: 1`, and `{"status":"pending"}`.
Poll the same URL to obtain the completed result. Retries join the existing job,
even after the original request disconnects. Completed results are retained for
five seconds; at most 32 entries are retained. Another height while the worker
is busy receives HTTP 503 with `Retry-After: 1`. The admin UI handles polling.
This is a read-only preview; replay does not change share or payout accounting.

Luck/effort reads use `share_read_work`, combining minute summaries, unapplied
deltas, and raw boundary shares within one MVCC snapshot. Boundary reads use
the accepted-share covering index. The all-status summary remains available
for reads that need rejected counts. No additional index is required.

The companion Seine change caches global hashrate for 30 seconds, adds 0–2
seconds of jitter to its five-second telemetry cadence, honors 503 backoff,
and only falls back to full miner details when the balance route is missing
(404/405). These reductions require operators to install an updated miner;
deploying the pool cannot change already-running miner clients.

Checks:

```sh
npm --prefix frontend test
npm --prefix frontend run build
cargo test -p blocknet-pool-api-app
# Run each database test against its own empty, disposable database:
BLOCKNET_POOL_TEST_POSTGRES_URL='...' cargo test -p pool-runtime minute_read_windows_match_raw_shares_before_and_after_mutations_postgres
BLOCKNET_POOL_TEST_POSTGRES_URL='...' cargo test -p pool-runtime luck_page_respects_tip_and_inclusive_share_boundaries_postgres
BLOCKNET_POOL_TEST_POSTGRES_URL='...' cargo test -p blocknet-pool-api-app detail_overload_preserves_cache_hits_and_balance_access_postgres
```

`scripts/measure_read_concurrency.py` makes at most eight concurrent GETs and
stops after a stage with an error or response over one second. Its default is
64 requests, with pauses between stages. Run from the server for application
latency or with `--base-url https://bntpool.com` for network-inclusive latency.
Success at this bounded workload is not a guarantee under arbitrary overload.
