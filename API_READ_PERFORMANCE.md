# API read behavior and verification

Miner detail reads admit two database loads at a time. A cold request waits up
to 50 ms for a slot, then returns HTTP 503 with `Retry-After: 5`. Cache hits do
not consume a slot. Cancellation retains the slot until blocking work finishes.
Per-IP telemetry rate limits remain enforced; HTTP 429 also carries Retry-After.
Browser reads coalesce overlapping requests and honor these cooldowns.

Public history/status reads (`/api/status`, `/api/stats/history`, `/api/blocks`,
`/api/luck`, `/api/payouts/recent`, `/api/miner/:address/hashrate`) are served
from a shared response cache (TTL 15 s; history 30 s; miner hashrate 10/30/60 s
for 1h/24h/longer ranges). A miss is single-flighted per route+parameters and
loads under one database read slot (two when `database_pool_size` > 4) with a
2 s admission wait and `api_public_db_statement_timeout_ms` (default 15000).
Only misses count against the per-client budget of 20 per 5 s; cache hits are
never limited. When a miss is rate limited, the read slot is busy, or the load
fails, a copy up to five minutes old is served (`X-Cache: stale`); without one
the response is 429 or 503 with `Retry-After`, or 500 for a failed load.
Client addresses come from `X-Real-IP` (`api_client_ip_header`) when the TCP
peer is loopback or in `api_trusted_proxies`, else the rightmost untrusted
`X-Forwarded-For` hop.

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

## Release adoption observations

`scripts/observe_miner_release.py --version 0.2.17 --output <directory>` takes
read-only samples every five minutes. It waits up to seven days for the first
worker reporting that version, then records a full 24-hour observation window.
It stops automatically and writes `summary.json` and `samples.jsonl`; it sends
no notifications and changes no running pool service. `--once` captures one
sample and can resume the same output directory later.

The job needs access to the local API config, API journal, and PostgreSQL via
`runuser -u postgres`. SQL is read-only with a two-second timeout. Version
counts describe workers submitting shares in the last five minutes, not all
connected miners. No addresses, IPs, credentials, or raw log messages are saved.
HTTP 429/503 counts come from journal entries in disjoint time windows; samples
also record the API start time so a routine restart can be identified. P99 is
the API's rolling successful-request percentile, not a 24-hour histogram.
Sampling errors are recorded explicitly. Summary totals cover the whole watch;
use the timestamped samples and `first_adoption_epoch` to compare before/after.

A bounded service can be started after syncing the script through the normal
`deploy_bntpool.sh --api-only --skip-build --skip-ui-build` path (unchanged
binary hashes preserve the running services):

```sh
sudo systemd-run --unit=seine-release-observation-0-2-17 \
  --property=Nice=10 --property=CPUQuota=10% --property=MemoryMax=128M \
  --property=RuntimeMaxSec=8d --property=Restart=on-failure --property=RestartSec=30s \
  /usr/bin/python3 /opt/blocknet/blocknet-pool/scripts/observe_miner_release.py \
  --version 0.2.17 --output /var/lib/blocknet/release-observations/seine-0.2.17
```

The transient service survives process failures but not a host reboot; rerun
it with the same output directory to resume. It never restarts the API to test
startup behavior. On completion, inspect the final summary and timestamped
samples before deciding whether more tuning is warranted.
