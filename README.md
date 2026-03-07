<h1 align="center">Blocknet Pool (Rust)</h1>

<p align="center">
  Blocknet mining pool server in Rust (stratum + API + payouts).
</p>

<p align="center">
  <img src="https://img.shields.io/badge/status-testing%2Fdev-aaff00?style=flat-square&labelColor=000" alt="Status">
  <img src="https://img.shields.io/badge/language-rust-aaff00?style=flat-square&labelColor=000" alt="Rust">
  <img src="https://img.shields.io/badge/license-BSD--3--Clause-aaff00?style=flat-square&labelColor=000" alt="License">
</p>

## Runtime Modes

The repo now ships three binaries:

- `blocknet-pool-rs`: combined API + Stratum runtime (legacy/dev convenience)
- `blocknet-pool-api`: API/UI process only
- `blocknet-pool-stratum`: Stratum + payouts + maintenance process only

For production, prefer the split services so API/UI deploys do not drop Stratum connections.

## Deploy (bntpool)

From the local repo root:

```bash
./scripts/deploy_bntpool.sh
```

Use split-service migration only when moving from the legacy combined service or when you intentionally need to reinstall the systemd unit files:

```bash
./scripts/deploy_bntpool.sh --migrate-split
```

What it does:

- builds frontend bundle locally (unless `--skip-ui-build`)
- builds the split release binaries locally
- rsyncs pool source to `bntpool:/opt/blocknet/blocknet-pool`
- uploads the locally built binaries to the server
- restarts only the changed service(s):
  - `blocknet-pool-api.service`
  - `blocknet-pool-stratum.service`
- frontend-only changes still restart `blocknet-pool-api.service` because the UI bundle is embedded into that binary
- tails recent logs for both services

## Build Blocknet Daemon For bntpool

When you need a fresh `blocknet` daemon binary for the pool host, build it
locally from this repo using the sibling daemon checkout instead of compiling on
`bntpool`.

From `blocknet-pool/`:

```bash
./scripts/build_blocknet_daemon.sh
```

That writes the daemon artifact to `build/blocknet-linux-amd64`.

To stage it on the server in one step:

```bash
./scripts/build_blocknet_daemon.sh --upload bntpool
```

By default the script reads source from `../blocknet`, derives the required Go
version from that repo's `go.mod`, builds through the daemon repo's Dockerfile,
and uploads to `/opt/blocknet/blocknet/blocknet.new`. Swapping the live daemon
binary and restarting the daemon remain manual steps so restart timing stays
explicit.

## Local Development

```bash
npm --prefix frontend ci
npm --prefix frontend run build
cargo build --release --bin blocknet-pool-api --no-default-features --features api
cargo build --release --bin blocknet-pool-stratum --no-default-features --features stratum
cargo run --release
# runs the combined API + Stratum binary for local/dev use
# if missing, config.json and .env are created automatically
# edit .env and set BLOCKNET_WALLET_PASSWORD
```

Run the split binaries directly when you want production-like local behavior:

```bash
cargo run --release --bin blocknet-pool-api --no-default-features --features api
cargo run --release --bin blocknet-pool-stratum --no-default-features --features stratum
```

Custom config:

```bash
cargo run --release -- --config /path/to/config.json
```

## Frontend

Web UI is built with React + TypeScript + Vite from `frontend/`.

Local dev:

```bash
cd frontend
npm install
npm run dev
```

Build the embedded API bundle:

```bash
cd frontend
npm ci
npm run build
```

Build output is written to `frontend/dist/` and embedded into the API binary during API builds. A fresh clone must build the frontend before `cargo build` or `cargo run` for `blocknet-pool-api` or the combined binary.

The embedded assets are served at:

- `GET /` / `GET /ui` (index)
- `GET /ui-assets/app.js`
- `GET /ui-assets/app.css`

Because the UI bundle is embedded into the API binary, frontend-only changes still require rebuilding and restarting the API service.

## Daemon API Auth

The pool can authenticate to the daemon with either:

- `daemon_token` in `config.json`
- `daemon_cookie_path` in `config.json`
- auto-discovery of `api.cookie` via `daemon_data_dir` and running daemon process metadata

When a daemon request returns `401 unauthorized`, the pool will refresh the token from cookie once and retry.

## Database Backend

Default backend is SQLite (`database_path`).

To use Postgres, set `database_url` in `config.json`, for example:

```json
{
  "database_url": "postgres://user:password@127.0.0.1:5432/blocknet_pool"
}
```

When `database_url` is set, Postgres is used automatically and `database_path` is ignored.
`database_pool_size` controls Postgres connection fan-out (default `4`).

### Production SQLite -> Postgres Migration

Use the included migration script to preserve existing pool history:

```bash
scripts/migrate_sqlite_to_postgres.sh \
  --sqlite /var/lib/blocknet-pool/pool.db \
  --postgres 'postgres://blocknet:REPLACE_ME@127.0.0.1:5432/blocknet_pool'
```

Recommended order:

1. Stop the running pool service(s).
2. Run the migration script.
3. Set `database_url` in `/etc/blocknet/pool/config.json`.
4. Start the pool service(s).
5. Verify `/api/stats` and admin endpoints.

## Transport Security

- Stratum now defaults to loopback bind (`stratum_host=127.0.0.1`). Set `stratum_host` explicitly to expose it.
- API TLS is supported via:
  - `api_tls_cert_path`
  - `api_tls_key_path`
- If only one TLS path is set, startup logs a warning and serves HTTP.
- If Stratum is exposed publicly, place it behind a TLS terminator.

## Runtime Components

- API/UI server
- Stratum server
- Template/job manager
- Validation engine (bounded queues)
- Persistent storage (SQLite/Postgres)
- Payout processor
- DB/meta-backed live snapshot bridge for split-service API fallbacks

## API Surface

Public endpoints (no API key required):

- `GET /api/info`
- `GET /api/stats`
- `GET /api/stats/history`
- `GET /api/stats/insights`
- `GET /api/luck`
- `GET /api/status`
- `GET /api/events`
- `GET /api/blocks`
- `GET /api/payouts/recent`
- `GET /api/miner/{address}`
- `GET /api/miner/{address}/balance`
- `GET /api/miner/{address}/hashrate`

Protected endpoints (API key required):

- `GET /api/miners`
- `GET /api/payouts`
- `GET /api/fees`
- `GET /api/health`
- `GET /api/daemon/logs/stream`

When `api_key` is unset, protected endpoints return `503 api key not configured`.

Accepted headers:

- `x-api-key: <api_key>`
- `Authorization: Bearer <api_key>`

Paged/filterable list mode:

- `paged=true` enables paged response shape (`items` + `page`).
- Shared query params: `limit`, `offset`.
- `GET /api/miners`: `search`, `sort`.
- `GET /api/blocks`: `finder`, `status`, `sort`.
- `GET /api/payouts`: `address`, `tx_hash`, `sort`.
- `GET /api/fees`: `fee_address`, `sort`.

Daemon log stream details:

- Admin UI includes a live daemon log viewer tab.
- Stream endpoint: `GET /api/daemon/logs/stream?tail=200`.
- Log source fallback order:
  - `journalctl -a -u blocknetd.service`
  - `tail -F <daemon_data_dir>/debug.log`

## Web UI

- `GET /` (dashboard)
- `GET /ui` (alias)
- Multi-tab WebUI includes:
  - pool + onboarding info (`/api/info`)
  - API key auth UX for protected routes
  - miner lookup
  - miners/blocks/payouts/fees tables with filter + pagination
  - live trend charts
  - operator health panel (`/api/health`)
  - live daemon logs panel (`/api/daemon/logs/stream`)

## Stratum Notes

- Login supports protocol negotiation (`protocol_version`, `capabilities`)
- Login rejects malformed payout addresses early (base58 + checksum-compatible Blocknet stealth address validation)
- `stratum_submit_v2_required=true` (default): requires protocol v2 + `submit_claimed_hash`
- `stratum_submit_v2_required=false`: allows legacy submits without `claimed_hash` (full verification path)
- Per-connection submit rate limiting is enabled (`stratum_submit_rate_limit_window`, `stratum_submit_rate_limit_max`)
- Queue pressure returns `server busy, retry` (no inline bypass)
- Per-connection vardiff retargeting is enabled by default to target a small number of shares per window (`vardiff_*` config keys)
- Vardiff difficulty is cached per `address+worker` and reused on reconnect/restart when the hint is fresh (1h TTL), reducing post-restart ramp-up.
- Default vardiff profile assumes a weak baseline miner and aims for ~10 shares / 5 minutes (`initial_share_difficulty=60`, `vardiff_target_shares=10`)
- Template refresh identity uses stable tip fields (`height`, `network_target`, `prev_hash`) to avoid daemon template churn while still refreshing on meaningful tip/template transitions.
- Assignment submits on a previous template are accepted only inside a short grace window (`stale_submit_grace`, default `5s`) based on when the share was received.
- Daemon SSE tip events (`/api/events`) are enabled by default (`sse_enabled=true`) and mark templates stale from `new_block` `hash` + `height`.
- Timestamp-only `new_block` changes do not trigger refreshes; only hash/height changes can trigger staleness.
- Same-height hash-change refresh is disabled by default (`refresh_on_same_height=false`) to avoid replay churn; enable it only if you want immediate same-height reorg reaction.

## Review Follow-Ups (2026-03-03)

- Risk escalation persistence now uses an atomic update path per address.
- Accepted-share hashrate tracking keeps a 1-hour window with a hard in-memory cap.
- Template refresh matching now uses stable identity fields to catch meaningful same-height updates.
- API key comparison is currently direct string equality by design for this pool deployment model; this is accepted for now and is not treated as a blocker.

## Payout Safeguards

- `payouts_enabled` toggles payout sending globally.
- `payout_pause_file` pauses payouts when the file exists.
- `pplns_window_duration` controls the time-based PPLNS lookback window (default `6h`).
- `payout_wait_priority_threshold` promotes queued payouts to longest-waiting-first after they have waited at least the configured duration (default `6h`).
- `payout_min_verified_ratio` is a hard verified-difficulty gate. Keeping it near the sampler coverage can exclude honest miners due to sampling variance and vardiff.
- `payout_provisional_cap_multiplier` caps aged provisional difficulty relative to verified difficulty. Prefer a cap over a hard ratio cutoff when you want reduced credit instead of zero credit.
- Keep `sample_rate` and `min_sample_every` comfortably above the payout policy's effective verified-share target so honest miners do not flap around the cutoff.
- Optional caps:
  - `payout_max_recipients_per_tick`
  - `payout_max_total_per_tick`
  - `payout_max_per_recipient`

## Retention & Summary Rollups

- Retention worker runs on `retention_interval`.
- Old rows are rolled up into summary tables before prune:
  - `share_daily_summaries`
  - `payout_daily_summaries`
- Retention controls:
  - `shares_retention`
  - `payouts_retention`
- `get_total_share_count` and total rejected share metrics include rolled-up share summaries.

## CI Coverage

- Added CI smoke harness: `scripts/ci_e2e_smoke.sh` (mock daemon + real pool process + Stratum/API probe).

## Daemon Requirements

The pool expects a running Blocknet daemon API with mining + wallet routes enabled.

At minimum:

- `GET /api/status`
- `GET /api/mining/blocktemplate`
- `POST /api/mining/submitblock`
- `GET /api/block/{id}`
- `GET /api/wallet/address`
- `GET /api/wallet/balance`
- `POST /api/wallet/send`
- `POST /api/wallet/load`
- `POST /api/wallet/unlock`

## Notes

- This repository no longer uses Go; runtime is Rust-only.
