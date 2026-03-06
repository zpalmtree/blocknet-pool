<h1 align="center">Blocknet Pool (Rust)</h1>

<p align="center">
  Blocknet mining pool server in Rust (stratum + API + payouts).
</p>

<p align="center">
  <img src="https://img.shields.io/badge/status-testing%2Fdev-aaff00?style=flat-square&labelColor=000" alt="Status">
  <img src="https://img.shields.io/badge/language-rust-aaff00?style=flat-square&labelColor=000" alt="Rust">
  <img src="https://img.shields.io/badge/license-BSD--3--Clause-aaff00?style=flat-square&labelColor=000" alt="License">
</p>

## Quick Start

```bash
cargo build --release
cargo run --release
# if missing, config.json and .env are created automatically
# edit .env and set BLOCKNET_WALLET_PASSWORD
```

Custom config:

```bash
cargo run --release -- --config /path/to/config.json
```

## Frontend

Web UI is now built with React + TypeScript + Vite from `frontend/`.

Local dev:

```bash
cd frontend
npm install
npm run dev
```

Build static bundle embedded by the Rust API:

```bash
cd frontend
npm run build
```

Build output is written to `src/ui/dist/` and served at:

- `GET /` / `GET /ui` (index)
- `GET /ui-assets/app.js`
- `GET /ui-assets/app.css`

## Deploy (bntpool)

From the local repo root:

```bash
./scripts/deploy_bntpool.sh
```

What it does:

- builds frontend bundle locally (unless `--skip-ui-build`)
- rsyncs pool source to `bntpool:/opt/blocknet/blocknet-pool`
- builds `--release` on the server
- restarts `blocknet-pool.service`
- tails recent service logs

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

1. Stop `blocknet-pool.service`.
2. Run the migration script.
3. Set `database_url` in `/etc/blocknet/pool/config.json`.
4. Start `blocknet-pool.service`.
5. Verify `/api/stats` and admin endpoints.

## Transport Security

- Stratum now defaults to loopback bind (`stratum_host=127.0.0.1`). Set `stratum_host` explicitly to expose it.
- API TLS is supported via:
  - `api_tls_cert_path`
  - `api_tls_key_path`
- If only one TLS path is set, startup logs a warning and serves HTTP.
- If Stratum is exposed publicly, place it behind a TLS terminator.

## Runtime Components

- Stratum server
- Template/job manager
- Validation engine (bounded queues)
- Persistent storage (SQLite/Postgres)
- Payout processor
- HTTP API

## Core API Endpoints

- `GET /api/stats`
- `GET /api/info`
- `GET /api/miner/{address}`
- `GET /api/miner/{address}/balance`
- `GET /api/miners`
- `GET /api/blocks`
- `GET /api/payouts`
- `GET /api/fees`
- `GET /api/health`
- `GET /api/daemon/logs/stream`

Paged/filterable list mode (protected endpoints):

- `paged=true` enables paged response shape (`items` + `page`).
- Shared query params: `limit`, `offset`.
- `GET /api/miners`: `search`, `sort`.
- `GET /api/blocks`: `finder`, `status`, `sort`.
- `GET /api/payouts`: `address`, `tx_hash`, `sort`.
- `GET /api/fees`: `fee_address`, `sort`.

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

## API Auth

Public endpoints (no API key required):

- `GET /api/info`
- `GET /api/stats`
- `GET /api/miner/{address}`

Protected endpoints (API key required):

- `GET /api/miners`
- `GET /api/blocks`
- `GET /api/payouts`
- `GET /api/fees`
- `GET /api/health`

When `api_key` is unset, protected endpoints return `503 api key not configured`.

Accepted headers:

- `x-api-key: <api_key>`
- `Authorization: Bearer <api_key>`

## Admin Daemon Logs

- Admin UI includes a live daemon log viewer tab.
- Backend stream endpoint: `GET /api/daemon/logs/stream?tail=200`.
- Log source fallback order:
  - `journalctl -a -u blocknetd.service`
  - `tail -F <daemon_data_dir>/debug.log`

## Payout Safeguards

- `payouts_enabled` toggles payout sending globally.
- `payout_pause_file` pauses payouts when the file exists.
- `pplns_window_duration` controls the time-based PPLNS lookback window (default `6h`).
- `payout_provisional_cap_multiplier=0` disables the cap on aged provisional share weight; positive values cap provisional difficulty relative to verified difficulty.
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
