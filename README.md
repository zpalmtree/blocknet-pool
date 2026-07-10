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

The repo ships four runtime binaries from a Cargo workspace:

- `blocknet-pool-api`: API/UI process only
- `blocknet-pool-stratum`: Stratum + payouts + maintenance process only
- `blocknet-pool-monitor`: on-host monitoring sampler + Prometheus endpoint
- `blocknet-pool-recoveryd`: privileged local recovery agent for daemon cutover/sync/rebuild workflows

For production, prefer the split services so API/UI deploys do not drop Stratum connections.

Workspace layout:

- `apps/`: service-specific app packages
- `crates/`: shared library packages with explicit dependency boundaries
- `frontend/`: embedded React/Vite UI for the API binary

## Deploy (bntpool)

From the local repo root:

```bash
./scripts/deploy_bntpool.sh
```

What it does:

- builds frontend bundle locally (unless `--skip-ui-build`)
- builds the split release binaries on Linux:
  - set `BNTPOOL_REMOTE_BUILD=1` to build on `bntpool` after source sync
  - or set `BNTPOOL_LOCAL_BUILD_IMAGE` with `DOCKER_DEFAULT_PLATFORM=linux/amd64` to build locally in Docker
- rsyncs pool source to `bntpool:/opt/blocknet/blocknet-pool`
- uploads locally built binaries, or uses host-built binaries when `BNTPOOL_REMOTE_BUILD=1`
- verifies deployed binary architecture before restarting services
- writes `/opt/blocknet/blocknet-pool/deploy-info.txt`
- restarts only the changed service(s):
  - `blocknet-pool-api.service`
  - `blocknet-pool-stratum.service`
  - `blocknet-pool-monitor.service`
- frontend-only changes still restart `blocknet-pool-api.service` because the UI bundle is embedded into that binary
- tails recent logs and runs `scripts/verify_bntpool_deploy.sh`

From macOS, do not run a plain local binary deploy. Use the remote build path:

```bash
BNTPOOL_REMOTE_BUILD=1 ./scripts/deploy_bntpool.sh
```

Run the production smoke verifier without deploying:

```bash
./scripts/verify_bntpool_deploy.sh
```

## Monitoring Stack

The pool now ships a dedicated on-host monitor and repo-managed monitoring assets:

- `blocknet-pool-monitor.service`: probes API, Stratum, Postgres, daemon, wallet, share freshness, and payout queue state every `10s`
- The monitor uses the same `api_key` as protected API routes when it samples API performance telemetry.
- DB-backed heartbeats and incidents drive `/api/status`
- Prometheus + Alertmanager + node exporter + blackbox exporter configs live under `deploy/monitoring/`
- Cloudflare Worker assets for outside-in public HTTP probes live under `deploy/cloudflare/monitor-worker/`

Provision the on-host monitoring packages and configs on `bntpool` with:

```bash
./scripts/provision_bntpool_monitoring.sh
```

That script installs Prometheus, Alertmanager, node exporter, blackbox exporter, and the local Discord relay. Grafana provisioning files are included in-repo and can be installed separately if Grafana is present on the host.

## Build Blocknet Daemon For bntpool

When you need a fresh `blocknet-core` daemon binary for the pool host, build it
locally from this repo using the sibling daemon checkout instead of compiling on
`bntpool`. The build and deploy scripts source the daemon `pool` branch from the
sibling `../blocknet-core` repo through a temporary git worktree, so they do
not require switching your current local checkout first. If that branch/ref is
missing or stale locally, fetch it before building.

From `blocknet-pool/`:

```bash
./scripts/build_blocknet_daemon.sh
```

That writes the daemon artifact to `build/blocknet-core-linux-amd64`.

To stage it on the server without restarting the daemon yet:

```bash
./scripts/build_blocknet_daemon.sh --upload bntpool
```

For the full repeatable deploy path, including the managed `blocknetd@primary`
and `blocknetd@standby` units, release directory rotation, symlink switch,
restart, and `/api/status` verification:

```bash
./scripts/deploy_blocknet_daemon_bntpool.sh
```

By default the build script reads source from `../blocknet-core`, derives the required Go
version from that repo's `go.mod`, resolves branch `pool` from the daemon repo,
builds through the daemon repo's Dockerfile, writes local artifact metadata next
to the binary, and uploads to `/opt/blocknet/blocknet-core/blocknet.new`. The
full deploy script names releases with the branch prefix, for example
`pool-<sha>`, and records `build-info.txt` in each release directory on the
server.

## Local Development

```bash
npm --prefix frontend ci
npm --prefix frontend run build
cargo build --release -p blocknet-pool-api-app --bin blocknet-pool-api
cargo build --release -p blocknet-pool-stratum-app --bin blocknet-pool-stratum
cargo build --release -p blocknet-pool-monitor-app --bin blocknet-pool-monitor
cargo build --release -p blocknet-pool-recoveryd-app --bin blocknet-pool-recoveryd
cp config.example.json config.json
# edit config.json and set database_url, api_key, monitor_ingest_secret, daemon_cookie_path, and pool addresses
# optional: create .env next to config.json with BLOCKNET_WALLET_PASSWORD=...
cargo run --release -p blocknet-pool-api-app --bin blocknet-pool-api
cargo run --release -p blocknet-pool-stratum-app --bin blocknet-pool-stratum
cargo run --release -p blocknet-pool-monitor-app --bin blocknet-pool-monitor
# run the API and Stratum binaries in separate terminals
# run the monitor binary in a third terminal if you want the DB-backed status page/metrics loop
```

Custom config:

```bash
cargo run --release -p blocknet-pool-api-app --bin blocknet-pool-api -- --config /path/to/config.json
cargo run --release -p blocknet-pool-stratum-app --bin blocknet-pool-stratum -- --config /path/to/config.json
cargo run --release -p blocknet-pool-monitor-app --bin blocknet-pool-monitor -- --config /path/to/config.json
cargo run --release -p blocknet-pool-recoveryd-app --bin blocknet-pool-recoveryd -- --config /path/to/config.json
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

Build output is written to `frontend/dist/` and embedded into the API binary during API builds. A fresh clone must build the frontend before `cargo build` or `cargo run` for `blocknet-pool-api`.

The embedded assets are served at:

- `GET /` (index)
- `GET /ui-assets/app.js`
- `GET /ui-assets/app.css`

Because the UI bundle is embedded into the API binary, frontend-only changes still require rebuilding and restarting the API service.

## Daemon API Auth

The pool authenticates to the daemon with `daemon_cookie_path` in `config.json`.
When a daemon request returns `401 unauthorized`, the pool refreshes the token from the configured cookie path once and retries.

## Database Backend

The pool uses Postgres. Set `database_url` in `config.json`, for example:

```json
{
  "database_url": "postgres://user:password@127.0.0.1:5432/blocknet_pool"
}
```

`database_url` is required. `database_pool_size` controls Postgres connection fan-out
(default `4`).

## Transport Security

- Stratum now defaults to loopback bind (`stratum_host=127.0.0.1`). Set `stratum_host` explicitly to expose it.
- The API server is plaintext HTTP and should stay bound to loopback behind nginx or another TLS terminator.
- If Stratum is exposed publicly, place it behind a TLS terminator.

## Runtime Components

- API/UI server
- Stratum server
- Template/job manager
- Validation engine (bounded queues)
- Persistent storage (Postgres)
- Payout processor
- DB/meta-backed live runtime snapshot for split-service API status

## API Surface

Public endpoints (no API key required):

- `GET /api/info`
- `GET /api/checkpoints`
- `GET /checkpoints.dat`
- `GET /api/stats`
- `GET /api/stats/history`
- `GET /api/stats/insights`
- `GET /api/luck`
- `GET /api/status`
- `GET /api/blocks`
- `GET /api/payouts/recent`
- `GET /api/miner/{address}`
- `GET /api/miner/{address}/balance`
- `GET /api/miner/{address}/hashrate`

`/checkpoints.dat` serves the latest daemon-generated checkpoint file. By
default the API reads `<daemon_data_dir>/checkpoints.dat`; set
`checkpoints_path` in `config.json` to override that path.

Protected endpoints (API key required):

- `GET /api/miners`
- `GET /api/health`
- `GET /api/daemon/logs/stream`

`api_key` is required at API and monitor startup. `monitor_ingest_secret` is required for the signed Cloudflare monitor ingest endpoint.

Accepted header:

- `x-api-key: <api_key>`

Paged/filterable list endpoints:

- Shared query params: `limit`, `offset`.
- `GET /api/miners`: `search`, `sort`.
- `GET /api/blocks`: `status`.

Daemon log stream details:

- Admin UI includes a live daemon log viewer tab.
- Stream endpoint: `GET /api/daemon/logs/stream?tail=200`.
- Log source fallback order:
  - `journalctl` for the active recovery daemon, then the managed primary and standby units
  - `tail -F <daemon_data_dir>/debug.log`

## Web UI

- `GET /` (dashboard)
- Multi-tab WebUI includes:
  - pool + onboarding info (`/api/info`)
  - API key auth UX for protected routes
  - miner lookup
  - public blocks, payouts, luck, and status pages
  - admin miners, balances, shares, rewards, recovery, and logs tabs
  - live trend charts
  - operator health panel (`/api/health`)
  - live daemon logs panel (`/api/daemon/logs/stream`)

## Stratum Notes

- Login requires protocol v2 with declared capabilities.
- Login rejects malformed payout addresses early (base58 + checksum-compatible Blocknet stealth address validation)
- Login requires protocol v2 + `submit_claimed_hash`; submits without `claimed_hash` are rejected.
- Per-connection submit rate limiting is enabled.
- Queue pressure returns `server busy, retry` (no inline bypass)
- Per-connection vardiff retargeting is enabled by default to target a small number of shares per window (`vardiff_*` config keys)
- Vardiff difficulty is cached per `address+worker` and reused on reconnect/restart when the hint is fresh (1h TTL), reducing post-restart ramp-up.
- Default vardiff profile assumes a weak baseline miner and aims for ~10 shares / 5 minutes (`initial_share_difficulty=60`, `vardiff_target_shares=10`)
- Template refresh identity uses stable tip fields (`height`, `network_target`, `prev_hash`) to avoid daemon template churn while still refreshing on meaningful tip/template transitions.
- Assignment submits on a previous template are accepted only inside a short grace window (`stale_submit_grace`, default `8s`) based on when the share was received.
- Daemon SSE tip events mark templates stale from `new_block` `hash` + `height`.
- Timestamp-only `new_block` changes do not trigger refreshes; only hash/height changes can trigger staleness.
- Same-height hash-change refresh is enabled by default (`refresh_on_same_height=true`) so the pool reacts immediately to same-height reorgs. Set it to `false` only if an operator intentionally wants to coalesce those events.
- Daemons that advertise `template_expires_at_unix_ms` receive a cheap liveness renewal every 30 seconds (and never later than half-life) without replacing miner work. A rejected or malformed renewal queues a forced template replacement.
- A successful SSE connection/reconnect forces a same-tip template rotation because daemon template IDs are process-local. Against older daemons without renewal support, the pool bounds polling-only restart detection with the same 30-second rotation cadence.

## Payout Safeguards

- `payouts_enabled` toggles payout sending globally.
- `payout_pause_file` pauses payouts when the file exists.
- `pplns_window_duration` controls the time-based PPLNS lookback window (default `6h`).
- `payout_provisional_cap_multiplier` caps aged provisional difficulty relative to verified difficulty so undersampled miners receive reduced provisional credit instead of being fully excluded.
- Keep `sample_rate` and `min_sample_every` comfortably above the payout policy's full-credit target so honest miners do not sit under the cap for long.
- Optional caps:
  - `payout_max_recipients_per_tick`
  - `payout_max_total_per_tick`
  - `payout_max_per_recipient`

## Retention & Summary Rollups

- Retention worker runs hourly, keeping detailed shares for 90 days and payout rows for 365 days.
- Old rows are rolled up into summary tables before prune:
  - `share_daily_summaries`
  - `payout_daily_summaries`
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
