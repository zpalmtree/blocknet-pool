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

## Runtime Components

- Stratum server
- Template/job manager
- Validation engine (bounded queues)
- Persistent storage (SQLite)
- Payout processor
- HTTP API

## Core API Endpoints

- `GET /api/stats`
- `GET /api/miner/{address}`
- `GET /api/miners`
- `GET /api/blocks`
- `GET /api/payouts`
- `GET /api/fees`

## Stratum Notes

- Login supports protocol negotiation (`protocol_version`, `capabilities`)
- `stratum_submit_v2_required=true` (default): requires protocol v2 + `submit_claimed_hash`
- `stratum_submit_v2_required=false`: allows legacy submits without `claimed_hash` (full verification path)
- Queue pressure returns `server busy, retry` (no inline bypass)
- Per-connection vardiff retargeting is enabled by default to target a small number of shares per window (`vardiff_*` config keys)
- Default vardiff profile assumes a weak baseline miner and aims for ~10 shares / 5 minutes (`initial_share_difficulty=60`, `vardiff_target_shares=10`)
- Template refresh identity is intentionally conservative (`height` + `network_target`) to avoid spamming miners with new jobs when template responses change only in ephemeral fields. Tradeoff: same-height template updates that keep the same target may not trigger a new job immediately.
- Assignment submits on a previous template are accepted only inside a short grace window (`stale_submit_grace`, default `5s`) based on when the share was received.
- Daemon SSE tip events (`/api/events`) are enabled by default (`sse_enabled=true`) and mark templates stale from `new_block` `hash` + `height`.
- Timestamp-only `new_block` changes do not trigger refreshes; only hash/height changes can trigger staleness.
- Same-height hash-change refresh is disabled by default (`refresh_on_same_height=false`) to avoid replay churn; enable it only if you want immediate same-height reorg reaction.

## Review Follow-Ups (2026-03-03)

- Address risk escalation (`strikes`, quarantine extension, force-verify extension) should use an atomic update path per address. Current behavior is functionally correct in single-threaded cases but can lose increments under concurrent escalations.
- Accepted-share hashrate tracking currently keeps a full 1-hour time window in memory. Add a hard count cap so high-throughput pools do not grow this queue without bound.
- Template refresh matching should remain resistant to timestamp-only churn, but should refresh when stable template fields change. Candidate fields to include in identity comparison are `height`, `network_target`, `template_id`, and `header_base` (or an equivalent stable digest of header/base template material).
- API key comparison is currently direct string equality by design for this pool deployment model; this is accepted for now and is not treated as a blocker.

## API Auth

Public endpoints (no API key required):

- `GET /api/stats`
- `GET /api/miner/{address}`

Protected endpoints (API key required):

- `GET /api/miners`
- `GET /api/blocks`
- `GET /api/payouts`
- `GET /api/fees`

When `api_key` is unset, protected endpoints return `503 api key not configured`.

Accepted headers:

- `x-api-key: <api_key>`
- `Authorization: Bearer <api_key>`

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
