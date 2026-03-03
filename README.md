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
- Validation engine (bounded queues, candidate priority)
- Persistent storage (SQLite)
- Payout processor
- HTTP API

## Core API Endpoints

- `GET /api/stats`
- `GET /api/miners`
- `GET /api/miner/{address}`
- `GET /api/blocks`
- `GET /api/payouts`

## Stratum Notes

- Login supports protocol negotiation (`protocol_version`, `capabilities`)
- Submit supports legacy and v2 payloads
- Candidate shares are prioritized in validation
- Queue pressure behavior preserves legacy inline handling for no-claimed-hash submits
- Per-connection vardiff retargeting is enabled by default to target a small number of shares per window (`vardiff_*` config keys)
- Default vardiff profile assumes a weak baseline miner and aims for ~10 shares / 5 minutes (`initial_share_difficulty=60`, `vardiff_target_shares=10`)

## API Auth

Set `api_key` in `config.json` to require authentication for `/api/*` routes.

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
