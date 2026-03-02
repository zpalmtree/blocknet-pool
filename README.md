<p align="center">
  <img src="static/blocknet.png" width="128" height="128" alt="Blocknet">
</p>

<h1 align="center">Blocknet Pool</h1>

<p align="center">
  Simple Blocknet pool server (stratum + web UI + payouts).<br>
  Testing/dev focused right now.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/status-testing%2Fdev-aaff00?style=flat-square&labelColor=000" alt="Status">
  <img src="https://img.shields.io/badge/deployment-bntpool.com-aaff00?style=flat-square&labelColor=000" alt="Deployment">
  <img src="https://img.shields.io/badge/go-1.25.5-aaff00?style=flat-square&labelColor=000" alt="Go">
  <img src="https://img.shields.io/badge/license-BSD--3--Clause-aaff00?style=flat-square&labelColor=000" alt="License">
</p>

---

## Status

- Public test deployment: `https://bntpool.com`
- Project website: `https://blocknetcrypto.com`
- No official stratum miner in this repo yet
- This project is currently best treated as testing/dev software

---

## Before You Run It

- Argon2id uses about **2GB RAM per mining thread**
- Power + hardware cost can be high
- This is usually not a profit-first setup

### Trust Model (Important)

- Best fit: one trusted operator combining their own devices/workers
- Verify every share: strongest safety, highest resource cost
- Verify only a sample: lower cost, higher poisoning/reputation risk

---

## Quick Start

```bash
go build
./blocknet-pool init
# edit config.json (see required keys below)
# edit .env and set BLOCKNET_WALLET_PASSWORD
./blocknet-pool
```

Custom config:

```bash
./blocknet-pool -c /path/to/config.json
```

---

## Required Config Keys

In `config.json`, set these first:

- `pool_name`
- `pool_url`
- `stratum_port`
- `api_host`
- `api_port`
- `daemon_api`
- `daemon_data_dir` (for `api.cookie`)
- `validation_mode` (`probabilistic` or `full`)
- `max_verifiers`
- `max_validation_queue`
- `sample_rate`
- `warmup_shares`
- `min_sample_every`
- `invalid_sample_threshold`
- `invalid_sample_min`
- `forced_verify_duration`
- `quarantine_duration`
- `max_quarantine_duration`
- `provisional_share_delay`
- `max_provisional_shares`
- `stratum_submit_v2_required`
- `pool_fee_flat` / `pool_fee_pct`
- `payout_scheme` (`pplns` recommended)
- `pplns_window_duration` (example: `24h`)
- `blocks_before_payout`
- `min_payout_amount`
- `api_key` (optional, but recommended for public API)

Compatibility notes:

- `daemon_binary` exists in config but is not used by runtime
- `pool_wallet_address` is deprecated (runtime reads daemon wallet address)

---

## Daemon Requirements

Run a Blocknet daemon with API enabled and reachable by this pool.

Required daemon endpoints:

- `GET /api/status`
- `GET /api/mining/blocktemplate`
- `POST /api/mining/submitblock`
- `GET /api/events`
- `GET /api/block/{id}`
- `GET /api/wallet/address`
- `GET /api/wallet/balance`
- `POST /api/wallet/send`
- `POST /api/wallet/load`
- `POST /api/wallet/unlock`

Daemon auth token:

- use `daemon_token` if provided
- else pool reads `<daemon_data_dir>/api.cookie`

---

## Basic Verification

After startup:

1) Open UI: `http://<api_host>:<api_port>/`  
2) Check stats: `GET /api/pool/stats`  
3) Check logs: `GET /api/pool/logs?lines=200` or local `pool.log`

---

## Payout Behavior

Payout flow:

- Confirm found blocks.
- Distribute rewards (`pplns` or `proportional`).
- Send wallet payouts above `min_payout_amount`.

Wallet auto recovery:

- `BLOCKNET_WALLET_PASSWORD` (required for automatic wallet load/unlock)

Safety:

- If wallet is locked, pool tries unlock if password env is set.
- If no wallet is loaded, pool auto-loads and unlocks when password env is set.
- View-only wallet => payouts are skipped.

---

## Routes

Web pages:

- `/`
- `/miners/{address}`
- `/blocks`
- `/payouts`

Pool API:

- `/api/pool/stats`
- `/api/pool/miners`
- `/api/pool/blocks`
- `/api/pool/payouts`
- `/api/pool/miner/{address}`
- `/api/pool/logs?lines=N`

API security:

- if `api_key` is set, all `/api/pool/*` routes require auth
- use `Authorization: Bearer <key>` or `X-API-Key: <key>`
- rate limit is about 30 requests/minute per IP

---

## Stratum Limits

- max 4096 total connections
- max 16 connections per source IP
- miner must login within 30s
- duplicate shares are rejected (memory + 24h persistent dedup)

Stratum submit compatibility:

- v1: `{job_id, nonce}`
- v2: `{job_id, nonce, claimed_hash}`
- set `stratum_submit_v2_required=true` to require v2 payloads

Validation behavior:

- `validation_mode=probabilistic` is the default for scale
- candidate shares (potential blocks) are always fully verified
- regular shares are sampled with warmup + periodic forced checks
- addresses can be forced into full verification based on invalid sample rate
- high-risk addresses are persisted in DB with quarantine + force-verify state
- provisional (unverified) shares are capped per address via `max_provisional_shares`
