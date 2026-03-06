#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Migrate a blocknet-pool SQLite database into PostgreSQL.

Usage:
  scripts/migrate_sqlite_to_postgres.sh --sqlite /path/to/pool.db --postgres postgres://user:pass@127.0.0.1:5432/blocknet_pool

Notes:
  - The destination tables are truncated before import.
  - This script creates the expected pool schema/indexes if missing.
  - Run with pool service stopped to avoid writing while copying.
EOF
}

sqlite_db=""
postgres_url=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sqlite)
      sqlite_db="${2:-}"
      shift 2
      ;;
    --postgres)
      postgres_url="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${sqlite_db}" || -z "${postgres_url}" ]]; then
  usage >&2
  exit 1
fi

if [[ ! -f "${sqlite_db}" ]]; then
  echo "sqlite file not found: ${sqlite_db}" >&2
  exit 1
fi

for cmd in sqlite3 psql; do
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "missing required command: ${cmd}" >&2
    exit 1
  fi
done

run_psql() {
  psql "${postgres_url}" -v ON_ERROR_STOP=1 "$@"
}

sqlite_has_table() {
  local table="$1"
  local found
  found="$(sqlite3 "${sqlite_db}" "SELECT 1 FROM sqlite_master WHERE type='table' AND name='${table}' LIMIT 1;")"
  [[ "${found}" == "1" ]]
}

copy_csv_table() {
  local table="$1"
  local cols="$2"
  local select_sql="$3"
  local out_file="$4"

  if ! sqlite_has_table "${table}"; then
    echo "[skip] sqlite table missing: ${table}"
    return
  fi

  local count
  count="$(sqlite3 "${sqlite_db}" "SELECT COUNT(*) FROM ${table};")"
  if [[ "${count}" == "0" ]]; then
    echo "[ok] ${table}: 0 rows"
    return
  fi

  sqlite3 -csv -noheader "${sqlite_db}" "${select_sql}" > "${out_file}"
  run_psql -c "\\copy ${table} (${cols}) FROM '${out_file}' WITH (FORMAT csv)"
  echo "[ok] ${table}: ${count} rows"
}

tmp_dir="$(mktemp -d -t bntpool-sqlite2pg-XXXXXX)"
trap 'rm -rf "${tmp_dir}"' EXIT

echo "==> ensuring postgres schema"
run_psql <<'SQL'
CREATE TABLE IF NOT EXISTS shares (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    miner TEXT NOT NULL,
    worker TEXT NOT NULL,
    difficulty BIGINT NOT NULL,
    nonce BIGINT NOT NULL,
    status TEXT NOT NULL,
    was_sampled BOOLEAN NOT NULL,
    block_hash TEXT,
    created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shares_created_at ON shares(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_shares_miner_created ON shares(miner, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_shares_miner_status_created ON shares(miner, status, created_at DESC);

CREATE TABLE IF NOT EXISTS blocks (
    height BIGINT PRIMARY KEY,
    hash TEXT NOT NULL,
    difficulty BIGINT NOT NULL,
    finder TEXT NOT NULL,
    finder_worker TEXT NOT NULL,
    reward BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    confirmed BOOLEAN NOT NULL,
    orphaned BOOLEAN NOT NULL,
    paid_out BOOLEAN NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_blocks_finder ON blocks(finder);
CREATE INDEX IF NOT EXISTS idx_blocks_finder_height ON blocks(finder, height DESC);
CREATE INDEX IF NOT EXISTS idx_blocks_unconfirmed ON blocks(confirmed, orphaned, height ASC);

CREATE TABLE IF NOT EXISTS balances (
    address TEXT PRIMARY KEY,
    pending BIGINT NOT NULL,
    paid BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS payouts (
    id BIGSERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    amount BIGINT NOT NULL,
    fee BIGINT NOT NULL DEFAULT 0,
    tx_hash TEXT NOT NULL,
    timestamp BIGINT NOT NULL
);
ALTER TABLE payouts ADD COLUMN IF NOT EXISTS fee BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_payouts_timestamp ON payouts(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_payouts_address_timestamp ON payouts(address, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_payouts_tx_hash ON payouts(tx_hash);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS seen_shares (
    job_id TEXT NOT NULL,
    nonce BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    PRIMARY KEY (job_id, nonce)
);
CREATE INDEX IF NOT EXISTS idx_seen_shares_expiry ON seen_shares(expires_at);

CREATE TABLE IF NOT EXISTS pending_payouts (
    address TEXT PRIMARY KEY,
    amount BIGINT NOT NULL,
    initiated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS pool_fee_events (
    id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL UNIQUE,
    amount BIGINT NOT NULL,
    fee_address TEXT NOT NULL,
    timestamp BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pool_fee_events_timestamp ON pool_fee_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_pool_fee_events_fee_address ON pool_fee_events(fee_address);

CREATE TABLE IF NOT EXISTS address_risk (
    address TEXT PRIMARY KEY,
    strikes BIGINT NOT NULL,
    last_reason TEXT,
    last_event_at BIGINT,
    quarantined_until BIGINT,
    force_verify_until BIGINT
);

CREATE TABLE IF NOT EXISTS vardiff_hints (
    address TEXT NOT NULL,
    worker TEXT NOT NULL,
    difficulty BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (address, worker)
);
CREATE INDEX IF NOT EXISTS idx_vardiff_hints_updated_at ON vardiff_hints(updated_at DESC);

CREATE TABLE IF NOT EXISTS stat_snapshots (
    id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    hashrate DOUBLE PRECISION NOT NULL,
    miners INTEGER NOT NULL,
    workers INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_stat_snapshots_timestamp ON stat_snapshots(timestamp DESC);

CREATE TABLE IF NOT EXISTS share_daily_summaries (
    day_start BIGINT PRIMARY KEY,
    accepted_count BIGINT NOT NULL,
    rejected_count BIGINT NOT NULL,
    accepted_difficulty BIGINT NOT NULL,
    unique_miners BIGINT NOT NULL,
    unique_workers BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_share_daily_summaries_day_start
    ON share_daily_summaries(day_start DESC);

CREATE TABLE IF NOT EXISTS payout_daily_summaries (
    day_start BIGINT PRIMARY KEY,
    payout_count BIGINT NOT NULL,
    total_amount BIGINT NOT NULL,
    total_fee BIGINT NOT NULL,
    unique_recipients BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_payout_daily_summaries_day_start
    ON payout_daily_summaries(day_start DESC);
SQL

echo "==> truncating destination tables"
run_psql -c "TRUNCATE TABLE shares, blocks, balances, payouts, meta, seen_shares, pending_payouts, pool_fee_events, address_risk, vardiff_hints, stat_snapshots, share_daily_summaries, payout_daily_summaries RESTART IDENTITY;"

echo "==> importing rows from sqlite"
copy_csv_table "shares" "id,job_id,miner,worker,difficulty,nonce,status,was_sampled,block_hash,created_at" \
  "SELECT id,job_id,miner,worker,difficulty,nonce,status,was_sampled,block_hash,created_at FROM shares ORDER BY id;" \
  "${tmp_dir}/shares.csv"

copy_csv_table "blocks" "height,hash,difficulty,finder,finder_worker,reward,timestamp,confirmed,orphaned,paid_out" \
  "SELECT height,hash,difficulty,finder,finder_worker,reward,timestamp,confirmed,orphaned,paid_out FROM blocks ORDER BY height;" \
  "${tmp_dir}/blocks.csv"

copy_csv_table "balances" "address,pending,paid" \
  "SELECT address,pending,paid FROM balances;" \
  "${tmp_dir}/balances.csv"

copy_csv_table "payouts" "id,address,amount,fee,tx_hash,timestamp" \
  "SELECT id,address,amount,fee,tx_hash,timestamp FROM payouts ORDER BY id;" \
  "${tmp_dir}/payouts.csv"

if sqlite_has_table "meta"; then
  meta_count="$(sqlite3 "${sqlite_db}" "SELECT COUNT(*) FROM meta;")"
  if [[ "${meta_count}" != "0" ]]; then
    sqlite3 -csv -noheader "${sqlite_db}" "SELECT key,hex(value) FROM meta;" > "${tmp_dir}/meta.csv"
    run_psql <<SQL
CREATE TEMP TABLE _meta_import (
  key TEXT,
  value_hex TEXT
);
\copy _meta_import (key, value_hex) FROM '${tmp_dir}/meta.csv' WITH (FORMAT csv);
INSERT INTO meta(key, value)
SELECT key, decode(value_hex, 'hex')
FROM _meta_import;
DROP TABLE _meta_import;
SQL
  fi
  echo "[ok] meta: ${meta_count} rows"
else
  echo "[skip] sqlite table missing: meta"
fi

copy_csv_table "seen_shares" "job_id,nonce,expires_at" \
  "SELECT job_id,nonce,expires_at FROM seen_shares;" \
  "${tmp_dir}/seen_shares.csv"

copy_csv_table "pending_payouts" "address,amount,initiated_at" \
  "SELECT address,amount,initiated_at FROM pending_payouts;" \
  "${tmp_dir}/pending_payouts.csv"

copy_csv_table "pool_fee_events" "id,block_height,amount,fee_address,timestamp" \
  "SELECT id,block_height,amount,fee_address,timestamp FROM pool_fee_events ORDER BY id;" \
  "${tmp_dir}/pool_fee_events.csv"

copy_csv_table "address_risk" "address,strikes,last_reason,last_event_at,quarantined_until,force_verify_until" \
  "SELECT address,strikes,last_reason,last_event_at,quarantined_until,force_verify_until FROM address_risk;" \
  "${tmp_dir}/address_risk.csv"

copy_csv_table "vardiff_hints" "address,worker,difficulty,updated_at" \
  "SELECT address,worker,difficulty,updated_at FROM vardiff_hints;" \
  "${tmp_dir}/vardiff_hints.csv"

copy_csv_table "stat_snapshots" "id,timestamp,hashrate,miners,workers" \
  "SELECT id,timestamp,hashrate,miners,workers FROM stat_snapshots ORDER BY id;" \
  "${tmp_dir}/stat_snapshots.csv"

copy_csv_table "share_daily_summaries" "day_start,accepted_count,rejected_count,accepted_difficulty,unique_miners,unique_workers" \
  "SELECT day_start,accepted_count,rejected_count,accepted_difficulty,unique_miners,unique_workers FROM share_daily_summaries ORDER BY day_start;" \
  "${tmp_dir}/share_daily_summaries.csv"

copy_csv_table "payout_daily_summaries" "day_start,payout_count,total_amount,total_fee,unique_recipients" \
  "SELECT day_start,payout_count,total_amount,total_fee,unique_recipients FROM payout_daily_summaries ORDER BY day_start;" \
  "${tmp_dir}/payout_daily_summaries.csv"

echo "==> aligning sequences"
run_psql <<'SQL'
SELECT setval(
  pg_get_serial_sequence('shares', 'id'),
  COALESCE((SELECT MAX(id) FROM shares), 1),
  EXISTS(SELECT 1 FROM shares)
);
SELECT setval(
  pg_get_serial_sequence('payouts', 'id'),
  COALESCE((SELECT MAX(id) FROM payouts), 1),
  EXISTS(SELECT 1 FROM payouts)
);
SELECT setval(
  pg_get_serial_sequence('pool_fee_events', 'id'),
  COALESCE((SELECT MAX(id) FROM pool_fee_events), 1),
  EXISTS(SELECT 1 FROM pool_fee_events)
);
SELECT setval(
  pg_get_serial_sequence('stat_snapshots', 'id'),
  COALESCE((SELECT MAX(id) FROM stat_snapshots), 1),
  EXISTS(SELECT 1 FROM stat_snapshots)
);
SQL

echo "==> verifying row counts"
run_psql <<'SQL'
SELECT 'shares' AS table_name, COUNT(*) AS rows FROM shares
UNION ALL SELECT 'blocks', COUNT(*) FROM blocks
UNION ALL SELECT 'balances', COUNT(*) FROM balances
UNION ALL SELECT 'payouts', COUNT(*) FROM payouts
UNION ALL SELECT 'meta', COUNT(*) FROM meta
UNION ALL SELECT 'seen_shares', COUNT(*) FROM seen_shares
UNION ALL SELECT 'pending_payouts', COUNT(*) FROM pending_payouts
UNION ALL SELECT 'pool_fee_events', COUNT(*) FROM pool_fee_events
UNION ALL SELECT 'address_risk', COUNT(*) FROM address_risk
UNION ALL SELECT 'vardiff_hints', COUNT(*) FROM vardiff_hints
UNION ALL SELECT 'stat_snapshots', COUNT(*) FROM stat_snapshots
UNION ALL SELECT 'share_daily_summaries', COUNT(*) FROM share_daily_summaries
UNION ALL SELECT 'payout_daily_summaries', COUNT(*) FROM payout_daily_summaries
ORDER BY table_name;
SQL

echo "migration complete"
