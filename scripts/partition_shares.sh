#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Convert the plain shares table to weekly partitions online (scripts/partition_shares.sql).

Usage:
  scripts/partition_shares.sh [options] <step>

Steps, in order (each is idempotent and safe to rerun):
  preflight     read-only checks; prints the cutoff and table/index sizes
  parent        create shares_partitioned, its indexes, triggers and partitions
  unique-index  build the (id, created_at) unique index concurrently
  check         add CHECK (created_at < cutoff) NOT VALID, then validate it
  swap          rename and attach in one short transaction, then ANALYZE
  migrate       parent, unique-index, check and swap in sequence
  status        show progress
  rollback      undo parent/unique-index/check; only before the swap

Options:
  --db CONNINFO       psql connection string (default: psql's PG* environment)
  --dry-run           print the DDL instead of running it
  --rehearse          swap only: run the swap transaction, then roll it back
  --cutoff UNIX       Monday 00:00 UTC to split at (default: the first Monday
                      at least --min-lead-hours away; recorded by `parent`)
  --min-lead-hours N  default 48
  --weeks N           weekly partitions the parent step creates (default 4)
  --lock-retries N    retries after a lock timeout (default 20)

Example on bntpool (run in tmux; unique-index and check take minutes). The
postgres user cannot read /opt/blocknet, so copy both files somewhere it can:
  d=/var/lib/postgresql/partition_shares
  sudo install -d -o postgres -g postgres -m 0750 "$d"
  sudo install -o postgres -g postgres -m 0640 scripts/partition_shares.sql "$d/"
  sudo install -o postgres -g postgres -m 0750 scripts/partition_shares.sh "$d/"
  cd /tmp && sudo -u postgres "$d/partition_shares.sh" --db 'dbname=blocknet_pool' preflight
bntpool was converted on 2026-10-29 (shares_legacy holds rows before
2026-11-02 00:00 UTC); `status` reports the layout.
EOF
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
sql_file="${script_dir}/partition_shares.sql"
db=""
dry_run=off
rehearse=off
cutoff=""
min_lead_hours=48
weeks=4
lock_retries=20
step=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db) db="$2"; shift 2 ;;
    --dry-run) dry_run=on; shift ;;
    --rehearse) rehearse=on; shift ;;
    --cutoff) cutoff="$2"; shift 2 ;;
    --min-lead-hours) min_lead_hours="$2"; shift 2 ;;
    --weeks) weeks="$2"; shift 2 ;;
    --lock-retries) lock_retries="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    *)
      if [[ -n "$step" ]]; then
        echo "only one step per run" >&2
        exit 2
      fi
      step="$1"
      shift
      ;;
  esac
done

if [[ -z "$step" ]]; then
  usage >&2
  exit 2
fi

run_step() {
  local name="$1"
  local attempt=0
  local log
  log="$(mktemp)"
  trap 'rm -f "$log"' RETURN
  while true; do
    echo "--- ${name} ($(date -u '+%Y-%m-%d %H:%M:%S UTC'))"
    local args=(-X -v ON_ERROR_STOP=1
      -v "ps_step=${name}" -v "ps_dry_run=${dry_run}" -v "ps_rehearse=${rehearse}"
      -v "ps_cutoff=${cutoff}" -v "ps_min_lead_hours=${min_lead_hours}" -v "ps_weeks=${weeks}"
      -f "$sql_file")
    if [[ -n "$db" ]]; then
      args=(-d "$db" "${args[@]}")
    fi
    if psql "${args[@]}" 2>&1 | tee "$log"; then
      if ! grep -q "ERROR:" "$log"; then
        return 0
      fi
    fi
    # Lock timeouts are expected under load; everything else stops the run.
    if grep -q "lock timeout" "$log" && (( attempt < lock_retries )); then
      attempt=$((attempt + 1))
      echo "lock timeout; retry ${attempt}/${lock_retries} in 5s" >&2
      sleep 5
      continue
    fi
    echo "step ${name} failed" >&2
    return 1
  done
}

case "$step" in
  preflight|parent|unique-index|check|swap|status|rollback)
    run_step "$step"
    ;;
  migrate)
    if [[ "$dry_run" == on || "$rehearse" == on ]]; then
      echo "dry-run or rehearse one step at a time; later steps need the earlier ones applied" >&2
      exit 2
    fi
    for name in preflight parent unique-index check swap; do
      run_step "$name"
    done
    run_step status
    ;;
  *)
    echo "unknown step: $step" >&2
    usage >&2
    exit 2
    ;;
esac
