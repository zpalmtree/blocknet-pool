#!/usr/bin/env bash
# One step of the connection-scale load test: mock daemon + gateway + one
# Stratum engine on this host, then pool_conn_load.py shards against the
# gateway. Writes summary.json and resource samples to --out.
#
#   scripts/conn_loadtest.sh --bin-dir target/release --postgres-url URL \
#     --addresses-file addrs.txt --connections 30000 --out results/30k
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bin_dir=""
postgres_url=""
addresses_file=""
connections=10000
shards=8
source_ips="127.0.1.1-127.0.1.64"
shares_per_sec=0.003
ramp_secs=120
duration_secs=300
block_secs=60
max_connections=200000
max_per_ip=200000
out=""
socket=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bin-dir) bin_dir="$2"; shift 2 ;;
    --postgres-url) postgres_url="$2"; shift 2 ;;
    --addresses-file) addresses_file="$2"; shift 2 ;;
    --connections) connections="$2"; shift 2 ;;
    --shards) shards="$2"; shift 2 ;;
    --source-ips) source_ips="$2"; shift 2 ;;
    --shares-per-sec) shares_per_sec="$2"; shift 2 ;;
    --ramp-secs) ramp_secs="$2"; shift 2 ;;
    --duration-secs) duration_secs="$2"; shift 2 ;;
    --block-secs) block_secs="$2"; shift 2 ;;
    --max-connections) max_connections="$2"; shift 2 ;;
    --max-per-ip) max_per_ip="$2"; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    --socket) socket="$2"; shift 2 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
[[ -n "$bin_dir" && -n "$postgres_url" && -n "$addresses_file" && -n "$out" ]] || {
  echo "need --bin-dir, --postgres-url, --addresses-file and --out" >&2; exit 2; }

mkdir -p "$out"
out="$(cd "$out" && pwd)"
run="$out/run"
mkdir -p "$run"
daemon_port=18899
stratum_port=3333
api_port=18898
gateway_metrics=127.0.0.1:18897
# Unix socket paths are limited to ~107 bytes.
socket="${socket:-$run/gateway.sock}"
rm -f "$socket"
pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

python3 - "$SCRIPT_DIR/../config.example.json" "$run/config.json" "$addresses_file" <<PY
import json, sys
cfg = json.load(open(sys.argv[1]))
fee_address = next(l.strip() for l in open(sys.argv[3]) if l.strip())
cfg.update({
    "pool_fee_wallet_address": fee_address, "warmup_shares": 0,
    "stratum_host": "0.0.0.0", "stratum_port": $stratum_port,
    "api_host": "127.0.0.1", "api_port": $api_port,
    "daemon_api": "http://127.0.0.1:$daemon_port",
    "database_url": "$postgres_url",
    "gateway_socket": "$socket", "gateway_metrics_addr": "$gateway_metrics",
    "gateway_max_miners": $max_connections,
    "stratum_max_connections": $max_connections,
    "stratum_max_connections_per_ip": $max_per_ip,
    "validation_mode": "probabilistic", "sample_rate": 0.0, "min_sample_every": 0,
    "growth_submit_rate": 0.0,
    "payouts_enabled": False, "pool_fee_pct": 0.0,
    "blocks_before_payout": 1000000, "job_timeout": "30s",
})
json.dump(cfg, open(sys.argv[2], "w"), indent=2)
PY

python3 "$SCRIPT_DIR/mock_daemon.py" --port "$daemon_port" --block-secs "$block_secs" \
  --wallet-address "$(grep -m1 . "$addresses_file")" \
  >"$run/daemon.log" 2>&1 &
pids+=($!)
RUST_LOG=warn "$bin_dir/blocknet-pool-gateway" --config "$run/config.json" >"$run/gateway.log" 2>&1 &
gateway_pid=$!; pids+=($gateway_pid)
for _ in $(seq 1 50); do [[ -S "$socket" ]] && break; sleep 0.2; done
BLOCKNET_POOL_INSTANCE=a RUST_LOG=warn "$bin_dir/blocknet-pool-stratum" --config "$run/config.json" \
  >"$run/stratum.log" 2>&1 &
stratum_pid=$!; pids+=($stratum_pid)
for _ in $(seq 1 150); do
  curl -sf "http://$gateway_metrics/metrics" | grep -q '^blocknet_pool_gateway_engines 1' && break
  sleep 1
done
curl -sf "http://$gateway_metrics/metrics" | grep -q '^blocknet_pool_gateway_engines 1' || {
  echo "engine never connected to the gateway" >&2; tail -20 "$run/stratum.log" >&2; exit 1; }

# Resource samples every 5 s: gateway and engine CPU% and RSS, gateway metrics.
(
  while sleep 5; do
    ts=$(date +%s)
    for name in gateway:$gateway_pid stratum:$stratum_pid; do
      read -r cpu rss < <(ps -o %cpu=,rss= -p "${name#*:}" || echo "0 0")
      echo "$ts ${name%%:*} cpu=$cpu rss_kb=$rss"
    done
    curl -sf "http://$gateway_metrics/metrics" \
      | grep -E '^blocknet_pool_gateway_(miners|held_connections|rejected_per_ip_total) |latency_millis_(sum|count)\{lane="(miner_line_wait|engine_frame_wait)"' \
      | sed "s/^/$ts /"
  done
) >"$out/samples.txt" 2>&1 &
pids+=($!)

shard_pids=()
for s in $(seq 0 $((shards - 1))); do
  python3 "$SCRIPT_DIR/pool_conn_load.py" --port "$stratum_port" --connections "$connections" \
    --shard "$s" --shards "$shards" --source-ips "$source_ips" --addresses-file "$addresses_file" \
    --shares-per-sec "$shares_per_sec" --ramp-secs "$ramp_secs" --duration-secs "$duration_secs" \
    --out "$out/shard-$s.json" 2>"$out/shard-$s.log" &
  shard_pids+=($!)
done
for pid in "${shard_pids[@]}"; do wait "$pid"; done
python3 "$SCRIPT_DIR/pool_conn_load.py" --merge "$out"/shard-*.json >"$out/summary.json"
cat "$out/summary.json"
