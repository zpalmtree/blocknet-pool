#!/usr/bin/env bash
# Reproduce the 2026-10-21 "server busy" incident against one Stratum binary:
# a steady population of independent miners, a small set of addresses under
# forced verification, and partway through a CPU farm (hundreds of
# connections, one address and worker name, spread over a few dozen IPs).
#
# Uses scripts/mock_daemon.py and the hashrate mode of scripts/pool_load.py.
# Shares are fake (claimed_hash = target), so validation is configured to
# never sample regular shares (sample_rate=0, warmup_shares=0,
# min_sample_every=0) and source penalties run in shadow mode. The forced
# population's shares still take the synchronous Argon2 verify path, which is
# the lane-stall mechanism under test; they fail verification, which is fine
# because invalid_sample_threshold=1.0 keeps that from escalating. Temporary
# provisional-backlog/payout-coverage holds are disabled (see the config).
#
# Run it once per binary (baseline, candidate) with the same arguments.
#
# Scaling used for the 2026-10-25 comparison. The local baseline accepts ~290
# shares/s against ~57/s in production, so vardiff is time-scaled 5x
# (--vardiff-window 1m, 10 shares: a 6s target) to keep production's ratios
# at production-like connection counts: a steady population at ~0.5x the
# ceiling, a farm whose equilibrium is another ~0.5x and whose connect burst
# at the initial difficulty is ~3.5x:
#   --base-workers 780 --base-hashrate 10 --farm-workers 800 --farm-hashrate 68
#   --farm-ips 30 --duration-secs 480 --farm-start-secs 90
#   --vardiff-window 1m --vardiff-target-shares 10
# (--farm-hashrate 200 is the stress variant). Each Argon2 verifier holds a
# 2 GiB arena, so mind memory limits; the summary prints the Stratum peak RSS
# and its cgroup's memory.events.
#
# Valid addresses (checksummed, with real Ristretto keys) can be generated
# with curve25519-dalek: two RistrettoPoint::mul_base points, base58 of
# spend || view || sha3("blocknet_stealth_address_checksum" ||
# "blocknet_mainnet" || spend || view)[..4].
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  BLOCKNET_POOL_BENCH_POSTGRES_URL=postgres://... \
  bash scripts/bench_busy_incident.sh \
    --stratum-bin <path> --addresses-file <path> --output-dir <path> \
    [--label <name>] [--duration-secs 480] [--farm-start-secs 90] \
    [--base-workers 300] [--base-hashrate 20] \
    [--farm-workers 800] [--farm-hashrate 15] [--farm-ips 30] [--farm-ramp-secs 60] \
    [--forced-workers 3] [--forced-hashrate 10] \
    [--vardiff-window 5m] [--vardiff-target-shares 10] \
    [--block-secs 15] [--extra-config '{"key": value}']

The addresses file needs one valid miner address per line: base workers use
lines [0, base), the farm uses line base, forced workers use the lines after.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stratum_bin=""
addresses_file=""
output_dir=""
label="run"
duration_secs=480
farm_start_secs=90
base_workers=300
base_hashrate=20
farm_workers=800
farm_hashrate=15
farm_ips=30
farm_ramp_secs=60
forced_workers=3
forced_hashrate=10
vardiff_window="5m"
vardiff_target_shares=10
block_secs=15
extra_config="{}"
postgres_url="${BLOCKNET_POOL_BENCH_POSTGRES_URL:-${BLOCKNET_POOL_TEST_POSTGRES_URL:-}}"

while (($#)); do
    case "$1" in
        --stratum-bin) stratum_bin="$2"; shift 2 ;;
        --addresses-file) addresses_file="$2"; shift 2 ;;
        --output-dir) output_dir="$2"; shift 2 ;;
        --label) label="$2"; shift 2 ;;
        --duration-secs) duration_secs="$2"; shift 2 ;;
        --farm-start-secs) farm_start_secs="$2"; shift 2 ;;
        --base-workers) base_workers="$2"; shift 2 ;;
        --base-hashrate) base_hashrate="$2"; shift 2 ;;
        --farm-workers) farm_workers="$2"; shift 2 ;;
        --farm-hashrate) farm_hashrate="$2"; shift 2 ;;
        --farm-ips) farm_ips="$2"; shift 2 ;;
        --farm-ramp-secs) farm_ramp_secs="$2"; shift 2 ;;
        --forced-workers) forced_workers="$2"; shift 2 ;;
        --forced-hashrate) forced_hashrate="$2"; shift 2 ;;
        --vardiff-window) vardiff_window="$2"; shift 2 ;;
        --vardiff-target-shares) vardiff_target_shares="$2"; shift 2 ;;
        --block-secs) block_secs="$2"; shift 2 ;;
        --extra-config) extra_config="$2"; shift 2 ;;
        --help|-h) usage; exit 0 ;;
        *) echo "error: unknown argument: $1" >&2; usage; exit 1 ;;
    esac
done

if [[ -z "$stratum_bin" || -z "$addresses_file" || -z "$output_dir" || -z "$postgres_url" ]]; then
    usage
    exit 1
fi
stratum_bin="$(cd "$(dirname "$stratum_bin")" && pwd)/$(basename "$stratum_bin")"
mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd)"

base_port="$((21000 + (RANDOM % 8000)))"
daemon_port="$base_port"
stratum_port="$((base_port + 1))"
schema="blocknet_pool_busy_${$}_$RANDOM"
daemon_pid=""
stratum_pid=""
load_pids=()

cleanup() {
    for pid in "${load_pids[@]}" "$stratum_pid" "$daemon_pid"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    psql "$postgres_url" -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS ${schema} CASCADE" >/dev/null 2>&1 || true
}
trap cleanup EXIT

psql "$postgres_url" -v ON_ERROR_STOP=1 -c "CREATE SCHEMA ${schema}" >/dev/null
database_url="$(python3 - "$postgres_url" "$schema" <<'PY'
import sys
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
parts = urlsplit(sys.argv[1])
query = parse_qsl(parts.query, keep_blank_values=True)
query.append(("options", f"-c search_path={sys.argv[2]}"))
print(urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, quote_via=quote), parts.fragment)))
PY
)"

config_path="$output_dir/config.json"
python3 - "$config_path" "$daemon_port" "$stratum_port" "$database_url" \
    "$vardiff_window" "$vardiff_target_shares" "$extra_config" <<'PY'
import json
import sys
path, daemon_port, stratum_port, database_url, window, target_shares, extra = sys.argv[1:8]
cfg = {
    "pool_name": "busy incident bench",
    "stratum_host": "127.0.0.1",
    "stratum_port": int(stratum_port),
    "daemon_api": f"http://127.0.0.1:{daemon_port}",
    "daemon_cookie_path": "/nonexistent/api.cookie",
    "database_url": database_url,
    "pool_fee_wallet_address": "",
    "payouts_enabled": False,
    # Production vardiff and validation-lane shape.
    "enable_vardiff": True,
    "initial_share_difficulty": 60,
    "vardiff_window": window,
    "vardiff_target_shares": int(target_shares),
    "vardiff_retarget_interval": "5s",
    "vardiff_tolerance": 0.25,
    "regular_submit_queue": 512,
    # One regular verifier: each Argon2 verifier holds a 2 GiB arena, and the
    # whole run must fit comfortably in memory (see the cgroup note in the
    # summary). Production runs 3.
    "regular_verifiers": 1,
    "audit_verifiers": 1,
    "database_pool_size": 8,
    # Fake shares: never sample regular shares, penalties in shadow mode, and
    # keep failed forced verifies from escalating into quarantine.
    "validation_mode": "probabilistic",
    "sample_rate": 0.0,
    "warmup_shares": 0,
    "min_sample_every": 0,
    "source_penalties_enabled": False,
    "invalid_sample_threshold": 1.0,
    # Temporary backlog/coverage holds persist forced_until, which makes the
    # engine sync-verify every share of the held address. With fake shares
    # and no sampling every address would be held, so disable both holds and
    # leave the seeded force-verify addresses as the only sync-verify load.
    "provisional_share_delay": "0s",
    "payout_provisional_cap_multiplier": 0.0,
}
cfg.update(json.loads(extra))
with open(path, "w", encoding="utf-8") as f:
    json.dump(cfg, f, indent=2)
    f.write("\n")
PY

python3 "$SCRIPT_DIR/mock_daemon.py" --port "$daemon_port" --block-secs "$block_secs" \
    >"$output_dir/mock-daemon.log" 2>&1 &
daemon_pid="$!"

wait_port() {
    python3 - "$1" "$2" <<'PY'
import socket, sys, time
port, timeout = int(sys.argv[1]), float(sys.argv[2])
deadline = time.time() + timeout
while time.time() < deadline:
    s = socket.socket()
    s.settimeout(0.3)
    try:
        s.connect(("127.0.0.1", port))
        sys.exit(0)
    except OSError:
        time.sleep(0.2)
    finally:
        s.close()
sys.exit(1)
PY
}
wait_port "$daemon_port" 20

(
    cd "$output_dir"
    ulimit -n 65536
    RUST_LOG="${RUST_LOG:-warn}" exec "$stratum_bin" --config "$config_path"
) >"$output_dir/stratum.log" 2>&1 &
stratum_pid="$!"
if ! wait_port "$stratum_port" 90; then
    echo "error: stratum did not start (see $output_dir/stratum.log)" >&2
    exit 1
fi

forced_offset="$((base_workers + 1))"
if ((forced_workers > 0)); then
    # Production saw a handful of addresses on forced verification at any
    # time; their shares take the synchronous full-verify path.
    sed -n "$((forced_offset + 1)),$((forced_offset + forced_workers))p" "$addresses_file" \
        | while read -r address; do
            psql "$database_url" -v ON_ERROR_STOP=1 -q -c \
                "INSERT INTO address_risk (address, strikes, last_reason, last_event_at, force_verify_until)
                 VALUES ('${address}', 0, 'bench forced verify', EXTRACT(EPOCH FROM now())::bigint,
                         EXTRACT(EPOCH FROM now())::bigint + 86400)
                 ON CONFLICT (address) DO UPDATE SET force_verify_until = EXCLUDED.force_verify_until"
        done
fi

run_population() {
    local name="$1"; shift
    python3 "$SCRIPT_DIR/pool_load.py" --host 127.0.0.1 --stratum-port "$stratum_port" \
        --addresses-file "$addresses_file" --label "$name" \
        --output "$output_dir/${name}.json" "$@" >"$output_dir/${name}.log" 2>&1 &
    load_pids+=("$!")
}

echo "[bench:$label] stratum=$stratum_bin port=$stratum_port schema=$schema"
run_population base --workers "$base_workers" --hashrate "$base_hashrate" \
    --address-mode unique --address-offset 0 --worker-prefix rig \
    --source-ips "127.0.2.1-127.0.2.200" --ramp-secs 20 --duration-secs "$duration_secs"
if ((forced_workers > 0)); then
    run_population forced --workers "$forced_workers" --hashrate "$forced_hashrate" \
        --address-mode unique --address-offset "$forced_offset" --worker-prefix forced \
        --source-ips "127.0.3.1-127.0.3.10" --ramp-secs 5 --duration-secs "$duration_secs"
fi
if ((farm_workers > 0)); then
    sleep "$farm_start_secs"
    echo "[bench:$label] farm connecting: $farm_workers conns from $farm_ips IPs"
    run_population farm --workers "$farm_workers" --hashrate "$farm_hashrate" \
        --address-mode shared --address-offset "$base_workers" --worker-prefix seine-3081 \
        --source-ips "127.0.1.1-127.0.1.${farm_ips}" --ramp-secs "$farm_ramp_secs" \
        --duration-secs "$((duration_secs - farm_start_secs))"
fi

for pid in "${load_pids[@]}"; do
    wait "$pid" || true
done
load_pids=()
# Peak resident memory of the Stratum process, to spot runs squeezed by a
# memory limit (a throttled run is not a valid comparison).
grep -E "VmHWM|VmRSS" "/proc/$stratum_pid/status" >"$output_dir/stratum_memory.txt" 2>/dev/null || true
cat "/sys/fs/cgroup$(cut -d: -f3 /proc/$stratum_pid/cgroup)/memory.events" \
    >>"$output_dir/stratum_memory.txt" 2>/dev/null || true

timeout 30 psql "$database_url" -At -c \
    "SELECT convert_from(value, 'UTF8') FROM meta WHERE key = 'live_runtime_snapshot_v1'" \
    >"$output_dir/runtime_snapshot.json" 2>/dev/null || true

python3 - "$output_dir" "$label" "$farm_start_secs" <<'PY' | tee "$output_dir/summary.txt"
import json
import os
import sys

out_dir, label, farm_start = sys.argv[1], sys.argv[2], int(sys.argv[3])

def load(name):
    path = os.path.join(out_dir, f"{name}.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)

base, farm, forced = load("base"), load("farm"), load("forced")
pops = {"base": base, "farm": farm, "forced": forced}

# Align timelines on wall-clock seconds since the base population started.
offsets = {"base": 0, "forced": 0, "farm": farm_start}
series = {}
for name, data in pops.items():
    if not data:
        continue
    for row in data["timeline"]:
        t = row["t"] + offsets[name]
        series.setdefault(t, {})[name] = row

def window(t0, t1, key, names=("base", "farm", "forced")):
    total = 0
    for t in range(t0, t1):
        for name in names:
            row = series.get(t, {}).get(name)
            if row:
                total += row.get(key, 0)
    return total / max(1, t1 - t0)

end = max(series) if series else 0
flood0 = farm_start + 30
pre = (20, farm_start)
flood = (flood0, end - 5)

ceiling = 0.0
span = min(30, max(1, end - 5 - flood0))
for t in range(flood0, max(flood0 + 1, end - 5 - span + 1), 5):
    ceiling = max(ceiling, window(t, t + span, "accepted"))

print(f"== {label} ==")
print(f"pre-flood   accepted/s total={window(*pre, 'accepted'):.1f} busy/s={window(*pre, 'busy'):.1f}")
print(f"flood       accepted/s total={window(*flood, 'accepted'):.1f} (best {span}s window {ceiling:.1f})")
print(f"flood       busy/s total={window(*flood, 'busy'):.1f} rate_limited/s={window(*flood, 'rate_limited'):.1f}")
for name in ("base", "farm", "forced"):
    if not pops.get(name):
        continue
    sub = window(*flood, "submitted", (name,))
    acc = window(*flood, "accepted", (name,))
    busy = window(*flood, "busy", (name,))
    pct = 100.0 * busy / sub if sub else 0.0
    print(f"flood {name:>6}: submitted/s={sub:.1f} accepted/s={acc:.1f} busy/s={busy:.1f} busy%={pct:.1f}")

def diff_track(name):
    data = pops.get(name)
    if not data:
        return ""
    rows = [r for r in data["timeline"] if "diff_median" in r]
    if not rows:
        return ""
    peak = max(r["diff_median"] for r in rows)
    last = rows[-5:]
    med = sorted(r["diff_median"] for r in last)[len(last) // 2]
    low = min(r["diff_min"] for r in last)
    return f"median peak={peak} end={med} end-min={low}"

for name in ("base", "farm"):
    print(f"difficulty {name:>6}: {diff_track(name)}")
for name, data in pops.items():
    if data:
        top = list(data["reasons"].items())[:4]
        print(f"reasons {name:>6}: {top}")

try:
    with open(os.path.join(out_dir, "runtime_snapshot.json"), encoding="utf-8") as f:
        runtime = json.loads(f.read() or "null")
except (OSError, ValueError):
    runtime = None
if runtime:
    unprocessed = runtime.get("submit", {}).get("unprocessed")
    if unprocessed:
        print(f"stratum unprocessed counters: {unprocessed}")
    validation = runtime.get("validation", {})
    keys = ("validation_duration", "sync_full_verifies", "overload_mode")
    print("stratum validation: " + ", ".join(f"{k}={validation.get(k)}" for k in keys if k in validation))
mem_path = os.path.join(out_dir, "stratum_memory.txt")
if os.path.exists(mem_path):
    with open(mem_path, encoding="utf-8") as f:
        print("stratum memory: " + " ".join(line.strip() for line in f))
PY
