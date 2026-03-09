#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
TEST_POSTGRES_URL_ENV="BLOCKNET_POOL_TEST_POSTGRES_URL"

DAEMON_PID=""
API_PID=""
STRATUM_PID=""
POSTGRES_SCHEMA=""
BASE_POSTGRES_URL="${BLOCKNET_POOL_TEST_POSTGRES_URL:-}"

cleanup() {
  local status=$?
  if [[ ${status} -ne 0 ]]; then
    echo "--- api log ---" >&2
    cat "${API_LOG}" >&2 || true
    echo "--- stratum log ---" >&2
    cat "${STRATUM_LOG}" >&2 || true
    echo "--- mock daemon log ---" >&2
    cat "${DAEMON_LOG}" >&2 || true
  fi
  if [[ -n "${API_PID}" ]] && kill -0 "${API_PID}" 2>/dev/null; then
    kill "${API_PID}" 2>/dev/null || true
    wait "${API_PID}" 2>/dev/null || true
  fi
  if [[ -n "${STRATUM_PID}" ]] && kill -0 "${STRATUM_PID}" 2>/dev/null; then
    kill "${STRATUM_PID}" 2>/dev/null || true
    wait "${STRATUM_PID}" 2>/dev/null || true
  fi
  if [[ -n "${DAEMON_PID}" ]] && kill -0 "${DAEMON_PID}" 2>/dev/null; then
    kill "${DAEMON_PID}" 2>/dev/null || true
    wait "${DAEMON_PID}" 2>/dev/null || true
  fi
  if [[ -n "${POSTGRES_SCHEMA}" && -n "${BASE_POSTGRES_URL}" ]]; then
    psql "${BASE_POSTGRES_URL}" -v ON_ERROR_STOP=1 \
      -c "DROP SCHEMA IF EXISTS ${POSTGRES_SCHEMA} CASCADE" >/dev/null 2>&1 || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

BASE_PORT="$((20000 + (RANDOM % 10000)))"
DAEMON_PORT="${BASE_PORT}"
API_PORT="$((BASE_PORT + 1))"
STRATUM_PORT="$((BASE_PORT + 2))"

if [[ -z "${BASE_POSTGRES_URL}" ]]; then
  echo "[smoke] set ${TEST_POSTGRES_URL_ENV} to a Postgres connection URL" >&2
  exit 1
fi
if ! command -v psql >/dev/null 2>&1; then
  echo "[smoke] psql is required for Postgres-backed smoke tests" >&2
  exit 1
fi

POSTGRES_SCHEMA="blocknet_pool_ci_${$}_$RANDOM$RANDOM"
psql "${BASE_POSTGRES_URL}" -v ON_ERROR_STOP=1 \
  -c "CREATE SCHEMA IF NOT EXISTS ${POSTGRES_SCHEMA}" >/dev/null
DATABASE_URL="$(python3 - "${BASE_POSTGRES_URL}" "${POSTGRES_SCHEMA}" <<'PY'
import sys
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

base = sys.argv[1]
schema = sys.argv[2]
parts = urlsplit(base)
query = parse_qsl(parts.query, keep_blank_values=True)
query.append(("options", f"-c search_path={schema}"))
print(urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment)))
PY
)"

CONFIG_PATH="${TMP_DIR}/config.json"
DAEMON_LOG="${TMP_DIR}/mock-daemon.log"
API_LOG="${TMP_DIR}/api.log"
STRATUM_LOG="${TMP_DIR}/stratum.log"

cat >"${CONFIG_PATH}" <<JSON
{
  "pool_name": "ci smoke",
  "pool_url": "http://127.0.0.1:${API_PORT}",
  "stratum_host": "127.0.0.1",
  "stratum_port": ${STRATUM_PORT},
  "api_host": "127.0.0.1",
  "api_port": ${API_PORT},
  "daemon_api": "http://127.0.0.1:${DAEMON_PORT}",
  "database_url": "${DATABASE_URL}",
  "pool_wallet_address": "ci-pool-wallet",
  "sse_enabled": false,
  "enable_vardiff": false,
  "validation_mode": "probabilistic",
  "sample_rate": 0.0,
  "warmup_shares": 0,
  "min_sample_every": 0,
  "stratum_submit_v2_required": true,
  "payouts_enabled": false,
  "shares_retention": "",
  "payouts_retention": ""
}
JSON

python3 -u - "${DAEMON_PORT}" >"${DAEMON_LOG}" 2>&1 <<'PY' &
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

port = int(sys.argv[1])
template = {
    "block": {
        "header": {
            "height": 1,
            "difficulty": 1,
            "prevhash": "00" * 32,
        },
        "reward": 1000000000,
    },
    "target": ("00" * 31) + "01",
    "header_base": "00" * 92,
    "reward_address_used": "",
    "template_id": "ci-template-1",
}

class Handler(BaseHTTPRequestHandler):
    server_version = "mock-daemon/1.0"

    def _send_json(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/api/status":
            return self._send_json(
                200,
                {
                    "peer_id": "mock-peer",
                    "peers": 1,
                    "chain_height": 100,
                    "best_hash": "ab" * 32,
                    "total_work": 1,
                    "mempool_size": 0,
                    "mempool_bytes": 0,
                    "syncing": False,
                    "identity_age": "1m",
                },
            )
        if path == "/api/mining/blocktemplate":
            return self._send_json(200, template)
        if path.startswith("/api/block/"):
            return self._send_json(
                200,
                {"height": 1, "hash": "cd" * 32, "reward": 1000000000, "difficulty": 1, "timestamp": 1, "tx_count": 1},
            )
        return self._send_json(404, {"error": "not found"})

    def do_POST(self):
        path = urlparse(self.path).path
        content_len = int(self.headers.get("Content-Length", "0"))
        if content_len:
            _ = self.rfile.read(content_len)
        if path == "/api/mining/submitblock":
            return self._send_json(200, {"accepted": False, "hash": "", "height": 0})
        return self._send_json(404, {"error": "not found"})

ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
PY
DAEMON_PID="$!"

wait_for_port() {
  local port="$1"
  local label="$2"
  for _ in $(seq 1 480); do
    if python3 - "$port" <<'PY'
import socket, sys
p = int(sys.argv[1])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(0.2)
try:
    s.connect(("127.0.0.1", p))
    s.close()
    raise SystemExit(0)
except Exception:
    raise SystemExit(1)
PY
    then
      return 0
    fi
    sleep 0.25
  done
  echo "[smoke] timed out waiting for ${label} on port ${port}" >&2
  return 1
}

if ! wait_for_port "${DAEMON_PORT}" "mock daemon"; then
  echo "--- mock daemon log ---" >&2
  cat "${DAEMON_LOG}" >&2 || true
  exit 1
fi

(
  cd "${ROOT_DIR}"
  cargo run --quiet -p blocknet-pool-stratum-app --bin blocknet-pool-stratum -- --config "${CONFIG_PATH}"
) >"${STRATUM_LOG}" 2>&1 &
STRATUM_PID="$!"

(
  cd "${ROOT_DIR}"
  cargo run --quiet -p blocknet-pool-api-app --bin blocknet-pool-api -- --config "${CONFIG_PATH}"
) >"${API_LOG}" 2>&1 &
API_PID="$!"

if ! wait_for_port "${STRATUM_PORT}" "stratum"; then
  echo "--- stratum log ---" >&2
  cat "${STRATUM_LOG}" >&2 || true
  echo "--- api log ---" >&2
  cat "${API_LOG}" >&2 || true
  echo "--- mock daemon log ---" >&2
  cat "${DAEMON_LOG}" >&2 || true
  exit 1
fi
if ! wait_for_port "${API_PORT}" "api"; then
  echo "--- api log ---" >&2
  cat "${API_LOG}" >&2 || true
  echo "--- stratum log ---" >&2
  cat "${STRATUM_LOG}" >&2 || true
  echo "--- mock daemon log ---" >&2
  cat "${DAEMON_LOG}" >&2 || true
  exit 1
fi

python3 - "${STRATUM_PORT}" "${API_PORT}" <<'PY'
import json
import socket
import sys
import time
import urllib.request

stratum_port = int(sys.argv[1])
api_port = int(sys.argv[2])

ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

def b58encode(raw: bytes) -> str:
    n = int.from_bytes(raw, "big")
    out = []
    while n > 0:
        n, rem = divmod(n, 58)
        out.append(ALPHABET[rem])
    zeros = 0
    for b in raw:
        if b == 0:
            zeros += 1
        else:
            break
    return ("1" * zeros) + ("".join(reversed(out)) if out else "")

address = b58encode(bytes([0x11] * 64))
s = socket.create_connection(("127.0.0.1", stratum_port), timeout=10)
s.settimeout(10.0)

def send_message(payload):
    data = (json.dumps(payload) + "\n").encode("utf-8")
    s.sendall(data)

def next_message(timeout_s: float):
    deadline = time.time() + timeout_s
    raw = bytearray()
    while time.time() < deadline:
        remaining = max(0.1, deadline - time.time())
        s.settimeout(remaining)
        try:
            chunk = s.recv(1)
        except socket.timeout:
            continue
        if not chunk:
            raise SystemExit("stratum connection closed before response")
        if chunk == b"\n":
            line = raw.decode("utf-8", errors="replace").strip()
            if not line:
                raw.clear()
                continue
            return json.loads(line)
        if chunk != b"\r":
            raw.extend(chunk)
    raise TimeoutError("timed out waiting for stratum message")

send_message({
    "id": 1,
    "method": "login",
    "params": {
        "address": address,
        "worker": "ci-rig",
        "protocol_version": 2,
        "capabilities": ["submit_claimed_hash"]
    }
})

login_ok = False
job_id = None
nonce_start = None
deadline = time.time() + 30.0
while time.time() < deadline:
    try:
        msg = next_message(1.0)
    except TimeoutError:
        continue
    if msg.get("id") == 1 and msg.get("error"):
        raise SystemExit(f"login failed: {msg}")
    if msg.get("id") == 1 and msg.get("status") == "ok":
        login_ok = True
    if msg.get("method") == "job":
        params = msg.get("params", {})
        job_id = params.get("job_id")
        nonce_start = params.get("nonce_start")
        break

if not login_ok:
    raise SystemExit("did not receive successful login response")
if not job_id or nonce_start is None:
    raise SystemExit("did not receive job after login")

send_message({
    "id": 2,
    "method": "submit",
    "params": {
        "job_id": job_id,
        "nonce": int(nonce_start),
        "claimed_hash": ("00" * 31) + "02"
    }
})

submit_ok = False
deadline = time.time() + 30.0
while time.time() < deadline:
    try:
        msg = next_message(1.0)
    except TimeoutError:
        continue
    if msg.get("id") != 2:
        continue
    if msg.get("error"):
        raise SystemExit(f"submit failed: {msg}")
    submit_ok = msg.get("status") == "ok"
    break

if not submit_ok:
    raise SystemExit("submit did not return ok")

stats = None
for _ in range(20):
    with urllib.request.urlopen(f"http://127.0.0.1:{api_port}/api/stats", timeout=5) as resp:
        stats = json.loads(resp.read().decode("utf-8"))
    accepted = int(stats.get("pool", {}).get("shares_accepted", 0))
    if accepted >= 1:
        break
if int(stats.get("pool", {}).get("shares_accepted", 0)) < 1:
    raise SystemExit(f"expected shares_accepted >= 1, got: {stats}")

print("smoke e2e ok")
PY

echo "[smoke] passed"
