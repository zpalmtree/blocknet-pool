#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Deploy the external Cloudflare Worker probe for primary pool monitoring.

Usage:
  scripts/deploy_cloudflare_monitor_worker.sh

Environment overrides:
  BNTPOOL_HOST                SSH host alias used to read monitor_ingest_secret (default: bntpool)
  BNTPOOL_REMOTE_CONFIG       Remote pool config path (default: /etc/blocknet/pool/config.json)
  BNTPOOL_ALLOW_RETIRED_HOST  Set to 1 to allow reading config from oldpool / 5.161.113.120
  CLOUDFLARE_ENV_FILE         File containing CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID
                              (default: /media/Code/js/trmnl/world-recipes/.env)
  CLOUDFLARE_WRANGLER_BIN     Wrangler entrypoint JS file
                              (default: /media/Code/js/trmnl/f1-results/node_modules/wrangler/bin/wrangler.js)
  CLOUDFLARE_WORKER_NAME      Worker name (default: blocknet-pool-monitor)
  CLOUDFLARE_KV_NAMESPACE     KV namespace title (default: blocknet-pool-monitor-state)
  PUBLIC_MONITOR_URL          Public monitor URL (default: https://bntpool.com/api/monitor/public)
  INGEST_URL                  Signed ingest URL (default: https://bntpool.com/api/monitor/ingest/cloudflare)
  DISCORD_WEBHOOK_URL         Optional worker-side Discord webhook secret
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

host="${BNTPOOL_HOST:-bntpool}"
remote_config="${BNTPOOL_REMOTE_CONFIG:-/etc/blocknet/pool/config.json}"
allow_retired_host="${BNTPOOL_ALLOW_RETIRED_HOST:-0}"
cf_env_file="${CLOUDFLARE_ENV_FILE:-/media/Code/js/trmnl/world-recipes/.env}"
wrangler_bin="${CLOUDFLARE_WRANGLER_BIN:-/media/Code/js/trmnl/f1-results/node_modules/wrangler/bin/wrangler.js}"
worker_name="${CLOUDFLARE_WORKER_NAME:-blocknet-pool-monitor}"
kv_namespace_title="${CLOUDFLARE_KV_NAMESPACE:-blocknet-pool-monitor-state}"
public_monitor_url="${PUBLIC_MONITOR_URL:-https://bntpool.com/api/monitor/public}"
ingest_url="${INGEST_URL:-https://bntpool.com/api/monitor/ingest/cloudflare}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/.." && pwd)"
worker_dir="${repo_dir}/deploy/cloudflare/monitor-worker"
wrangler_template="${worker_dir}/wrangler.toml.example"

case "${host}" in
  oldpool|*5.161.113.120*)
    if [[ "${allow_retired_host}" != "1" ]]; then
      echo "refusing to target retired host '${host}'; use bntpool for the primary host or set BNTPOOL_ALLOW_RETIRED_HOST=1 to override" >&2
      exit 1
    fi
    ;;
esac

if [[ ! -f "${cf_env_file}" ]]; then
  echo "Cloudflare env file not found: ${cf_env_file}" >&2
  exit 1
fi
if [[ ! -f "${wrangler_bin}" ]]; then
  echo "Wrangler binary not found: ${wrangler_bin}" >&2
  exit 1
fi
if [[ ! -f "${wrangler_template}" ]]; then
  echo "Wrangler template not found: ${wrangler_template}" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${cf_env_file}"
set +a

if [[ -z "${CLOUDFLARE_API_TOKEN:-}" || -z "${CLOUDFLARE_ACCOUNT_ID:-}" ]]; then
  echo "CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID must be set in ${cf_env_file}" >&2
  exit 1
fi

ingest_secret="${BLOCKNET_POOL_MONITOR_INGEST_SECRET:-}"
if [[ -z "${ingest_secret}" ]]; then
  ingest_secret="$(ssh "${host}" "sudo python3 - <<'PY'
import json
from pathlib import Path
path = Path('${remote_config}')
data = json.loads(path.read_text())
print(str(data.get('monitor_ingest_secret', '')).strip())
PY")"
fi

if [[ -z "${ingest_secret}" ]]; then
  echo "monitor_ingest_secret is empty on ${host}:${remote_config}. Run scripts/provision_bntpool_monitoring.sh first." >&2
  exit 1
fi

cf_api() {
  local method="$1"
  local url="$2"
  local body="${3:-}"
  if [[ -n "${body}" ]]; then
    curl -fsS -X "${method}" \
      -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
      -H 'Content-Type: application/json' \
      --data "${body}" \
      "${url}"
  else
    curl -fsS -X "${method}" \
      -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
      "${url}"
  fi
}

echo "==> ensuring Cloudflare KV namespace exists"
namespace_id="$(
  cf_api GET "https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/storage/kv/namespaces?page=1&per_page=100" \
    | python3 -c '
import json
import sys

target = sys.argv[1]
payload = json.load(sys.stdin)
for item in payload.get("result", []):
    if item.get("title") == target:
        print(item.get("id", ""))
        break
' "${kv_namespace_title}"
)"
if [[ -z "${namespace_id}" ]]; then
  namespace_id="$(
    cf_api POST "https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/storage/kv/namespaces" \
      "{\"title\":\"${kv_namespace_title}\"}" \
      | python3 -c '
import json
import sys

payload = json.load(sys.stdin)
print(payload["result"]["id"])
'
  )"
fi

tmp_config="$(mktemp "${worker_dir}/wrangler.XXXXXX.toml")"
cleanup() {
  rm -f "${tmp_config}"
}
trap cleanup EXIT

python3 - "${wrangler_template}" "${tmp_config}" "${worker_name}" "${namespace_id}" "${public_monitor_url}" "${ingest_url}" <<'PY'
from pathlib import Path
import sys

template = Path(sys.argv[1]).read_text()
output = Path(sys.argv[2])
worker_name = sys.argv[3]
namespace_id = sys.argv[4]
public_monitor_url = sys.argv[5]
ingest_url = sys.argv[6]

lines = []
for line in template.splitlines():
    if line.startswith("name = "):
        lines.append(f'name = "{worker_name}"')
    elif line.startswith('id = "REPLACE_WITH_KV_NAMESPACE_ID"'):
        lines.append(f'id = "{namespace_id}"')
    elif line.startswith('PUBLIC_MONITOR_URL = '):
        lines.append(f'PUBLIC_MONITOR_URL = "{public_monitor_url}"')
    elif line.startswith('INGEST_URL = '):
        lines.append(f'INGEST_URL = "{ingest_url}"')
    else:
        lines.append(line)
output.write_text("\n".join(lines) + "\n")
PY

echo "==> deploying Cloudflare worker ${worker_name}"
(cd "${worker_dir}" && \
  CLOUDFLARE_API_TOKEN="${CLOUDFLARE_API_TOKEN}" \
  CLOUDFLARE_ACCOUNT_ID="${CLOUDFLARE_ACCOUNT_ID}" \
  node "${wrangler_bin}" deploy --config "${tmp_config}")

echo "==> syncing Cloudflare worker secrets"
printf '%s' "${ingest_secret}" | (
  cd "${worker_dir}" && \
    CLOUDFLARE_API_TOKEN="${CLOUDFLARE_API_TOKEN}" \
    CLOUDFLARE_ACCOUNT_ID="${CLOUDFLARE_ACCOUNT_ID}" \
    node "${wrangler_bin}" secret put INGEST_SECRET --name "${worker_name}" --config "${tmp_config}"
)
if [[ -n "${DISCORD_WEBHOOK_URL:-}" ]]; then
  printf '%s' "${DISCORD_WEBHOOK_URL}" | (
    cd "${worker_dir}" && \
      CLOUDFLARE_API_TOKEN="${CLOUDFLARE_API_TOKEN}" \
      CLOUDFLARE_ACCOUNT_ID="${CLOUDFLARE_ACCOUNT_ID}" \
      node "${wrangler_bin}" secret put DISCORD_WEBHOOK_URL --name "${worker_name}" --config "${tmp_config}"
  )
else
  echo "==> DISCORD_WEBHOOK_URL not set; leaving worker-side Discord paging unset"
fi

echo "Cloudflare worker deployed: ${worker_name}"
