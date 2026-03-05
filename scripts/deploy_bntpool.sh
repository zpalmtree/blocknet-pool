#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Deploy blocknet-pool to the bntpool server.

Usage:
  scripts/deploy_bntpool.sh [--skip-build]

Environment overrides:
  BNTPOOL_HOST        SSH host alias (default: bntpool)
  BNTPOOL_REMOTE_DIR  Remote pool directory (default: /opt/blocknet/blocknet-pool)
  BNTPOOL_SERVICE     Systemd service name (default: blocknet-pool.service)
EOF
}

skip_build=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build)
      skip_build=1
      shift
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

host="${BNTPOOL_HOST:-bntpool}"
remote_dir="${BNTPOOL_REMOTE_DIR:-/opt/blocknet/blocknet-pool}"
service="${BNTPOOL_SERVICE:-blocknet-pool.service}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/.." && pwd)"

echo "==> syncing source to ${host}:${remote_dir}"
rsync -az --delete \
  --exclude='.git' \
  --exclude='target' \
  --exclude='pool.db' \
  --exclude='data' \
  --exclude='.env' \
  --exclude='config.json' \
  --exclude='scripts/__pycache__/' \
  "${repo_dir}/" "${host}:${remote_dir}/"

if [[ "${skip_build}" -eq 0 ]]; then
  echo "==> building release binary on ${host}"
  ssh "${host}" "set -euo pipefail; export PATH=/home/blocknet/.cargo/bin:\$PATH; cd '${remote_dir}'; cargo build --release"
fi

echo "==> restarting ${service}"
ssh "${host}" "set -euo pipefail; sudo systemctl restart '${service}'; sudo systemctl is-active '${service}'"

echo "==> recent service logs"
ssh "${host}" "set -euo pipefail; sudo journalctl -u '${service}' --no-pager -n 40"
