#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Deploy blocknet-pool to the bntpool server.

Usage:
  scripts/deploy_bntpool.sh [--skip-build] [--skip-ui-build]

Environment overrides:
  BNTPOOL_HOST        SSH host alias (default: bntpool)
  BNTPOOL_REMOTE_DIR  Remote pool directory (default: /opt/blocknet/blocknet-pool)
  BNTPOOL_SERVICE     Systemd service name (default: blocknet-pool.service)
  BNTPOOL_FORCE_RESTART  Set to 1 to force a restart even when binary hash is unchanged
EOF
}

skip_build=0
skip_ui_build=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build)
      skip_build=1
      shift
      ;;
    --skip-ui-build)
      skip_ui_build=1
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
force_restart="${BNTPOOL_FORCE_RESTART:-0}"
remote_bin="${remote_dir}/target/release/blocknet-pool-rs"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/.." && pwd)"

if [[ "${skip_ui_build}" -eq 0 ]]; then
  echo "==> building web ui bundle locally"
  if ! command -v npm >/dev/null 2>&1; then
    echo "npm is required to build frontend bundle (or run with --skip-ui-build)" >&2
    exit 1
  fi
  npm --prefix "${repo_dir}/frontend" ci
  npm --prefix "${repo_dir}/frontend" run build
fi

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

echo "==> reading current remote binary hash"
before_hash="$(ssh "${host}" "set -euo pipefail; if [[ -f '${remote_bin}' ]]; then sha256sum '${remote_bin}' | awk '{print \$1}'; else echo '__missing__'; fi")"

if [[ "${skip_build}" -eq 0 ]]; then
  echo "==> building release binary on ${host}"
  ssh "${host}" "set -euo pipefail; export PATH=/home/blocknet/.cargo/bin:\$PATH; cd '${remote_dir}'; cargo build --release"
fi

echo "==> reading updated remote binary hash"
after_hash="$(ssh "${host}" "set -euo pipefail; if [[ -f '${remote_bin}' ]]; then sha256sum '${remote_bin}' | awk '{print \$1}'; else echo '__missing__'; fi")"

if [[ "${force_restart}" == "1" || "${before_hash}" != "${after_hash}" ]]; then
  echo "==> restarting ${service}"
  ssh "${host}" "set -euo pipefail; sudo systemctl restart '${service}'; sudo systemctl is-active '${service}'"
else
  echo "==> binary unchanged; skipping restart (set BNTPOOL_FORCE_RESTART=1 to override)"
  ssh "${host}" "set -euo pipefail; sudo systemctl is-active '${service}'"
fi

echo "==> recent service logs"
ssh "${host}" "set -euo pipefail; sudo journalctl -u '${service}' --no-pager -n 40"
