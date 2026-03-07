#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Deploy blocknet-pool to the bntpool server.

Usage:
  scripts/deploy_bntpool.sh [--skip-build] [--skip-ui-build] [--migrate-split]

Environment overrides:
  BNTPOOL_HOST             SSH host alias (default: bntpool)
  BNTPOOL_REMOTE_DIR       Remote pool directory (default: /opt/blocknet/blocknet-pool)
  BNTPOOL_API_SERVICE      Systemd API service name (default: blocknet-pool-api.service)
  BNTPOOL_STRATUM_SERVICE  Systemd Stratum service name (default: blocknet-pool-stratum.service)
  BNTPOOL_LEGACY_SERVICE   Legacy combined service to disable on split migration (default: blocknet-pool.service)
  BNTPOOL_FORCE_RESTART    Set to 1 to force a restart even when binary hashes are unchanged
  BNTPOOL_LOCAL_BUILD_IMAGE  Optional Docker image used for local builds
EOF
}

skip_build=0
skip_ui_build=0
migrate_split=0
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
    --migrate-split)
      migrate_split=1
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
api_service="${BNTPOOL_API_SERVICE:-blocknet-pool-api.service}"
stratum_service="${BNTPOOL_STRATUM_SERVICE:-blocknet-pool-stratum.service}"
legacy_service="${BNTPOOL_LEGACY_SERVICE:-blocknet-pool.service}"
force_restart="${BNTPOOL_FORCE_RESTART:-0}"
local_build_image="${BNTPOOL_LOCAL_BUILD_IMAGE:-}"
remote_api_bin="${remote_dir}/target/release/blocknet-pool-api"
remote_stratum_bin="${remote_dir}/target/release/blocknet-pool-stratum"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/.." && pwd)"
host_uid="$(id -u)"
host_gid="$(id -g)"
local_api_bin="${repo_dir}/target/release/blocknet-pool-api"
local_stratum_bin="${repo_dir}/target/release/blocknet-pool-stratum"

remote_hash() {
  local path="$1"
  ssh "${host}" "set -euo pipefail; if [[ -f '${path}' ]]; then sha256sum '${path}' | awk '{print \$1}'; else echo '__missing__'; fi"
}

build_locally() {
  if [[ -n "${local_build_image}" ]]; then
    if ! command -v docker >/dev/null 2>&1; then
      echo "docker is required when BNTPOOL_LOCAL_BUILD_IMAGE is set" >&2
      exit 1
    fi
    docker run --rm \
      -v "${repo_dir}:/work" \
      -w /work \
      -e HOST_UID="${host_uid}" \
      -e HOST_GID="${host_gid}" \
      "${local_build_image}" \
      bash -lc "set -euo pipefail; export PATH=/usr/local/cargo/bin:\$PATH; cargo build --release --bin blocknet-pool-api --no-default-features --features api; cargo build --release --bin blocknet-pool-stratum --no-default-features --features stratum; chown -R \"${host_uid}:${host_gid}\" /work/target"
  else
    cargo build --release --bin blocknet-pool-api --no-default-features --features api
    cargo build --release --bin blocknet-pool-stratum --no-default-features --features stratum
  fi
}

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
  --exclude='frontend/node_modules' \
  --exclude='frontend/dist' \
  --exclude='pool.db' \
  --exclude='data' \
  --exclude='.env' \
  --exclude='config.json' \
  --exclude='scripts/__pycache__/' \
  "${repo_dir}/" "${host}:${remote_dir}/"

echo "==> reading current remote binary hashes"
before_api_hash="$(remote_hash "${remote_api_bin}")"
before_stratum_hash="$(remote_hash "${remote_stratum_bin}")"

if [[ "${skip_build}" -eq 0 ]]; then
  echo "==> building release binaries locally"
  build_locally
  echo "==> uploading locally built binaries to ${host}"
  ssh "${host}" "set -euo pipefail; mkdir -p '${remote_dir}/target/release'"
  rsync -az \
    "${local_api_bin}" \
    "${local_stratum_bin}" \
    "${host}:${remote_dir}/target/release/"
fi

echo "==> reading updated remote binary hashes"
after_api_hash="$(remote_hash "${remote_api_bin}")"
after_stratum_hash="$(remote_hash "${remote_stratum_bin}")"

if [[ "${migrate_split}" == "1" ]]; then
  echo "==> installing split systemd units"
  ssh "${host}" "set -euo pipefail; \
    sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-api.service' '/etc/systemd/system/${api_service}'; \
    sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-stratum.service' '/etc/systemd/system/${stratum_service}'; \
    sudo systemctl daemon-reload; \
    sudo systemctl disable --now '${legacy_service}' >/dev/null 2>&1 || true; \
    sudo systemctl enable '${api_service}' '${stratum_service}'"
fi

restart_api=0
restart_stratum=0
if [[ "${migrate_split}" == "1" || "${force_restart}" == "1" || "${before_api_hash}" != "${after_api_hash}" ]]; then
  restart_api=1
fi
if [[ "${migrate_split}" == "1" || "${force_restart}" == "1" || "${before_stratum_hash}" != "${after_stratum_hash}" ]]; then
  restart_stratum=1
fi

if [[ "${restart_api}" == "1" ]]; then
  echo "==> restarting ${api_service}"
  ssh "${host}" "set -euo pipefail; sudo systemctl restart '${api_service}'; sudo systemctl is-active '${api_service}'"
else
  echo "==> ${api_service} unchanged; leaving it running"
  ssh "${host}" "set -euo pipefail; sudo systemctl is-active '${api_service}'"
fi

if [[ "${restart_stratum}" == "1" ]]; then
  echo "==> restarting ${stratum_service}"
  ssh "${host}" "set -euo pipefail; sudo systemctl restart '${stratum_service}'; sudo systemctl is-active '${stratum_service}'"
else
  echo "==> ${stratum_service} unchanged; leaving it running"
  ssh "${host}" "set -euo pipefail; sudo systemctl is-active '${stratum_service}'"
fi

echo "==> recent API service logs"
ssh "${host}" "set -euo pipefail; sudo journalctl -u '${api_service}' --no-pager -n 30"

echo "==> recent Stratum service logs"
ssh "${host}" "set -euo pipefail; sudo journalctl -u '${stratum_service}' --no-pager -n 30"
