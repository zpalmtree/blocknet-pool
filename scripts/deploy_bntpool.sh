#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Deploy blocknet-pool to the current primary pool host.

Usage:
  scripts/deploy_bntpool.sh [--skip-build] [--skip-ui-build] [--api-only] [--migrate-split] [--provision-monitoring] [--provision-recovery] [--deploy-cloudflare] [--monitor-only]

Environment overrides:
  BNTPOOL_HOST             SSH host alias (default: bntpool)
  BNTPOOL_REMOTE_DIR       Remote pool directory (default: /opt/blocknet/blocknet-pool)
  BNTPOOL_API_SERVICE      Systemd API service name (default: blocknet-pool-api.service)
  BNTPOOL_STRATUM_SERVICE  Systemd Stratum service name (default: blocknet-pool-stratum.service)
  BNTPOOL_MONITOR_SERVICE  Systemd monitor service name (default: blocknet-pool-monitor.service)
  BNTPOOL_RECOVERY_SERVICE Systemd recovery service name (default: blocknet-pool-recoveryd.service)
  BNTPOOL_RECOVERY_SOCKET  Systemd recovery socket name (default: blocknet-pool-recoveryd.socket)
  BNTPOOL_LEGACY_SERVICE   Legacy combined service to disable on split migration (default: blocknet-pool.service)
  BNTPOOL_ALLOW_RETIRED_HOST  Set to 1 to allow explicit deploys to oldpool / 5.161.113.120
  BNTPOOL_FORCE_RESTART    Set to 1 to force a restart even when binary hashes are unchanged
  BNTPOOL_LOCAL_BUILD_IMAGE  Optional Docker image used for local builds
EOF
}

skip_build=0
skip_ui_build=0
api_only=0
migrate_split=0
provision_monitoring=0
provision_recovery=0
deploy_cloudflare=0
monitor_only=0
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
    --api-only)
      api_only=1
      shift
      ;;
    --migrate-split)
      migrate_split=1
      shift
      ;;
    --provision-monitoring)
      provision_monitoring=1
      shift
      ;;
    --provision-recovery)
      provision_recovery=1
      shift
      ;;
    --deploy-cloudflare)
      deploy_cloudflare=1
      shift
      ;;
    --monitor-only)
      monitor_only=1
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

if [[ "${api_only}" == "1" && "${monitor_only}" == "1" ]]; then
  echo "--api-only and --monitor-only cannot be combined" >&2
  exit 1
fi

if [[ "${api_only}" == "1" && ("${migrate_split}" == "1" || "${provision_monitoring}" == "1" || "${provision_recovery}" == "1" || "${deploy_cloudflare}" == "1") ]]; then
  echo "--api-only cannot be combined with provisioning, split migration, or Cloudflare deploy flags" >&2
  exit 1
fi

host="${BNTPOOL_HOST:-bntpool}"
remote_dir="${BNTPOOL_REMOTE_DIR:-/opt/blocknet/blocknet-pool}"
api_service="${BNTPOOL_API_SERVICE:-blocknet-pool-api.service}"
stratum_service="${BNTPOOL_STRATUM_SERVICE:-blocknet-pool-stratum.service}"
monitor_service="${BNTPOOL_MONITOR_SERVICE:-blocknet-pool-monitor.service}"
recovery_service="${BNTPOOL_RECOVERY_SERVICE:-blocknet-pool-recoveryd.service}"
recovery_socket="${BNTPOOL_RECOVERY_SOCKET:-blocknet-pool-recoveryd.socket}"
legacy_service="${BNTPOOL_LEGACY_SERVICE:-blocknet-pool.service}"
allow_retired_host="${BNTPOOL_ALLOW_RETIRED_HOST:-0}"
force_restart="${BNTPOOL_FORCE_RESTART:-0}"
local_build_image="${BNTPOOL_LOCAL_BUILD_IMAGE:-}"
remote_api_bin="${remote_dir}/target/release/blocknet-pool-api"
remote_stratum_bin="${remote_dir}/target/release/blocknet-pool-stratum"
remote_monitor_bin="${remote_dir}/target/release/blocknet-pool-monitor"
remote_recovery_bin="${remote_dir}/target/release/blocknet-pool-recoveryd"
remote_postgres_dropin_dir="/etc/systemd/system/postgresql@.service.d"
remote_postgres_dropin="${remote_postgres_dropin_dir}/restart-blocknet-pool.conf"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/.." && pwd)"
host_uid="$(id -u)"
host_gid="$(id -g)"
local_api_bin="${repo_dir}/target/release/blocknet-pool-api"
local_stratum_bin="${repo_dir}/target/release/blocknet-pool-stratum"
local_monitor_bin="${repo_dir}/target/release/blocknet-pool-monitor"
local_recovery_bin="${repo_dir}/target/release/blocknet-pool-recoveryd"
source_rsync_args=(
  -rltzE
  --delete
  --omit-dir-times
  --no-owner
  --no-group
)
binary_rsync_args=(
  -rtz
  --omit-dir-times
  --no-owner
  --no-group
  --chmod=F755
)

guard_retired_host() {
  case "$1" in
    oldpool|*5.161.113.120*)
      if [[ "${allow_retired_host}" != "1" ]]; then
        echo "refusing to target retired host '$1'; use bntpool for the primary host or set BNTPOOL_ALLOW_RETIRED_HOST=1 to override" >&2
        exit 1
      fi
      ;;
  esac
}

ensure_remote_write_access() {
  if ! ssh "${host}" "set -euo pipefail; mkdir -p '${remote_dir}' '${remote_dir}/target/release'; test -w '${remote_dir}' && test -w '${remote_dir}/target/release'"; then
    echo "remote deploy user cannot write to ${remote_dir} on ${host}; fix the host alias or primary-host permissions before deploying" >&2
    exit 1
  fi
}

guard_retired_host "${host}"
ensure_remote_write_access

deploy_api=1
deploy_stratum=1
deploy_monitor=1
deploy_recovery=1
if [[ "${api_only}" == "1" ]]; then
  deploy_stratum=0
  deploy_monitor=0
  deploy_recovery=0
elif [[ "${monitor_only}" == "1" ]]; then
  deploy_api=0
  deploy_stratum=0
  deploy_recovery=0
fi

if [[ "${provision_monitoring}" == "1" ]]; then
  echo "==> provisioning monitoring stack on ${host}"
  BNTPOOL_HOST="${host}" BNTPOOL_REMOTE_DIR="${remote_dir}" \
    bash "${repo_dir}/scripts/provision_bntpool_monitoring.sh"
fi

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
    local build_cmd
    build_cmd=""
    if [[ "${deploy_api}" == "1" ]]; then
      build_cmd+="cargo build --release -p blocknet-pool-api-app --bin blocknet-pool-api; "
    fi
    if [[ "${deploy_stratum}" == "1" ]]; then
      build_cmd+="cargo build --release -p blocknet-pool-stratum-app --bin blocknet-pool-stratum; "
    fi
    if [[ "${deploy_monitor}" == "1" ]]; then
      build_cmd+="cargo build --release -p blocknet-pool-monitor-app --bin blocknet-pool-monitor; "
    fi
    if [[ "${deploy_recovery}" == "1" ]]; then
      build_cmd+="cargo build --release -p blocknet-pool-recoveryd-app --bin blocknet-pool-recoveryd; "
    fi
    docker run --rm \
      -v "${repo_dir}:/work" \
      -w /work \
      -e HOST_UID="${host_uid}" \
      -e HOST_GID="${host_gid}" \
      "${local_build_image}" \
      bash -lc "set -euo pipefail; export PATH=/usr/local/cargo/bin:\$PATH; ${build_cmd}; chown -R \"${host_uid}:${host_gid}\" /work/target"
  else
    if [[ "${deploy_api}" == "1" ]]; then
      cargo build --release -p blocknet-pool-api-app --bin blocknet-pool-api
    fi
    if [[ "${deploy_stratum}" == "1" ]]; then
      cargo build --release -p blocknet-pool-stratum-app --bin blocknet-pool-stratum
    fi
    if [[ "${deploy_monitor}" == "1" ]]; then
      cargo build --release -p blocknet-pool-monitor-app --bin blocknet-pool-monitor
    fi
    if [[ "${deploy_recovery}" == "1" ]]; then
      cargo build --release -p blocknet-pool-recoveryd-app --bin blocknet-pool-recoveryd
    fi
  fi
}

if [[ "${deploy_api}" == "1" && "${skip_ui_build}" -eq 0 ]]; then
  echo "==> building web ui bundle locally"
  if ! command -v npm >/dev/null 2>&1; then
    echo "npm is required to build frontend bundle (or run with --skip-ui-build)" >&2
    exit 1
  fi
  npm --prefix "${repo_dir}/frontend" ci
  npm --prefix "${repo_dir}/frontend" run build
fi

echo "==> syncing source to ${host}:${remote_dir}"
rsync "${source_rsync_args[@]}" \
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
before_api_hash="__skipped__"
before_stratum_hash="__skipped__"
before_monitor_hash="__skipped__"
before_recovery_hash="__skipped__"
if [[ "${deploy_api}" == "1" ]]; then
  before_api_hash="$(remote_hash "${remote_api_bin}")"
fi
if [[ "${deploy_stratum}" == "1" ]]; then
  before_stratum_hash="$(remote_hash "${remote_stratum_bin}")"
fi
if [[ "${deploy_monitor}" == "1" ]]; then
  before_monitor_hash="$(remote_hash "${remote_monitor_bin}")"
fi
if [[ "${deploy_recovery}" == "1" ]]; then
  before_recovery_hash="$(remote_hash "${remote_recovery_bin}")"
fi

if [[ "${skip_build}" -eq 0 ]]; then
  echo "==> building release binaries locally"
  build_locally
  echo "==> uploading locally built binaries to ${host}"
  ssh "${host}" "set -euo pipefail; mkdir -p '${remote_dir}/target/release'"
  rsync_args=( "${binary_rsync_args[@]}" )
  if [[ "${deploy_api}" == "1" ]]; then
    rsync_args+=( "${local_api_bin}" )
  fi
  if [[ "${deploy_stratum}" == "1" ]]; then
    rsync_args+=( "${local_stratum_bin}" )
  fi
  if [[ "${deploy_monitor}" == "1" ]]; then
    rsync_args+=( "${local_monitor_bin}" )
  fi
  if [[ "${deploy_recovery}" == "1" ]]; then
    rsync_args+=( "${local_recovery_bin}" )
  fi
  rsync "${rsync_args[@]}" "${host}:${remote_dir}/target/release/"
fi

echo "==> reading updated remote binary hashes"
after_api_hash="__skipped__"
after_stratum_hash="__skipped__"
after_monitor_hash="__skipped__"
after_recovery_hash="__skipped__"
if [[ "${deploy_api}" == "1" ]]; then
  after_api_hash="$(remote_hash "${remote_api_bin}")"
fi
if [[ "${deploy_stratum}" == "1" ]]; then
  after_stratum_hash="$(remote_hash "${remote_stratum_bin}")"
fi
if [[ "${deploy_monitor}" == "1" ]]; then
  after_monitor_hash="$(remote_hash "${remote_monitor_bin}")"
fi
if [[ "${deploy_recovery}" == "1" ]]; then
  after_recovery_hash="$(remote_hash "${remote_recovery_bin}")"
fi

echo "==> installing managed systemd assets"
systemd_cmd="set -euo pipefail; "
if [[ "${deploy_api}" == "1" ]]; then
  systemd_cmd+="sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-api.service' '/etc/systemd/system/${api_service}'; "
fi
if [[ "${deploy_stratum}" == "1" ]]; then
  systemd_cmd+="sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-stratum.service' '/etc/systemd/system/${stratum_service}'; "
fi
if [[ "${deploy_monitor}" == "1" ]]; then
  systemd_cmd+="sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-monitor.service' '/etc/systemd/system/${monitor_service}'; "
fi
if [[ "${deploy_recovery}" == "1" ]]; then
  systemd_cmd+="sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-recoveryd.service' '/etc/systemd/system/${recovery_service}'; "
  systemd_cmd+="sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-recoveryd.socket' '/etc/systemd/system/${recovery_socket}'; "
  systemd_cmd+="sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknetd@.service' '/etc/systemd/system/blocknetd@.service'; "
  systemd_cmd+="sudo install -d -m 0755 '${remote_postgres_dropin_dir}'; "
  systemd_cmd+="sudo install -m 0644 '${remote_dir}/deploy/systemd/postgresql@.service.d/restart-blocknet-pool.conf' '${remote_postgres_dropin}'; "
fi
systemd_cmd+="sudo systemctl daemon-reload; "
if [[ "${deploy_monitor}" == "1" ]]; then
  systemd_cmd+="sudo systemctl enable '${monitor_service}'; "
fi
if [[ "${deploy_recovery}" == "1" ]]; then
  systemd_cmd+="sudo systemctl enable '${recovery_socket}'; "
fi
ssh "${host}" "${systemd_cmd}"

if [[ "${provision_recovery}" == "1" ]]; then
  echo "==> provisioning recovery topology on ${host}"
  BNTPOOL_HOST="${host}" BNTPOOL_REMOTE_DIR="${remote_dir}" \
    bash "${repo_dir}/scripts/provision_bntpool_recovery.sh"
fi

if [[ "${migrate_split}" == "1" ]]; then
  echo "==> enabling split systemd units"
  ssh "${host}" "set -euo pipefail; \
    sudo systemctl disable --now '${legacy_service}' >/dev/null 2>&1 || true; \
    sudo systemctl enable '${api_service}' '${stratum_service}'"
fi

restart_api=0
restart_stratum=0
restart_monitor=0
restart_recovery=0
if [[ "${deploy_api}" == "1" && ("${migrate_split}" == "1" || "${force_restart}" == "1" || "${before_api_hash}" != "${after_api_hash}") ]]; then
  restart_api=1
fi
if [[ "${deploy_stratum}" == "1" && ("${migrate_split}" == "1" || "${force_restart}" == "1" || "${before_stratum_hash}" != "${after_stratum_hash}") ]]; then
  restart_stratum=1
fi
if [[ "${deploy_monitor}" == "1" && ("${migrate_split}" == "1" || "${force_restart}" == "1" || "${before_monitor_hash}" != "${after_monitor_hash}") ]]; then
  restart_monitor=1
fi
if [[ "${deploy_recovery}" == "1" && ("${force_restart}" == "1" || "${before_recovery_hash}" != "${after_recovery_hash}" || "${provision_recovery}" == "1") ]]; then
  restart_recovery=1
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

if [[ "${restart_monitor}" == "1" ]]; then
  echo "==> restarting ${monitor_service}"
  ssh "${host}" "set -euo pipefail; sudo systemctl restart '${monitor_service}'; sudo systemctl is-active '${monitor_service}'"
else
  echo "==> ${monitor_service} unchanged; leaving it running"
  ssh "${host}" "set -euo pipefail; sudo systemctl is-active '${monitor_service}'"
fi

if [[ "${restart_recovery}" == "1" ]]; then
  echo "==> restarting ${recovery_service}"
  ssh "${host}" "set -euo pipefail; sudo systemctl restart '${recovery_service}' || sudo systemctl start '${recovery_service}'; sudo systemctl is-active '${recovery_service}'"
else
  echo "==> ${recovery_service} unchanged; leaving it running"
  ssh "${host}" "set -euo pipefail; sudo systemctl is-active '${recovery_service}' >/dev/null 2>&1 || sudo systemctl is-active '${recovery_socket}'"
fi

echo "==> recent API service logs"
if [[ "${deploy_api}" == "0" ]]; then
  ssh "${host}" "set -euo pipefail; sudo systemctl is-active '${api_service}'"
else
  ssh "${host}" "set -euo pipefail; sudo journalctl -u '${api_service}' --no-pager -n 30"
fi

echo "==> recent Stratum service logs"
if [[ "${deploy_stratum}" == "0" ]]; then
  ssh "${host}" "set -euo pipefail; sudo systemctl is-active '${stratum_service}'"
else
  ssh "${host}" "set -euo pipefail; sudo journalctl -u '${stratum_service}' --no-pager -n 30"
fi

echo "==> recent Monitor service logs"
if [[ "${deploy_monitor}" == "0" ]]; then
  ssh "${host}" "set -euo pipefail; sudo systemctl is-active '${monitor_service}'"
else
  ssh "${host}" "set -euo pipefail; sudo journalctl -u '${monitor_service}' --no-pager -n 30"
fi

echo "==> recent Recovery service logs"
if [[ "${deploy_recovery}" == "0" ]]; then
  ssh "${host}" "set -euo pipefail; sudo systemctl is-active '${recovery_service}' >/dev/null 2>&1 || sudo systemctl is-active '${recovery_socket}'"
else
  ssh "${host}" "set -euo pipefail; sudo journalctl -u '${recovery_service}' --no-pager -n 30 || true"
fi

if [[ "${deploy_cloudflare}" == "1" ]]; then
  echo "==> deploying Cloudflare monitor worker"
  BNTPOOL_HOST="${host}" \
    bash "${repo_dir}/scripts/deploy_cloudflare_monitor_worker.sh"
fi
