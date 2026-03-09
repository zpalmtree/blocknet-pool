#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Deploy a host-built blocknet-core daemon binary to bntpool.

Usage:
  scripts/deploy_blocknet_daemon_bntpool.sh [--skip-build] [--release-id VALUE]

Environment overrides:
  BNTPOOL_HOST                  SSH host alias (default: bntpool)
  BNTPOOL_DAEMON_SERVICE        Systemd service name (default: blocknetd.service)
  BNTPOOL_DAEMON_REMOTE_ROOT    Remote daemon root (default: /opt/blocknet/blocknet-core)
  BNTPOOL_DAEMON_LOCAL_BINARY   Local daemon artifact (default: build/blocknet-core-linux-amd64)
  BLOCKNET_DAEMON_REPO          Local blocknet-core repo (default: ../blocknet-core)
EOF
}

skip_build=0
release_id=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build)
      skip_build=1
      shift
      ;;
    --release-id)
      release_id="$2"
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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/.." && pwd)"
workspace_dir="$(cd "${repo_dir}/.." && pwd)"
daemon_repo="${BLOCKNET_DAEMON_REPO:-${workspace_dir}/blocknet-core}"

host="${BNTPOOL_HOST:-bntpool}"
service="${BNTPOOL_DAEMON_SERVICE:-blocknetd.service}"
remote_root="${BNTPOOL_DAEMON_REMOTE_ROOT:-/opt/blocknet/blocknet-core}"
remote_releases_dir="${remote_root}/releases"
remote_current="${remote_root}/current"
local_binary="${BNTPOOL_DAEMON_LOCAL_BINARY:-${repo_dir}/build/blocknet-core-linux-amd64}"
unit_file="${repo_dir}/deploy/systemd/blocknetd.service"
template_unit_file="${repo_dir}/deploy/systemd/blocknetd@.service"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

print_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1"
  else
    shasum -a 256 "$1"
  fi
}

require_command ssh
require_command scp
require_command curl

if [[ ! -f "${daemon_repo}/go.mod" ]]; then
  echo "blocknet-core repo not found at ${daemon_repo}" >&2
  exit 1
fi

if [[ ! -f "${unit_file}" ]]; then
  echo "managed daemon systemd unit not found at ${unit_file}" >&2
  exit 1
fi

if [[ ! -f "${template_unit_file}" ]]; then
  echo "managed daemon template systemd unit not found at ${template_unit_file}" >&2
  exit 1
fi

if [[ "${skip_build}" != "1" ]]; then
  echo "==> building blocknet-core daemon locally"
  "${repo_dir}/scripts/build_blocknet_daemon.sh" \
    --daemon-repo "${daemon_repo}" \
    --output "${local_binary}"
fi

if [[ ! -f "${local_binary}" ]]; then
  echo "local daemon artifact not found at ${local_binary}" >&2
  exit 1
fi

if [[ -z "${release_id}" ]]; then
  if release_id="$(git -C "${daemon_repo}" describe --tags --always --dirty 2>/dev/null)"; then
    :
  else
    release_id="$(date +%Y%m%d%H%M%S)"
  fi
fi

remote_release_dir="${remote_releases_dir}/${release_id}"
remote_tmp_binary="${remote_release_dir}/blocknet.new"
remote_binary="${remote_release_dir}/blocknet"
remote_tmp_unit="/tmp/blocknetd.service.$$"
remote_tmp_template_unit="/tmp/blocknetd@.service.$$"

echo "==> daemon artifact"
echo "release_id=${release_id}"
print_sha256 "${local_binary}"

echo "==> ensuring remote release directories on ${host}"
ssh "${host}" "set -euo pipefail; mkdir -p '${remote_release_dir}' '${remote_root}'"

echo "==> uploading daemon binary"
scp "${local_binary}" "${host}:${remote_tmp_binary}"

echo "==> uploading managed systemd unit"
scp "${unit_file}" "${host}:${remote_tmp_unit}"
scp "${template_unit_file}" "${host}:${remote_tmp_template_unit}"

echo "==> installing release and restarting ${service}"
ssh "${host}" "set -euo pipefail; \
  install -m 0755 '${remote_tmp_binary}' '${remote_binary}'; \
  rm -f '${remote_tmp_binary}'; \
  ln -sfn '${remote_release_dir}' '${remote_root}/.current.new'; \
  mv -Tf '${remote_root}/.current.new' '${remote_current}'; \
  sudo install -m 0644 '${remote_tmp_unit}' '/etc/systemd/system/${service}'; \
  sudo install -m 0644 '${remote_tmp_template_unit}' '/etc/systemd/system/blocknetd@.service'; \
  rm -f '${remote_tmp_unit}'; \
  rm -f '${remote_tmp_template_unit}'; \
  sudo systemctl daemon-reload; \
  sudo systemctl enable '${service}' >/dev/null; \
  sudo systemctl restart '${service}'; \
  sudo systemctl is-active '${service}'"

echo "==> verifying daemon API"
ssh "${host}" "set -euo pipefail; \
  token=\$(cat /var/lib/blocknet/data/api.cookie); \
  curl -fsS -H \"Authorization: Bearer \${token}\" http://127.0.0.1:8332/api/status >/dev/null; \
  sudo journalctl -u '${service}' --no-pager -n 30"
