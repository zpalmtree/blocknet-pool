#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Deploy a host-built blocknet-core daemon binary to the current primary pool host.

Usage:
  scripts/deploy_blocknet_daemon_bntpool.sh [--skip-build] [--release-id VALUE] [--daemon-branch VALUE]

This script upgrades the inactive daemon first, verifies wallet/mining readiness,
then cuts over through recoveryd before upgrading the original active daemon.
Payouts are drained and paused across cutovers; original routing is restored.

Environment overrides:
  BNTPOOL_DAEMON_READY_TIMEOUT  Seconds per readiness/drain gate (default: 900)
  BNTPOOL_HOST                  SSH host alias (default: bntpool)
  BNTPOOL_DAEMON_REMOTE_ROOT    Remote daemon root (default: /opt/blocknet/blocknet-core)
  BNTPOOL_DAEMON_LOCAL_BINARY   Local daemon artifact (default: build/blocknet-core-linux-amd64)
  BLOCKNET_DAEMON_REPO          Local blocknet-core repo (default: ../blocknet-core)
  BLOCKNET_DAEMON_BRANCH        Required daemon git branch (default: pool)
  BNTPOOL_ALLOW_RETIRED_HOST    Set to 1 to allow explicit daemon deploys to oldpool / 5.161.113.120
EOF
}

skip_build=0
release_id=""
required_branch="${BLOCKNET_DAEMON_BRANCH:-pool}"
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
    --daemon-branch)
      required_branch="$2"
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
remote_root="${BNTPOOL_DAEMON_REMOTE_ROOT:-/opt/blocknet/blocknet-core}"
remote_releases_dir="${remote_root}/releases"
local_binary="${BNTPOOL_DAEMON_LOCAL_BINARY:-${repo_dir}/build/blocknet-core-linux-amd64}"
template_unit_file="${repo_dir}/deploy/systemd/blocknetd@.service"
allow_retired_host="${BNTPOOL_ALLOW_RETIRED_HOST:-0}"
ready_timeout="${BNTPOOL_DAEMON_READY_TIMEOUT:-900}"
rolling_helper="${script_dir}/rolling_daemon_deploy.py"
if [[ ! "$ready_timeout" =~ ^[1-9][0-9]*$ ]]; then
  echo "BNTPOOL_DAEMON_READY_TIMEOUT must be a positive integer" >&2
  exit 1
fi
# Values below are interpolated into remote shell commands. Reject shell syntax.
if [[ ! "$remote_root" =~ ^/[a-zA-Z0-9_./-]+$ ]]; then
  echo "invalid daemon remote root" >&2
  exit 1
fi
resolved_ref=""
source_revision=""
source_describe=""
release_suffix=""
local_metadata="${local_binary}.build-info"
metadata_branch=""
metadata_source_ref=""
metadata_revision=""
metadata_describe=""

case "${host}" in
  oldpool|*5.161.113.120*)
    if [[ "${allow_retired_host}" != "1" ]]; then
      echo "refusing to target retired host '${host}'; use bntpool for the primary host or set BNTPOOL_ALLOW_RETIRED_HOST=1 to override" >&2
      exit 1
    fi
    ;;
esac

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

resolve_daemon_ref() {
  local repo="$1"
  local branch="$2"
  local candidate=""

  if ! git -C "$repo" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "daemon repo at ${repo} is not a git worktree; cannot verify required branch '${branch}'" >&2
    exit 1
  fi

  if git -C "$repo" rev-parse --verify --quiet "${branch}^{commit}" >/dev/null 2>&1; then
    printf '%s\n' "$branch"
    return 0
  fi

  for candidate in "refs/heads/${branch}" "refs/remotes/origin/${branch}" "refs/remotes/upstream/${branch}"; do
    if git -C "$repo" show-ref --verify --quiet "$candidate"; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  echo "required daemon branch '${branch}' not found in ${repo} (checked local, origin, and upstream refs)" >&2
  exit 1
}

read_metadata_value() {
  local file="$1"
  local key="$2"
  local line=""

  line="$(grep -E "^${key}=" "$file" | head -n 1 || true)"
  if [[ -z "$line" ]]; then
    echo "missing ${key} in ${file}" >&2
    exit 1
  fi
  printf '%s\n' "${line#*=}"
}

require_command ssh
require_command scp
require_command curl
require_command git

if [[ ! -f "${daemon_repo}/go.mod" ]]; then
  echo "blocknet-core repo not found at ${daemon_repo}" >&2
  exit 1
fi

if [[ ! -f "${template_unit_file}" ]]; then
  echo "managed daemon template systemd unit not found at ${template_unit_file}" >&2
  exit 1
fi

resolved_ref="$(resolve_daemon_ref "${daemon_repo}" "${required_branch}")"
source_revision="$(git -C "${daemon_repo}" rev-parse "${resolved_ref}")"
source_describe="$(git -C "${daemon_repo}" describe --tags --always "${resolved_ref}" 2>/dev/null || git -C "${daemon_repo}" rev-parse --short "${resolved_ref}")"
release_suffix="$(git -C "${daemon_repo}" rev-parse --short "${resolved_ref}")"

if [[ "${skip_build}" != "1" ]]; then
  echo "==> building blocknet-core daemon locally"
  bash "${repo_dir}/scripts/build_blocknet_daemon.sh" \
    --daemon-repo "${daemon_repo}" \
    --daemon-branch "${required_branch}" \
    --output "${local_binary}"
fi

if [[ ! -f "${local_binary}" ]]; then
  echo "local daemon artifact not found at ${local_binary}" >&2
  exit 1
fi

if [[ ! -f "${local_metadata}" ]]; then
  echo "local daemon build metadata not found at ${local_metadata}; rebuild without --skip-build or provide a matching artifact" >&2
  exit 1
fi

metadata_branch="$(read_metadata_value "${local_metadata}" "branch")"
metadata_source_ref="$(read_metadata_value "${local_metadata}" "source_ref")"
metadata_revision="$(read_metadata_value "${local_metadata}" "revision")"
metadata_describe="$(read_metadata_value "${local_metadata}" "describe")"

if [[ "${metadata_branch}" != "${required_branch}" ]]; then
  echo "local daemon artifact branch '${metadata_branch}' does not match required branch '${required_branch}'" >&2
  exit 1
fi

if [[ "${metadata_revision}" != "${source_revision}" ]]; then
  echo "local daemon artifact revision '${metadata_revision}' does not match ${resolved_ref} at '${source_revision}'" >&2
  echo "rebuild the daemon artifact or update the requested daemon branch/ref before deploying" >&2
  exit 1
fi

if [[ -z "${release_id}" ]]; then
  release_id="${required_branch//\//-}-${release_suffix}"
fi

if [[ ! "$release_id" =~ ^[a-zA-Z0-9_-][a-zA-Z0-9_.-]*$ ]]; then
  echo "invalid daemon release ID" >&2
  exit 1
fi
remote_release_dir="${remote_releases_dir}/${release_id}"
remote_tmp_binary="${remote_release_dir}/blocknet.new"
remote_binary="${remote_release_dir}/blocknet"
remote_tmp_template_unit="/tmp/blocknetd@.service.$$"
remote_metadata="${remote_release_dir}/build-info.txt"

echo "==> daemon artifact"
echo "release_id=${release_id}"
echo "branch=${metadata_branch}"
echo "source_ref=${metadata_source_ref}"
echo "revision=${metadata_revision}"
print_sha256 "${local_binary}"

echo "==> checking live rolling-deploy prerequisites before uploading"
ssh "${host}" "sudo -n python3 - --check --timeout '${ready_timeout}'" < "${rolling_helper}"

echo "==> ensuring remote release directories on ${host}"
ssh "${host}" "set -euo pipefail; mkdir -p '${remote_release_dir}'; test -f /etc/blocknet/recovery/primary.env; test -f /etc/blocknet/recovery/standby.env"

echo "==> uploading daemon binary and build metadata"
scp "${local_binary}" "${host}:${remote_tmp_binary}"
scp "${local_metadata}" "${host}:${remote_metadata}.new"
scp "${template_unit_file}" "${host}:${remote_tmp_template_unit}"

echo "==> installing staged release"
# Rename rather than truncating a binary which may still be executing on a retry.
ssh "${host}" "set -euo pipefail; \
  chmod 0755 '${remote_tmp_binary}'; \
  mv -f '${remote_tmp_binary}' '${remote_binary}'; \
  mv -f '${remote_metadata}.new' '${remote_metadata}'"

echo "==> rolling daemon upgrade"
ssh "${host}" "sudo -n python3 - --release '${remote_release_dir}' --unit '${remote_tmp_template_unit}' --timeout '${ready_timeout}'" < "${rolling_helper}"
ssh "${host}" "rm -f '${remote_tmp_template_unit}'"
