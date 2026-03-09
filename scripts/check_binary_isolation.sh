#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/check_binary_isolation.sh [--profile dev|release]

Builds package binaries in a temporary copy of the repo, injects harmless
package-local markers, and verifies that unrelated service binaries keep the
same hash.
EOF
}

profile="release"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      profile="${2:-}"
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

case "${profile}" in
  dev|release) ;;
  *)
    echo "error: --profile must be dev or release" >&2
    exit 1
    ;;
esac

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required" >&2
  exit 1
fi

if ! command -v rsync >/dev/null 2>&1; then
  echo "error: rsync is required" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "${TMP_ROOT}"' EXIT

copy_repo() {
  local dest="$1"
  mkdir -p "${dest}"
  rsync -a \
    --exclude='.git' \
    --exclude='target' \
    --exclude='frontend/node_modules' \
    "${REPO_DIR}/" "${dest}/"
}

profile_dir() {
  if [[ "${profile}" == "release" ]]; then
    printf 'release'
  else
    printf 'debug'
  fi
}

build_pkg() {
  local repo="$1"
  local package="$2"
  local bin="$3"
  (
    cd "${repo}"
    cargo build --profile "${profile}" -p "${package}" --bin "${bin}" >/dev/null
  )
}

bin_hash() {
  local repo="$1"
  local bin="$2"
  sha256sum "${repo}/target/$(profile_dir)/${bin}" | awk '{print $1}'
}

append_marker() {
  local file="$1"
  local symbol="$2"
  local marker="$3"
  cat >>"${file}" <<EOF

#[used]
static ${symbol}: &[u8] = b"${marker}";
EOF
}

assert_changed() {
  local label="$1"
  local before="$2"
  local after="$3"
  if [[ "${before}" == "${after}" ]]; then
    echo "error: expected ${label} binary hash to change" >&2
    exit 1
  fi
}

assert_same() {
  local label="$1"
  local before="$2"
  local after="$3"
  if [[ "${before}" != "${after}" ]]; then
    echo "error: expected ${label} binary hash to stay unchanged" >&2
    exit 1
  fi
}

run_case() {
  local name="$1"
  local file="$2"
  local symbol="$3"
  local marker="$4"
  shift 4

  local case_dir="${TMP_ROOT}/${name}"
  copy_repo "${case_dir}"

  while [[ $# -gt 0 ]]; do
    build_pkg "${case_dir}" "$1" "$2"
    shift 2
  done

  local before_api="__skip__"
  local before_stratum="__skip__"
  local before_monitor="__skip__"
  local before_recovery="__skip__"
  [[ -x "${case_dir}/target/$(profile_dir)/blocknet-pool-api" ]] && before_api="$(bin_hash "${case_dir}" blocknet-pool-api)"
  [[ -x "${case_dir}/target/$(profile_dir)/blocknet-pool-stratum" ]] && before_stratum="$(bin_hash "${case_dir}" blocknet-pool-stratum)"
  [[ -x "${case_dir}/target/$(profile_dir)/blocknet-pool-monitor" ]] && before_monitor="$(bin_hash "${case_dir}" blocknet-pool-monitor)"
  [[ -x "${case_dir}/target/$(profile_dir)/blocknet-pool-recoveryd" ]] && before_recovery="$(bin_hash "${case_dir}" blocknet-pool-recoveryd)"

  append_marker "${case_dir}/${file}" "${symbol}" "${marker}"

  case "${name}" in
    api_only)
      build_pkg "${case_dir}" blocknet-pool-api-app blocknet-pool-api
      build_pkg "${case_dir}" blocknet-pool-stratum-app blocknet-pool-stratum
      assert_changed "api" "${before_api}" "$(bin_hash "${case_dir}" blocknet-pool-api)"
      assert_same "stratum" "${before_stratum}" "$(bin_hash "${case_dir}" blocknet-pool-stratum)"
      ;;
    recovery_only)
      build_pkg "${case_dir}" blocknet-pool-api-app blocknet-pool-api
      build_pkg "${case_dir}" blocknet-pool-stratum-app blocknet-pool-stratum
      build_pkg "${case_dir}" blocknet-pool-recoveryd-app blocknet-pool-recoveryd
      assert_changed "api" "${before_api}" "$(bin_hash "${case_dir}" blocknet-pool-api)"
      assert_changed "recoveryd" "${before_recovery}" "$(bin_hash "${case_dir}" blocknet-pool-recoveryd)"
      assert_same "stratum" "${before_stratum}" "$(bin_hash "${case_dir}" blocknet-pool-stratum)"
      ;;
    monitor_only)
      build_pkg "${case_dir}" blocknet-pool-api-app blocknet-pool-api
      build_pkg "${case_dir}" blocknet-pool-monitor-app blocknet-pool-monitor
      assert_changed "monitor" "${before_monitor}" "$(bin_hash "${case_dir}" blocknet-pool-monitor)"
      assert_same "api" "${before_api}" "$(bin_hash "${case_dir}" blocknet-pool-api)"
      ;;
    stratum_only)
      build_pkg "${case_dir}" blocknet-pool-api-app blocknet-pool-api
      build_pkg "${case_dir}" blocknet-pool-stratum-app blocknet-pool-stratum
      assert_changed "stratum" "${before_stratum}" "$(bin_hash "${case_dir}" blocknet-pool-stratum)"
      assert_same "api" "${before_api}" "$(bin_hash "${case_dir}" blocknet-pool-api)"
      ;;
    *)
      echo "error: unknown case ${name}" >&2
      exit 1
      ;;
  esac

  echo "[ok] ${name}"
}

run_case \
  api_only \
  apps/blocknet-pool-api/src/lib.rs \
  "__BINARY_ISOLATION_MARKER_API" \
  "api-isolation-marker" \
  blocknet-pool-api-app blocknet-pool-api \
  blocknet-pool-stratum-app blocknet-pool-stratum

run_case \
  recovery_only \
  crates/pool-recovery/src/lib.rs \
  "__BINARY_ISOLATION_MARKER_RECOVERY" \
  "recovery-isolation-marker" \
  blocknet-pool-api-app blocknet-pool-api \
  blocknet-pool-stratum-app blocknet-pool-stratum \
  blocknet-pool-recoveryd-app blocknet-pool-recoveryd

run_case \
  monitor_only \
  apps/blocknet-pool-monitor/src/lib.rs \
  "__BINARY_ISOLATION_MARKER_MONITOR" \
  "monitor-isolation-marker" \
  blocknet-pool-api-app blocknet-pool-api \
  blocknet-pool-monitor-app blocknet-pool-monitor

run_case \
  stratum_only \
  apps/blocknet-pool-stratum/src/main.rs \
  "__BINARY_ISOLATION_MARKER_STRATUM" \
  "stratum-isolation-marker" \
  blocknet-pool-api-app blocknet-pool-api \
  blocknet-pool-stratum-app blocknet-pool-stratum

echo "binary isolation checks passed"
