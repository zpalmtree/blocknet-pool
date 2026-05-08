#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/.." && pwd)"
workspace_dir="$(cd "${repo_dir}/.." && pwd)"
daemon_repo="${BLOCKNET_DAEMON_REPO:-${workspace_dir}/blocknet-core}"
dockerfile="${daemon_repo}/docker/Dockerfile"
image_tag="blocknet-daemon-builder-local"
platform="linux/amd64"
output_path="${repo_dir}/build/blocknet-core-linux-amd64"
metadata_path=""
upload_host=""
remote_path="/opt/blocknet/blocknet-core/blocknet.new"
go_version=""
required_branch="${BLOCKNET_DAEMON_BRANCH:-pool}"
resolved_ref=""
source_revision=""
source_describe=""
build_dir=""
container_id=""

usage() {
  cat <<'EOF'
Usage: scripts/build_blocknet_daemon.sh [options]

Build a server-compatible Blocknet daemon binary from the sibling `blocknet-core`
repo using Docker, then optionally upload it to a remote host. The script builds
from the requested daemon branch via a temporary git worktree and does not
modify the caller's current checkout.

Options:
  --daemon-repo PATH     Path to the Blocknet daemon repo
  --daemon-branch VALUE  Required daemon git branch (default: pool)
  --output PATH          Local output path for the built binary
  --metadata PATH        Local output path for build metadata
  --upload HOST          Upload the built binary to HOST over ssh/scp
  --remote-path PATH     Remote destination path when --upload is set
  --image TAG            Docker image tag for the builder image
  --platform VALUE       Docker build platform (default: linux/amd64)
  --go-version VALUE     Override the Go toolchain version used in Docker
  --help                 Show this help text

Environment overrides:
  BLOCKNET_DAEMON_REPO    Path to the Blocknet daemon repo (default: ../blocknet-core)
  BLOCKNET_DAEMON_BRANCH  Required daemon git branch (default: pool)

Examples:
  scripts/build_blocknet_daemon.sh
  scripts/build_blocknet_daemon.sh --upload bntpool
  scripts/build_blocknet_daemon.sh --daemon-repo /path/to/blocknet-core --upload bntpool
EOF
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
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

write_build_metadata() {
  local path="$1"
  cat > "$path" <<EOF
branch=${required_branch}
source_ref=${resolved_ref}
revision=${source_revision}
describe=${source_describe}
built_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
source_repo=${daemon_repo}
EOF
}

cleanup() {
  if [[ -n "${container_id:-}" ]]; then
    docker rm -f "$container_id" >/dev/null 2>&1 || true
  fi
  if [[ -n "${build_dir:-}" ]] && git -C "${daemon_repo}" worktree list --porcelain | grep -Fq "worktree ${build_dir}"; then
    git -C "${daemon_repo}" worktree remove --force "${build_dir}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${build_dir:-}" ]]; then
    rm -rf "${build_dir}" >/dev/null 2>&1 || true
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --daemon-repo)
      daemon_repo="$2"
      dockerfile="${daemon_repo}/docker/Dockerfile"
      shift 2
      ;;
    --daemon-branch)
      required_branch="$2"
      shift 2
      ;;
    --output)
      output_path="$2"
      shift 2
      ;;
    --metadata)
      metadata_path="$2"
      shift 2
      ;;
    --upload)
      upload_host="$2"
      shift 2
      ;;
    --remote-path)
      remote_path="$2"
      shift 2
      ;;
    --image)
      image_tag="$2"
      shift 2
      ;;
    --platform)
      platform="$2"
      shift 2
      ;;
    --go-version)
      go_version="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_command docker
require_command git
if [[ -n "$upload_host" ]]; then
  require_command ssh
  require_command scp
fi

if [[ ! -f "${daemon_repo}/go.mod" ]]; then
  echo "Blocknet core repo not found at ${daemon_repo}" >&2
  exit 1
fi

if [[ ! -f "$dockerfile" ]]; then
  echo "Daemon Dockerfile not found at ${dockerfile}" >&2
  exit 1
fi

if [[ -z "${metadata_path}" ]]; then
  metadata_path="${output_path}.build-info"
fi

resolved_ref="$(resolve_daemon_ref "${daemon_repo}" "${required_branch}")"
source_revision="$(git -C "${daemon_repo}" rev-parse "${resolved_ref}")"
source_describe="$(git -C "${daemon_repo}" describe --tags --always "${resolved_ref}" 2>/dev/null || git -C "${daemon_repo}" rev-parse --short "${resolved_ref}")"
build_dir="$(mktemp -d "${TMPDIR:-/tmp}/blocknet-daemon-build.XXXXXX")"
trap cleanup EXIT
git -C "${daemon_repo}" worktree add --detach "${build_dir}" "${resolved_ref}" >/dev/null
dockerfile="${build_dir}/docker/Dockerfile"

if [[ -z "$go_version" ]]; then
  go_version="$(awk '/^go / {print $2; exit}' "${build_dir}/go.mod")"
fi

if [[ -z "$go_version" ]]; then
  echo "Failed to determine Go version from ${build_dir}/go.mod" >&2
  exit 1
fi

if [[ "$go_version" =~ ^[0-9]+\.[0-9]+$ ]]; then
  go_version="${go_version}.0"
fi

mkdir -p "$(dirname "$output_path")"
mkdir -p "$(dirname "$metadata_path")"

echo "Building Blocknet daemon from ${daemon_repo}"
echo "Using daemon branch ${required_branch} from ${resolved_ref} at $(git -C "${daemon_repo}" rev-parse --short "${resolved_ref}")"
echo "Using Go ${go_version} on platform ${platform}"

if docker buildx version >/dev/null 2>&1; then
  docker buildx build \
    --load \
    --platform "$platform" \
    --build-arg "GO_VERSION=${go_version}" \
    -f "$dockerfile" \
    --target builder \
    -t "$image_tag" \
    "$build_dir"
else
  docker build \
    --platform "$platform" \
    --build-arg "GO_VERSION=${go_version}" \
    -f "$dockerfile" \
    --target builder \
    -t "$image_tag" \
    "$build_dir"
fi

container_id="$(docker create --platform "$platform" "$image_tag")"

docker cp "$container_id:/build/blocknet" "$output_path"
chmod +x "$output_path"
write_build_metadata "$metadata_path"

echo "Built ${output_path}"
print_sha256 "$output_path"
echo "Wrote build metadata to ${metadata_path}"

if [[ -n "$upload_host" ]]; then
  remote_dir="$(dirname "$remote_path")"
  echo "Ensuring remote directory exists on ${upload_host}:${remote_dir}"
  ssh "$upload_host" "mkdir -p '$remote_dir'"
  echo "Uploading to ${upload_host}:${remote_path}"
  scp "$output_path" "${upload_host}:${remote_path}"
  cat <<EOF
Upload complete.

Next step on ${upload_host}:
  Stop the Blocknet daemon in a maintenance window, replace the live binary with
  ${remote_path}, and then restart the daemon.
EOF
fi
