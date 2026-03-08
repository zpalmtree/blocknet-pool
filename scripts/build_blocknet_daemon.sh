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
upload_host=""
remote_path="/opt/blocknet/blocknet-core/blocknet.new"
go_version=""

usage() {
  cat <<'EOF'
Usage: scripts/build_blocknet_daemon.sh [options]

Build a server-compatible Blocknet daemon binary from the sibling `blocknet-core`
repo using Docker, then optionally upload it to a remote host.

Options:
  --daemon-repo PATH     Path to the Blocknet daemon repo
  --output PATH          Local output path for the built binary
  --upload HOST          Upload the built binary to HOST over ssh/scp
  --remote-path PATH     Remote destination path when --upload is set
  --image TAG            Docker image tag for the builder image
  --platform VALUE       Docker build platform (default: linux/amd64)
  --go-version VALUE     Override the Go toolchain version used in Docker
  --help                 Show this help text

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

while [[ $# -gt 0 ]]; do
  case "$1" in
    --daemon-repo)
      daemon_repo="$2"
      dockerfile="${daemon_repo}/docker/Dockerfile"
      shift 2
      ;;
    --output)
      output_path="$2"
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

if [[ -z "$go_version" ]]; then
  go_version="$(awk '/^go / {print $2; exit}' "${daemon_repo}/go.mod")"
fi

if [[ -z "$go_version" ]]; then
  echo "Failed to determine Go version from ${daemon_repo}/go.mod" >&2
  exit 1
fi

if [[ "$go_version" =~ ^[0-9]+\.[0-9]+$ ]]; then
  go_version="${go_version}.0"
fi

mkdir -p "$(dirname "$output_path")"

echo "Building Blocknet daemon from ${daemon_repo}"
echo "Using Go ${go_version} on platform ${platform}"

if docker buildx version >/dev/null 2>&1; then
  docker buildx build \
    --load \
    --platform "$platform" \
    --build-arg "GO_VERSION=${go_version}" \
    -f "$dockerfile" \
    --target builder \
    -t "$image_tag" \
    "$daemon_repo"
else
  docker build \
    --platform "$platform" \
    --build-arg "GO_VERSION=${go_version}" \
    -f "$dockerfile" \
    --target builder \
    -t "$image_tag" \
    "$daemon_repo"
fi

container_id="$(docker create --platform "$platform" "$image_tag")"
cleanup() {
  if [[ -n "${container_id:-}" ]]; then
    docker rm -f "$container_id" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

docker cp "$container_id:/build/blocknet" "$output_path"
chmod +x "$output_path"

echo "Built ${output_path}"
print_sha256 "$output_path"

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
