#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 0 ]]; then
  echo 'Usage: scripts/prepare_bntpool_tls.sh (BNTPOOL_HOST defaults to bntpool)' >&2
  exit 2
fi
host="${BNTPOOL_HOST:-bntpool}"
case "$host" in
  oldpool|*5.161.113.120*) echo 'refusing to prepare TLS on the retired host' >&2; exit 1 ;;
esac
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
remote_script=/opt/blocknet/blocknet-pool/scripts/prepare_gateway_tls.py
rsync -rt --protect-args "$script_dir/prepare_gateway_tls.py" "$host:$remote_script"
ssh "$host" "sudo -n python3 '$remote_script' --install"
