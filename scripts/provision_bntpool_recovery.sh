#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Provision the repeatable daemon recovery topology on the current primary pool host.

Usage:
  scripts/provision_bntpool_recovery.sh

Environment overrides:
  BNTPOOL_HOST                SSH host alias (default: bntpool)
  BNTPOOL_REMOTE_DIR          Remote pool directory (default: /opt/blocknet/blocknet-pool)
  BNTPOOL_POOL_CONFIG         Pool config path on host (default: /etc/blocknet/pool/config.json)
  BNTPOOL_RECOVERY_DIR        Recovery config dir on host (default: /etc/blocknet/recovery)
  BNTPOOL_ALLOW_RETIRED_HOST  Set to 1 to allow explicit provisioning on oldpool / 5.161.113.120
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

host="${BNTPOOL_HOST:-bntpool}"
remote_dir="${BNTPOOL_REMOTE_DIR:-/opt/blocknet/blocknet-pool}"
pool_config="${BNTPOOL_POOL_CONFIG:-/etc/blocknet/pool/config.json}"
recovery_dir="${BNTPOOL_RECOVERY_DIR:-/etc/blocknet/recovery}"
allow_retired_host="${BNTPOOL_ALLOW_RETIRED_HOST:-0}"

case "${host}" in
  oldpool|*5.161.113.120*)
    if [[ "${allow_retired_host}" != "1" ]]; then
      echo "refusing to target retired host '${host}'; use bntpool for the primary host or set BNTPOOL_ALLOW_RETIRED_HOST=1 to override" >&2
      exit 1
    fi
    ;;
esac

remote_nginx_conf="/etc/nginx/conf.d/blocknet-daemon-proxy.conf"
remote_nginx_active="/etc/nginx/blocknet-daemon-active-upstream.inc"
remote_nginx_active_legacy="/etc/nginx/conf.d/blocknet-daemon-active-upstream.conf"
remote_active_cookie="/etc/blocknet/pool/daemon-active.api.cookie"
remote_secret="/etc/blocknet/recovery/pool-wallet.json"
remote_recovery_state_dir="/var/lib/blocknet-recovery"
remote_primary_env="${recovery_dir}/primary.env"
remote_standby_env="${recovery_dir}/standby.env"
remote_primary_cookie="/var/lib/blocknet/data/api.cookie"

echo "==> installing recovery topology assets on ${host}"
ssh "${host}" "set -euo pipefail; \
  sudo install -d -m 0755 '${recovery_dir}' '${remote_recovery_state_dir}' '/etc/blocknet/pool' '/var/lib/blocknet-standby/data'; \
  sudo chown -R blocknet:blocknet '${remote_recovery_state_dir}' '/var/lib/blocknet-standby'; \
  sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknetd@.service' '/etc/systemd/system/blocknetd@.service'; \
  sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-recoveryd.service' '/etc/systemd/system/blocknet-pool-recoveryd.service'; \
  sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-recoveryd.socket' '/etc/systemd/system/blocknet-pool-recoveryd.socket'; \
  sudo install -m 0644 '${remote_dir}/deploy/nginx/blocknet-daemon-proxy.conf' '${remote_nginx_conf}'"

echo "==> writing managed daemon instance env files"
ssh "${host}" "set -euo pipefail; cat <<'EOF' | sudo tee '${remote_primary_env}' >/dev/null
BLOCKNET_INSTANCE_API=127.0.0.1:18331
BLOCKNET_INSTANCE_WALLET=/var/lib/blocknet/wallet.dat
BLOCKNET_INSTANCE_DATA=/var/lib/blocknet/data
BLOCKNET_INSTANCE_LISTEN=/ip4/0.0.0.0/tcp/28080
BLOCKNET_P2P_KEY=/var/lib/blocknet/data/identity.key
EOF
cat <<'EOF' | sudo tee '${remote_standby_env}' >/dev/null
BLOCKNET_INSTANCE_API=127.0.0.1:18332
BLOCKNET_INSTANCE_WALLET=/var/lib/blocknet-standby/wallet.dat
BLOCKNET_INSTANCE_DATA=/var/lib/blocknet-standby/data
BLOCKNET_INSTANCE_LISTEN=/ip4/0.0.0.0/tcp/28081
BLOCKNET_P2P_KEY=/var/lib/blocknet-standby/data/identity.key
EOF
sudo chmod 0644 '${remote_primary_env}' '${remote_standby_env}'"

echo "==> seeding active daemon proxy and stable cookie path"
ssh "${host}" "set -euo pipefail; \
  sudo rm -f '${remote_nginx_active_legacy}'; \
  printf '%s\n' 'proxy_pass http://127.0.0.1:18331;' | sudo tee '${remote_nginx_active}' >/dev/null; \
  sudo ln -sfn '${remote_primary_cookie}' '${remote_active_cookie}'"

echo "==> creating pool wallet recovery secret stub if missing"
ssh "${host}" "set -euo pipefail; \
  if [[ ! -f '${remote_secret}' ]]; then \
    printf '%s\n' '{\"mnemonic\":\"\",\"password\":\"\"}' | sudo tee '${remote_secret}' >/dev/null; \
    sudo chmod 0600 '${remote_secret}'; \
  fi"

echo "==> updating pool config for stable daemon proxy and recovery settings"
ssh "${host}" "set -euo pipefail; sudo python3 - <<'PY'
import json
from pathlib import Path

path = Path('${pool_config}')
cfg = json.loads(path.read_text())
cfg['daemon_api'] = 'http://127.0.0.1:8332'
cfg['daemon_cookie_path'] = '/etc/blocknet/pool/daemon-active.api.cookie'
cfg['payout_pause_file'] = '/etc/blocknet/pool/payouts.pause'
recovery = cfg.setdefault('recovery', {})
recovery['enabled'] = True
recovery['socket_path'] = '/run/blocknet-recoveryd.sock'
recovery['state_path'] = '/var/lib/blocknet-recovery/state.json'
recovery['secret_path'] = '/etc/blocknet/recovery/pool-wallet.json'
recovery['proxy_include_path'] = '/etc/nginx/blocknet-daemon-active-upstream.inc'
recovery['active_cookie_path'] = '/etc/blocknet/pool/daemon-active.api.cookie'
recovery['primary'] = {
    'service': 'blocknetd@primary.service',
    'api': 'http://127.0.0.1:18331',
    'wallet_path': '/var/lib/blocknet/wallet.dat',
    'data_dir': '/var/lib/blocknet/data',
    'cookie_path': '/var/lib/blocknet/data/api.cookie',
}
recovery['standby'] = {
    'service': 'blocknetd@standby.service',
    'api': 'http://127.0.0.1:18332',
    'wallet_path': '/var/lib/blocknet-standby/wallet.dat',
    'data_dir': '/var/lib/blocknet-standby/data',
    'cookie_path': '/var/lib/blocknet-standby/data/api.cookie',
}
path.write_text(json.dumps(cfg, indent=2) + '\n')
PY
sudo chown root:root '${pool_config}'"

echo "==> validating nginx and enabling recovery socket"
ssh "${host}" "set -euo pipefail; \
  sudo systemctl daemon-reload; \
  sudo systemctl disable --now blocknetd.service >/dev/null 2>&1 || true; \
  sudo systemctl enable blocknetd@primary.service blocknetd@standby.service >/dev/null; \
  sudo nginx -t; \
  sudo systemctl enable --now blocknet-pool-recoveryd.socket >/dev/null; \
  sudo systemctl reload nginx; \
  sudo systemctl is-active blocknet-pool-recoveryd.socket"

echo "==> recovery topology provisioned"
ssh "${host}" "set -euo pipefail; \
  if [[ ! -s '${remote_secret}' || \$(sudo cat '${remote_secret}') == '{\"mnemonic\":\"\",\"password\":\"\"}' ]]; then \
    echo 'note: /etc/blocknet/recovery/pool-wallet.json is still a placeholder; populate it before using standby wallet rebuild'; \
  fi"
