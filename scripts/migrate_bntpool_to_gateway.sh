#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
One-time switch of the primary pool host from a single Stratum service to
the Stratum gateway (STRATUM_GATEWAY.md). Miners reconnect once, during
the swap; after it, Stratum deploys hand miners over without disconnecting.

Run after the new binaries are on the host without a restart:
  scripts/deploy_bntpool.sh --stratum-only --no-restart
  scripts/migrate_bntpool_to_gateway.sh

Steps on the host:
  1. preflight: gateway binary present, Stratum binary supports the gateway,
     gateway not already enabled
  2. back up config.json and set gateway_socket
  3. install the gateway and blocknet-pool-stratum@ units
  4. stop blocknet-pool-stratum (no new engine starts until it has exited, so
     only one process ever owns the legacy found-block outbox), start the
     gateway on :3333, then start blocknet-pool-stratum@a; miners' logins are
     held by the gateway until the engine is up
  5. verify, then enable the new units and disable the old one

  scripts/migrate_bntpool_to_gateway.sh --rollback
restores the backed-up config and the single Stratum service.

Environment overrides:
  BNTPOOL_HOST                SSH host alias (default: bntpool)
  BNTPOOL_REMOTE_DIR          Remote pool directory (default: /opt/blocknet/blocknet-pool)
  BNTPOOL_ALLOW_RETIRED_HOST  Set to 1 to allow oldpool / 5.161.113.120
USAGE
}

mode=migrate
case "${1:-}" in
  "") ;;
  --rollback) mode=rollback ;;
  -h|--help) usage; exit 0 ;;
  *) usage >&2; exit 2 ;;
esac

host="${BNTPOOL_HOST:-bntpool}"
remote_dir="${BNTPOOL_REMOTE_DIR:-/opt/blocknet/blocknet-pool}"
case "${host}" in
  oldpool|*5.161.113.120*)
    if [[ "${BNTPOOL_ALLOW_RETIRED_HOST:-0}" != "1" ]]; then
      echo "refusing to target the retired host ${host}" >&2
      exit 1
    fi
    ;;
esac

config=/etc/blocknet/pool/config.json
socket=/run/blocknet-pool/gateway.sock
backup_glob="${config}.bak-pre-gateway-*"

if [[ "${mode}" == "rollback" ]]; then
  echo "==> rolling back ${host} to the single Stratum service"
  ssh "${host}" "set -euo pipefail; \
    backup=\$(ls -1t ${backup_glob} 2>/dev/null | head -1); \
    [[ -n \"\$backup\" ]] || { echo 'no pre-gateway config backup found' >&2; exit 1; }; \
    for i in a b; do sudo systemctl disable --now --quiet blocknet-pool-stratum@\$i.service 2>/dev/null || true; done; \
    sudo systemctl disable --now --quiet blocknet-pool-gateway.service || true; \
    sudo install -m 0640 -o root -g blocknet \"\$backup\" ${config}; \
    sudo systemctl enable --now --quiet blocknet-pool-stratum.service; \
    for _ in \$(seq 1 30); do systemctl is-active --quiet blocknet-pool-stratum.service && break; sleep 1; done; \
    systemctl is-active blocknet-pool-stratum.service; \
    echo \"restored \$backup\""
  exit 0
fi

echo "==> preflight on ${host}"
ssh "${host}" "set -euo pipefail; \
  test -x '${remote_dir}/target/release/blocknet-pool-gateway' || { echo 'gateway binary missing; run deploy_bntpool.sh --stratum-only --no-restart first' >&2; exit 1; }; \
  grep -q 'connected to stratum gateway' '${remote_dir}/target/release/blocknet-pool-stratum' || { echo 'Stratum binary predates gateway support; deploy it with --no-restart first' >&2; exit 1; }; \
  test -f '${remote_dir}/deploy/systemd/blocknet-pool-gateway.service' && test -f '${remote_dir}/deploy/systemd/blocknet-pool-stratum@.service' || { echo 'unit files missing under ${remote_dir}/deploy/systemd' >&2; exit 1; }; \
  if [[ \"\$(systemctl is-enabled blocknet-pool-gateway.service 2>/dev/null || true)\" == enabled ]]; then echo 'gateway already enabled; nothing to migrate' >&2; exit 1; fi; \
  systemctl is-active --quiet blocknet-pool-stratum.service || { echo 'blocknet-pool-stratum is not running' >&2; exit 1; }"

echo "==> backing up ${config} and setting gateway_socket"
ssh "${host}" "set -euo pipefail; \
  sudo cp -p ${config} ${config}.bak-pre-gateway-\$(date -u +%Y%m%dT%H%M%SZ); \
  sudo python3 - <<'PY'
import json, os
path = '${config}'
with open(path) as f:
    cfg = json.load(f)
cfg['gateway_socket'] = '${socket}'
tmp = path + '.tmp'
with open(tmp, 'w') as f:
    json.dump(cfg, f, indent=2)
    f.write('\n')
st = os.stat(path)
os.chown(tmp, st.st_uid, st.st_gid)
os.chmod(tmp, st.st_mode & 0o7777)
os.replace(tmp, path)
PY"

echo "==> installing gateway and engine units"
ssh "${host}" "set -euo pipefail; \
  sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-gateway.service' /etc/systemd/system/blocknet-pool-gateway.service; \
  sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-stratum@.service' /etc/systemd/system/blocknet-pool-stratum@.service; \
  sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-monitor.service' /etc/systemd/system/blocknet-pool-monitor.service; \
  sudo install -d -m 0755 /etc/systemd/system/postgresql@.service.d; \
  sudo install -m 0644 '${remote_dir}/deploy/systemd/postgresql@.service.d/restart-blocknet-pool.conf' /etc/systemd/system/postgresql@.service.d/restart-blocknet-pool.conf; \
  sudo systemctl daemon-reload"

echo "==> swapping: stop Stratum, start the gateway, start blocknet-pool-stratum@a (miners reconnect once)"
ssh "${host}" "set -euo pipefail; \
  started=\$(date +%s.%N); \
  sudo systemctl stop --no-block blocknet-pool-stratum.service; \
  for _ in \$(seq 1 300); do \
    if ! systemctl is-active --quiet blocknet-pool-stratum.service && ! ss -ltn '( sport = :3333 )' | grep -q 3333; then break; fi; \
    sleep 0.1; \
  done; \
  sudo systemctl start blocknet-pool-gateway.service; \
  sudo systemctl start blocknet-pool-stratum@a.service; \
  echo \"swap took \$(awk -v start=\$started -v end=\$(date +%s.%N) 'BEGIN { printf \"%.1f\", end - start }') s\""

echo "==> verifying"
ssh "${host}" "set -euo pipefail; \
  for _ in \$(seq 1 60); do \
    engines=\$(curl -s 127.0.0.1:24786/metrics | awk '/^blocknet_pool_gateway_engines /{print \$2}'); \
    [[ \"\$engines\" == 1 ]] && break; sleep 1; \
  done; \
  [[ \"\$engines\" == 1 ]] || { echo 'engine did not connect to the gateway; run --rollback' >&2; sudo journalctl -u blocknet-pool-stratum@a --no-pager -n 40 >&2; exit 1; }; \
  sleep 20; \
  curl -s 127.0.0.1:24786/metrics | grep -E 'gateway_(miners|miners_without_engine|engines|busy_replies_total)'; \
  systemctl is-active blocknet-pool-gateway.service blocknet-pool-stratum@a.service"

echo "==> enabling the gateway units and disabling the single Stratum unit"
ssh "${host}" "set -euo pipefail; \
  sudo systemctl enable --quiet blocknet-pool-gateway.service blocknet-pool-stratum@a.service; \
  sudo systemctl disable --quiet blocknet-pool-stratum.service"

echo "==> done. Stratum deploys now hand miners over: scripts/deploy_bntpool.sh --stratum-only"
