#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Apply the host hardening baseline on the current primary pool host. Idempotent.

  - nginx: rate limits, overwritten forwarding headers, HSTS, hidden version
  - nightly accounting database backup timer
  - ownership: the blocknet service user can read config and code but cannot
    change anything root or the deploy user later executes or loads

Usage:
  scripts/provision_bntpool_hardening.sh

Environment overrides:
  BNTPOOL_HOST                SSH host alias (default: bntpool)
  BNTPOOL_REMOTE_DIR          Remote pool directory (default: /opt/blocknet/blocknet-pool)
  BNTPOOL_ALLOW_RETIRED_HOST  Set to 1 to allow explicit provisioning on oldpool / 5.161.113.120
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

host="${BNTPOOL_HOST:-bntpool}"
remote_dir="${BNTPOOL_REMOTE_DIR:-/opt/blocknet/blocknet-pool}"
allow_retired_host="${BNTPOOL_ALLOW_RETIRED_HOST:-0}"

case "${host}" in
  oldpool|*5.161.113.120*)
    if [[ "${allow_retired_host}" != "1" ]]; then
      echo "refusing to target retired host '${host}'; use bntpool for the primary host or set BNTPOOL_ALLOW_RETIRED_HOST=1 to override" >&2
      exit 1
    fi
    ;;
esac

echo "==> enforcing ownership on ${host}"
ssh "${host}" "set -euo pipefail; \
  sudo chown root:root /etc/blocknet; sudo chmod 0755 /etc/blocknet; \
  sudo chown root:blocknet /etc/blocknet/pool; sudo chmod 0750 /etc/blocknet/pool; \
  for f in /etc/blocknet/pool/config.json /etc/blocknet/pool/.env /etc/blocknet/pool/.api_key /etc/blocknet/monitoring.env; do \
    if sudo test -f \"\$f\"; then sudo chown root:blocknet \"\$f\"; sudo chmod 0640 \"\$f\"; fi; \
  done; \
  sudo install -d -o root -g root -m 0755 /var/lib/blocknet-recovery; \
  sudo chown root:blocknet /opt/blocknet; sudo chmod 0750 /opt/blocknet; \
  sudo find '${remote_dir}' -path '${remote_dir}/data' -prune -o -exec chown -h \"\$(id -un):blocknet\" {} +; \
  sudo find '${remote_dir}' -path '${remote_dir}/data' -prune -o ! -type l -exec chmod go-w {} +; \
  sudo install -d -o blocknet -g blocknet -m 0750 '${remote_dir}/data'; \
  if sudo test -d /opt/blocknet/blocknet-core; then \
    sudo find /opt/blocknet/blocknet-core -mindepth 0 ! -name current -exec chown -h \"\$(id -un):blocknet\" {} +; \
    sudo chmod -R go-w /opt/blocknet/blocknet-core; \
  fi; \
  if sudo test -f /etc/sudoers.d/90-blocknet-bootstrap; then echo 'warning: blocknet still has sudo via /etc/sudoers.d/90-blocknet-bootstrap' >&2; fi"

echo "==> installing nginx hardening"
ssh "${host}" "set -euo pipefail; \
  sudo install -d -m 0755 /etc/nginx/snippets; \
  sudo install -m 0644 '${remote_dir}/deploy/nginx/bntpool-limits.conf' /etc/nginx/conf.d/bntpool-limits.conf; \
  sudo install -m 0644 '${remote_dir}/deploy/nginx/bntpool-proxy.conf' /etc/nginx/snippets/bntpool-proxy.conf; \
  sudo cp -a /etc/nginx/sites-available/bntpool.com /etc/nginx/sites-available/.bntpool.com.prev; \
  sudo install -m 0644 '${remote_dir}/deploy/nginx/bntpool.com.conf' /etc/nginx/sites-available/bntpool.com; \
  if sudo nginx -t; then sudo systemctl reload nginx; else \
    sudo cp -a /etc/nginx/sites-available/.bntpool.com.prev /etc/nginx/sites-available/bntpool.com; \
    echo 'nginx -t failed; restored the previous site config' >&2; exit 1; fi"

echo "==> installing nightly database backup"
ssh "${host}" "set -euo pipefail; \
  sudo install -d -o postgres -g postgres -m 0700 /var/backups/blocknet-pool; \
  sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-db-backup.service' /etc/systemd/system/blocknet-pool-db-backup.service; \
  sudo install -m 0644 '${remote_dir}/deploy/systemd/blocknet-pool-db-backup.timer' /etc/systemd/system/blocknet-pool-db-backup.timer; \
  sudo systemctl daemon-reload; \
  sudo systemctl enable --now blocknet-pool-db-backup.timer"

echo "==> done"
