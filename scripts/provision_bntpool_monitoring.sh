#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Provision the dedicated monitoring stack on the current primary pool host.

Usage:
  scripts/provision_bntpool_monitoring.sh [--skip-packages]

Environment overrides:
  BNTPOOL_HOST            SSH host alias (default: bntpool)
  BNTPOOL_REMOTE_DIR      Remote pool directory (default: /opt/blocknet/blocknet-pool)
  BNTPOOL_ALLOW_RETIRED_HOST  Set to 1 to allow explicit provisioning on oldpool / 5.161.113.120
  INSTALL_GRAFANA         Set to 1 to install Grafana provisioning files when Grafana exists
EOF
}

skip_packages=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-packages)
      skip_packages=1
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

host="${BNTPOOL_HOST:-bntpool}"
remote_dir="${BNTPOOL_REMOTE_DIR:-/opt/blocknet/blocknet-pool}"
install_grafana="${INSTALL_GRAFANA:-0}"
allow_retired_host="${BNTPOOL_ALLOW_RETIRED_HOST:-0}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/.." && pwd)"
rsync_args=(
  -rltzE
  --omit-dir-times
  --no-owner
  --no-group
)

case "${host}" in
  oldpool|*5.161.113.120*)
    if [[ "${allow_retired_host}" != "1" ]]; then
      echo "refusing to target retired host '${host}'; use bntpool for the primary host or set BNTPOOL_ALLOW_RETIRED_HOST=1 to override" >&2
      exit 1
    fi
    ;;
esac

echo "==> syncing monitoring assets to ${host}:${remote_dir}"
ssh "${host}" "set -euo pipefail; mkdir -p '${remote_dir}' '${remote_dir}/scripts'"
rsync "${rsync_args[@]}" \
  "${repo_dir}/deploy" \
  "${host}:${remote_dir}/"
rsync "${rsync_args[@]}" \
  "${repo_dir}/scripts/alertmanager_discord_relay.py" \
  "${host}:${remote_dir}/scripts/"

if [[ "${skip_packages}" -eq 0 ]]; then
  echo "==> installing monitoring packages"
  ssh "${host}" "set -euo pipefail; \
    sudo apt-get update; \
    sudo apt-get install -y prometheus prometheus-alertmanager prometheus-node-exporter prometheus-blackbox-exporter python3"
else
  echo "==> skipping package install"
fi

echo "==> installing monitoring configs and units"
ssh "${host}" "REMOTE_DIR='${remote_dir}' bash -se" <<'EOF'
set -euo pipefail

alertmanager_unit="$(systemctl list-unit-files --type=service --no-legend | awk '/^(prometheus-alertmanager|alertmanager)\.service / {print $1; exit}')"
node_exporter_unit="$(systemctl list-unit-files --type=service --no-legend | awk '/^(prometheus-node-exporter|node-exporter)\.service / {print $1; exit}')"
blackbox_unit="$(systemctl list-unit-files --type=service --no-legend | awk '/^(prometheus-blackbox-exporter|blackbox-exporter)\.service / {print $1; exit}')"
if [[ -z "${alertmanager_unit}" || -z "${node_exporter_unit}" || -z "${blackbox_unit}" ]]; then
  echo 'failed to detect monitoring unit names after package install' >&2
  exit 1
fi

sudo install -d -m 0755 /etc/prometheus/rules
sudo install -d -m 0755 /etc/prometheus
sudo install -d -m 0755 /etc/alertmanager
sudo install -d -m 0755 /etc/systemd/system/prometheus.service.d
sudo install -d -m 0755 "/etc/systemd/system/${alertmanager_unit}.d"
sudo install -d -m 0755 "/etc/systemd/system/${blackbox_unit}.d"
sudo install -d -m 0755 "/etc/systemd/system/${node_exporter_unit}.d"

sudo install -m 0644 "${REMOTE_DIR}/deploy/monitoring/prometheus/prometheus.yml" /etc/prometheus/prometheus.yml
sudo install -m 0644 "${REMOTE_DIR}/deploy/monitoring/prometheus/blocknet-pool.rules.yml" /etc/prometheus/rules/blocknet-pool.rules.yml
sudo install -m 0644 "${REMOTE_DIR}/deploy/monitoring/prometheus/blackbox.yml" /etc/prometheus/blackbox.yml
sudo install -m 0644 "${REMOTE_DIR}/deploy/monitoring/alertmanager/alertmanager.yml" /etc/alertmanager/alertmanager.yml
sudo install -m 0644 "${REMOTE_DIR}/deploy/systemd/prometheus.service.d/override.conf" /etc/systemd/system/prometheus.service.d/override.conf
sudo install -m 0644 "${REMOTE_DIR}/deploy/systemd/alertmanager.service.d/override.conf" "/etc/systemd/system/${alertmanager_unit}.d/override.conf"
sudo install -m 0644 "${REMOTE_DIR}/deploy/systemd/prometheus-blackbox-exporter.service.d/override.conf" "/etc/systemd/system/${blackbox_unit}.d/override.conf"
sudo install -m 0644 "${REMOTE_DIR}/deploy/systemd/prometheus-node-exporter.service.d/override.conf" "/etc/systemd/system/${node_exporter_unit}.d/override.conf"
sudo install -m 0644 "${REMOTE_DIR}/deploy/systemd/blocknet-pool-alertmanager-discord-relay.service" /etc/systemd/system/blocknet-pool-alertmanager-discord-relay.service
sudo chmod 0755 "${REMOTE_DIR}/scripts/alertmanager_discord_relay.py"

sudo python3 - <<'PY'
import json
import secrets
from pathlib import Path

path = Path('/etc/blocknet/pool/config.json')
data = json.loads(path.read_text())
if not str(data.get('monitor_ingest_secret', '')).strip():
    data['monitor_ingest_secret'] = secrets.token_hex(32)
    path.write_text(json.dumps(data, indent=2) + '\n')
PY

if [[ ! -f /etc/blocknet/monitoring.env ]]; then
  sudo install -d -m 0755 /etc/blocknet
  sudo install -m 0640 "${REMOTE_DIR}/deploy/monitoring/monitoring.env.example" /etc/blocknet/monitoring.env
fi

sudo promtool check config /etc/prometheus/prometheus.yml >/dev/null
sudo promtool check rules /etc/prometheus/rules/blocknet-pool.rules.yml >/dev/null
if command -v amtool >/dev/null 2>&1; then
  sudo amtool check-config /etc/alertmanager/alertmanager.yml >/dev/null
fi

sudo systemctl daemon-reload
EOF

if [[ "${install_grafana}" == "1" ]]; then
  echo "==> installing Grafana provisioning files when Grafana is present"
  ssh "${host}" "set -euo pipefail; \
    if [[ -d /etc/grafana ]]; then \
      sudo install -d -m 0755 /etc/grafana/provisioning/datasources; \
      sudo install -d -m 0755 /etc/grafana/provisioning/dashboards; \
      sudo install -d -m 0755 /var/lib/grafana/dashboards/blocknet-pool; \
      sudo install -m 0644 '${remote_dir}/deploy/monitoring/grafana/grafana.ini' /etc/grafana/grafana.ini; \
      sudo install -m 0644 '${remote_dir}/deploy/monitoring/grafana/provisioning/datasources/prometheus.yml' /etc/grafana/provisioning/datasources/prometheus.yml; \
      sudo install -m 0644 '${remote_dir}/deploy/monitoring/grafana/provisioning/dashboards/dashboards.yml' /etc/grafana/provisioning/dashboards/dashboards.yml; \
      sudo install -m 0644 '${remote_dir}/deploy/monitoring/grafana/dashboards/blocknet-pool-overview.json' /var/lib/grafana/dashboards/blocknet-pool/blocknet-pool-overview.json; \
      sudo systemctl restart grafana-server; \
    else \
      echo 'Grafana not installed; skipped provisioning files'; \
    fi"
fi

echo "==> enabling monitoring services"
ssh "${host}" "bash -se" <<'EOF'
set -euo pipefail

alertmanager_unit="$(systemctl list-unit-files --type=service --no-legend | awk '/^(prometheus-alertmanager|alertmanager)\.service / {print $1; exit}')"
node_exporter_unit="$(systemctl list-unit-files --type=service --no-legend | awk '/^(prometheus-node-exporter|node-exporter)\.service / {print $1; exit}')"
blackbox_unit="$(systemctl list-unit-files --type=service --no-legend | awk '/^(prometheus-blackbox-exporter|blackbox-exporter)\.service / {print $1; exit}')"

sudo systemctl enable prometheus "${alertmanager_unit}" "${node_exporter_unit}" "${blackbox_unit}" blocknet-pool-alertmanager-discord-relay.service
sudo systemctl restart "${node_exporter_unit}" "${blackbox_unit}" blocknet-pool-alertmanager-discord-relay.service "${alertmanager_unit}" prometheus
sudo systemctl is-active prometheus "${alertmanager_unit}" "${node_exporter_unit}" "${blackbox_unit}" blocknet-pool-alertmanager-discord-relay.service
EOF

echo "==> recent monitoring logs"
ssh "${host}" "bash -se" <<'EOF'
set -euo pipefail

alertmanager_unit="$(systemctl list-unit-files --type=service --no-legend | awk '/^(prometheus-alertmanager|alertmanager)\.service / {print $1; exit}')"
sudo journalctl -u prometheus --no-pager -n 20
printf '\n'
sudo journalctl -u "${alertmanager_unit}" --no-pager -n 20
printf '\n'
sudo journalctl -u blocknet-pool-alertmanager-discord-relay.service --no-pager -n 20
EOF

cat <<'EOF'
Monitoring stack installed.

Next steps:
  1. Set DISCORD_WEBHOOK_URL in /etc/blocknet/monitoring.env on the primary host.
  2. Set DISCORD_BOT_TOKEN there as well if the chain-height divergence bot should page Discord directly.
  3. Deploy the pool repo so blocknet-pool-monitor.service and the monitor binary are installed.
  4. Deploy the Cloudflare monitor worker once the API-side monitor_ingest_secret is in place.
  5. Reload/restart blocknet-pool-alertmanager-discord-relay.service after the webhook is set.
  6. Install Grafana separately if desired, then rerun with INSTALL_GRAFANA=1.
EOF
