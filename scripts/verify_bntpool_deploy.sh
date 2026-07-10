#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Verify the current bntpool app deployment without changing service state.

Usage:
  scripts/verify_bntpool_deploy.sh

Environment overrides:
  BNTPOOL_HOST             SSH host alias (default: bntpool)
  BNTPOOL_REMOTE_DIR       Remote pool directory (default: /opt/blocknet/blocknet-pool)
  BNTPOOL_API_SERVICE      Systemd API service name (default: blocknet-pool-api.service)
  BNTPOOL_STRATUM_SERVICE  Systemd Stratum service name (default: blocknet-pool-stratum.service)
  BNTPOOL_MONITOR_SERVICE  Systemd monitor service name (default: blocknet-pool-monitor.service)
  BNTPOOL_RECOVERY_SERVICE Systemd recovery service name (default: blocknet-pool-recoveryd.service)
  BNTPOOL_RECOVERY_SOCKET  Systemd recovery socket name (default: blocknet-pool-recoveryd.socket)
  BNTPOOL_VERIFY_SINCE     journalctl --since value (default: 15 minutes ago)
  BNTPOOL_VERIFY_READY_ATTEMPTS  API readiness attempts at 5-second intervals (default: 24)
  BNTPOOL_ALLOW_RETIRED_HOST  Set to 1 to allow explicit checks against oldpool / 5.161.113.120
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

host="${BNTPOOL_HOST:-bntpool}"
remote_dir="${BNTPOOL_REMOTE_DIR:-/opt/blocknet/blocknet-pool}"
api_service="${BNTPOOL_API_SERVICE:-blocknet-pool-api.service}"
stratum_service="${BNTPOOL_STRATUM_SERVICE:-blocknet-pool-stratum.service}"
monitor_service="${BNTPOOL_MONITOR_SERVICE:-blocknet-pool-monitor.service}"
recovery_service="${BNTPOOL_RECOVERY_SERVICE:-blocknet-pool-recoveryd.service}"
recovery_socket="${BNTPOOL_RECOVERY_SOCKET:-blocknet-pool-recoveryd.socket}"
since="${BNTPOOL_VERIFY_SINCE:-15 minutes ago}"
ready_attempts="${BNTPOOL_VERIFY_READY_ATTEMPTS:-24}"
allow_retired_host="${BNTPOOL_ALLOW_RETIRED_HOST:-0}"

if [[ ! "${ready_attempts}" =~ ^[1-9][0-9]*$ ]]; then
  echo "BNTPOOL_VERIFY_READY_ATTEMPTS must be a positive integer" >&2
  exit 1
fi

case "${host}" in
  oldpool|*5.161.113.120*)
    if [[ "${allow_retired_host}" != "1" ]]; then
      echo "refusing to target retired host '${host}'; use bntpool for the primary host or set BNTPOOL_ALLOW_RETIRED_HOST=1 to override" >&2
      exit 1
    fi
    ;;
esac

ssh "${host}" "set -euo pipefail; \
  remote_dir='${remote_dir}'; \
  api_service='${api_service}'; \
  stratum_service='${stratum_service}'; \
  monitor_service='${monitor_service}'; \
  recovery_service='${recovery_service}'; \
  recovery_socket='${recovery_socket}'; \
  since='${since}'; \
  ready_attempts='${ready_attempts}'; \
  echo '==> deploy metadata'; \
  if [[ -f \"\${remote_dir}/deploy-info.txt\" ]]; then cat \"\${remote_dir}/deploy-info.txt\"; else echo 'deploy-info.txt missing'; fi; \
  echo '==> service active checks'; \
  for svc in \"\${api_service}\" \"\${stratum_service}\" \"\${monitor_service}\"; do \
    systemctl is-active --quiet \"\${svc}\" || { sudo systemctl status \"\${svc}\" --no-pager -l || true; exit 1; }; \
    echo \"active	\${svc}\"; \
  done; \
  if systemctl is-active --quiet \"\${recovery_service}\"; then \
    echo \"active	\${recovery_service}\"; \
  elif systemctl is-active --quiet \"\${recovery_socket}\"; then \
    echo \"active	\${recovery_socket}\"; \
  else \
    sudo systemctl status \"\${recovery_service}\" \"\${recovery_socket}\" --no-pager -l || true; \
    exit 1; \
  fi; \
  echo '==> service state'; \
  for svc in \"\${api_service}\" \"\${stratum_service}\" \"\${monitor_service}\" \"\${recovery_service}\" \"\${recovery_socket}\"; do \
    echo \"service	\${svc}\"; \
    systemctl show \"\${svc}\" --property=ActiveState,SubState,Result,ExecMainStartTimestamp,ExecMainPID,NRestarts --no-pager || true; \
  done; \
  echo '==> binary architecture'; \
  for bin in blocknet-pool-api blocknet-pool-stratum blocknet-pool-monitor blocknet-pool-recoveryd; do \
    path=\"\${remote_dir}/target/release/\${bin}\"; \
    test -x \"\${path}\"; \
    desc=\$(file -b \"\${path}\"); \
    case \"\${desc}\" in \
      *'ELF 64-bit'*'x86-64'*) hash=\$(sha256sum \"\${path}\" | cut -d ' ' -f 1); printf 'binary	%s	%s	%s\n' \"\${bin}\" \"\${hash}\" \"\${desc}\" ;; \
      *) echo \"\${bin} has incompatible binary format: \${desc}\" >&2; exit 1 ;; \
    esac; \
  done; \
  echo '==> API readiness'; \
  api_ready=0; \
  for attempt in \$(seq 1 \"\${ready_attempts}\"); do \
    if curl -fsS --max-time 2 http://127.0.0.1:24783/api/info >/dev/null 2>&1; then \
      api_ready=1; \
      break; \
    fi; \
    if [[ \"\${attempt}\" -eq \"\${ready_attempts}\" ]]; then break; fi; \
    echo \"API not ready: attempt \${attempt}/\${ready_attempts}; retrying in 5s\"; \
    sleep 5; \
  done; \
  if [[ \"\${api_ready}\" != \"1\" ]]; then \
    echo \"API did not become ready after \${ready_attempts} attempts\" >&2; \
    sudo systemctl status \"\${api_service}\" --no-pager -l || true; \
    exit 1; \
  fi; \
  echo '==> listener state'; \
  ss -ltnp 2>/dev/null | awk 'NR==1 || /:24783|:3333/' || true; \
  ss -ltn | awk 'NR > 1 { if (\$4 ~ /:24783$/) api = 1; if (\$4 ~ /:3333$/) stratum = 1 } END { if (!api) { print \"missing API listener on :24783\"; exit 1 } if (!stratum) { print \"missing Stratum listener on :3333\"; exit 1 } }'; \
  tmp_status=\$(mktemp); \
  tmp_health=\$(mktemp); \
  cleanup() { rm -f \"\${tmp_status}\" \"\${tmp_health}\"; }; \
  trap cleanup EXIT; \
  echo '==> public API checks'; \
  for path in /api/info /api/status /api/stats /api/blocks /api/payouts/recent; do \
    printf 'curl	http://127.0.0.1:24783%s	' \"\${path}\"; \
    curl -fsS --max-time 10 -o \"\${tmp_status}\" -w 'http=%{http_code} bytes=%{size_download} time=%{time_total}\n' \"http://127.0.0.1:24783\${path}\"; \
  done; \
  api_key=\$(python3 - <<'PY'
import json
print(json.load(open('/etc/blocknet/pool/config.json'))['api_key'])
PY
  ); \
  echo '==> protected health check'; \
  curl -fsS --max-time 10 -H \"x-api-key: \${api_key}\" -o \"\${tmp_health}\" -w 'curl	http://127.0.0.1:24783/api/health	http=%{http_code} bytes=%{size_download} time=%{time_total}\n' http://127.0.0.1:24783/api/health; \
  echo '==> status health check'; \
  status_ok=0; \
  for attempt in \$(seq 1 12); do \
    curl -fsS --max-time 10 http://127.0.0.1:24783/api/status -o \"\${tmp_status}\"; \
    if python3 - \"\${tmp_status}\" \"\${tmp_health}\" <<'PY'
import json
import sys

status = json.load(open(sys.argv[1]))
health = json.load(open(sys.argv[2]))
ongoing = [item for item in status.get('incidents', []) if item.get('ongoing')]
services = status.get('services', {})
unhealthy = [
    name
    for name, value in services.items()
    if isinstance(value, dict) and value.get('observed') and not value.get('healthy')
]
print(f\"status_healthy={status.get('healthy')}\")
print(f\"ongoing_incidents={len(ongoing)}\")
print(f\"unhealthy_services={','.join(unhealthy) if unhealthy else 'none'}\")
print(f\"daemon_reachable={status.get('daemon', {}).get('reachable')}\")
print(f\"daemon_syncing={status.get('daemon', {}).get('syncing')}\")
print(f\"connected_miners={health.get('pool_activity', {}).get('connected_miners')}\")
if not status.get('healthy'):
    raise SystemExit('status endpoint is not healthy')
if ongoing:
    raise SystemExit('status endpoint reports ongoing incidents')
if unhealthy:
    raise SystemExit('status endpoint reports unhealthy services')
PY
    then \
      status_ok=1; \
      break; \
    fi; \
    if [[ \"\${attempt}\" -eq 12 ]]; then break; fi; \
    echo \"status health not settled (attempt \${attempt}/12); retrying in 5s\"; \
    sleep 5; \
  done; \
  if [[ \"\${status_ok}\" != \"1\" ]]; then exit 1; fi; \
  echo '==> warning logs'; \
  for svc in \"\${api_service}\" \"\${stratum_service}\" \"\${monitor_service}\" \"\${recovery_service}\"; do \
    echo \"warnings	\${svc}\"; \
    sudo journalctl -u \"\${svc}\" --since \"\${since}\" -p warning --no-pager -n 80 || true; \
  done"
