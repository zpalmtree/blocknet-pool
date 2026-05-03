import { type ReactNode, useCallback, useEffect, useState } from "react";

import type { ApiClient } from "../api/client";
import { EmptyTableRow } from "../components/EmptyTableRow";
import { fmtSeconds, formatPct, timeAgo, timestampTitle } from "../lib/format";
import type { StatusResponse } from "../types";

interface StatusPageProps {
  api: ApiClient;
  liveTick: number;
}

type StatusServiceKey = "public_http" | "api" | "stratum" | "database" | "daemon";
type UptimeRow = StatusResponse["uptime"][number];

const REACHABILITY_SERVICES: { label: string; key: StatusServiceKey }[] = [
  { label: "Public HTTP", key: "public_http" },
  { label: "API", key: "api" },
  { label: "Stratum", key: "stratum" },
  { label: "Database", key: "database" },
  { label: "Daemon", key: "daemon" },
];

const UPTIME_COLUMNS: { label: string; value: (row: UptimeRow) => ReactNode }[] = [
  { label: "Public HTTP", value: (row) => formatPct(row.public_http_up_pct, 2) },
  { label: "API", value: (row) => formatPct(row.api_up_pct, 2) },
  { label: "Stratum", value: (row) => formatPct(row.stratum_up_pct, 2) },
  { label: "Pool", value: (row) => formatPct(row.pool_up_pct, 2) },
  { label: "Database", value: (row) => formatPct(row.database_up_pct, 2) },
  { label: "Daemon", value: (row) => formatPct(row.daemon_up_pct, 2) },
  { label: "Local Samples", value: (row) => row.sample_count },
  { label: "External Samples", value: (row) => row.external_sample_count },
];

function poolState(status: StatusResponse | null): string {
  if (!status) return "-";
  return status.healthy ? "Healthy" : "Degraded";
}

function serviceState(status: StatusResponse | null, key: StatusServiceKey): string {
  if (!status) return "-";
  const service = status.services[key];
  if (!service.observed) return "Unknown";
  return service.healthy ? "Online" : "Down";
}

function syncState(status: StatusResponse | null): string {
  if (!status) return "-";
  if (!status.daemon.reachable) return "Offline";
  return status.daemon.syncing ? "Syncing" : "Ready";
}

function templateState(status: StatusResponse | null): string {
  if (!status?.template.observed) return "Unknown";
  return status.template.fresh ? "Fresh" : "Stale";
}

function fmtRefreshLag(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return "-";
  if (ms < 1000) return "<1s";
  return fmtSeconds(Math.max(1, Math.floor(ms / 1000)));
}

function StatusStatCard({ label, value, mono = false }: { label: ReactNode; value: ReactNode; mono?: boolean }) {
  return (
    <div className="stat-card">
      <div className="label">{label}</div>
      <div className={mono ? "value mono" : "value"}>{value}</div>
    </div>
  );
}

export function StatusPage({ api, liveTick }: StatusPageProps) {
  const [status, setStatus] = useState<StatusResponse | null>(null);

  const loadStatus = useCallback(async () => {
    try {
      const d = await api.getStatus();
      setStatus(d);
    } catch {
      setStatus(null);
    }
  }, [api]);

  useEffect(() => {
    void loadStatus();
  }, [loadStatus]);

  useEffect(() => {
    if (liveTick <= 0 || liveTick % 6 !== 0) return;
    void loadStatus();
  }, [liveTick, loadStatus]);

  const primaryUptimeLabel = status?.uptime[0]?.label;
  const uptimeLabel = (base: string) => (primaryUptimeLabel ? `${base} (${primaryUptimeLabel})` : base);
  const templateAge =
    status?.template.age_seconds != null ? fmtSeconds(status.template.age_seconds) : "-";

  return (
    <div id="page-status">
      <div className="page-header">
        <span className="page-kicker">Pool Monitoring</span>
        <h1>Blocknet pool status</h1>
        <p className="page-intro">
          Monitor public reachability, API health, Stratum freshness, database
          reachability, daemon state, and recent incident history from the
          public status page.
        </p>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">Pool Reachability</div>
        <div className="stats-card-group-grid stats-grid-dense">
          <StatusStatCard label="Pool" value={poolState(status)} />
          {REACHABILITY_SERVICES.map((service) => (
            <StatusStatCard key={service.key} label={service.label} value={serviceState(status, service.key)} />
          ))}
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">Job Template</div>
        <div className="stats-card-group-grid stats-grid-dense">
          <StatusStatCard label="Template Refresh" value={templateState(status)} />
          <StatusStatCard label="Refresh Lag" value={fmtRefreshLag(status?.template.last_refresh_millis)} mono />
          <StatusStatCard label="Current Template Age" value={templateAge} mono />
          <StatusStatCard label="Sync State" value={syncState(status)} />
          <StatusStatCard label="Chain Height" value={status?.daemon.chain_height ?? "-"} mono />
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">Sampling</div>
        <div className="stats-card-group-grid stats-grid-dense">
          <StatusStatCard label="API Uptime" value={status ? fmtSeconds(status.pool_uptime_seconds || 0) : "-"} mono />
          <StatusStatCard label={uptimeLabel("Local Samples")} value={status?.uptime[0]?.sample_count ?? "-"} mono />
          <StatusStatCard
            label={uptimeLabel("External Samples")}
            value={status?.uptime[0]?.external_sample_count ?? "-"}
            mono
          />
        </div>
      </div>

      <div className="section">
        <div className="section-header">
          <div>
            <h2>Historical Uptime</h2>
            <p className="section-lead">
              Local samples come from the on-box monitor. External samples come
              from the Cloudflare public probe.
            </p>
          </div>
        </div>
        <div className="card table-scroll">
          <table>
            <thead>
              <tr>
                <th>Window</th>
                {UPTIME_COLUMNS.map((column) => (
                  <th key={column.label}>{column.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {!status?.uptime.length ? (
                <EmptyTableRow colSpan={9}>No status samples yet</EmptyTableRow>
              ) : (
                status.uptime.map((row) => (
                  <tr key={row.label}>
                    <td>{row.label}</td>
                    {UPTIME_COLUMNS.map((column) => (
                      <td key={column.label}>{column.value(row)}</td>
                    ))}
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="section">
        <div className="section-header">
          <h2>Incident History</h2>
        </div>
        <div className="card table-scroll">
          <table>
            <thead>
              <tr>
                <th>Kind</th>
                <th>Severity</th>
                <th>Started</th>
                <th>Duration</th>
                <th>Status</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {!status?.incidents.length ? (
                <EmptyTableRow colSpan={6}>No incidents recorded</EmptyTableRow>
              ) : (
                status.incidents.map((incident) => (
                  <tr
                    key={`${incident.id}-${incident.kind}-${incident.started_at}`}
                  >
                    <td>{incident.kind}</td>
                    <td>
                      <span
                        className={`round-chip ${incident.severity === "critical" ? "is-critical" : "is-warn"}`}
                      >
                        {incident.severity}
                      </span>
                    </td>
                    <td title={timestampTitle(incident.started_at)}>
                      {timeAgo(incident.started_at)}
                    </td>
                    <td>
                      {incident.duration_seconds != null
                        ? fmtSeconds(incident.duration_seconds)
                        : "-"}
                    </td>
                    <td>{incident.ongoing ? "Open" : "Resolved"}</td>
                    <td className="status-incident-message-cell">
                      <div
                        className="status-incident-message"
                        title={incident.message}
                      >
                        {incident.message}
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

    </div>
  );
}
