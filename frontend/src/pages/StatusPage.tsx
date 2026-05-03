import { type ReactNode, useCallback, useEffect, useState } from "react";

import type { ApiClient } from "../api/client";
import { EmptyTableRow } from "../components/EmptyTableRow";
import { StatCard } from "../components/StatCard";
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

  const daemon = status?.daemon;
  const template = status?.template;
  const primaryUptimeLabel = status?.uptime[0]?.label;
  const uptimeLabel = (base: string) => (primaryUptimeLabel ? `${base} (${primaryUptimeLabel})` : base);
  const poolStateLabel = status ? (status.healthy ? "Healthy" : "Degraded") : "-";
  const syncStateLabel = !daemon ? "-" : !daemon.reachable ? "Offline" : daemon.syncing ? "Syncing" : "Ready";
  const templateStateLabel = template?.observed ? (template.fresh ? "Fresh" : "Stale") : "Unknown";
  const templateAge = template?.age_seconds != null ? fmtSeconds(template.age_seconds) : "-";
  const refreshLagMillis = template?.last_refresh_millis;
  const refreshLagLabel =
    refreshLagMillis == null || !Number.isFinite(refreshLagMillis)
      ? "-"
      : refreshLagMillis < 1000
        ? "<1s"
        : fmtSeconds(Math.max(1, Math.floor(refreshLagMillis / 1000)));

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
          <StatCard label="Pool" value={poolStateLabel} />
          {REACHABILITY_SERVICES.map((service) => {
            const health = status?.services[service.key];
            const value = !health ? "-" : !health.observed ? "Unknown" : health.healthy ? "Online" : "Down";
            return <StatCard key={service.key} label={service.label} value={value} />;
          })}
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">Job Template</div>
        <div className="stats-card-group-grid stats-grid-dense">
          <StatCard label="Template Refresh" value={templateStateLabel} />
          <StatCard label="Refresh Lag" value={refreshLagLabel} mono />
          <StatCard label="Current Template Age" value={templateAge} mono />
          <StatCard label="Sync State" value={syncStateLabel} />
          <StatCard label="Chain Height" value={daemon?.chain_height ?? "-"} mono />
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">Sampling</div>
        <div className="stats-card-group-grid stats-grid-dense">
          <StatCard label="API Uptime" value={status ? fmtSeconds(status.pool_uptime_seconds || 0) : "-"} mono />
          <StatCard label={uptimeLabel("Local Samples")} value={status?.uptime[0]?.sample_count ?? "-"} mono />
          <StatCard
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
