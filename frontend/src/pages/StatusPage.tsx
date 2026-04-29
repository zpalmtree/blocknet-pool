import { useCallback, useEffect, useState } from "react";

import type { ApiClient } from "../api/client";
import { fmtSeconds, timeAgo, toUnixMs } from "../lib/format";
import type { StatusResponse } from "../types";

interface StatusPageProps {
  active: boolean;
  api: ApiClient;
  liveTick: number;
}

function pct(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "-";
  return `${value.toFixed(2)}%`;
}

function poolState(status: StatusResponse | null): string {
  if (!status) return "-";
  return status.pool?.healthy ? "Healthy" : "Degraded";
}

function serviceState(
  status: StatusResponse | null,
  key: "public_http" | "api" | "stratum" | "database" | "daemon",
): string {
  if (!status) return "-";
  const service = status.services?.[key];
  if (!service?.observed) return "Unknown";
  return service.healthy ? "Online" : "Down";
}

function syncState(status: StatusResponse | null): string {
  if (!status) return "-";
  if (!status.daemon?.reachable) return "Offline";
  return status.daemon?.syncing ? "Syncing" : "Ready";
}

function templateState(status: StatusResponse | null): string {
  if (!status?.template?.observed) return "Unknown";
  return status.template.fresh ? "Fresh" : "Stale";
}

function fmtRefreshLag(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return "-";
  if (ms < 1000) return "<1s";
  return fmtSeconds(Math.max(1, Math.floor(ms / 1000)));
}

export function StatusPage({ active, api, liveTick }: StatusPageProps) {
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
    if (!active) return;
    void loadStatus();
  }, [active, loadStatus]);

  useEffect(() => {
    if (!active || liveTick <= 0 || liveTick % 6 !== 0) return;
    void loadStatus();
  }, [active, liveTick, loadStatus]);

  const primaryUptimeLabel = status?.uptime?.[0]?.label;

  return (
    <div className={active ? "page active" : "page"} id="page-status">
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
          <div className="stat-card">
            <div className="label">Pool</div>
            <div className="value">{poolState(status)}</div>
          </div>
          <div className="stat-card">
            <div className="label">Public HTTP</div>
            <div className="value">{serviceState(status, "public_http")}</div>
          </div>
          <div className="stat-card">
            <div className="label">API</div>
            <div className="value">{serviceState(status, "api")}</div>
          </div>
          <div className="stat-card">
            <div className="label">Stratum</div>
            <div className="value">{serviceState(status, "stratum")}</div>
          </div>
          <div className="stat-card">
            <div className="label">Database</div>
            <div
              className="value"
              title={
                !status?.pool?.database_reachable
                  ? (status?.pool?.error ?? undefined)
                  : undefined
              }
            >
              {serviceState(status, "database")}
            </div>
          </div>
          <div className="stat-card">
            <div className="label">Daemon</div>
            <div className="value">{serviceState(status, "daemon")}</div>
          </div>
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">Job Template</div>
        <div className="stats-card-group-grid stats-grid-dense">
          <div className="stat-card">
            <div className="label">Template Refresh</div>
            <div className="value">{templateState(status)}</div>
          </div>
          <div className="stat-card">
            <div className="label">Refresh Lag</div>
            <div className="value mono">
              {fmtRefreshLag(status?.template?.last_refresh_millis)}
            </div>
          </div>
          <div className="stat-card">
            <div className="label">Current Template Age</div>
            <div className="value mono">
              {status?.template?.age_seconds != null
                ? fmtSeconds(status.template.age_seconds)
                : "-"}
            </div>
          </div>
          <div className="stat-card">
            <div className="label">Sync State</div>
            <div className="value">{syncState(status)}</div>
          </div>
          <div className="stat-card">
            <div className="label">Chain Height</div>
            <div className="value mono">
              {status?.daemon?.chain_height ?? "-"}
            </div>
          </div>
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">Sampling</div>
        <div className="stats-card-group-grid stats-grid-dense">
          <div className="stat-card">
            <div className="label">API Uptime</div>
            <div className="value mono">
              {status ? fmtSeconds(status.pool_uptime_seconds || 0) : "-"}
            </div>
          </div>
          <div className="stat-card">
            <div className="label">
              {primaryUptimeLabel
                ? `Local Samples (${primaryUptimeLabel})`
                : "Local Samples"}
            </div>
            <div className="value mono">
              {status?.uptime?.[0]?.sample_count ?? "-"}
            </div>
          </div>
          <div className="stat-card">
            <div className="label">
              {primaryUptimeLabel
                ? `External Samples (${primaryUptimeLabel})`
                : "External Samples"}
            </div>
            <div className="value mono">
              {status?.uptime?.[0]?.external_sample_count ?? "-"}
            </div>
          </div>
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
                <th>Public HTTP</th>
                <th>API</th>
                <th>Stratum</th>
                <th>Pool</th>
                <th>Database</th>
                <th>Daemon</th>
                <th>Local Samples</th>
                <th>External Samples</th>
              </tr>
            </thead>
            <tbody>
              {!status?.uptime?.length ? (
                <tr>
                  <td
                    colSpan={9}
                    style={{ textAlign: "center", color: "var(--muted)" }}
                  >
                    No status samples yet
                  </td>
                </tr>
              ) : (
                status.uptime.map((row) => (
                  <tr key={row.label}>
                    <td>{row.label}</td>
                    <td>{pct(row.public_http_up_pct)}</td>
                    <td>{pct(row.api_up_pct)}</td>
                    <td>{pct(row.stratum_up_pct)}</td>
                    <td>{pct(row.pool_up_pct)}</td>
                    <td>{pct(row.database_up_pct)}</td>
                    <td>{pct(row.daemon_up_pct)}</td>
                    <td>{row.sample_count}</td>
                    <td>{row.external_sample_count ?? 0}</td>
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
              {!status?.incidents?.length ? (
                <tr>
                  <td
                    colSpan={6}
                    style={{ textAlign: "center", color: "var(--muted)" }}
                  >
                    No incidents recorded
                  </td>
                </tr>
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
                    <td
                      title={new Date(
                        toUnixMs(incident.started_at),
                      ).toLocaleString()}
                    >
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

      <div className="seo-copy-grid">
        <div className="card seo-copy-card">
          <h3>Outside-In Reachability</h3>
          <p>
            Track whether the public API is reachable from outside the box, not
            just whether local processes are up.
          </p>
        </div>
        <div className="card seo-copy-card">
          <h3>Stratum Freshness</h3>
          <p>
            Catch cases where miners stop getting fresh work even though the
            pool site and daemon still respond.
          </p>
        </div>
        <div className="card seo-copy-card">
          <h3>Incident Tracking</h3>
          <p>
            Compare recent incidents against{" "}
            <a href="/payouts">payout timing</a> or{" "}
            <a href="/luck">round history</a> when you need to investigate
            operational issues.
          </p>
        </div>
      </div>
    </div>
  );
}
