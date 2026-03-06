import { useCallback, useEffect, useState } from 'react';

import type { ApiClient } from '../api/client';
import { fmtSeconds, timeAgo, toUnixMs } from '../lib/format';
import type { StatusResponse } from '../types';

interface StatusPageProps {
  active: boolean;
  api: ApiClient;
  liveTick: number;
}

function pct(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toFixed(2)}%`;
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
    if (!active || liveTick <= 0) return;
    void loadStatus();
  }, [active, liveTick, loadStatus]);

  return (
    <div className={active ? 'page active' : 'page'} id="page-status">
      <div className="page-header">
        <span className="page-kicker">Pool Monitoring</span>
        <h1>Blocknet pool status</h1>
        <p className="page-intro">
          Monitor uptime, daemon reachability, sync state, and recent incident history from the public status page.
        </p>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="label">Daemon</div>
          <div className="value">{status?.daemon?.reachable ? 'Online' : 'Offline'}</div>
        </div>
        <div className="stat-card">
          <div className="label">Sync State</div>
          <div className="value">{status?.daemon?.syncing ? 'Syncing' : 'Ready'}</div>
        </div>
        <div className="stat-card">
          <div className="label">Chain Height</div>
          <div className="value mono">{status?.daemon?.chain_height ?? '-'}</div>
        </div>
        <div className="stat-card">
          <div className="label">Pool Uptime</div>
          <div className="value mono">{status ? fmtSeconds(status.pool_uptime_seconds || 0) : '-'}</div>
        </div>
      </div>

      <div className="section">
        <div className="section-header">
          <h2>Historical Uptime</h2>
        </div>
        <div className="card table-scroll">
          <table>
            <thead>
              <tr>
                <th>Window</th>
                <th>Uptime</th>
                <th>Samples</th>
              </tr>
            </thead>
            <tbody>
              {!status?.uptime?.length ? (
                <tr>
                  <td colSpan={3} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                    No status samples yet
                  </td>
                </tr>
              ) : (
                status.uptime.map((row) => (
                  <tr key={row.label}>
                    <td>{row.label}</td>
                    <td>{pct(row.up_pct)}</td>
                    <td>{row.sample_count}</td>
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
                  <td colSpan={6} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                    No incidents recorded
                  </td>
                </tr>
              ) : (
                status.incidents.map((incident) => (
                  <tr key={`${incident.id}-${incident.kind}-${incident.started_at}`}>
                    <td>{incident.kind}</td>
                    <td>
                      <span className={`round-chip ${incident.severity === 'critical' ? 'is-critical' : 'is-warn'}`}>
                        {incident.severity}
                      </span>
                    </td>
                    <td title={new Date(toUnixMs(incident.started_at)).toLocaleString()}>{timeAgo(incident.started_at)}</td>
                    <td>{incident.duration_seconds != null ? fmtSeconds(incident.duration_seconds) : '-'}</td>
                    <td>{incident.ongoing ? 'Open' : 'Resolved'}</td>
                    <td>{incident.message}</td>
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
