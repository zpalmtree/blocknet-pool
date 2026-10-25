import { t } from '../i18n';
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
  { label: t("m541"), key: "public_http" },
  { label: "API", key: "api" },
  { label: t("m402"), key: "stratum" },
  { label: t("m542"), key: "database" },
  { label: t("m543"), key: "daemon" },
];

const UPTIME_COLUMNS: { label: string; value: (row: UptimeRow) => ReactNode }[] = [
  { label: t("m541"), value: (row) => formatPct(row.public_http_up_pct, 2) },
  { label: "API", value: (row) => formatPct(row.api_up_pct, 2) },
  { label: t("m402"), value: (row) => formatPct(row.stratum_up_pct, 2) },
  { label: t("m403"), value: (row) => formatPct(row.pool_up_pct, 2) },
  { label: t("m542"), value: (row) => formatPct(row.database_up_pct, 2) },
  { label: t("m543"), value: (row) => formatPct(row.daemon_up_pct, 2) },
  { label: t("m544"), value: (row) => row.sample_count },
  { label: t("m545"), value: (row) => row.external_sample_count },
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
  const poolStateLabel = status ? (status.healthy ? t('m048') : t('m576')) : "-";
  const syncStateLabel = !daemon ? "-" : !daemon.reachable ? t('m601') : daemon.syncing ? t('m573') : t('m572');
  const templateStateLabel = template?.observed ? (template.fresh ? t('m602') : t('m603')) : t('m525');
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
        <span className="page-kicker">{t("m546")}</span>
        <h1>{t("m547")}</h1>
        <p className="page-intro">{t("m548")}</p>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">{t("m549")}</div>
        <div className="stats-card-group-grid stats-grid-dense">
          <StatCard label={t("m403")} value={poolStateLabel} />
          {REACHABILITY_SERVICES.map((service) => {
            const health = status?.services[service.key];
            const value = !health ? "-" : !health.observed ? t('m525') : health.healthy ? t('m604') : t('m605');
            return <StatCard key={service.key} label={service.label} value={value} />;
          })}
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">{t("m550")}</div>
        <div className="stats-card-group-grid stats-grid-dense">
          <StatCard label={t("m551")} value={templateStateLabel} />
          <StatCard label={t("m552")} value={refreshLagLabel} mono />
          <StatCard label={t("m553")} value={templateAge} mono />
          <StatCard label={t("m554")} value={syncStateLabel} />
          <StatCard label={t("m340")} value={daemon?.chain_height ?? "-"} mono />
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">{t("m555")}</div>
        <div className="stats-card-group-grid stats-grid-dense">
          <StatCard label={t("m556")} value={status ? fmtSeconds(status.pool_uptime_seconds || 0) : "-"} mono />
          <StatCard label={uptimeLabel(t('m544'))} value={status?.uptime[0]?.sample_count ?? "-"} mono />
          <StatCard
            label={uptimeLabel(t('m545'))}
            value={status?.uptime[0]?.external_sample_count ?? "-"}
            mono
          />
        </div>
      </div>

      <div className="section">
        <div className="section-header">
          <div>
            <h2>{t("m557")}</h2>
            <p className="section-lead">{t("m558")}</p>
          </div>
        </div>
        <div className="card table-scroll">
          <table>
            <thead>
              <tr>
                <th>{t("m103")}</th>
                {UPTIME_COLUMNS.map((column) => (
                  <th key={column.label}>{column.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {!status?.uptime.length ? (<EmptyTableRow colSpan={9}>{t("m559")}</EmptyTableRow>) : (status.uptime.map((row) => (
                  <tr key={row.label}>
                    <td>{row.label}</td>
                    {UPTIME_COLUMNS.map((column) => (
                      <td key={column.label}>{column.value(row)}</td>
                    ))}
                  </tr>
                )))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="section">
        <div className="section-header">
          <h2>{t("m560")}</h2>
        </div>
        <div className="card table-scroll">
          <table>
            <thead>
              <tr>
                <th>{t("m561")}</th>
                <th>{t("m562")}</th>
                <th>{t("m356")}</th>
                <th>{t("m563")}</th>
                <th>{t("m006")}</th>
                <th>{t("m358")}</th>
              </tr>
            </thead>
            <tbody>
              {!status?.incidents.length ? (<EmptyTableRow colSpan={6}>{t("m564")}</EmptyTableRow>) : (status.incidents.map((incident) => (
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
                      {incident.duration_seconds != null ? fmtSeconds(incident.duration_seconds) : "-"}
                    </td>
                    <td>{incident.ongoing ? t("m565") : t("m566")}</td>
                    <td className="status-incident-message-cell">
                      <div
                        className="status-incident-message"
                        title={incident.message}
                      >
                        {incident.message}
                      </div>
                    </td>
                  </tr>
                )))}
            </tbody>
          </table>
        </div>
      </div>

    </div>
  );
}
