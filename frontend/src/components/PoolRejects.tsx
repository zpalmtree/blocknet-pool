import { t, tx } from '../i18n';
import { useCallback, useEffect, useState } from 'react';

import type { ApiClient } from '../api/client';
import { RangeTabs } from './RangeTabs';
import { formatPct, ratioPct } from '../lib/format';
import type { StatsInsightsResponse } from '../types';

type RejectionWindowRange = '1h' | '24h' | '7d';

function RejectionMetric({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rejection-metric">
      <div className="label">{label}</div>
      <div className="value mono">{value}</div>
    </div>
  );
}

// Pool-wide share rejections, shown on the status page.
export function PoolRejects({ api, liveTick }: { api: ApiClient; liveTick: number }) {
  const [rejectionRange, setRejectionRange] = useState<RejectionWindowRange>('1h');
  const [insights, setInsights] = useState<StatsInsightsResponse | null>(null);

  const loadRejections = useCallback(async () => {
    try {
      const d = await api.getStatsInsights(rejectionRange);
      setInsights(d);
    } catch {
      setInsights(null);
    }
  }, [api, rejectionRange]);

  useEffect(() => {
    void loadRejections();
  }, [loadRejections]);

  useEffect(() => {
    if (liveTick <= 0 || liveTick % 6 !== 0) return;
    void loadRejections();
  }, [liveTick, loadRejections]);

  const rejectionWindow = insights?.rejections ?? null;
  const rejectionChecked = (rejectionWindow?.accepted ?? 0) + (rejectionWindow?.rejected ?? 0);
  const rejectionRatePct = ratioPct(rejectionWindow?.rejected, rejectionChecked);
  const topWindowReason = rejectionWindow?.by_reason?.[0];
  const topTotalReason = rejectionWindow?.totals_by_reason?.[0];
  const totalRejected = (rejectionWindow?.totals_by_reason ?? []).reduce((sum, reason) => sum + reason.count, 0);
  const hasWindowRejections = (rejectionWindow?.rejected ?? 0) > 0;

  return (
    <div className="section">
      <div className="section-header">
        <h2>{t("m527")}</h2>
        <RangeTabs<RejectionWindowRange> options={['1h', '24h', '7d']} value={rejectionRange} onChange={setRejectionRange} />
      </div>
      <div className="card rejection-card">
        <div className="rejection-overview">
          <RejectionMetric label={t("m528")} value={rejectionChecked} />
          <RejectionMetric label={t("m084")} value={rejectionWindow?.rejected ?? 0} />
          <RejectionMetric label={t("m503")} value={formatPct(rejectionRatePct)} />
          <RejectionMetric label={t("m529")} value={topWindowReason?.reason || t("m530")} />
        </div>

        {!hasWindowRejections ? (<p className="rejection-empty">{tx("m532", [<span className="mono">{totalRejected}</span>, topTotalReason ? (<>{tx("m531", [<span className="mono">{topTotalReason.reason}</span>])}</>) : null])}</p>) : (<div className="rejection-breakdown">
            <div className="rejection-note">{tx("m533", [<span className="mono">{rejectionRange}</span>])}</div>
            <div className="rejection-list">
              {(rejectionWindow?.by_reason ?? []).slice(0, 4).map((reason) => {
                const windowPct =
                  (rejectionWindow?.rejected ?? 0) > 0
                    ? (reason.count / (rejectionWindow?.rejected ?? 0)) * 100
                    : 0;

                return (
                  <div key={reason.reason} className="rejection-row">
                    <div>
                      <div className="rejection-row-head">
                        <span>{reason.reason}</span>
                        <span className="mono">{formatPct(windowPct)}</span>
                      </div>
                      <div className="rejection-row-bar">
                        <div className="rejection-row-fill" style={{ width: `${Math.max(windowPct, 4)}%` }} />
                      </div>
                    </div>
                    <div className="rejection-row-count mono">
                      {reason.count}
                      <span>/</span>
                      {rejectionWindow?.rejected ?? 0}
                    </div>
                  </div>
                );
              })}
            </div>
            <div className="rejection-note">{tx("m535", [<span className="mono">{totalRejected}</span>, topTotalReason ? (<>{tx("m534", [<span className="mono">{topTotalReason.reason}</span>])}</>) : null])}</div>
          </div>)}
      </div>
    </div>
  );
}
