import { t, tx, localizePath, locale } from '../i18n';
import { useCallback, useEffect, useState } from 'react';

import type { ApiClient } from '../api/client';
import { BlockStatusBadge } from '../components/BlockStatusBadge';
import { EmptyTableRow } from '../components/EmptyTableRow';
import { ExplorerLink } from '../components/ExplorerLink';
import { HashrateChart } from '../components/HashrateChart';
import { PayoutStatusBadge } from '../components/PayoutStatusBadge';
import { PayoutTxLinks } from '../components/PayoutTxLinks';
import { RangeTabs } from '../components/RangeTabs';
import { StatCard } from '../components/StatCard';
import {
  effortLabel,
  formatCoins,
  formatCompactCoins,
  formatPct,
  fmtSeconds,
  humanRate,
  roundToneClass,
  stratumUrl,
  timestampTitle,
  timeAgo,
  timeUntil,
  toUnixMs,
} from '../lib/format';
import type { ThemeMode } from '../lib/theme';
import type {
  HashratePoint,
  InfoResponse,
  PayoutItem,
  Range,
  StatsInsightsResponse,
  StatsResponse,
} from '../types';

interface DashboardPageProps {
  api: ApiClient;
  poolInfo: InfoResponse | null;
  liveTick: number;
  theme: ThemeMode;
}

export function DashboardPage({ api, poolInfo, liveTick, theme }: DashboardPageProps) {
  const [copyState, setCopyState] = useState<'idle' | 'copied' | 'error'>('idle');
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [insights, setInsights] = useState<StatsInsightsResponse | null>(null);
  const [payouts, setPayouts] = useState<PayoutItem[]>([]);
  const [range, setRange] = useState<Range>('7d');
  const [history, setHistory] = useState<HashratePoint[]>([]);

  const refreshStats = useCallback(async () => {
    try {
      const d = await api.getStats();
      setStats(d);
    } catch {
      // handled by api client
    }
  }, [api]);

  const loadInsights = useCallback(async () => {
    try {
      const d = await api.getStatsInsights();
      setInsights(d);
    } catch {
      setInsights(null);
    }
  }, [api]);

  const loadPayouts = useCallback(async () => {
    try {
      const d = await api.getRecentPayouts(5, 0);
      setPayouts(d.items);
    } catch {
      setPayouts([]);
    }
  }, [api]);

  const loadHistory = useCallback(async () => {
    try {
      const d = await api.getStatsHistory(range);
      setHistory(d);
    } catch {
      setHistory([]);
    }
  }, [api, range]);

  useEffect(() => {
    void refreshStats();
    void loadInsights();
    void loadPayouts();
  }, [loadInsights, loadPayouts, refreshStats]);

  useEffect(() => {
    if (liveTick <= 0) return;
    if (liveTick % 2 === 0) {
      void refreshStats();
    }
    if (liveTick % 6 === 0) {
      void loadInsights();
      void loadHistory();
      void loadPayouts();
    }
  }, [liveTick, loadHistory, loadInsights, loadPayouts, refreshStats]);

  useEffect(() => {
    void loadHistory();
  }, [loadHistory, range]);

  const poolUrl = poolInfo ? stratumUrl(poolInfo.stratum_port, poolInfo.pool_url) : null;
  const fee = poolInfo?.pool_fee_pct;
  const feeLabel = fee != null && Number.isFinite(fee)
    ? `${fee.toLocaleString(locale, { maximumFractionDigits: 20 })}%` : '-';

  useEffect(() => {
    if (copyState !== 'copied') return;
    const timer = window.setTimeout(() => setCopyState('idle'), 2000);
    return () => window.clearTimeout(timer);
  }, [copyState]);

  const copyStratum = useCallback(async () => {
    if (!poolUrl) return;
    try {
      await navigator.clipboard.writeText(poolUrl);
      setCopyState('copied');
    } catch {
      setCopyState('error');
    }
  }, [poolUrl]);

  const round = insights?.round;
  const payoutEta = insights?.payout_eta;
  const dashboardLuckHistory = (insights?.luck_history ?? []).filter((row) => !row.orphaned);
  const hiddenOrphanRounds = Math.max(0, (insights?.luck_history.length ?? 0) - dashboardLuckHistory.length);
  const latestSolvedBlock = dashboardLuckHistory[0];
  const nextSweepLabel = payoutEta?.next_sweep_at ? timeUntil(payoutEta.next_sweep_at) : '-';
  const payoutShortfall =
    payoutEta?.wallet_spendable == null
      ? 0
      : Math.max(0, payoutEta.pending_total_amount - payoutEta.wallet_spendable);
  const payoutLiquidityConstrained = payoutEta != null && payoutEta.pending_total_amount > 0 && payoutShortfall > 0;

  const avgLuck = insights?.avg_effort_pct;
  const roundEffortPct = round?.effort_pct;
  const roundTone = roundToneClass(roundEffortPct);
  const cappedRoundEffortPct = Math.min(Math.max(roundEffortPct ?? 0, 0), 250);
  const roundProgressPct = Number.isFinite(cappedRoundEffortPct) ? (cappedRoundEffortPct / 250) * 100 : 0;
  return (
    <div id="page-dashboard">
      <header className="dashboard-header">
        <div className="dashboard-heading">
          <span className="page-kicker">{t('m001')}</span>
          <h1>{t('m399')}</h1>
          <p className="page-intro">{t('dashboardIntro')}</p>
        </div>
        <div className="dashboard-fee">
          <span className="dashboard-fee-label">{t('poolFee')}</span>
          <strong id="s-pool-fee"><bdi>{feeLabel}</bdi></strong>
        </div>
        <div className="dashboard-actions page-actions">
          <a className="btn btn-primary" href={localizePath('/start')}>{t('m002')}</a>
          <a className="dashboard-calculator" href={localizePath('/blog/') + 'estimate-blocknet-mining-rewards/#mining-calculator'}>
            {t('rewardsCalculator')}
            <span aria-hidden="true">↗</span>
          </a>
        </div>
        <div className="dashboard-connection">
          <span className="dashboard-connection-label">{t('m402')}</span>
          <div className="copy-field">
            <code className="copy-field-value" id="stratum-url" dir="ltr">{poolUrl ?? '-'}</code>
            <button
              className={`copy-field-btn${copyState === 'copied' ? ' is-copied' : ''}`}
              type="button"
              onClick={copyStratum}
              disabled={!poolUrl}
              aria-label={t('m387')}
            >
              <span role="status">{copyState === 'copied' ? t('m386') : t('m392')}</span>
            </button>
          </div>
          {copyState === 'error' && <p className="dashboard-copy-error" role="alert">{t('copyUnavailable')}</p>}
        </div>
      </header>

      <div className="stats-card-group">
        <div className="stats-card-group-title">{t("m403")}</div>
        <div className="stats-card-group-grid">
          <StatCard label={t("m072")} value={stats?.pool?.miners ?? '-'} id="s-miners" />
          <StatCard
            label={t("m404")}
            value={<bdi dir="ltr">{humanRate(stats?.pool?.raw_hashrate ?? stats?.pool?.hashrate ?? 0)}</bdi>}
            id="s-hashrate"
          />
          <StatCard
            label={t("m408")}
            value={stats?.chain.network_hashrate ? humanRate(stats.chain.network_hashrate) : '-'}
            id="s-net-hashrate"
          />
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">{t("m003")}</div>
        <div className="stats-card-group-grid">
          <StatCard label={t("m409")} value={stats?.chain.current_job_height ?? '-'} id="s-current-block" mono />
          <StatCard
            label={t("m410")}
            value={latestSolvedBlock?.block_height ?? '-'}
            id="s-last-solved-block"
            title={latestSolvedBlock ? timestampTitle(latestSolvedBlock.timestamp) : undefined}
            mono
          />
          <StatCard label={t("m411")} value={stats?.pool?.blocks_found ?? '-'} id="s-blocks" />
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">{t("m412")}</div>
        <div className="stats-card-group-grid">
          <StatCard label={t("m413")} value={formatPct(avgLuck)} id="s-avg-luck" />
          <StatCard label={t("m414")} value={stats?.pool?.orphaned_blocks ?? '-'} id="s-orphaned-blocks" />
          <StatCard
            label={t("m415")}
            value={formatPct(stats?.pool?.orphan_rate_30d_pct)}
            title={t("m416", [formatPct(stats?.pool?.orphan_rate_pct)])}
            id="s-orphan-rate"
          />
        </div>
      </div>

      <div className="section">
        <div className="section-header">
          <h2>{t("m417")}</h2>
          <span className={`round-chip ${roundTone}`}>
            {effortLabel(roundEffortPct)}
          </span>
        </div>
        <div className="card">
          <div className="round-meta">
            <div>
              <span className="label">{t("m418")}</span>
              <div className="value mono">{formatPct(roundEffortPct)}</div>
            </div>
            <div>
              <span className="label">{t("m419")}</span>
              <div className="value mono">{formatPct(round?.timer_effort_pct)}</div>
            </div>
            <div>
              <span className="label">{t("m420")}</span>
              <div className="value mono">{round?.expected_block_seconds ? fmtSeconds(Math.floor(round.expected_block_seconds)) : '-'}</div>
            </div>
            <div>
              <span className="label">{t("m421")}</span>
              <div className="value mono">{round ? fmtSeconds(round.elapsed_seconds) : '-'}</div>
            </div>
          </div>

          <div className="round-progress-wrap">
            <div className="round-progress-track">
              <div className={`round-progress-fill ${roundTone}`} style={{ width: `${roundProgressPct}%` }} />
              <div className="round-marker marker-50">50%</div>
              <div className="round-marker marker-100">100%</div>
              <div className="round-marker marker-200">200%</div>
            </div>
          </div>

          <p className="round-note">{t("m422")}</p>
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">{t("m004")}</div>
        <div className="stats-card-group-grid">
          <StatCard
            label={t("m270")}
            value={payoutEta ? formatCompactCoins(payoutEta.pending_total_amount) : '-'}
            meta={payoutShortfall ? t("m423", [formatCompactCoins(payoutShortfall)]) : t("m424")}
            mono
            title={payoutEta ? t("m425", [formatCoins(payoutEta.pending_total_amount)]) : undefined}
          />
          <StatCard
            label={t("m426")}
            value={stats?.pool?.paid_to_miners_total != null ? formatCompactCoins(stats.pool.paid_to_miners_total) : '-'}
            meta={stats?.pool?.paid_to_miners_total != null ? formatCoins(stats.pool.paid_to_miners_total) : ''}
            id="s-total-paid"
            mono
            title={stats?.pool?.paid_to_miners_total != null ? t("m427", [formatCoins(stats.pool.paid_to_miners_total)]) : undefined}
          />
          <StatCard
            label={t("m428")}
            value={nextSweepLabel}
            valueTitle={timestampTitle(payoutEta?.next_sweep_at) || undefined}
            mono
          />
        </div>
      </div>

      {payoutEta && payoutLiquidityConstrained && (<div
          className="card"
          style={{
            marginTop: 18,
            marginBottom: 24,
            background: 'rgba(247, 180, 75, 0.12)',
            borderColor: 'rgba(247, 180, 75, 0.45)',
          }}
        >
          <div style={{ color: 'var(--text)', fontSize: 13 }}>{tx("m429", [<span className="mono">{formatCoins(payoutEta.wallet_spendable ?? 0)}</span>, <span className="mono">{formatCoins(payoutEta.wallet_pending ?? 0)}</span>, <span className="mono">{formatCoins(payoutEta.pending_total_amount)}</span>, <span className="mono">{formatCoins(payoutShortfall)}</span>])}</div>
        </div>)}

      <div className="section">
        <div className="section-header">
          <h2>{t("m404")}</h2>
          <RangeTabs<Range> id="hashrate-ranges" options={['1h', '24h', '7d', '30d']} value={range} onChange={setRange} />
        </div>
        <HashrateChart data={history} range={range} theme={theme} />
      </div>

      <div className="section">
        <div className="section-header">
          <h2>{t("m430")}</h2>
          <a href={localizePath("/luck")} className="view-all">{t("m431")}</a>
        </div>
        {hiddenOrphanRounds > 0 && (<div style={{ marginBottom: 12, color: 'var(--muted)', fontSize: 13 }}>{t('m609', [hiddenOrphanRounds])}</div>)}
        <div className="card table-scroll">
          <table>
            <thead>
              <tr>
                <th>{t("m098")}</th>
                <th>{t("m375")}</th>
                <th>{t("m376")}</th>
                <th>{t("m006")}</th>
                <th>{t("m435")}</th>
              </tr>
            </thead>
            <tbody>
              {!dashboardLuckHistory.length ? (<EmptyTableRow colSpan={5}>{t("m436")}</EmptyTableRow>) : (dashboardLuckHistory.map((row) => (
                  <tr key={`${row.block_height}-${row.block_hash}`}>
                    <td>
                      <ExplorerLink kind="block" value={row.block_hash}>{row.block_height}</ExplorerLink>
                    </td>
                    <td>
                      <span className={`round-chip ${roundToneClass(row.effort_pct)}`}>{formatPct(row.effort_pct)}</span>
                    </td>
                    <td>{fmtSeconds(row.duration_seconds)}</td>
                    <td>
                      <BlockStatusBadge confirmed={row.confirmed} orphaned={row.orphaned} />
                    </td>
                    <td title={timestampTitle(row.timestamp)}>{timeAgo(row.timestamp)}</td>
                  </tr>
                )))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="section">
        <div className="section-header">
          <h2>{t("m437")}</h2>
          <a href={localizePath("/payouts")} className="view-all">{t("m431")}</a>
        </div>
        <div className="card table-scroll">
          <table>
            <thead>
              <tr>
                <th>{t("m438")}</th>
                <th>{t("m439")}</th>
                <th>TX</th>
                <th>{t("m006")}</th>
                <th>{t("m377")}</th>
              </tr>
            </thead>
            <tbody id="dash-payouts-body">
              {!payouts.length ? (<EmptyTableRow colSpan={5}>{t("m440")}</EmptyTableRow>) : (payouts.map((p, idx) => (
                  <tr key={`${toUnixMs(p.timestamp)}-${idx}`}>
                    <td>{formatCoins(p.total_amount)}</td>
                    <td>{p.recipient_count}</td>
                    <td>
                      <PayoutTxLinks hashes={p.tx_hashes} />
                    </td>
                    <td>
                      <PayoutStatusBadge status={p.confirmed ? 'confirmed' : 'unconfirmed'} />
                    </td>
                    <td title={timestampTitle(p.timestamp)}>{timeAgo(p.timestamp)}</td>
                  </tr>
                )))}
            </tbody>
          </table>
        </div>
      </div>

    </div>
  );
}
