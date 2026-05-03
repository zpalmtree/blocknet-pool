import { useCallback, useEffect, useState } from 'react';

import type { ApiClient } from '../api/client';
import { BlockStatusBadge } from '../components/BlockStatusBadge';
import { EmptyTableRow } from '../components/EmptyTableRow';
import { HashrateChart } from '../components/HashrateChart';
import { PayoutTxLinks } from '../components/PayoutTxLinks';
import {
  effortLabel,
  formatCoins,
  formatCompactCoins,
  formatPct,
  fmtSeconds,
  humanRate,
  roundToneClass,
  stratumUrl,
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

function barWidth(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value) || value <= 0) return '0%';
  const clamped = Math.min(value, 250);
  return `${(clamped / 250) * 100}%`;
}

export function DashboardPage({ api, poolInfo, liveTick, theme }: DashboardPageProps) {
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

  const copyStratum = useCallback(() => {
    void navigator.clipboard.writeText(stratumUrl(poolInfo?.stratum_port, poolInfo?.pool_url));
  }, [poolInfo?.pool_url, poolInfo?.stratum_port]);

  const round = insights?.round;
  const payoutEta = insights?.payout_eta;
  const dashboardLuckHistory = (insights?.luck_history ?? []).filter((row) => !row.orphaned);
  const hiddenOrphanRounds = Math.max(0, (insights?.luck_history.length ?? 0) - dashboardLuckHistory.length);
  const latestSolvedBlock = dashboardLuckHistory[0];
  const nextSweepAt = toUnixMs(payoutEta?.next_sweep_at);
  const nextSweepLabel = payoutEta?.next_sweep_at ? timeUntil(payoutEta.next_sweep_at) : '-';
  const payoutShortfall =
    payoutEta?.wallet_spendable == null
      ? 0
      : Math.max(0, payoutEta.pending_total_amount - payoutEta.wallet_spendable);
  const payoutLiquidityConstrained = payoutEta != null && payoutEta.pending_total_amount > 0 && payoutShortfall > 0;

  const avgLuck = insights?.avg_effort_pct;
  return (
    <div id="page-dashboard">
      <div className="page-header">
        <span className="page-kicker">Blocknet Mining Pool</span>
        <h1>Live Blocknet pool dashboard</h1>
        <p className="page-intro">
          Track pool hashrate, round luck, recent blocks, payout timing, and current chain conditions from the public
          dashboard.
        </p>
      </div>

      <div className="stratum-bar">
        <span style={{ fontSize: 14, fontWeight: 600, color: 'var(--muted)' }}>Stratum</span>
        <span className="endpoint" id="stratum-url">
          {stratumUrl(poolInfo?.stratum_port, poolInfo?.pool_url)}
        </span>
        <button className="copy-btn" onClick={copyStratum}>
          Copy
        </button>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">Pool</div>
        <div className="stats-card-group-grid">
          <div className="stat-card">
            <div className="label">Connected Miners</div>
            <div className="value" id="s-miners">{stats?.pool?.miners ?? '-'}</div>
          </div>
          <div className="stat-card">
            <div className="label">Pool Hashrate</div>
            <div className="value" id="s-hashrate">{humanRate(stats?.pool?.hashrate ?? 0)}</div>
          </div>
          <div className="stat-card">
            <div className="label">Network Hashrate</div>
            <div className="value" id="s-net-hashrate">{stats?.chain.network_hashrate ? humanRate(stats.chain.network_hashrate) : '-'}</div>
          </div>
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">Blocks</div>
        <div className="stats-card-group-grid">
          <div className="stat-card">
            <div className="label">Current Block</div>
            <div className="value mono" id="s-current-block">{stats?.chain.current_job_height ?? '-'}</div>
          </div>
          <div className="stat-card" title={latestSolvedBlock ? new Date(toUnixMs(latestSolvedBlock.timestamp)).toLocaleString() : undefined}>
            <div className="label">Last Solved Block</div>
            <div className="value mono" id="s-last-solved-block">{latestSolvedBlock?.block_height ?? '-'}</div>
          </div>
          <div className="stat-card">
            <div className="label">Blocks Found</div>
            <div className="value" id="s-blocks">{stats?.pool?.blocks_found ?? '-'}</div>
          </div>
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">Luck & Health</div>
        <div className="stats-card-group-grid">
          <div className="stat-card">
            <div className="label">Average Luck</div>
            <div className="value" id="s-avg-luck">{formatPct(avgLuck)}</div>
          </div>
          <div className="stat-card">
            <div className="label">Unique Orphans</div>
            <div className="value" id="s-orphaned-blocks">{stats?.pool?.orphaned_blocks ?? '-'}</div>
          </div>
          <div className="stat-card">
            <div className="label">Orphan Rate</div>
            <div className="value" id="s-orphan-rate">{formatPct(stats?.pool?.orphan_rate_pct)}</div>
          </div>
        </div>
      </div>

      <div className="section">
        <div className="section-header">
          <h2>Round Progress</h2>
          <span className={`round-chip ${roundToneClass(round?.effort_pct)}`}>
            {effortLabel(round?.effort_pct)}
          </span>
        </div>
        <div className="card">
          <div className="round-meta">
            <div>
              <span className="label">Round Effort</span>
              <div className="value mono">{formatPct(round?.effort_pct)}</div>
            </div>
            <div>
              <span className="label">Elapsed vs ETA</span>
              <div className="value mono">{formatPct(round?.timer_effort_pct)}</div>
            </div>
            <div>
              <span className="label">Expected Block Time</span>
              <div className="value mono">{round?.expected_block_seconds ? fmtSeconds(Math.floor(round.expected_block_seconds)) : '-'}</div>
            </div>
            <div>
              <span className="label">Round Elapsed</span>
              <div className="value mono">{round ? fmtSeconds(round.elapsed_seconds) : '-'}</div>
            </div>
          </div>

          <div className="round-progress-wrap">
            <div className="round-progress-track">
              <div className={`round-progress-fill ${roundToneClass(round?.effort_pct)}`} style={{ width: barWidth(round?.effort_pct) }} />
              <div className="round-marker marker-50">50%</div>
              <div className="round-marker marker-100">100%</div>
              <div className="round-marker marker-200">200%</div>
            </div>
          </div>

          <p className="round-note">
            Luck is probabilistic. Rounds above 100% are normal and do not imply pool issues.
          </p>
        </div>
      </div>

      <div className="stats-card-group">
        <div className="stats-card-group-title">Payouts</div>
        <div className="stats-card-group-grid">
          <div
            className="stat-card"
            title={
              payoutEta
                ? `${formatCoins(payoutEta.pending_total_amount)} currently queued`
                : undefined
            }
          >
            <div className="label">Payout Queue</div>
            <div className="value mono">
              {payoutEta ? formatCompactCoins(payoutEta.pending_total_amount) : '-'}
            </div>
            <div className="stat-meta">
              {payoutShortfall ? `${formatCompactCoins(payoutShortfall)} short` : 'funded'}
            </div>
          </div>
          <div
            className="stat-card"
            title={
              stats?.pool?.paid_to_miners_total != null
                ? `${formatCoins(stats.pool.paid_to_miners_total)} paid to miners`
                : undefined
            }
          >
            <div className="label">Total BNT Paid</div>
            <div className="value mono" id="s-total-paid">
              {stats?.pool?.paid_to_miners_total != null ? formatCompactCoins(stats.pool.paid_to_miners_total) : '-'}
            </div>
            <div className="stat-meta">
              {stats?.pool?.paid_to_miners_total != null ? formatCoins(stats.pool.paid_to_miners_total) : ''}
            </div>
          </div>
          <div className="stat-card">
            <div className="label">Next Sweep</div>
            <div className="value mono" title={nextSweepAt ? new Date(nextSweepAt).toLocaleString() : undefined}>
              {nextSweepLabel}
            </div>
          </div>
        </div>
      </div>

      {payoutEta && payoutLiquidityConstrained && (
        <div
          className="card"
          style={{
            marginTop: 18,
            marginBottom: 24,
            background: 'rgba(247, 180, 75, 0.12)',
            borderColor: 'rgba(247, 180, 75, 0.45)',
          }}
        >
          <div style={{ color: 'var(--text)', fontSize: 13 }}>
            Pool payouts are currently liquidity constrained. Spendable wallet balance:{' '}
            <span className="mono">{formatCoins(payoutEta.wallet_spendable ?? 0)}</span>. Locked/confirming wallet
            balance: <span className="mono">{formatCoins(payoutEta.wallet_pending ?? 0)}</span>. Queued:{' '}
            <span className="mono">{formatCoins(payoutEta.pending_total_amount)}</span>. Shortfall:{' '}
            <span className="mono">{formatCoins(payoutShortfall)}</span>. Those locked funds are
            already in the pool wallet and should become spendable as blocks mature. The ETA above is based on recent
            payout cadence and may slip until liquidity is restored.
          </div>
        </div>
      )}

      <div className="section">
        <div className="section-header">
          <h2>Pool Hashrate</h2>
          <div className="range-tabs" id="hashrate-ranges">
            {(['1h', '24h', '7d', '30d'] as Range[]).map((r) => (
              <button key={r} className={range === r ? 'active' : ''} onClick={() => setRange(r)}>
                {r}
              </button>
            ))}
          </div>
        </div>
        <HashrateChart data={history} range={range} theme={theme} />
      </div>

      <div className="section">
        <div className="section-header">
          <h2>Pool Luck History</h2>
          <a href="/luck" className="view-all">
            View All
          </a>
        </div>
        {hiddenOrphanRounds > 0 && (
          <div style={{ marginBottom: 12, color: 'var(--muted)', fontSize: 13 }}>
            Showing canonical rounds only on the dashboard. {hiddenOrphanRounds} orphaned
            {hiddenOrphanRounds === 1 ? ' row is' : ' rows are'} hidden here.
          </div>
        )}
        <div className="card table-scroll">
          <table>
            <thead>
              <tr>
                <th>Block</th>
                <th>Effort</th>
                <th>Round Time</th>
                <th>Status</th>
                <th>Found</th>
              </tr>
            </thead>
            <tbody>
              {!dashboardLuckHistory.length ? (
                <EmptyTableRow colSpan={5}>No round history yet</EmptyTableRow>
              ) : (
                dashboardLuckHistory.map((row) => (
                  <tr key={`${row.block_height}-${row.block_hash}`}>
                    <td>
                      <a href={`https://explorer.blocknetcrypto.com/block/${row.block_hash}`} target="_blank" rel="noopener">
                        {row.block_height}
                      </a>
                    </td>
                    <td>
                      <span className={`round-chip ${roundToneClass(row.effort_pct)}`}>{formatPct(row.effort_pct)}</span>
                    </td>
                    <td>{fmtSeconds(row.duration_seconds)}</td>
                    <td>
                      <BlockStatusBadge confirmed={row.confirmed} orphaned={row.orphaned} />
                    </td>
                    <td title={new Date(toUnixMs(row.timestamp)).toLocaleString()}>{timeAgo(row.timestamp)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="section">
        <div className="section-header">
          <h2>Recent Payouts</h2>
          <a href="/payouts" className="view-all">
            View All
          </a>
        </div>
        <div className="card table-scroll">
          <table>
            <thead>
              <tr>
                <th>Amount</th>
                <th>Miners Paid</th>
                <th>TX</th>
                <th>Status</th>
                <th>Time</th>
              </tr>
            </thead>
            <tbody id="dash-payouts-body">
              {!payouts.length ? (
                <EmptyTableRow colSpan={5}>No payouts yet</EmptyTableRow>
              ) : (
                payouts.map((p, idx) => (
                  <tr key={`${toUnixMs(p.timestamp)}-${idx}`}>
                    <td>{formatCoins(p.total_amount)}</td>
                    <td>{p.recipient_count}</td>
                    <td>
                      <PayoutTxLinks hashes={p.tx_hashes} />
                    </td>
                    <td>
                      <span className={`badge ${p.confirmed ? 'badge-confirmed' : 'badge-pending'}`}>
                        {p.confirmed ? 'confirmed' : 'unconfirmed'}
                      </span>
                    </td>
                    <td title={new Date(toUnixMs(p.timestamp)).toLocaleString()}>{timeAgo(p.timestamp)}</td>
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
