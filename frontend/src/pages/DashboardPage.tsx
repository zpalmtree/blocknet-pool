import { useCallback, useEffect, useState } from 'react';

import type { ApiClient } from '../api/client';
import { HashrateChart } from '../components/HashrateChart';
import { PayoutTxLinks } from '../components/PayoutTxLinks';
import { formatCoins, fmtSeconds, humanRate, stratumUrl, timeAgo, toUnixMs } from '../lib/format';
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
  active: boolean;
  api: ApiClient;
  poolInfo: InfoResponse | null;
  liveTick: number;
  theme: ThemeMode;
}

function fmtPct(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toFixed(1)}%`;
}

function barWidth(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value) || value <= 0) return '0%';
  const clamped = Math.min(value, 250);
  return `${(clamped / 250) * 100}%`;
}

function toneClass(tone: string | undefined): string {
  if (tone === 'critical') return 'is-critical';
  if (tone === 'warn') return 'is-warn';
  return 'is-ok';
}

export function DashboardPage({ active, api, poolInfo, liveTick, theme }: DashboardPageProps) {
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [insights, setInsights] = useState<StatsInsightsResponse | null>(null);
  const [payouts, setPayouts] = useState<PayoutItem[]>([]);
  const [range, setRange] = useState<Range>('1h');
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
      const d = await api.getRecentPayouts({ limit: 5, offset: 0 });
      setPayouts(d.items || []);
    } catch {
      setPayouts([]);
    }
  }, [api]);

  const loadHistory = useCallback(async () => {
    try {
      const d = await api.getStatsHistory(range);
      setHistory(d || []);
    } catch {
      setHistory([]);
    }
  }, [api, range]);

  useEffect(() => {
    if (!active) return;
    void refreshStats();
    void loadInsights();
    void loadPayouts();
    void loadHistory();
  }, [active, loadHistory, loadInsights, loadPayouts, refreshStats]);

  useEffect(() => {
    if (!active || liveTick <= 0) return;
    void refreshStats();
    void loadInsights();
    void loadHistory();
    if (liveTick % 2 === 0) {
      void loadPayouts();
    }
  }, [active, liveTick, loadHistory, loadInsights, loadPayouts, refreshStats]);

  useEffect(() => {
    if (!active) return;
    void loadHistory();
  }, [active, loadHistory, range]);

  const copyStratum = useCallback(() => {
    void navigator.clipboard.writeText(stratumUrl(poolInfo?.stratum_port, poolInfo?.pool_url));
  }, [poolInfo?.pool_url, poolInfo?.stratum_port]);

  const round = insights?.round;
  const payoutEta = insights?.payout_eta;

  return (
    <div className={active ? 'page active' : 'page'} id="page-dashboard">
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

      <div className="stats-grid">
        <div className="stat-card">
          <div className="label">Connected Miners</div>
          <div className="value" id="s-miners">
            {stats?.pool?.miners ?? '-'}
          </div>
        </div>
        <div className="stat-card">
          <div className="label">Pool Hashrate</div>
          <div className="value" id="s-hashrate">
            {humanRate(stats?.pool?.hashrate ?? 0)}
          </div>
        </div>
        <div className="stat-card">
          <div className="label">Network Hashrate</div>
          <div className="value" id="s-net-hashrate">
            {stats?.chain?.network_hashrate ? humanRate(stats.chain.network_hashrate) : '-'}
          </div>
        </div>
        <div className="stat-card">
          <div className="label">Current Block</div>
          <div className="value mono" id="s-current-block">
            {stats?.chain?.current_job_height ?? '-'}
          </div>
        </div>
        <div className="stat-card">
          <div className="label">Blocks Found</div>
          <div className="value" id="s-blocks">
            {stats?.pool?.blocks_found ?? '-'}
          </div>
        </div>
        <div className="stat-card">
          <div className="label">Orphaned Blocks</div>
          <div className="value" id="s-orphaned-blocks">
            {stats?.pool?.orphaned_blocks ?? '-'}
          </div>
        </div>
        <div className="stat-card">
          <div className="label">Orphan Rate</div>
          <div className="value" id="s-orphan-rate">
            {fmtPct(stats?.pool?.orphan_rate_pct)}
          </div>
        </div>
      </div>

      <div className="section">
        <div className="section-header">
          <h2>Round Progress</h2>
          <span className={`round-chip ${toneClass(round?.effort_band?.tone)}`}>
            {round?.effort_band?.label || 'loading'}
          </span>
        </div>
        <div className="card">
          <div className="round-meta">
            <div>
              <span className="label">Round Effort</span>
              <div className="value mono">{fmtPct(round?.effort_pct)}</div>
            </div>
            <div>
              <span className="label">Elapsed vs ETA</span>
              <div className="value mono">{fmtPct(round?.timer_effort_pct)}</div>
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
              <div className={`round-progress-fill ${toneClass(round?.effort_band?.tone)}`} style={{ width: barWidth(round?.effort_pct) }} />
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

      <div className="stats-grid">
        <div className="stat-card">
          <div className="label">Pending Payouts</div>
          <div className="value mono">{payoutEta?.pending_count ?? '-'}</div>
        </div>
        <div className="stat-card">
          <div className="label">Pending Amount</div>
          <div className="value mono">{formatCoins(payoutEta?.pending_total_amount ?? 0)}</div>
        </div>
        <div className="stat-card">
          <div className="label">Payout ETA</div>
          <div className="value mono">{payoutEta?.eta_seconds != null ? fmtSeconds(payoutEta.eta_seconds) : '-'}</div>
        </div>
        <div className="stat-card">
          <div className="label">Typical Payout Interval</div>
          <div className="value mono">{payoutEta?.typical_interval_seconds ? fmtSeconds(payoutEta.typical_interval_seconds) : '-'}</div>
        </div>
      </div>

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
              {!insights?.luck_history?.length ? (
                <tr>
                  <td colSpan={5} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                    No round history yet
                  </td>
                </tr>
              ) : (
                insights.luck_history.map((row) => (
                  <tr key={`${row.block_height}-${row.block_hash}`}>
                    <td>
                      <a href={`https://explorer.blocknetcrypto.com/block/${row.block_hash}`} target="_blank" rel="noopener">
                        {row.block_height}
                      </a>
                    </td>
                    <td>
                      <span className={`round-chip ${toneClass(row.effort_band?.tone)}`}>{fmtPct(row.effort_pct)}</span>
                    </td>
                    <td>{fmtSeconds(row.duration_seconds)}</td>
                    <td>
                      {row.orphaned ? (
                        <span className="badge badge-orphaned">orphaned</span>
                      ) : row.confirmed ? (
                        <span className="badge badge-confirmed">confirmed</span>
                      ) : (
                        <span className="badge badge-pending">pending</span>
                      )}
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
                <th>Time</th>
              </tr>
            </thead>
            <tbody id="dash-payouts-body">
              {!payouts.length ? (
                <tr>
                  <td colSpan={4} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                    No payouts yet
                  </td>
                </tr>
              ) : (
                payouts.map((p, idx) => (
                  <tr key={`${toUnixMs(p.timestamp)}-${idx}`}>
                    <td>{formatCoins(p.total_amount)}</td>
                    <td>{p.recipient_count}</td>
                    <td>
                      <PayoutTxLinks hashes={p.tx_hashes} />
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
