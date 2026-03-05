import { useCallback, useEffect, useState } from 'react';

import type { ApiClient } from '../api/client';
import { BlockStatusBadge } from '../components/BlockStatusBadge';
import { HashrateChart } from '../components/HashrateChart';
import { PayoutTxLinks } from '../components/PayoutTxLinks';
import { formatCoins, humanRate, stratumUrl, timeAgo, toUnixMs } from '../lib/format';
import type { BlockItem, HashratePoint, InfoResponse, PayoutItem, Range, StatsResponse } from '../types';

interface DashboardPageProps {
  active: boolean;
  api: ApiClient;
  poolInfo: InfoResponse | null;
}

export function DashboardPage({ active, api, poolInfo }: DashboardPageProps) {
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [blocks, setBlocks] = useState<BlockItem[]>([]);
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

  const loadBlocks = useCallback(async () => {
    try {
      const d = await api.getBlocks({ paged: 'true', limit: 5, offset: 0, sort: 'height_desc' });
      setBlocks(d.items || []);
    } catch {
      setBlocks([]);
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
    void loadBlocks();
    void loadPayouts();
    void loadHistory();

    const timer = window.setInterval(() => {
      void refreshStats();
    }, 5000);

    return () => window.clearInterval(timer);
  }, [active, loadBlocks, loadHistory, loadPayouts, refreshStats]);

  useEffect(() => {
    if (!active) return;
    void loadHistory();
  }, [active, loadHistory, range]);

  const copyStratum = useCallback(() => {
    void navigator.clipboard.writeText(stratumUrl(poolInfo?.stratum_port));
  }, [poolInfo?.stratum_port]);

  return (
    <div className={active ? 'page active' : 'page'} id="page-dashboard">
      <div className="stratum-bar">
        <span style={{ fontSize: 14, fontWeight: 600, color: 'var(--muted)' }}>Stratum</span>
        <span className="endpoint" id="stratum-url">
          {stratumUrl(poolInfo?.stratum_port)}
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
          <div className="label">Blocks Found</div>
          <div className="value" id="s-blocks">
            {stats?.pool?.blocks_found ?? '-'}
          </div>
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
        <HashrateChart data={history} range={range} />
      </div>

      <div className="section">
        <div className="section-header">
          <h2>Recent Blocks</h2>
          <a href="#/blocks" className="view-all">
            View All
          </a>
        </div>
        <div className="card table-scroll">
          <table>
            <thead>
              <tr>
                <th>Height</th>
                <th>Reward</th>
                <th>Status</th>
                <th>Time</th>
              </tr>
            </thead>
            <tbody id="dash-blocks-body">
              {!blocks.length ? (
                <tr>
                  <td colSpan={4} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                    No blocks found yet
                  </td>
                </tr>
              ) : (
                blocks.map((b) => (
                  <tr key={`${b.height}-${b.hash}`}>
                    <td>
                      <a href={`https://explorer.blocknetcrypto.com/block/${b.hash}`} target="_blank" rel="noopener">
                        {b.height}
                      </a>
                    </td>
                    <td>{formatCoins(b.reward)}</td>
                    <td>
                      <BlockStatusBadge confirmed={b.confirmed} orphaned={b.orphaned} />
                    </td>
                    <td title={new Date(toUnixMs(b.timestamp)).toLocaleString()}>{timeAgo(b.timestamp)}</td>
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
          <a href="#/payouts" className="view-all">
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
