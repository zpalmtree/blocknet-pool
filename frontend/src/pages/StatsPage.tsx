import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { ApiClient } from '../api/client';
import { HashrateChart } from '../components/HashrateChart';
import { LAST_MINER_LOOKUP_KEY } from '../lib/storage';
import { formatCoins, humanRate, timeAgo, toUnixMs } from '../lib/format';
import type { ThemeMode } from '../lib/theme';
import type { HashratePoint, MinerResponse, Range, StatsInsightsResponse } from '../types';

interface StatsPageProps {
  active: boolean;
  api: ApiClient;
  liveTick: number;
  theme: ThemeMode;
}

function fmtPct(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toFixed(1)}%`;
}

type RejectionWindowRange = '1h' | '24h' | '7d';

const RECENT_SHARES_LIMIT = 20;

export function StatsPage({ active, api, liveTick, theme }: StatsPageProps) {
  const [minerInput, setMinerInput] = useState(localStorage.getItem(LAST_MINER_LOOKUP_KEY) || '');
  const [minerAddress, setMinerAddress] = useState('');
  const [minerData, setMinerData] = useState<MinerResponse | null>(null);
  const [range, setRange] = useState<Range>('1h');
  const [history, setHistory] = useState<HashratePoint[]>([]);
  const [rejectionRange, setRejectionRange] = useState<RejectionWindowRange>('1h');
  const [rejectionWindow, setRejectionWindow] = useState<StatsInsightsResponse['rejections']['window'] | null>(null);
  const minerAddressRef = useRef(minerAddress);
  const lookupRequestSeq = useRef(0);

  useEffect(() => {
    minerAddressRef.current = minerAddress;
  }, [minerAddress]);

  const loadMinerLookup = useCallback(
    async (input?: string) => {
      const addr = (input ?? minerInput).trim();
      if (!addr) return;
      const requestId = ++lookupRequestSeq.current;
      try {
        const d = await api.getMiner(addr);
        if (requestId !== lookupRequestSeq.current) return;
        setMinerAddress(addr);
        setMinerInput(addr);
        localStorage.setItem(LAST_MINER_LOOKUP_KEY, addr);
        setMinerData(d);
      } catch {
        // handled by api client
      }
    },
    [api, minerInput]
  );

  const refreshMinerData = useCallback(async () => {
    if (!minerAddress) return;
    const addr = minerAddress;
    try {
      const d = await api.getMiner(addr);
      if (minerAddressRef.current !== addr) return;
      setMinerData(d);
    } catch {
      // handled by api client
    }
  }, [api, minerAddress]);

  const loadMinerHashrate = useCallback(async () => {
    if (!minerAddress) return;
    const addr = minerAddress;
    try {
      const d = await api.getMinerHashrate(addr, range);
      if (minerAddressRef.current !== addr) return;
      setHistory(d || []);
    } catch {
      if (minerAddressRef.current === addr) {
        setHistory([]);
      }
    }
  }, [api, minerAddress, range]);

  const loadRejections = useCallback(async () => {
    try {
      const d = await api.getStatsInsights(rejectionRange);
      setRejectionWindow(d.rejections?.window || null);
    } catch {
      setRejectionWindow(null);
    }
  }, [api, rejectionRange]);

  useEffect(() => {
    if (!active) return;

    const stored = localStorage.getItem(LAST_MINER_LOOKUP_KEY) || '';
    if (!minerAddress) {
      if (!minerInput.trim() && stored) {
        setMinerInput(stored);
      }

      if (stored) {
        void loadMinerLookup(stored);
        return;
      }

      if (minerInput.trim()) {
        void loadMinerLookup(minerInput);
      }
    }
  }, [active, loadMinerLookup, minerAddress, minerInput]);

  useEffect(() => {
    if (!active || !minerAddress) return;
    void loadMinerHashrate();
  }, [active, loadMinerHashrate, minerAddress, range]);

  useEffect(() => {
    if (!active || !minerAddress) return;
    void refreshMinerData();
  }, [active, minerAddress, refreshMinerData]);

  useEffect(() => {
    if (!active) return;
    void loadRejections();
  }, [active, loadRejections]);

  useEffect(() => {
    if (!active || liveTick <= 0) return;
    if (minerAddress) {
      void refreshMinerData();
    }
    if (liveTick % 2 === 0) {
      if (minerAddress) {
        void loadMinerHashrate();
      }
      void loadRejections();
    }
  }, [active, liveTick, minerAddress, refreshMinerData, loadMinerHashrate, loadRejections]);

  const lookupDisabled = useMemo(() => {
    const addr = minerInput.trim();
    const sameAsLoaded = !!minerAddress && addr === minerAddress;
    return !addr || sameAsLoaded;
  }, [minerAddress, minerInput]);

  const minerAvgDiff = useMemo(() => {
    const shares = minerData?.shares || [];
    const totalAccepted = minerData?.total_accepted || 0;
    if (!shares.length || totalAccepted <= 0) return '-';
    const take = Math.min(shares.length, totalAccepted);
    const sum = shares.slice(0, take).reduce((acc, s) => acc + (s.difficulty || 0), 0);
    if (!sum) return '-';
    return (sum / take).toFixed(0);
  }, [minerData]);

  const minerOldestShareDate = useMemo(() => {
    const shares = minerData?.shares || [];
    let oldest = 0;
    for (const s of shares) {
      const t = toUnixMs(s.created_at);
      if (!t) continue;
      if (!oldest || t < oldest) oldest = t;
    }
    return oldest ? new Date(oldest).toLocaleDateString() : '-';
  }, [minerData]);

  const pendingConfirmed = minerData?.balance?.pending_confirmed ?? minerData?.balance?.pending ?? 0;
  const pendingEstimated = minerData?.balance?.pending_estimated ?? minerData?.pending_estimate?.estimated_pending ?? 0;
  const pendingTotal = minerData?.balance?.pending_total ?? pendingConfirmed + pendingEstimated;
  const minerAccepted = minerData?.total_accepted ?? 0;
  const minerRejected = minerData?.total_rejected ?? 0;
  const minerChecked = minerAccepted + minerRejected;
  const minerRejectRate = minerChecked > 0 ? (minerRejected / minerChecked) * 100 : null;
  const recentShares = minerData?.shares?.slice(0, RECENT_SHARES_LIMIT) || [];

  const rejectionChecked = (rejectionWindow?.accepted ?? 0) + (rejectionWindow?.rejected ?? 0);
  const topWindowReason = rejectionWindow?.by_reason?.[0];
  const topTotalReason = rejectionWindow?.totals_by_reason?.[0];
  const hasWindowRejections = (rejectionWindow?.rejected ?? 0) > 0;

  return (
    <div className={active ? 'page active' : 'page'} id="page-stats">
      <h2>My Stats</h2>
      <div className="card" style={{ marginBottom: 24 }}>
        <div className="lookup-form" style={{ display: 'flex', gap: 10, marginBottom: 0, flexWrap: 'wrap' }}>
          <input
            type="text"
            placeholder="Enter your wallet address"
            style={{ flex: 1, minWidth: 200 }}
            value={minerInput}
            onChange={(e) => setMinerInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !lookupDisabled) {
                void loadMinerLookup();
              }
            }}
          />
          <button
            className={`btn btn-primary ${minerAddress && minerInput.trim() === minerAddress ? 'is-faded' : ''}`}
            disabled={lookupDisabled}
            onClick={() => void loadMinerLookup()}
          >
            Lookup
          </button>
          <button
            className="btn btn-secondary"
            onClick={() => {
              lookupRequestSeq.current += 1;
              setMinerInput('');
              setMinerAddress('');
              setMinerData(null);
              setHistory([]);
              localStorage.removeItem(LAST_MINER_LOOKUP_KEY);
            }}
          >
            Clear
          </button>
        </div>
      </div>

      {minerData && (
        <div id="lookup-result">
          <div className="stats-grid stats-grid-dense" style={{ marginBottom: 24 }}>
            <div className="stat-card">
              <div className="label">Hashrate</div>
              <div className="value">{humanRate(minerData.hashrate || 0)}</div>
            </div>
            <div className="stat-card">
              <div className="label">Pending Balance</div>
              <div className="value">{formatCoins(pendingTotal)}</div>
              <div className="stat-meta">
                <div>
                  Confirmed <span className="mono">{formatCoins(pendingConfirmed)}</span>
                </div>
                <div>
                  Estimated <span className="mono">{formatCoins(pendingEstimated)}</span>
                </div>
              </div>
            </div>
            <div className="stat-card">
              <div className="label">Paid Balance</div>
              <div className="value">{formatCoins(minerData.balance?.paid || 0)}</div>
            </div>
            <div className="stat-card">
              <div className="label">Blocks Found</div>
              <div className="value">{(minerData.blocks_found || []).length}</div>
            </div>
            <div className="stat-card">
              <div className="label">Shares Accepted</div>
              <div className="value">{minerAccepted}</div>
            </div>
            <div className="stat-card">
              <div className="label">Shares Rejected</div>
              <div className="value">{minerRejected}</div>
            </div>
            <div className="stat-card">
              <div className="label">Reject Rate</div>
              <div className="value">{fmtPct(minerRejectRate)}</div>
            </div>
            <div className="stat-card">
              <div className="label">Avg Difficulty</div>
              <div className="value">{minerAvgDiff}</div>
            </div>
            <div className="stat-card">
              <div className="label">Mining Since</div>
              <div className="value">{minerOldestShareDate}</div>
            </div>
          </div>

          {minerData.pending_note && (
            <div
              className="card"
              style={{ marginBottom: 24, background: 'var(--accent-light)', borderColor: 'var(--accent)' }}
            >
              <div style={{ color: 'var(--text)', fontSize: 13 }}>{minerData.pending_note}</div>
            </div>
          )}

          {!!minerData.pending_estimate?.blocks?.length && (
            <div className="section">
              <h2>Pending From Unconfirmed Blocks</h2>
              <div className="card table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Height</th>
                      <th>Estimated Credit</th>
                      <th>Block Reward</th>
                      <th>Confirms Left</th>
                      <th>Time</th>
                    </tr>
                  </thead>
                  <tbody>
                    {minerData.pending_estimate.blocks.map((b) => (
                      <tr key={`${b.height}-${b.hash}`}>
                        <td>
                          <a href={`https://explorer.blocknetcrypto.com/block/${b.hash || ''}`} target="_blank" rel="noopener">
                            {b.height}
                          </a>
                        </td>
                        <td>{formatCoins(b.estimated_credit)}</td>
                        <td>{formatCoins(b.reward)}</td>
                        <td>{b.confirmations_remaining}</td>
                        <td title={new Date(toUnixMs(b.timestamp)).toLocaleString()}>{timeAgo(b.timestamp)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <div style={{ marginTop: 10, fontSize: 12, color: 'var(--muted)' }}>
                  Estimates are provisional until blocks are confirmed and can drop if a block is orphaned.
                </div>
              </div>
            </div>
          )}

          <div className="section">
            <div className="section-header">
              <h2>Hashrate History</h2>
              <div className="range-tabs">
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
            <h2>Workers</h2>
            <div className="card table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Worker</th>
                    <th>Hashrate</th>
                    <th>Accepted</th>
                    <th>Rejected</th>
                    <th>Last Share</th>
                  </tr>
                </thead>
                <tbody>
                  {!minerData.workers?.length ? (
                    <tr>
                      <td colSpan={5} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No workers
                      </td>
                    </tr>
                  ) : (
                    minerData.workers.map((w) => (
                      <tr key={w.worker}>
                        <td>{w.worker || 'default'}</td>
                        <td>{humanRate(w.hashrate || 0)}</td>
                        <td>{w.accepted || 0}</td>
                        <td>{w.rejected || 0}</td>
                        <td title={w.last_share_at ? new Date(toUnixMs(w.last_share_at)).toLocaleString() : ''}>
                          {timeAgo(w.last_share_at)}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>

          <div className="section">
            <h2>Recent Payouts</h2>
            <div className="card table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Amount</th>
                    <th>Fee</th>
                    <th>Tx</th>
                    <th>Time</th>
                  </tr>
                </thead>
                <tbody>
                  {!minerData.payouts?.length ? (
                    <tr>
                      <td colSpan={4} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No payouts yet
                      </td>
                    </tr>
                  ) : (
                    minerData.payouts.map((p) => (
                      <tr key={`${p.id}-${p.tx_hash}`}>
                        <td>{formatCoins(p.amount)}</td>
                        <td>{formatCoins(p.fee || 0)}</td>
                        <td>
                          <a href={`https://explorer.blocknetcrypto.com/tx/${p.tx_hash || ''}`} target="_blank" rel="noopener">
                            {p.tx_hash || '-'}
                          </a>
                        </td>
                        <td title={new Date(toUnixMs(p.timestamp)).toLocaleString()}>{timeAgo(p.timestamp)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>

          <div className="section">
            <div className="section-header">
              <h2>Pool Rejects</h2>
              <div className="range-tabs">
                {(['1h', '24h', '7d'] as RejectionWindowRange[]).map((r) => (
                  <button key={r} className={rejectionRange === r ? 'active' : ''} onClick={() => setRejectionRange(r)}>
                    {r}
                  </button>
                ))}
              </div>
            </div>
            <div className="card rejection-card">
              <div className="rejection-overview">
                <div className="rejection-metric">
                  <div className="label">Checked</div>
                  <div className="value mono">{rejectionChecked}</div>
                </div>
                <div className="rejection-metric">
                  <div className="label">Rejected</div>
                  <div className="value mono">{rejectionWindow?.rejected ?? 0}</div>
                </div>
                <div className="rejection-metric">
                  <div className="label">Reject Rate</div>
                  <div className="value mono">{fmtPct(rejectionWindow?.rejection_rate_pct)}</div>
                </div>
                <div className="rejection-metric">
                  <div className="label">Top Reason</div>
                  <div className="value mono">{topWindowReason?.reason || 'none'}</div>
                </div>
              </div>

              {!hasWindowRejections ? (
                <p className="rejection-empty">
                  No rejects recorded in the selected window. All-time rejects:{' '}
                  <span className="mono">{rejectionWindow?.total_rejected ?? 0}</span>
                  {topTotalReason ? (
                    <>
                      {' '}
                      • most common reason <span className="mono">{topTotalReason.reason}</span>
                    </>
                  ) : null}
                </p>
              ) : (
                <div className="rejection-breakdown">
                  <div className="rejection-note">
                    Selected window: <span className="mono">{rejectionRange}</span>
                  </div>
                  <div className="rejection-list">
                    {(rejectionWindow?.by_reason || []).slice(0, 4).map((reason) => {
                      const windowPct =
                        (rejectionWindow?.rejected ?? 0) > 0
                          ? (reason.count / (rejectionWindow?.rejected ?? 0)) * 100
                          : 0;

                      return (
                        <div key={reason.reason} className="rejection-row">
                          <div>
                            <div className="rejection-row-head">
                              <span>{reason.reason}</span>
                              <span className="mono">{fmtPct(windowPct)}</span>
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
                  <div className="rejection-note">
                    All-time rejects: <span className="mono">{rejectionWindow?.total_rejected ?? 0}</span>
                    {topTotalReason ? (
                      <>
                        {' '}
                        • all-time top reason <span className="mono">{topTotalReason.reason}</span>
                      </>
                    ) : null}
                  </div>
                </div>
              )}
            </div>
          </div>

          <div className="section">
            <div className="section-header">
              <h2>Recent Shares</h2>
              <span className="section-meta">Latest {RECENT_SHARES_LIMIT}</span>
            </div>
            <div className="card table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Job</th>
                    <th>Worker</th>
                    <th>Difficulty</th>
                    <th>Status</th>
                    <th>Time</th>
                  </tr>
                </thead>
                <tbody>
                  {!recentShares.length ? (
                    <tr>
                      <td colSpan={5} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No shares
                      </td>
                    </tr>
                  ) : (
                    recentShares.map((s, idx) => (
                      <tr key={`${s.job_id}-${idx}`}>
                        <td>{s.job_id || ''}</td>
                        <td>{s.worker || ''}</td>
                        <td>{s.difficulty}</td>
                        <td>{s.status || ''}</td>
                        <td title={new Date(toUnixMs(s.created_at)).toLocaleString()}>{timeAgo(s.created_at)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
