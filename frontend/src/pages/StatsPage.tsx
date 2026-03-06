import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { ApiClient } from '../api/client';
import { HashrateChart } from '../components/HashrateChart';
import { LAST_MINER_LOOKUP_KEY } from '../lib/storage';
import { formatCoins, formatFee, humanRate, timeAgo, toUnixMs } from '../lib/format';
import type { ThemeMode } from '../lib/theme';
import type {
  HashratePoint,
  MinerPendingBlockEstimate,
  MinerResponse,
  Range,
  StatsInsightsResponse,
} from '../types';

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

function toneClass(tone: string | null | undefined): string {
  if (tone === 'critical') return 'is-critical';
  if (tone === 'warn') return 'is-warn';
  return 'is-ok';
}

function previewStatusCopy(block: MinerPendingBlockEstimate): string | null {
  switch (block.validation_state) {
    case 'ready':
      return block.credit_withheld ? 'Temporarily withheld while validation catches up' : null;
    case 'finder_fallback':
      return 'Using finder fallback because no share window was recorded';
    case 'awaiting_delay':
      return 'Waiting for recent shares to clear the provisional delay';
    case 'awaiting_shares':
      return 'Waiting for enough verified shares';
    case 'awaiting_ratio':
      return 'Waiting for enough verified work';
    case 'extra_verification':
      return 'Under additional verification';
    default:
      return block.credit_withheld ? 'Preview withheld while validation catches up' : 'Validation in progress';
  }
}

type RejectionWindowRange = '1h' | '24h' | '7d';

const RECENT_SHARES_LIMIT = 20;
const HANDLE_RE = /^[$@]?[a-z0-9][a-z0-9_.\-]{0,62}$/i;
const BLOCKNET_ID_API = 'https://blocknet.id/api/v1/resolve';

function looksLikeHandle(raw: string): boolean {
  if (raw.startsWith('$') || raw.startsWith('@')) return true;
  if (raw.length < 25 && HANDLE_RE.test(raw)) return true;
  return false;
}

async function resolveHandle(api: ApiClient, raw: string): Promise<{ address: string; handle: string } | null> {
  const handle = raw.replace(/^[$@]/, '');
  const data = await api.fetchJson<{ address: string; handle: string }>(
    `${BLOCKNET_ID_API}/${encodeURIComponent(handle)}`
  );
  return { address: data.address, handle: data.handle };
}

export function StatsPage({ active, api, liveTick, theme }: StatsPageProps) {
  const [minerInput, setMinerInput] = useState(localStorage.getItem(LAST_MINER_LOOKUP_KEY) || '');
  const [minerAddress, setMinerAddress] = useState('');
  const [minerData, setMinerData] = useState<MinerResponse | null>(null);
  const [range, setRange] = useState<Range>('1h');
  const [history, setHistory] = useState<HashratePoint[]>([]);
  const [rejectionRange, setRejectionRange] = useState<RejectionWindowRange>('1h');
  const [insights, setInsights] = useState<StatsInsightsResponse | null>(null);
  const minerAddressRef = useRef(minerAddress);
  const lookupRequestSeq = useRef(0);
  const [resolving, setResolving] = useState(false);
  const [resolvedHandle, setResolvedHandle] = useState<string | null>(null);

  useEffect(() => {
    minerAddressRef.current = minerAddress;
  }, [minerAddress]);

  const loadMinerLookup = useCallback(
    async (input?: string) => {
      let addr = (input ?? minerInput).trim();
      let resolved: { address: string; handle: string } | null = null;
      if (!addr) return;
      const requestId = ++lookupRequestSeq.current;

      if (looksLikeHandle(addr)) {
        setResolving(true);
        try {
          setResolvedHandle(null);
          resolved = await resolveHandle(api, addr);
          if (!resolved) {
            if (requestId !== lookupRequestSeq.current) return;
            setResolvedHandle(null);
            return;
          }
          addr = resolved.address;
        } catch {
          if (requestId !== lookupRequestSeq.current) return;
          setResolvedHandle(null);
          return;
        } finally {
          if (requestId === lookupRequestSeq.current) {
            setResolving(false);
          }
        }
      } else {
        setResolvedHandle(null);
      }
      try {
        const d = await api.getMiner(addr);
        if (requestId !== lookupRequestSeq.current) return;
        setMinerAddress(addr);
        setMinerInput(addr);
        localStorage.setItem(LAST_MINER_LOOKUP_KEY, addr);
        setMinerData(d);
        setResolvedHandle(resolved?.handle ?? null);
      } catch {
        setResolvedHandle(null);
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
      setInsights(d);
    } catch {
      setInsights(null);
    }
  }, [api, rejectionRange]);

  useEffect(() => {
    if (!active) return;

    const stored = localStorage.getItem(LAST_MINER_LOOKUP_KEY) || '';
    if (stored && stored !== minerAddress) {
      if (stored !== minerInput) {
        setMinerInput(stored);
      }
      void loadMinerLookup(stored);
      return;
    }

    if (!minerAddress && minerInput.trim()) {
      void loadMinerLookup(minerInput);
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
    const raw = minerInput.trim();
    if (!raw) return true;
    if (looksLikeHandle(raw)) return false;
    return !!minerAddress && raw === minerAddress;
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
  const pendingEstimated = minerData?.pending_estimate?.estimated_pending ?? 0;
  const pendingQueued = minerData?.balance?.pending_queued ?? minerData?.pending_payout?.amount ?? 0;
  const pendingUnqueued = minerData?.balance?.pending_unqueued ?? Math.max(0, pendingConfirmed - pendingQueued);
  const payoutEta = insights?.payout_eta ?? null;
  const rejectionWindow = insights?.rejections?.window ?? null;
  const minerAccepted = minerData?.total_accepted ?? 0;
  const minerRejected = minerData?.total_rejected ?? 0;
  const minerChecked = minerAccepted + minerRejected;
  const minerRejectRate = minerChecked > 0 ? (minerRejected / minerChecked) * 100 : null;
  const recentShares = minerData?.shares?.slice(0, RECENT_SHARES_LIMIT) || [];
  const previewBlocks = minerData?.pending_estimate?.blocks ?? [];

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
            placeholder="Wallet address or $name"
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
            disabled={lookupDisabled || resolving}
            onClick={() => void loadMinerLookup()}
          >
            {resolving ? 'Resolving…' : 'Lookup'}
          </button>
          <button
            className="btn btn-secondary"
            onClick={() => {
              lookupRequestSeq.current += 1;
              setMinerInput('');
              setMinerAddress('');
              setMinerData(null);
              setHistory([]);
              setResolvedHandle(null);
              localStorage.removeItem(LAST_MINER_LOOKUP_KEY);
            }}
          >
            Clear
          </button>
          {resolvedHandle && minerAddress && (
            <span className="resolved-badge" style={{ alignSelf: 'center', fontSize: '0.85em', color: 'var(--success, #4caf50)' }}>
              ${resolvedHandle} → {minerAddress.slice(0, 8)}…{minerAddress.slice(-6)}
            </span>
          )}
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
              <div className="label">Confirmed Rewards</div>
              <div className="value">{formatCoins(pendingConfirmed)}</div>
            </div>
            <div className="stat-card">
              <div className="label">Estimated Rewards</div>
              <div className="value">{formatCoins(pendingEstimated)}</div>
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

          <div
            className="card"
            style={{
              marginBottom: 24,
              padding: '12px 16px',
              fontSize: 13,
              color: 'var(--muted)',
              display: 'flex',
              flexWrap: 'wrap',
              gap: '6px 18px',
            }}
          >
            <span>
              Confirmed rewards: <span className="mono">{formatCoins(pendingQueued)}</span> already queued,{' '}
              <span className="mono">{formatCoins(pendingUnqueued)}</span> still waiting to be queued.
            </span>
            <span>
              Estimated rewards:{' '}
              {previewBlocks.length ? (
                <>
                  based on <span className="mono">{previewBlocks.length}</span> recent unconfirmed block
                  {previewBlocks.length === 1 ? '' : 's'}
                </>
              ) : (
                'no unconfirmed block estimate right now'
              )}
              ; can still change.
            </span>
          </div>

          {minerData.pending_note && (
            <div
              className="card"
              style={{ marginBottom: 24, background: 'var(--accent-light)', borderColor: 'var(--accent)' }}
            >
              <div style={{ color: 'var(--text)', fontSize: 13 }}>{minerData.pending_note}</div>
            </div>
          )}

          {minerData.payout_note && (
            <div
              className="card"
              style={{ marginBottom: 24, background: 'rgba(247, 180, 75, 0.12)', borderColor: 'rgba(247, 180, 75, 0.45)' }}
            >
              <div style={{ color: 'var(--text)', fontSize: 13 }}>{minerData.payout_note}</div>
            </div>
          )}

          {pendingQueued > 0 && !!payoutEta?.liquidity_constrained && (
            <div
              className="card"
              style={{ marginBottom: 24, background: 'rgba(247, 180, 75, 0.12)', borderColor: 'rgba(247, 180, 75, 0.45)' }}
            >
              <div style={{ color: 'var(--text)', fontSize: 13 }}>
                Your payout is queued, but pool wallet liquidity is currently tight. Spendable balance:{' '}
                <span className="mono">{formatCoins(payoutEta.wallet_spendable ?? 0)}</span>. Pool queue:{' '}
                <span className="mono">{formatCoins(payoutEta.pending_total_amount ?? 0)}</span>. Queued sends may wait
                until wallet funds are restored.
              </div>
            </div>
          )}

          {!!previewBlocks.length && (
            <div className="section">
              <h2>Estimated From Recent Blocks</h2>
              <div className="card table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Height</th>
                      <th>Estimated Credit</th>
                      <th>Status</th>
                      <th>Confirms Left</th>
                      <th>Time</th>
                    </tr>
                  </thead>
                  <tbody>
                    {previewBlocks.map((b) => {
                      const statusText = previewStatusCopy(b);
                      return (
                      <tr key={`${b.height}-${b.hash}`}>
                        <td>
                          <a href={`https://explorer.blocknetcrypto.com/block/${b.hash || ''}`} target="_blank" rel="noopener">
                            {b.height}
                          </a>
                        </td>
                        <td>{b.credit_withheld ? 'Withheld' : formatCoins(b.estimated_credit)}</td>
                        <td title={b.validation_detail || undefined}>
                          <div
                            style={{
                              display: 'inline-flex',
                              alignItems: 'center',
                              gap: 10,
                              whiteSpace: 'nowrap',
                              fontSize: 12,
                              color: 'var(--muted)',
                            }}
                          >
                            <span className={`round-chip ${toneClass(b.validation_tone)}`}>{b.validation_label || 'Pending'}</span>
                            {statusText ? <span>{statusText}</span> : null}
                          </div>
                        </td>
                        <td>{b.confirmations_remaining}</td>
                        <td title={new Date(toUnixMs(b.timestamp)).toLocaleString()}>{timeAgo(b.timestamp)}</td>
                      </tr>
                    )})}
                  </tbody>
                </table>
                <div style={{ marginTop: 10, fontSize: 12, color: 'var(--muted)' }}>
                  Estimate only. These amounts can still move until those blocks confirm or are orphaned.
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
                        <td>{formatFee(p.fee || 0)}</td>
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
