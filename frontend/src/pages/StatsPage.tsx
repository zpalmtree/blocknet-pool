import { startTransition, useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { ApiClient } from '../api/client';
import { EmptyTableRow } from '../components/EmptyTableRow';
import { ExplorerLink } from '../components/ExplorerLink';
import { HashrateChart } from '../components/HashrateChart';
import { PayoutStatusBadge } from '../components/PayoutStatusBadge';
import { RangeTabs } from '../components/RangeTabs';
import { StatCard } from '../components/StatCard';
import { LAST_MINER_LOOKUP_KEY } from '../lib/storage';
import { formatCoins, formatCompactCoins, formatFee, formatPct, humanRate, ratioPct, timeAgo, timestampTitle, toUnixMs } from '../lib/format';
import type { ThemeMode } from '../lib/theme';
import type {
  HashratePoint,
  MinerBalancePayload,
  MinerResponse,
  Range,
  StatsInsightsResponse,
} from '../types';

interface StatsPageProps {
  api: ApiClient;
  liveTick: number;
  theme: ThemeMode;
}

type RejectionWindowRange = '1h' | '24h' | '7d';

function looksLikeHandle(raw: string): boolean {
  return raw.startsWith('$') || raw.startsWith('@') || (raw.length < 25 && /^[$@]?[a-z0-9][a-z0-9_.\-]{0,62}$/i.test(raw));
}

const lookupResult = async <T,>(promise: Promise<T>) =>
  promise.then((value) => ({ ok: true as const, value })).catch(() => ({ ok: false as const }));

function RejectionMetric({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rejection-metric">
      <div className="label">{label}</div>
      <div className="value mono">{value}</div>
    </div>
  );
}

export function StatsPage({ api, liveTick, theme }: StatsPageProps) {
  const [minerInput, setMinerInput] = useState(localStorage.getItem(LAST_MINER_LOOKUP_KEY) || '');
  const [minerAddress, setMinerAddress] = useState('');
  const [minerData, setMinerData] = useState<MinerResponse | null>(null);
  const [minerBalanceData, setMinerBalanceData] = useState<MinerBalancePayload | null>(null);
  const [range, setRange] = useState<Range>('1h');
  const [history, setHistory] = useState<HashratePoint[]>([]);
  const [rejectionRange, setRejectionRange] = useState<RejectionWindowRange>('1h');
  const [insights, setInsights] = useState<StatsInsightsResponse | null>(null);
  const minerAddressRef = useRef(minerAddress);
  const lookupRequestSeq = useRef(0);
  const hashrateRequestKeyRef = useRef('');
  const [resolving, setResolving] = useState(false);
  const [lookupLoading, setLookupLoading] = useState(false);
  const [resolvedHandle, setResolvedHandle] = useState<string | null>(null);

  useEffect(() => {
    minerAddressRef.current = minerAddress;
  }, [minerAddress]);

  const refreshMinerData = useCallback(async () => {
    if (!minerAddress) return;
    const addr = minerAddress;
    try {
      const d = await api.getMiner(addr);
      if (minerAddressRef.current !== addr) return;
      startTransition(() => {
        setMinerData(d);
      });
    } catch {
      // handled by api client
    }
  }, [api, minerAddress]);

  const refreshMinerBalance = useCallback(async (includePendingEstimate: boolean) => {
    if (!minerAddress) return;
    const addr = minerAddress;
    try {
      const d = await api.getMinerBalance(addr, includePendingEstimate);
      if (minerAddressRef.current !== addr) return;
      startTransition(() => {
        setMinerBalanceData((current) => ({
          ...d,
          pending_estimate:
            !includePendingEstimate && current?.address === d.address
              ? current?.pending_estimate ?? d.pending_estimate
              : d.pending_estimate,
        }));
      });
    } catch {
      // handled by api client
    }
  }, [api, minerAddress]);

  const loadMinerHashrate = useCallback(async (addressOverride?: string, rangeOverride?: Range) => {
    const addr = (addressOverride ?? minerAddress).trim();
    const selectedRange = rangeOverride ?? range;
    if (!addr) return;
    const requestKey = `${addr}:${selectedRange}`;
    hashrateRequestKeyRef.current = requestKey;
    try {
      const d = await api.getMinerHashrate(addr, selectedRange);
      if (hashrateRequestKeyRef.current !== requestKey) return;
      startTransition(() => {
        setHistory(d);
      });
    } catch {
      if (hashrateRequestKeyRef.current === requestKey) {
        startTransition(() => {
          setHistory([]);
        });
      }
    }
  }, [api, minerAddress, range]);

  const loadMinerLookup = useCallback(
    async (input?: string) => {
      let addr = (input ?? minerInput).trim();
      let resolved: { address: string; handle: string } | null = null;
      if (!addr) return;
      const requestId = ++lookupRequestSeq.current;
      setLookupLoading(true);

      try {
        if (looksLikeHandle(addr)) {
          setResolving(true);
          try {
            setResolvedHandle(null);
            resolved = await api.resolveBlocknetHandle(addr.replace(/^[$@]/, ''));
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

        let committed = false;
        let resetPreviousResult = minerAddressRef.current !== addr;
        const commitLookup = () => {
          if (committed) return;
          committed = true;
          setMinerAddress(addr);
          setMinerInput(addr);
          localStorage.setItem(LAST_MINER_LOOKUP_KEY, addr);
          setResolvedHandle(resolved?.handle ?? null);
        };
        const clearPreviousResult = () => {
          if (!resetPreviousResult) return;
          resetPreviousResult = false;
          setMinerData(null);
          setMinerBalanceData(null);
          setHistory([]);
        };

        const requestKey = `${addr}:${range}`;
        hashrateRequestKeyRef.current = requestKey;

        const balancePromise = lookupResult(api.getMinerBalance(addr, true));
        const detailPromise = lookupResult(api.getMiner(addr));
        const hashratePromise = lookupResult(api.getMinerHashrate(addr, range));

        const balanceResult = await balancePromise;
        if (requestId !== lookupRequestSeq.current) return;
        if (balanceResult.ok) {
          commitLookup();
          startTransition(() => {
            clearPreviousResult();
            setMinerBalanceData(balanceResult.value);
          });
        }

        const detailResult = await detailPromise;
        if (requestId !== lookupRequestSeq.current) return;
        if (detailResult.ok) {
          commitLookup();
          startTransition(() => {
            clearPreviousResult();
            setMinerData(detailResult.value);
          });
        }

        const hashrateResult = await hashratePromise;
        if (requestId !== lookupRequestSeq.current) return;
        if (balanceResult.ok || detailResult.ok) {
          startTransition(() => {
            setHistory(hashrateResult.ok ? hashrateResult.value : []);
          });
        } else {
          setResolvedHandle(null);
        }
      } finally {
        if (requestId === lookupRequestSeq.current) {
          setLookupLoading(false);
        }
      }
    },
    [api, minerInput, range]
  );

  const loadRejections = useCallback(async () => {
    try {
      const d = await api.getStatsInsights(rejectionRange);
      setInsights(d);
    } catch {
      setInsights(null);
    }
  }, [api, rejectionRange]);

  useEffect(() => {
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
  }, [loadMinerLookup, minerAddress, minerInput]);

  const currentHashrateKey = minerAddress ? `${minerAddress}:${range}` : '';

  useEffect(() => {
    if (!minerAddress) return;
    if (hashrateRequestKeyRef.current === currentHashrateKey) return;
    void loadMinerHashrate();
  }, [currentHashrateKey, loadMinerHashrate, minerAddress]);

  useEffect(() => {
    void loadRejections();
  }, [loadRejections]);

  useEffect(() => {
    if (liveTick <= 0) return;
    if (minerAddress) {
      if (liveTick % 12 === 0) {
        void refreshMinerBalance(true);
      } else {
        void refreshMinerBalance(false);
      }
      if (liveTick % 6 === 0) {
        void refreshMinerData();
      }
    }
    if (liveTick % 2 === 0) {
      if (minerAddress) {
        void loadMinerHashrate();
      }
    }
    if (liveTick % 6 === 0) {
      void loadRejections();
    }
  }, [liveTick, minerAddress, refreshMinerBalance, refreshMinerData, loadMinerHashrate, loadRejections]);

  const lookupDisabled = useMemo(() => {
    const raw = minerInput.trim();
    if (!raw) return true;
    if (looksLikeHandle(raw)) return false;
    return !!minerAddress && raw === minerAddress;
  }, [minerAddress, minerInput]);

  const minerAvgDiff = useMemo(() => {
    if (!minerData) return '-';
    const shares = minerData.shares;
    const totalAccepted = minerData.total_accepted;
    if (!shares.length || totalAccepted <= 0) return '-';
    const take = Math.min(shares.length, totalAccepted);
    const sum = shares.slice(0, take).reduce((acc, s) => acc + (s.difficulty || 0), 0);
    if (!sum) return '-';
    return (sum / take).toFixed(0);
  }, [minerData]);

  const minerOldestShareDate = useMemo(() => {
    if (!minerData) return '-';
    const miningSince = toUnixMs(minerData.mining_since);
    if (miningSince) return new Date(miningSince).toLocaleDateString();

    const shares = minerData.shares;
    let oldest = 0;
    for (const s of shares) {
      const t = toUnixMs(s.created_at);
      if (!t) continue;
      if (!oldest || t < oldest) oldest = t;
    }
    return oldest ? new Date(oldest).toLocaleDateString() : '-';
  }, [minerData]);

  const liveBalance = minerBalanceData?.balance;
  const livePendingEstimate = minerBalanceData?.pending_estimate;
  const pendingConfirmed = liveBalance?.pending_confirmed ?? 0;
  const pendingEstimated = livePendingEstimate?.estimated_pending ?? 0;
  const pendingQueued = liveBalance?.pending_queued ?? 0;
  const payoutEta = insights?.payout_eta ?? null;
  const payoutShortfall =
    payoutEta?.wallet_spendable == null
      ? 0
      : Math.max(0, payoutEta.pending_total_amount - payoutEta.wallet_spendable);
  const payoutLiquidityConstrained = payoutEta != null && payoutEta.pending_total_amount > 0 && payoutShortfall > 0;
  const rejectionWindow = insights?.rejections ?? null;
  const minerAccepted = minerData ? minerData.total_accepted : 0;
  const minerRejected = minerData ? minerData.total_rejected : 0;
  const minerChecked = minerAccepted + minerRejected;
  const minerRejectRate = minerChecked > 0 ? (minerRejected / minerChecked) * 100 : null;
  const recentShares = minerData?.shares ?? [];
  const previewBlocks = livePendingEstimate?.blocks ?? [];
  const verificationHold = minerData?.verification_hold ?? null;
  const verificationReason = verificationHold?.reason?.trim() || 'share validation issue';
  const verificationPendingProvisional = verificationHold?.validation_pending_provisional ?? 0;
  const verificationStartedAt = toUnixMs(verificationHold?.started_at);
  const verificationOnlyUntil = toUnixMs(verificationHold?.verified_only_until);
  const verificationQuarantineUntil = toUnixMs(verificationHold?.quarantined_until);
  const latestHashratePoint = history.length > 0 ? history[history.length - 1] : null;
  const liveHashrate = minerData ? minerData.hashrate : latestHashratePoint?.hashrate ?? 0;
  const livePaid = liveBalance?.paid ?? 0;
  const showLookupResult = !!minerAddress && (!!minerData || !!minerBalanceData || history.length > 0);
  const minerDetailLoading = lookupLoading && !minerData;

  const rejectionChecked = (rejectionWindow?.accepted ?? 0) + (rejectionWindow?.rejected ?? 0);
  const rejectionRatePct = ratioPct(rejectionWindow?.rejected, rejectionChecked);
  const topWindowReason = rejectionWindow?.by_reason?.[0];
  const topTotalReason = rejectionWindow?.totals_by_reason?.[0];
  const totalRejected = (rejectionWindow?.totals_by_reason ?? []).reduce((sum, reason) => sum + reason.count, 0);
  const hasWindowRejections = (rejectionWindow?.rejected ?? 0) > 0;

  return (
    <div id="page-stats">
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
            disabled={lookupDisabled || resolving || lookupLoading}
            onClick={() => void loadMinerLookup()}
          >
            {resolving ? 'Resolving…' : lookupLoading ? 'Loading…' : 'Lookup'}
          </button>
          <button
            className="btn btn-secondary"
            onClick={() => {
              lookupRequestSeq.current += 1;
              hashrateRequestKeyRef.current = '';
              setMinerInput('');
              setMinerAddress('');
              setMinerData(null);
              setMinerBalanceData(null);
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

      {showLookupResult && (
        <div id="lookup-result">
          <div className="stats-card-group" style={{ marginBottom: 16 }}>
            <div className="stats-card-group-title">Balance</div>
            <div className="stats-card-group-grid">
              <StatCard
                label="Estimated Rewards"
                value={formatCompactCoins(pendingEstimated)}
                meta="Recent blocks still confirming"
                className="stat-card--flow"
                title={formatCoins(pendingEstimated)}
              />
              <StatCard
                label="Confirmed Rewards"
                value={formatCompactCoins(pendingConfirmed)}
                meta="Matured balance awaiting payout"
                className="stat-card--flow"
                title={formatCoins(pendingConfirmed)}
              />
              <StatCard
                label="Paid Balance"
                value={formatCompactCoins(livePaid)}
                meta="Already sent to this address"
                title={formatCoins(livePaid)}
              />
            </div>
          </div>

          <div className="stats-card-group" style={{ marginBottom: 24 }}>
            <div className="stats-card-group-title">Mining</div>
            <div className="stats-card-group-grid">
              <StatCard label="Hashrate" value={humanRate(liveHashrate)} />
              <StatCard label="Blocks Found" value={minerData ? minerData.blocks_found : '...'} />
              <StatCard label="Mining Since" value={minerData ? minerOldestShareDate : '...'} />
            </div>
          </div>

          <div className="stats-grid" style={{ marginBottom: 24 }}>
            <StatCard label="Shares Accepted" value={minerData ? minerAccepted : '...'} />
            <StatCard label="Shares Rejected" value={minerData ? minerRejected : '...'} />
            <StatCard label="Reject Rate" value={minerData ? formatPct(minerRejectRate) : '...'} />
            <StatCard label="Avg Difficulty" value={minerData ? minerAvgDiff : '...'} />
          </div>

          {verificationHold && (
            <div
              className="card"
              style={{ marginBottom: 24, background: 'rgba(247, 180, 75, 0.12)', borderColor: 'rgba(247, 180, 75, 0.45)' }}
            >
              <div style={{ color: 'var(--text)', fontSize: 13 }}>
                {verificationHold.mode === 'quarantined'
                  ? `Verification hold active. This address is quarantined until ${
                      timestampTitle(verificationQuarantineUntil) || 'the current hold expires'
                    }, so new submissions from this address are temporarily blocked.`
                  : `Verification hold active until ${
                      timestampTitle(verificationOnlyUntil) || 'the current hold expires'
                    }. Only fully verified shares count toward unconfirmed estimates and payout while this hold is active.`}
                {verificationHold.mode === 'quarantined' &&
                  verificationOnlyUntil &&
                  verificationOnlyUntil !== verificationQuarantineUntil && (
                    <> {`Verified-only credit continues until ${timestampTitle(verificationOnlyUntil)} after quarantine ends.`}</>
                  )}
                {verificationHold.validation_hold_cause === 'provisional_backlog' &&
                verificationPendingProvisional > 0
                  ? ` Reason: ${verificationReason}. ${verificationPendingProvisional} provisional share${
                      verificationPendingProvisional === 1 ? '' : 's'
                    } are still waiting for full verification.`
                  : ` Reason: ${verificationReason}.`}
                {verificationStartedAt && <> {`Started ${timestampTitle(verificationStartedAt)}.`}</>}
                {' Confirmed balance and completed payouts are unaffected.'}
              </div>
            </div>
          )}

          {minerDetailLoading && (
            <div className="card" style={{ marginBottom: 24, color: 'var(--muted)', fontSize: 13 }}>
              Loading worker, payout, and share history...
            </div>
          )}

          {pendingQueued > 0 && payoutEta && payoutLiquidityConstrained && (
            <div
              className="card"
              style={{ marginBottom: 24, background: 'rgba(247, 180, 75, 0.12)', borderColor: 'rgba(247, 180, 75, 0.45)' }}
            >
              <div style={{ color: 'var(--text)', fontSize: 13 }}>
                Your payout is queued, but pool wallet liquidity is currently tight. Spendable balance:{' '}
                <span className="mono">{formatCoins(payoutEta.wallet_spendable ?? 0)}</span>. Locked/confirming balance:{' '}
                <span className="mono">{formatCoins(payoutEta.wallet_pending ?? 0)}</span>. Pool queue:{' '}
                <span className="mono">{formatCoins(payoutEta.pending_total_amount)}</span>. Those locked funds are
                already in the pool wallet and should become spendable as blocks mature.
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
                      <th>Confirms Left</th>
                      <th>Time</th>
                    </tr>
                  </thead>
                  <tbody>
                    {previewBlocks.map((b) => (
                      <tr key={`${b.height}-${b.hash}`}>
                        <td>
                          <ExplorerLink kind="block" value={b.hash || ''}>{b.height}</ExplorerLink>
                        </td>
                        <td title={b.validation_detail || undefined}>
                          {b.credit_withheld
                            ? 'Withheld'
                            : b.validation_state === 'extra_verification'
                              ? `${formatCoins(b.estimated_credit)} (verified only)`
                              : formatCoins(b.estimated_credit)}
                        </td>
                        <td>{b.confirmations_remaining}</td>
                        <td title={timestampTitle(b.timestamp)}>{timeAgo(b.timestamp)}</td>
                      </tr>
                    ))}
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
              <RangeTabs<Range> options={['1h', '24h', '7d', '30d']} value={range} onChange={setRange} />
            </div>
            <HashrateChart data={history} range={range} theme={theme} />
          </div>

          {minerData && (
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
                    {!minerData.workers.length ? (
                      <EmptyTableRow colSpan={5}>No workers</EmptyTableRow>
                    ) : (
                      minerData.workers.map((w) => (
                        <tr key={w.worker}>
                          <td>{w.worker || 'default'}</td>
                          <td>{humanRate(w.hashrate || 0)}</td>
                          <td>{w.accepted || 0}</td>
                          <td>{w.rejected || 0}</td>
                          <td title={timestampTitle(w.last_share_at)}>
                            {timeAgo(w.last_share_at)}
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {minerData && (
            <div className="section">
              <h2>Recent Payouts</h2>
              <div className="card table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Amount</th>
                      <th>Fee</th>
                      <th>Tx</th>
                      <th>Status</th>
                      <th>Time</th>
                    </tr>
                  </thead>
                  <tbody>
                    {!minerData.payouts.length ? (
                      <EmptyTableRow colSpan={5}>No payouts yet</EmptyTableRow>
                    ) : (
                      minerData.payouts.map((p, payoutIndex) => {
                        const hasTx = Boolean(p.tx_hash.trim());
                        const status = p.confirmed ? 'confirmed' : hasTx ? 'unconfirmed' : 'queued';
                        return (
                          <tr key={`${p.tx_hash}-${toUnixMs(p.timestamp)}-${payoutIndex}`}>
                            <td>{formatCoins(p.amount)}</td>
                            <td>{formatFee(p.fee || 0)}</td>
                            <td>
                              {hasTx ? (
                                <ExplorerLink kind="tx" value={p.tx_hash}>{p.tx_hash}</ExplorerLink>
                              ) : (
                                '-'
                              )}
                            </td>
                            <td>
                              <PayoutStatusBadge status={status} />
                            </td>
                            <td title={timestampTitle(p.timestamp)}>{timeAgo(p.timestamp)}</td>
                          </tr>
                        );
                      })
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          <div className="section">
            <div className="section-header">
              <h2>Pool Rejects</h2>
              <RangeTabs<RejectionWindowRange> options={['1h', '24h', '7d']} value={rejectionRange} onChange={setRejectionRange} />
            </div>
            <div className="card rejection-card">
              <div className="rejection-overview">
                <RejectionMetric label="Checked" value={rejectionChecked} />
                <RejectionMetric label="Rejected" value={rejectionWindow?.rejected ?? 0} />
                <RejectionMetric label="Reject Rate" value={formatPct(rejectionRatePct)} />
                <RejectionMetric label="Top Reason" value={topWindowReason?.reason || 'none'} />
              </div>

              {!hasWindowRejections ? (
                <p className="rejection-empty">
                  No rejects recorded in the selected window. All-time rejects:{' '}
                  <span className="mono">{totalRejected}</span>
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
                  <div className="rejection-note">
                    All-time rejects: <span className="mono">{totalRejected}</span>
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

          {minerData && (
            <div className="section">
              <div className="section-header">
                <h2>Recent Shares</h2>
                <span className="section-meta">Latest shares</span>
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
                      <EmptyTableRow colSpan={5}>No shares</EmptyTableRow>
                    ) : (
                      recentShares.map((s, idx) => (
                        <tr key={`${s.job_id}-${idx}`}>
                          <td>{s.job_id || ''}</td>
                          <td>{s.worker || ''}</td>
                          <td>{s.difficulty}</td>
                          <td>{s.status || ''}</td>
                          <td title={timestampTitle(s.created_at)}>{timeAgo(s.created_at)}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
