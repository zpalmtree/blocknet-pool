import { startTransition, useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { ApiClient } from '../api/client';
import { HashrateChart } from '../components/HashrateChart';
import { LAST_MINER_LOOKUP_KEY } from '../lib/storage';
import { formatCoins, formatCompactCoins, formatFee, humanRate, timeAgo, toUnixMs } from '../lib/format';
import type { ThemeMode } from '../lib/theme';
import type {
  HashratePoint,
  MinerBalancePayload,
  MinerPendingEstimate,
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

function balancePayloadFromMiner(address: string, miner: MinerResponse): MinerBalancePayload | null {
  if (!miner.balance) return null;
  return {
    address,
    balance: miner.balance,
    pending_estimate: miner.pending_estimate,
    pending_payout: miner.pending_payout ?? null,
  };
}

function mergePendingEstimate(
  current: MinerPendingEstimate | undefined,
  next: MinerPendingEstimate | undefined,
  preserveCurrent: boolean
): MinerPendingEstimate | undefined {
  if (!preserveCurrent) return next;
  return current ?? next;
}

function mergeMinerBalancePayload(
  current: MinerBalancePayload | null,
  next: MinerBalancePayload,
  preservePendingEstimate: boolean
): MinerBalancePayload {
  const sameAddress = current?.address === next.address;
  return {
    ...next,
    pending_estimate: mergePendingEstimate(
      sameAddress ? current?.pending_estimate : undefined,
      next.pending_estimate,
      preservePendingEstimate
    ),
  };
}

function mergeMinerResponse(
  current: MinerResponse | null,
  next: MinerResponse,
  preservePendingEstimate: boolean
): MinerResponse {
  if (!preservePendingEstimate) return next;
  return {
    ...next,
    pending_estimate: mergePendingEstimate(current?.pending_estimate, next.pending_estimate, true),
    pending_note: current?.pending_note ?? next.pending_note,
  };
}

export function StatsPage({ active, api, liveTick, theme }: StatsPageProps) {
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

  const refreshMinerData = useCallback(async (includePendingEstimate: boolean) => {
    if (!minerAddress) return;
    const addr = minerAddress;
    try {
      const d = await api.getMiner(addr, includePendingEstimate, RECENT_SHARES_LIMIT);
      if (minerAddressRef.current !== addr) return;
      startTransition(() => {
        setMinerData((current) => mergeMinerResponse(current, d, !includePendingEstimate));
        setMinerBalanceData((current) => current?.address === addr ? current : balancePayloadFromMiner(addr, d));
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
        setMinerBalanceData((current) => mergeMinerBalancePayload(current, d, !includePendingEstimate));
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
        setHistory(d || []);
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

        const balancePromise = api
          .getMinerBalance(addr, true)
          .then((value) => ({ ok: true as const, value }))
          .catch(() => ({ ok: false as const }));
        const detailPromise = api
          .getMiner(addr, false, RECENT_SHARES_LIMIT)
          .then((value) => ({ ok: true as const, value }))
          .catch(() => ({ ok: false as const }));
        const hashratePromise = api
          .getMinerHashrate(addr, range)
          .then((value) => ({ ok: true as const, value }))
          .catch(() => ({ ok: false as const }));

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
            setMinerBalanceData((current) =>
              current?.address === addr ? current : balancePayloadFromMiner(addr, detailResult.value)
            );
          });
        }

        const hashrateResult = await hashratePromise;
        if (requestId !== lookupRequestSeq.current) return;
        if (balanceResult.ok || detailResult.ok) {
          startTransition(() => {
            setHistory(hashrateResult.ok ? (hashrateResult.value || []) : []);
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

  const currentHashrateKey = minerAddress ? `${minerAddress}:${range}` : '';

  useEffect(() => {
    if (!active || !minerAddress) return;
    if (hashrateRequestKeyRef.current === currentHashrateKey) return;
    void loadMinerHashrate();
  }, [active, currentHashrateKey, loadMinerHashrate, minerAddress]);

  useEffect(() => {
    if (!active) return;
    void loadRejections();
  }, [active, loadRejections]);

  useEffect(() => {
    if (!active || liveTick <= 0) return;
    if (minerAddress) {
      if (liveTick % 12 === 0) {
        void refreshMinerBalance(true);
      } else {
        void refreshMinerBalance(false);
      }
      if (liveTick % 12 === 0) {
        void refreshMinerData(true);
      } else if (liveTick % 6 === 0) {
        void refreshMinerData(false);
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
  }, [active, liveTick, minerAddress, refreshMinerBalance, refreshMinerData, loadMinerHashrate, loadRejections]);

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
    const miningSince = toUnixMs(minerData?.mining_since);
    if (miningSince) return new Date(miningSince).toLocaleDateString();

    const shares = minerData?.shares || [];
    let oldest = 0;
    for (const s of shares) {
      const t = toUnixMs(s.created_at);
      if (!t) continue;
      if (!oldest || t < oldest) oldest = t;
    }
    return oldest ? new Date(oldest).toLocaleDateString() : '-';
  }, [minerData]);

  const liveBalance = minerBalanceData?.balance ?? minerData?.balance;
  const livePendingPayout = minerBalanceData?.pending_payout ?? minerData?.pending_payout;
  const livePendingEstimate = minerBalanceData?.pending_estimate ?? minerData?.pending_estimate;
  const pendingConfirmed = liveBalance?.pending_confirmed ?? liveBalance?.pending ?? 0;
  const pendingEstimated = livePendingEstimate?.estimated_pending ?? 0;
  const pendingQueued = liveBalance?.pending_queued ?? livePendingPayout?.amount ?? 0;
  const payoutEta = insights?.payout_eta ?? null;
  const rejectionWindow = insights?.rejections?.window ?? null;
  const minerAccepted = minerData?.total_accepted ?? 0;
  const minerRejected = minerData?.total_rejected ?? 0;
  const minerChecked = minerAccepted + minerRejected;
  const minerRejectRate = minerChecked > 0 ? (minerRejected / minerChecked) * 100 : null;
  const recentShares = minerData?.shares?.slice(0, RECENT_SHARES_LIMIT) || [];
  const previewBlocks = livePendingEstimate?.blocks ?? [];
  const verificationHold = minerData?.verification_hold ?? null;
  const verificationReason = verificationHold?.reason?.trim() || 'share validation issue';
  const verificationPendingProvisional = verificationHold?.validation_pending_provisional ?? 0;
  const verificationStartedAt = toUnixMs(verificationHold?.started_at);
  const verificationOnlyUntil = toUnixMs(verificationHold?.verified_only_until);
  const verificationQuarantineUntil = toUnixMs(verificationHold?.quarantined_until);
  const latestHashratePoint = history.length > 0 ? history[history.length - 1] : null;
  const liveHashrate = minerData?.hashrate ?? latestHashratePoint?.hashrate ?? 0;
  const livePaid = liveBalance?.paid ?? minerData?.balance?.paid ?? 0;
  const showLookupResult = !!minerAddress && (!!minerData || !!minerBalanceData || history.length > 0);
  const minerDetailLoading = lookupLoading && !minerData;

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
              <div className="stat-card stat-card--flow" title={formatCoins(pendingEstimated)}>
                <div className="label">Estimated Rewards</div>
                <div className="value">{formatCompactCoins(pendingEstimated)}</div>
                <div className="stat-meta">Recent blocks still confirming</div>
              </div>
              <div className="stat-card stat-card--flow" title={formatCoins(pendingConfirmed)}>
                <div className="label">Confirmed Rewards</div>
                <div className="value">{formatCompactCoins(pendingConfirmed)}</div>
                <div className="stat-meta">Matured balance awaiting payout</div>
              </div>
              <div className="stat-card" title={formatCoins(livePaid)}>
                <div className="label">Paid Balance</div>
                <div className="value">{formatCompactCoins(livePaid)}</div>
                <div className="stat-meta">Already sent to this address</div>
              </div>
            </div>
          </div>

          <div className="stats-card-group" style={{ marginBottom: 24 }}>
            <div className="stats-card-group-title">Mining</div>
            <div className="stats-card-group-grid">
              <div className="stat-card">
                <div className="label">Hashrate</div>
                <div className="value">{humanRate(liveHashrate)}</div>
              </div>
              <div className="stat-card">
                <div className="label">Blocks Found</div>
                <div className="value">{minerData ? (minerData.blocks_found || []).length : '...'}</div>
              </div>
              <div className="stat-card">
                <div className="label">Mining Since</div>
                <div className="value">{minerData ? minerOldestShareDate : '...'}</div>
              </div>
            </div>
          </div>

          <div className="stats-grid" style={{ marginBottom: 24 }}>
            <div className="stat-card">
              <div className="label">Shares Accepted</div>
              <div className="value">{minerData ? minerAccepted : '...'}</div>
            </div>
            <div className="stat-card">
              <div className="label">Shares Rejected</div>
              <div className="value">{minerData ? minerRejected : '...'}</div>
            </div>
            <div className="stat-card">
              <div className="label">Reject Rate</div>
              <div className="value">{minerData ? fmtPct(minerRejectRate) : '...'}</div>
            </div>
            <div className="stat-card">
              <div className="label">Avg Difficulty</div>
              <div className="value">{minerData ? minerAvgDiff : '...'}</div>
            </div>
          </div>

          {verificationHold && (
            <div
              className="card"
              style={{ marginBottom: 24, background: 'rgba(247, 180, 75, 0.12)', borderColor: 'rgba(247, 180, 75, 0.45)' }}
            >
              <div style={{ color: 'var(--text)', fontSize: 13 }}>
                {verificationHold.mode === 'quarantined'
                  ? `Verification hold active. This address is quarantined until ${
                      verificationQuarantineUntil ? new Date(verificationQuarantineUntil).toLocaleString() : 'the current hold expires'
                    }, so new submissions from this address are temporarily blocked.`
                  : `Verification hold active until ${
                      verificationOnlyUntil ? new Date(verificationOnlyUntil).toLocaleString() : 'the current hold expires'
                    }. Only fully verified shares count toward unconfirmed estimates and payout while this hold is active.`}
                {verificationHold.mode === 'quarantined' &&
                  verificationOnlyUntil &&
                  verificationOnlyUntil !== verificationQuarantineUntil && (
                    <> {`Verified-only credit continues until ${new Date(verificationOnlyUntil).toLocaleString()} after quarantine ends.`}</>
                  )}
                {verificationHold.validation_hold_cause === 'provisional_backlog' &&
                verificationPendingProvisional > 0
                  ? ` Reason: ${verificationReason}. ${verificationPendingProvisional} provisional share${
                      verificationPendingProvisional === 1 ? '' : 's'
                    } are still waiting for full verification.`
                  : ` Reason: ${verificationReason}.`}
                {verificationStartedAt && <> {`Started ${new Date(verificationStartedAt).toLocaleString()}.`}</>}
                {' Confirmed balance and completed payouts are unaffected.'}
              </div>
            </div>
          )}

          {minerDetailLoading && (
            <div className="card" style={{ marginBottom: 24, color: 'var(--muted)', fontSize: 13 }}>
              Loading worker, payout, and share history...
            </div>
          )}

          {pendingQueued > 0 && !!payoutEta?.liquidity_constrained && (
            <div
              className="card"
              style={{ marginBottom: 24, background: 'rgba(247, 180, 75, 0.12)', borderColor: 'rgba(247, 180, 75, 0.45)' }}
            >
              <div style={{ color: 'var(--text)', fontSize: 13 }}>
                Your payout is queued, but pool wallet liquidity is currently tight. Spendable balance:{' '}
                <span className="mono">{formatCoins(payoutEta.wallet_spendable ?? 0)}</span>. Locked/confirming balance:{' '}
                <span className="mono">{formatCoins(payoutEta.wallet_pending ?? 0)}</span>. Pool queue:{' '}
                <span className="mono">{formatCoins(payoutEta.pending_total_amount ?? 0)}</span>. Those locked funds are
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
                    {previewBlocks.map((b) => {
                      return (
                      <tr key={`${b.height}-${b.hash}`}>
                        <td>
                          <a href={`https://explorer.blocknetcrypto.com/block/${b.hash || ''}`} target="_blank" rel="noopener">
                            {b.height}
                          </a>
                        </td>
                        <td title={b.validation_detail || undefined}>
                          {b.credit_withheld
                            ? 'Withheld'
                            : b.validation_state === 'extra_verification'
                              ? `${formatCoins(b.estimated_credit)} (verified only)`
                              : formatCoins(b.estimated_credit)}
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
                    {!minerData.payouts?.length ? (
                      <tr>
                        <td colSpan={5} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                          No payouts yet
                        </td>
                      </tr>
                    ) : (
                      minerData.payouts.map((p) => (
                        <tr key={`${p.id}-${p.tx_hash}-${toUnixMs(p.timestamp)}`}>
                          <td>{formatCoins(p.amount)}</td>
                          <td>{formatFee(p.fee || 0)}</td>
                          <td>
                            <a href={`https://explorer.blocknetcrypto.com/tx/${p.tx_hash || ''}`} target="_blank" rel="noopener">
                              {p.tx_hash || '-'}
                            </a>
                          </td>
                          <td>
                            <span className={`badge ${p.confirmed === false ? 'badge-pending' : 'badge-confirmed'}`}>
                              {p.confirmed === false ? 'unconfirmed' : 'confirmed'}
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
          )}

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

          {minerData && (
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
          )}
        </div>
      )}
    </div>
  );
}
