import { locale, t, tx, localizePath } from '../i18n';
import { startTransition, useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { ApiClient } from '../api/client';
import { EmptyTableRow } from '../components/EmptyTableRow';
import { ExplorerLink } from '../components/ExplorerLink';
import { HashrateChart } from '../components/HashrateChart';
import { PayoutStatusBadge } from '../components/PayoutStatusBadge';
import { RangeTabs } from '../components/RangeTabs';
import { StatCard } from '../components/StatCard';
import { MinerUpdateBanner } from '../components/MinerUpdateBanner';
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
  const [balanceUnavailable, setBalanceUnavailable] = useState(false);
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
        setBalanceUnavailable(false);
        setMinerBalanceData((current) => ({
          ...d,
          pending_estimate:
            !includePendingEstimate && current?.address === d.address
              ? current?.pending_estimate ?? d.pending_estimate
              : d.pending_estimate,
        }));
      });
    } catch {
      if (minerAddressRef.current === addr) {
        startTransition(() => {
          setBalanceUnavailable(true);
        });
      }
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
        setBalanceUnavailable(!balanceResult.ok);
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
    if (miningSince) return new Date(miningSince).toLocaleDateString(locale, { calendar: 'gregory' });

    const shares = minerData.shares;
    let oldest = 0;
    for (const s of shares) {
      const t = toUnixMs(s.created_at);
      if (!t) continue;
      if (!oldest || t < oldest) oldest = t;
    }
    return oldest ? new Date(oldest).toLocaleDateString(locale, { calendar: 'gregory' }) : '-';
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
  const verificationReason = verificationHold?.reason?.trim() || t('m599');
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
      <h2>{t("m005")}</h2>
      <MinerUpdateBanner miner={minerData} />
      <div className="card" style={{ marginBottom: 24 }}>
        <div className="lookup-form" style={{ display: 'flex', gap: 10, marginBottom: 0, flexWrap: 'wrap' }}>
          <input
            type="text"
            dir="ltr"
            placeholder={t("m486")}
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
            {resolving ? t("m487") : lookupLoading ? t("m094") : t("m488")}
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
              setBalanceUnavailable(false);
              setHistory([]);
              setResolvedHandle(null);
              localStorage.removeItem(LAST_MINER_LOOKUP_KEY);
            }}
          >{t("m013")}</button>
          {resolvedHandle && minerAddress && (<span className="resolved-badge" style={{ alignSelf: 'center', fontSize: '0.85em', color: 'var(--success, #4caf50)' }}>
              ${resolvedHandle} → {minerAddress.slice(0, 8)}…{minerAddress.slice(-6)}
            </span>)}
        </div>
      </div>

      <p className="section-lead" style={{ marginBottom: 16 }}><a href={`${localizePath("/blog/")}track-your-miner-on-bntpool/#zero-hashrate`}>{t("minerHelp")}</a></p>

      {showLookupResult && (<div id="lookup-result">
          {balanceUnavailable && (<div
              className="card"
              style={{ marginBottom: 16, background: 'rgba(247, 180, 75, 0.12)', borderColor: 'rgba(247, 180, 75, 0.45)' }}
            >
              <div style={{ color: 'var(--text)', fontSize: 13 }}>
                {minerBalanceData ? t("m489") : t("m490")}
              </div>
            </div>)}
          <div className="stats-card-group" style={{ marginBottom: 16 }}>
            <div className="stats-card-group-title">{t("m491")}</div>
            <div className="stats-card-group-grid">
              <StatCard
                label={t("m492")}
                value={minerBalanceData ? formatCompactCoins(pendingEstimated) : '—'}
                meta={t("m493")}
                className="stat-card--flow"
                title={minerBalanceData ? formatCoins(pendingEstimated) : undefined}
              />
              <StatCard
                label={t("m494")}
                value={minerBalanceData ? formatCompactCoins(pendingConfirmed) : '—'}
                meta={t("m495")}
                className="stat-card--flow"
                title={minerBalanceData ? formatCoins(pendingConfirmed) : undefined}
              />
              <StatCard
                label={t("m496")}
                value={minerBalanceData ? formatCompactCoins(livePaid) : '—'}
                meta={t("m497")}
                title={minerBalanceData ? formatCoins(livePaid) : undefined}
              />
            </div>
            <div style={{ marginTop: 10, fontSize: 12, color: 'var(--muted)' }}>{t("m498")}</div>
          </div>

          <div className="stats-card-group" style={{ marginBottom: 24 }}>
            <div className="stats-card-group-title">{t("m499")}</div>
            <div className="stats-card-group-grid">
              <StatCard label={t("m082")} value={humanRate(liveHashrate)} />
              <StatCard label={t("m411")} value={minerData ? minerData.blocks_found : '...'} />
              <StatCard label={t("m500")} value={minerData ? minerOldestShareDate : '...'} />
            </div>
          </div>

          <div className="stats-grid" style={{ marginBottom: 24 }}>
            <StatCard label={t("m501")} value={minerData ? minerAccepted : '...'} />
            <StatCard label={t("m502")} value={minerData ? minerRejected : '...'} />
            <StatCard label={t("m503")} value={minerData ? formatPct(minerRejectRate) : '...'} />
            <StatCard label={t("m504")} value={minerData ? minerAvgDiff : '...'} />
          </div>

          {verificationHold && (<div
              className="card"
              style={{ marginBottom: 24, background: 'rgba(247, 180, 75, 0.12)', borderColor: 'rgba(247, 180, 75, 0.45)' }}
            >
              <div style={{ color: 'var(--text)', fontSize: 13 }}>
                {verificationHold.mode === 'quarantined' ? t("m505", [timestampTitle(verificationQuarantineUntil) || t('m600')]) : t("m506", [timestampTitle(verificationOnlyUntil) || t('m600')])}
                {verificationHold.mode === 'quarantined' &&
                  verificationOnlyUntil &&
                  verificationOnlyUntil !== verificationQuarantineUntil && (<> {t("m507", [timestampTitle(verificationOnlyUntil)])}</>)}
                {verificationHold.validation_hold_cause === 'provisional_backlog' &&
                verificationPendingProvisional > 0 ? t('m610', [verificationReason, verificationPendingProvisional]) : t("m509", [verificationReason])}
                {verificationStartedAt && <> {t("m510", [timestampTitle(verificationStartedAt)])}</>}
                {t("m511")}
              </div>
            </div>)}

          {minerDetailLoading && (<div className="card" style={{ marginBottom: 24, color: 'var(--muted)', fontSize: 13 }}>{t("m512")}</div>)}

          {pendingQueued > 0 && payoutEta && payoutLiquidityConstrained && (<div
              className="card"
              style={{ marginBottom: 24, background: 'rgba(247, 180, 75, 0.12)', borderColor: 'rgba(247, 180, 75, 0.45)' }}
            >
              <div style={{ color: 'var(--text)', fontSize: 13 }}>{tx("m513", [<span className="mono">{formatCoins(payoutEta.wallet_spendable ?? 0)}</span>, <span className="mono">{formatCoins(payoutEta.wallet_pending ?? 0)}</span>, <span className="mono">{formatCoins(payoutEta.pending_total_amount)}</span>])}</div>
            </div>)}

          {!!previewBlocks.length && (<div className="section">
              <h2>{t("m514")}</h2>
              <div className="card table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>{t("m374")}</th>
                      <th>{t("m515")}</th>
                      <th>{t("m516")}</th>
                      <th>{t("m377")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {previewBlocks.map((b) => (
                      <tr key={`${b.height}-${b.hash}`}>
                        <td>
                          <ExplorerLink kind="block" value={b.hash || ''}>{b.height}</ExplorerLink>
                        </td>
                        <td title={b.validation_detail || undefined}>
                          {b.credit_withheld ? t("m517") : b.validation_state === 'extra_verification' ? t("m518", [formatCoins(b.estimated_credit)]) : formatCoins(b.estimated_credit)}
                        </td>
                        <td>{b.confirmations_remaining}</td>
                        <td title={timestampTitle(b.timestamp)}>{timeAgo(b.timestamp)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <div style={{ marginTop: 10, fontSize: 12, color: 'var(--muted)' }}>{t("m519")}</div>
              </div>
            </div>)}

          <div className="section">
            <div className="section-header">
              <h2>{t("m520")}</h2>
              <RangeTabs<Range> options={['1h', '24h', '7d', '30d']} value={range} onChange={setRange} />
            </div>
            <HashrateChart data={history} range={range} theme={theme} />
          </div>

          {minerData && (<div className="section">
              <h2 id="miner-workers" tabIndex={-1}>{t("m081")}</h2>
              <div className="card table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>{t("m521")}</th>
                      <th>{t("m522")}</th>
                      <th>{t("m082")}</th>
                      <th>{t("m083")}</th>
                      <th>{t("m084")}</th>
                      <th>{t("m085")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {!minerData.workers.length ? (<EmptyTableRow colSpan={6}>{t("m523")}</EmptyTableRow>) : (minerData.workers.map((w) => (
                        <tr key={w.worker}>
                          <td>{w.worker || t("m524")}</td>
                          <td className="mono">{w.miner_version?.trim() || t("m525")}</td>
                          <td>{humanRate(w.hashrate || 0)}</td>
                          <td>{w.accepted || 0}</td>
                          <td>{w.rejected || 0}</td>
                          <td title={timestampTitle(w.last_share_at)}>
                            {timeAgo(w.last_share_at)}
                          </td>
                        </tr>
                      )))}
                  </tbody>
                </table>
              </div>
            </div>)}

          {minerData && (<div className="section">
              <h2>{t("m437")}</h2>
              <div className="card table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>{t("m438")}</th>
                      <th>{t("m470")}</th>
                      <th>{t("m526")}</th>
                      <th>{t("m006")}</th>
                      <th>{t("m377")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {!minerData.payouts.length ? (<EmptyTableRow colSpan={5}>{t("m440")}</EmptyTableRow>) : (minerData.payouts.map((p, payoutIndex) => {
                        const hasTx = Boolean(p.tx_hash.trim());
                        const status = p.confirmed ? 'confirmed' : hasTx ? 'unconfirmed' : 'queued';
                        return (
                          <tr key={`${p.tx_hash}-${toUnixMs(p.timestamp)}-${payoutIndex}`}>
                            <td>{formatCoins(p.amount)}</td>
                            <td>{formatFee(p.fee || 0)}</td>
                            <td>
                              {hasTx ? (<ExplorerLink kind="tx" value={p.tx_hash}>{p.tx_hash}</ExplorerLink>) : ('-')}
                            </td>
                            <td>
                              <PayoutStatusBadge status={status} />
                            </td>
                            <td title={timestampTitle(p.timestamp)}>{timeAgo(p.timestamp)}</td>
                          </tr>
                        );
                      }))}
                  </tbody>
                </table>
              </div>
            </div>)}

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

          {minerData && (<div className="section">
              <div className="section-header">
                <h2>{t("m536")}</h2>
                <span className="section-meta">{t("m537")}</span>
              </div>
              <div className="card table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>{t("m538")}</th>
                      <th>{t("m521")}</th>
                      <th>{t("m539")}</th>
                      <th>{t("m006")}</th>
                      <th>{t("m377")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {!recentShares.length ? (<EmptyTableRow colSpan={5}>{t("m540")}</EmptyTableRow>) : (recentShares.map((s, idx) => (
                        <tr key={`${s.job_id}-${idx}`}>
                          <td>{s.job_id || ''}</td>
                          <td>{s.worker || ''}</td>
                          <td>{s.difficulty}</td>
                          <td>{s.status === 'accepted' ? t('m083') : s.status === 'rejected' ? t('m084') : s.status || ''}</td>
                          <td title={timestampTitle(s.created_at)}>{timeAgo(s.created_at)}</td>
                        </tr>
                      )))}
                  </tbody>
                </table>
              </div>
            </div>)}
        </div>)}
    </div>
  );
}
