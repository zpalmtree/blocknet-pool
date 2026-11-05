import { t, tx, localizePath } from '../i18n';
import { startTransition, useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { ApiClient } from '../api/client';
import { EmptyTableRow } from '../components/EmptyTableRow';
import { Pager } from '../components/Pager';
import { ExplorerLink } from '../components/ExplorerLink';
import { HashrateChart } from '../components/HashrateChart';
import { PayoutStatusBadge } from '../components/PayoutStatusBadge';
import { PayoutProgress } from '../components/PayoutProgress';
import { RangeTabs } from '../components/RangeTabs';
import { MiningType } from '../components/MiningType';
import { MinerUpdateBanner } from '../components/MinerUpdateBanner';
import { MinerSettings, SettingsLockIcon } from '../components/MinerSettings';
import { featureIsNew, markFeatureSeen } from '../lib/news';
import { knownExchange, validMemo } from '../lib/payout-destination';
import { readDestination, rememberDestination } from '../lib/storage';
import { formatCoins, formatCompactCoins, formatFee, humanRate, timeAgo, timestampTitle, toUnixMs } from '../lib/format';
import type { ThemeMode } from '../lib/theme';
import type {
  InfoResponse,
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

// Large farms report hundreds of rigs; page them so balance and history stay in reach.
const WORKERS_PAGE_SIZE = 20;
const PAYOUTS_PAGE_SIZE = 10;

// Solo miners log in as solo:<address>; look up the address itself.
function stripSoloLogin(raw: string): string {
  return raw.replace(/^solo:\s*/i, '');
}

function looksLikeHandle(raw: string): boolean {
  return raw.startsWith('$') || raw.startsWith('@') || (raw.length < 25 && /^[$@]?[a-z0-9][a-z0-9_.\-]{0,62}$/i.test(raw));
}

const lookupResult = async <T,>(promise: Promise<T>) =>
  promise.then((value) => ({ ok: true as const, value })).catch(() => ({ ok: false as const }));

export function StatsPage({ api, liveTick, theme }: StatsPageProps) {
  const [savedDestination] = useState(readDestination);
  const [minerInput, setMinerInput] = useState(savedDestination.address);
  const [memoInput, setMemoInput] = useState(savedDestination.memo);
  const [memoOpen, setMemoOpen] = useState(!!savedDestination.memo);
  const [appliedDestination, setAppliedDestination] = useState({ address: '', memo: '' });
  const [poolInfo, setPoolInfo] = useState<InfoResponse | null>(null);
  const [lookupError, setLookupError] = useState('');
  const exchange = knownExchange(poolInfo, minerInput);
  // A pasted `memo:` account key already includes its memo.
  const pastedAccount = minerInput.trim().startsWith('memo:');
  const showMemo = !pastedAccount && (memoOpen || !!exchange);
  useEffect(() => {
    let active = true;
    api.getInfo().then(info => { if (active) setPoolInfo(info); }).catch(() => {});
    return () => { active = false; };
  }, [api]);
  const [minerAddress, setMinerAddress] = useState('');
  const [minerData, setMinerData] = useState<MinerResponse | null>(null);
  const [minerBalanceData, setMinerBalanceData] = useState<MinerBalancePayload | null>(null);
  const [balanceUnavailable, setBalanceUnavailable] = useState(false);
  const [range, setRange] = useState<Range>('1h');
  const [history, setHistory] = useState<HashratePoint[]>([]);
  const [insights, setInsights] = useState<StatsInsightsResponse | null>(null);
  const minerAddressRef = useRef(minerAddress);
  const lookupRequestSeq = useRef(0);
  const hashrateRequestKeyRef = useRef('');
  const [resolving, setResolving] = useState(false);
  const [lookupLoading, setLookupLoading] = useState(false);
  const [resolvedHandle, setResolvedHandle] = useState<string | null>(null);
  const [workersOffset, setWorkersOffset] = useState(0);
  const [payoutsOffset, setPayoutsOffset] = useState(0);
  const [estimateHelpOpen, setEstimateHelpOpen] = useState(false);
  const [view, setView] = useState<'stats' | 'settings'>(() =>
    typeof location !== 'undefined' && location.hash === '#settings' ? 'settings' : 'stats');
  const [settingsUnlocked, setSettingsUnlocked] = useState(false);
  // A dot on the Settings tab while news announces it, until the miner opens it once.
  const [settingsNew, setSettingsNew] = useState(() => featureIsNew('settings'));
  const tabsRef = useRef<HTMLDivElement>(null);
  const [panelMinHeight, setPanelMinHeight] = useState(0);
  // A short panel replacing a long one would shrink the page under the
  // current scroll position and jump it up; keep the tabs where they are.
  const switchView = (next: 'stats' | 'settings') => {
    if (next === view) return;
    const tabs = tabsRef.current;
    if (tabs && typeof window !== 'undefined') {
      const panelTop = tabs.getBoundingClientRect().bottom + window.scrollY;
      setPanelMinHeight(Math.max(0, Math.ceil(window.scrollY + window.innerHeight - panelTop)));
    }
    setView(next);
  };

  // A late registry response must not leave address-only results under a memo warning.
  useEffect(() => {
    if (!exchange || memoInput) return;
    lookupRequestSeq.current += 1;
    minerAddressRef.current = '';
    hashrateRequestKeyRef.current = '';
    setMinerAddress('');
    setMinerData(null);
    setMinerBalanceData(null);
    setHistory([]);
    setLookupLoading(false);
  }, [exchange?.address, memoInput]);

  useEffect(() => {
    minerAddressRef.current = minerAddress;
    setWorkersOffset(0);
    setPayoutsOffset(0);
    setSettingsUnlocked(false);
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
    async (input?: string, memoOverride?: string) => {
      let addr = stripSoloLogin((input ?? minerInput).trim());
      const memo = addr.startsWith('memo:') ? '' : (memoOverride ?? memoInput);
      let resolved: { address: string; handle: string } | null = null;
      if (!addr || !validMemo(memo)) return;
      if (knownExchange(poolInfo, addr) && !memo) { setMemoOpen(true); return; }
      const requestId = ++lookupRequestSeq.current;
      setLookupLoading(true);
      setLookupError('');
      minerAddressRef.current = '';
      hashrateRequestKeyRef.current = '';
      setMinerAddress('');
      setMinerData(null);
      setMinerBalanceData(null);
      setHistory([]);

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

        if (requestId !== lookupRequestSeq.current) return;
        const walletAddress = addr;
        if (knownExchange(poolInfo, walletAddress) && !memo) {
          setMinerInput(walletAddress);
          setMemoOpen(true);
          return;
        }
        if (memo) {
          const destination = await api.resolvePayoutDestination(walletAddress, memo);
          if (requestId !== lookupRequestSeq.current) return;
          addr = destination.account;
        }
        let committed = false;
        let resetPreviousResult = minerAddressRef.current !== addr;
        const commitLookup = () => {
          if (committed) return;
          committed = true;
          minerAddressRef.current = addr;
          setMinerAddress(addr);
          setMinerInput(walletAddress);
          setAppliedDestination({ address: walletAddress, memo });
          rememberDestination({ address: walletAddress, memo });
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
      } catch {
        if (requestId === lookupRequestSeq.current) setLookupError(t('memoLookupFailed'));
      } finally {
        if (requestId === lookupRequestSeq.current) {
          setLookupLoading(false);
        }
      }
    },
    [api, minerInput, memoInput, poolInfo, range]
  );

  // Only the pool payout ETA is used here, for the liquidity notice.
  const loadInsights = useCallback(async () => {
    try {
      const d = await api.getStatsInsights();
      setInsights(d);
    } catch {
      setInsights(null);
    }
  }, [api]);

  const restoredLookup = useRef(false);
  useEffect(() => {
    if (restoredLookup.current) return;
    restoredLookup.current = true;
    if (savedDestination.address) void loadMinerLookup(savedDestination.address, savedDestination.memo);
  }, [loadMinerLookup, savedDestination]);

  const currentHashrateKey = minerAddress ? `${minerAddress}:${range}` : '';

  useEffect(() => {
    if (!minerAddress) return;
    if (hashrateRequestKeyRef.current === currentHashrateKey) return;
    void loadMinerHashrate();
  }, [currentHashrateKey, loadMinerHashrate, minerAddress]);

  useEffect(() => {
    void loadInsights();
  }, [loadInsights]);

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
      void loadInsights();
    }
  }, [liveTick, minerAddress, refreshMinerBalance, refreshMinerData, loadMinerHashrate, loadInsights]);

  const lookupDisabled = useMemo(() => {
    const raw = minerInput.trim();
    if (raw.startsWith('memo:')) return !!minerAddress && raw === appliedDestination.address;
    if (!raw || !validMemo(memoInput) || (!!exchange && !memoInput)) return true;
    if (looksLikeHandle(raw)) return false;
    return !!minerAddress && raw === appliedDestination.address && memoInput === appliedDestination.memo;
  }, [minerAddress, minerInput, memoInput, exchange, appliedDestination]);

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
  const previewBlocks = livePendingEstimate?.blocks ?? [];
  // The Type columns appear only for miners with solo blocks or rigs.
  const soloPreview = previewBlocks.some((block) => block.solo);
  const soloWorkers = !!minerData?.workers.some((worker) => worker.solo);
  const verificationHold = minerData?.verification_hold ?? null;
  const verificationReason = verificationHold?.reason?.trim() || t('m599');
  const verificationPendingProvisional = verificationHold?.validation_pending_provisional ?? 0;
  const verificationStartedAt = toUnixMs(verificationHold?.started_at);
  const verificationOnlyUntil = toUnixMs(verificationHold?.verified_only_until);
  const verificationQuarantineUntil = toUnixMs(verificationHold?.quarantined_until);
  const latestHashratePoint = history.length > 0 ? history[history.length - 1] : null;
  const liveHashrate = minerData ? minerData.hashrate : latestHashratePoint?.hashrate ?? 0;
  const livePaid = liveBalance?.paid ?? 0;
  // A refresh can shrink a list under the current page; fall back to the first page.
  const workersStart = minerData && workersOffset < minerData.workers.length ? workersOffset : 0;
  const payoutsStart = minerData && payoutsOffset < minerData.payouts.length ? payoutsOffset : 0;
  const showLookupResult = !(exchange && !memoInput) && !!minerAddress && (!!minerData || !!minerBalanceData || history.length > 0);
  const minerDetailLoading = lookupLoading && !minerData;
  const settingsOpen = showLookupResult && view === 'settings';
  useEffect(() => {
    if (!settingsOpen || !settingsNew) return;
    markFeatureSeen('settings');
    setSettingsNew(false);
  }, [settingsOpen, settingsNew]);
  const noHashrate = !!minerData && (!minerData.workers.length || liveHashrate <= 0);
  // Hashing with nothing estimated yet: the pool has not found a block with these shares.
  const estimateZero = !!minerBalanceData && pendingEstimated === 0 && liveHashrate > 0;
  const balanceValue = (amount: number) => minerBalanceData
    ? <span title={formatCoins(amount)}><bdi dir="ltr">{formatCompactCoins(amount)}</bdi></span>
    : '—';

  const clearLookup = () => {
    lookupRequestSeq.current += 1;
    hashrateRequestKeyRef.current = '';
    minerAddressRef.current = '';
    setMemoInput('');
    setMemoOpen(false);
    setLookupError('');
    setLookupLoading(false);
    setResolving(false);
    setMinerInput('');
    setMinerAddress('');
    setMinerData(null);
    setMinerBalanceData(null);
    setBalanceUnavailable(false);
    setHistory([]);
    setResolvedHandle(null);
    rememberDestination(null);
  };


  return (
    <div id="page-stats">
      <div className="page-header">
        <h1>{t("m616")}</h1>
      </div>
      <MinerUpdateBanner miner={minerData} />
      <div className="card stats-lookup">
        <div className="lookup-form">
          <input
            type="text"
            dir="ltr"
            placeholder={t("m486")}
            aria-label={t('m486')}
            value={minerInput}
            onChange={(e) => setMinerInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !lookupDisabled) {
                void loadMinerLookup();
              }
            }}
          />
          <button
            className={`btn btn-primary ${lookupDisabled ? 'is-faded' : ''}`}
            disabled={lookupDisabled || resolving || lookupLoading}
            onClick={() => void loadMinerLookup()}
          >
            {resolving ? t("m487") : lookupLoading ? t("m094") : t("m488")}
          </button>
        </div>
        <div className="stats-lookup-links">
          {!showMemo && !pastedAccount && <button type="button" className="link-button" onClick={() => setMemoOpen(true)}>{t('memoAdd')}</button>}
          {(minerInput || minerAddress) && <button type="button" className="link-button" onClick={clearLookup}>{t("m013")}</button>}
          {resolvedHandle && minerAddress && (<span className="resolved-badge" dir="ltr">
              ${resolvedHandle} → {appliedDestination.address.slice(0, 8)}…{appliedDestination.address.slice(-6)}
            </span>)}
        </div>
        {showMemo && <div className="memo-fields">
          <label htmlFor="stats-memo">{t('memoLabel')}</label>
          <input id="stats-memo" type="text" value={memoInput} autoComplete="off" spellCheck={false} dir="ltr"
            aria-invalid={!validMemo(memoInput) || (!!exchange && !memoInput)} aria-describedby="stats-memo-help"
            onChange={event => setMemoInput(event.target.value)}
            onKeyDown={event => { if (event.key === 'Enter' && !lookupDisabled && !lookupLoading) void loadMinerLookup(); }} />
          <p id="stats-memo-help" role={exchange && !memoInput ? 'alert' : undefined}>{exchange ? t('memoRequired', [exchange.name]) : t('memoLimit')}</p>
          {!validMemo(memoInput) && <p role="alert">{t('memoLimit')}</p>}
        </div>}
        {pastedAccount && minerAddress === minerInput.trim() && minerData?.payout_account && (
          <p className="stats-memo-account">
            {minerData.payout_account.exchange
              ? t('memoAccount', [minerData.payout_account.exchange])
              : t('memoAccountUnknown')}
          </p>
        )}
        {lookupError && <p className="stats-lookup-error" role="alert">{lookupError}</p>}
      </div>

      {!minerAddress && !lookupLoading && (<p className="section-lead" style={{ marginBottom: 16 }}>{tx("statsEmpty", [<a href={localizePath("/start")}>{t("m002")}</a>])}</p>)}

      {showLookupResult && (<div ref={tabsRef} className="sub-tabs stats-tabs" role="tablist" aria-label={t('m616')}>
          <button type="button" role="tab" id="stats-tab-stats" aria-selected={view === 'stats'} aria-controls="lookup-result"
            className={view === 'stats' ? 'active' : ''} onClick={() => switchView('stats')}>{t('statsTab')}</button>
          <button type="button" role="tab" id="stats-tab-settings" aria-selected={view === 'settings'} aria-controls="miner-settings"
            className={view === 'settings' ? 'active' : ''} onClick={() => switchView('settings')}>
            <SettingsLockIcon open={settingsUnlocked} />{t('settingsTab')}
            {settingsNew && <span className="tab-new-dot"><span className="sr-only">{t('newsNew')}</span></span>}</button>
        </div>)}

      {showLookupResult && view === 'settings' && (<div id="miner-settings" role="tabpanel" aria-labelledby="stats-tab-settings"
          style={panelMinHeight ? { minHeight: panelMinHeight } : undefined}>
          <MinerSettings api={api} account={minerAddress} onSaved={() => void refreshMinerBalance(false)}
            onUnlockedChange={setSettingsUnlocked} />
        </div>)}

      {showLookupResult && view === 'stats' && (<div id="lookup-result" role="tabpanel" aria-labelledby="stats-tab-stats"
          style={panelMinHeight ? { minHeight: panelMinHeight } : undefined}>
          {balanceUnavailable && (<div className="card stats-notice">{minerBalanceData ? t("m489") : t("m490")}</div>)}
          {verificationHold && (<div className="card stats-notice">
                {verificationHold.mode === 'quarantined' ? t("m505", [timestampTitle(verificationQuarantineUntil) || t('m600')]) : t("m506", [timestampTitle(verificationOnlyUntil) || t('m600')])}
                {verificationHold.mode === 'quarantined' &&
                  verificationOnlyUntil &&
                  verificationOnlyUntil !== verificationQuarantineUntil && (<> {t("m507", [timestampTitle(verificationOnlyUntil)])}</>)}
                {verificationHold.validation_hold_cause === 'provisional_backlog' &&
                verificationPendingProvisional > 0 ? t('m610', [verificationReason, verificationPendingProvisional]) : t("m509", [verificationReason])}
                {verificationStartedAt && <> {t("m510", [timestampTitle(verificationStartedAt)])}</>}
                {t("m511")}
            </div>)}
          {pendingQueued > 0 && payoutEta && payoutLiquidityConstrained && (<div className="card stats-notice">
              {tx("m513", [<span className="mono">{formatCoins(payoutEta.wallet_spendable ?? 0)}</span>, <span className="mono">{formatCoins(payoutEta.wallet_pending ?? 0)}</span>, <span className="mono">{formatCoins(payoutEta.pending_total_amount)}</span>])}
            </div>)}

          <section id="miner-balances" className="card stats-summary" aria-busy={minerDetailLoading}>
            <div className="stats-summary-top">
              <div className="stats-summary-hr">
                <span>{t("m082")}</span>
                <strong dir="ltr">{humanRate(liveHashrate)}</strong>
                {noHashrate && <a href={`${localizePath("/blog/")}track-your-miner-on-bntpool/#zero-hashrate`}>{t("minerHelp")}</a>}
              </div>
              <PayoutProgress
                api={api}
                memoAccount={!!appliedDestination.memo}
                accountMinimum={minerBalanceData?.min_payout_amount}
                confirmed={balanceUnavailable ? undefined : liveBalance?.pending_confirmed}
                queued={pendingQueued}
                liveTick={liveTick}
              />
            </div>
            <dl className="stats-summary-nums">
              <div>
                <dt>{t("m492")}{estimateZero && (
                  <button type="button" className="stats-help" aria-expanded={estimateHelpOpen} aria-controls="estimate-help"
                    aria-label={t('estimateZeroLabel')} title={t('estimateZeroLabel')} onClick={() => setEstimateHelpOpen((open) => !open)}>?</button>
                )}</dt>
                <dd>{balanceValue(pendingEstimated)}</dd>
                {estimateZero && estimateHelpOpen && <p id="estimate-help" className="stats-help-note">{t('estimateZeroHelp')}</p>}
              </div>
              <div><dt>{t("m494")}</dt><dd>{balanceValue(pendingConfirmed)}</dd></div>
              <div><dt>{t("m496")}</dt><dd>{balanceValue(livePaid)}</dd></div>
            </dl>
          </section>

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
                      {soloWorkers && <th>{t("typeColumn")}</th>}
                      <th>{t("m522")}</th>
                      <th>{t("m082")}</th>
                      <th>{t("m085")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {!minerData.workers.length ? (<EmptyTableRow colSpan={soloWorkers ? 5 : 4}>{t("m523")}</EmptyTableRow>) : (minerData.workers.slice(workersStart, workersStart + WORKERS_PAGE_SIZE).map((w) => (
                        <tr key={w.worker}>
                          <td>{w.worker || t("m524")}</td>
                          {soloWorkers && <td><MiningType solo={w.solo} /></td>}
                          <td className="mono">{w.miner_version?.trim() || t("m525")}</td>
                          <td>{humanRate(w.hashrate || 0)}</td>
                          <td title={timestampTitle(w.last_share_at)}>
                            {timeAgo(w.last_share_at)}
                          </td>
                        </tr>
                      )))}
                  </tbody>
                </table>
                {minerData.workers.length > WORKERS_PAGE_SIZE && (<Pager
                    offset={workersStart}
                    limit={WORKERS_PAGE_SIZE}
                    total={minerData.workers.length}
                    onPrev={() => setWorkersOffset(Math.max(0, workersStart - WORKERS_PAGE_SIZE))}
                    onNext={() => setWorkersOffset(workersStart + WORKERS_PAGE_SIZE)}
                  />)}
              </div>
            </div>)}

          {minerData && (<div className="section">
              <h2>{t("m437")}</h2>
              <div className="card table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>{t("m438")}</th>
                      <th>{t("m526")}</th>
                      <th>{t("m006")}</th>
                      <th>{t("m377")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {!minerData.payouts.length ? (<EmptyTableRow colSpan={4}>{t("m440")}</EmptyTableRow>) : (minerData.payouts.slice(payoutsStart, payoutsStart + PAYOUTS_PAGE_SIZE).map((p, payoutIndex) => {
                        const hasTx = Boolean(p.tx_hash.trim());
                        const status = p.confirmed ? 'confirmed' : hasTx ? 'unconfirmed' : 'queued';
                        return (
                          <tr key={`${p.tx_hash}-${toUnixMs(p.timestamp)}-${payoutIndex}`}>
                            <td title={p.fee ? t("m470") + ': ' + formatFee(p.fee) : undefined}>{formatCoins(p.amount)}</td>
                            <td>
                              {hasTx ? (<ExplorerLink kind="tx" value={p.tx_hash}>{p.tx_hash}</ExplorerLink>) : ('-')}
                            </td>
                            <td className="chip-cell">
                              <PayoutStatusBadge status={status} />
                            </td>
                            <td title={timestampTitle(p.timestamp)}>{timeAgo(p.timestamp)}</td>
                          </tr>
                        );
                      }))}
                  </tbody>
                </table>
                {minerData.payouts.length > PAYOUTS_PAGE_SIZE && (<Pager
                    offset={payoutsStart}
                    limit={PAYOUTS_PAGE_SIZE}
                    total={minerData.payouts.length}
                    onPrev={() => setPayoutsOffset(Math.max(0, payoutsStart - PAYOUTS_PAGE_SIZE))}
                    onNext={() => setPayoutsOffset(payoutsStart + PAYOUTS_PAGE_SIZE)}
                  />)}
              </div>
            </div>)}

          {!!previewBlocks.length && (<div className="section">
              <h2>{t("m514")}</h2>
              <div className="card table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>{t("m374")}</th>
                      {soloPreview && <th>{t("typeColumn")}</th>}
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
                        {soloPreview && <td><MiningType solo={b.solo} /></td>}
                        <td title={b.validation_detail || undefined}>
                          {b.credit_withheld ? t("m517") : b.validation_state === 'extra_verification' ? t("m518", [formatCoins(b.estimated_credit)]) : formatCoins(b.estimated_credit)}
                        </td>
                        <td>{b.confirmations_remaining}</td>
                        <td title={timestampTitle(b.timestamp)}>{timeAgo(b.timestamp)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <p className="stats-table-note">{t("m519")}</p>
              </div>
            </div>)}

        </div>)}
    </div>
  );
}
