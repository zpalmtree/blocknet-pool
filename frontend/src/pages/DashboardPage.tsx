import { t, tx, localizePath, locale } from '../i18n';
import { useCallback, useEffect, useState } from 'react';

import type { ApiClient } from '../api/client';
import { ExplorerLink } from '../components/ExplorerLink';
import { HashrateChart } from '../components/HashrateChart';
import { MiningType, POOL_ICON } from '../components/MiningType';
import { PoolGuide } from '../components/PoolGuide';
import { RangeTabs } from '../components/RangeTabs';
import {
  formatCoinAmount,
  formatCoins,
  formatCompactCoins,
  humanRate,
  shortAddr,
  stratumUrl,
  timestampTitle,
  timeAgo,
  toUnixMs,
} from '../lib/format';
import { knownExchange } from '../lib/payout-destination';
import { heroPayout, minimumPayoutAtoms } from '../lib/payout-progress';
import { readDestination, rememberDestination, type SavedDestination } from '../lib/storage';
import type { ThemeMode } from '../lib/theme';
import type {
  HashratePoint,
  InfoResponse,
  MinerBalancePayload,
  MinerResponse,
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
  onJumpToStats: (address: string, memo?: string) => void;
}

const RECENT_PAYOUT_MS = 60 * 60 * 1000;

interface ReturningMiner {
  status: 'loading' | 'ready' | 'failed';
  miner: MinerResponse | null;
  balance: MinerBalancePayload | null;
}

/** Loads the miner the visitor last looked up, so the front page can open on it. */
function useReturningMiner(api: ApiClient, destination: SavedDestination, liveTick: number): ReturningMiner {
  const [state, setState] = useState<ReturningMiner>({ status: destination.address ? 'loading' : 'failed', miner: null, balance: null });
  const refresh = Math.floor(liveTick / 6);
  useEffect(() => {
    if (!destination.address) {
      setState({ status: 'failed', miner: null, balance: null });
      return;
    }
    let active = true;
    void (async () => {
      try {
        // Exchange deposits share one address; the memo picks the account.
        const account = destination.memo
          ? (await api.resolvePayoutDestination(destination.address, destination.memo)).account
          : destination.address;
        const [miner, balance] = await Promise.allSettled([api.getMiner(account), api.getMinerBalance(account, false)]);
        if (!active) return;
        // Only well-formed responses count; anything else falls back to the first-visit view.
        const minerValue = miner.status === 'fulfilled' && Array.isArray(miner.value?.workers) ? miner.value : null;
        const balanceValue = balance.status === 'fulfilled' && typeof balance.value?.balance?.pending_confirmed === 'number'
          ? balance.value : null;
        setState((previous) => minerValue || balanceValue
          ? { status: 'ready', miner: minerValue, balance: balanceValue }
          // Keep the last good view through a failed refresh.
          : previous.status === 'ready' ? previous : { status: 'failed', miner: null, balance: null });
      } catch {
        if (active) setState((previous) => previous.status === 'ready' ? previous : { status: 'failed', miner: null, balance: null });
      }
    })();
    return () => { active = false; };
  }, [api, destination, refresh]);
  return state;
}

export function DashboardPage({ api, poolInfo, liveTick, theme, onJumpToStats }: DashboardPageProps) {
  const [destination, setDestination] = useState<SavedDestination>(readDestination);
  const [lookupInput, setLookupInput] = useState(destination.address);
  const [copyState, setCopyState] = useState<'idle' | 'copied' | 'error'>('idle');
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [insights, setInsights] = useState<StatsInsightsResponse | null>(null);
  const [payouts, setPayouts] = useState<PayoutItem[]>([]);
  const [range, setRange] = useState<Range>('7d');
  const [historyResult, setHistoryResult] = useState<{ range: Range; data: HashratePoint[] }>({ range, data: [] });
  const history = historyResult.range === range ? historyResult.data : [];
  const historyTick = Math.floor(liveTick / 6);
  const returning = useReturningMiner(api, destination, liveTick);
  const showMiner = !!destination.address && returning.status !== 'failed';

  const refreshStats = useCallback(async () => {
    try {
      setStats(await api.getStats());
    } catch {
      // handled by api client
    }
  }, [api]);

  const loadInsights = useCallback(async () => {
    try {
      setInsights(await api.getStatsInsights());
    } catch {
      setInsights(null);
    }
  }, [api]);

  const loadPayouts = useCallback(async () => {
    try {
      setPayouts((await api.getRecentPayouts(5, 0)).items);
    } catch {
      setPayouts([]);
    }
  }, [api]);

  useEffect(() => {
    void refreshStats();
    void loadInsights();
    void loadPayouts();
  }, [loadInsights, loadPayouts, refreshStats]);

  useEffect(() => {
    if (liveTick <= 0) return;
    if (liveTick % 2 === 0) void refreshStats();
    if (liveTick % 6 === 0) {
      void loadInsights();
      void loadPayouts();
    }
  }, [liveTick, loadInsights, loadPayouts, refreshStats]);

  useEffect(() => {
    let active = true;
    void api.getStatsHistory(range).then((data) => {
      if (active) setHistoryResult({ range, data });
    }).catch(() => {
      if (active) setHistoryResult({ range, data: [] });
    });
    return () => { active = false; };
  }, [api, range, historyTick]);

  useEffect(() => {
    if (copyState !== 'copied') return;
    const timer = window.setTimeout(() => setCopyState('idle'), 2000);
    return () => window.clearTimeout(timer);
  }, [copyState]);

  const poolUrl = poolInfo ? stratumUrl(poolInfo.stratum_port, poolInfo.pool_url) : null;
  const copyStratum = useCallback(async () => {
    if (!poolUrl) return;
    try {
      await navigator.clipboard.writeText(poolUrl);
      setCopyState('copied');
    } catch {
      setCopyState('error');
    }
  }, [poolUrl]);

  const forgetMiner = useCallback(() => {
    rememberDestination(null);
    setDestination({ address: '', memo: '' });
    setLookupInput('');
  }, []);

  const fee = poolInfo?.pool_fee_pct;
  const feeLabel = fee != null && Number.isFinite(fee) ? `${fee.toLocaleString(locale, { maximumFractionDigits: 20 })}%` : '-';

  const poolHashrate = stats?.pool?.raw_hashrate ?? stats?.pool?.hashrate ?? 0;
  const networkHashrate = stats?.chain.network_hashrate ?? 0;
  const networkShare = networkHashrate > 0 && poolHashrate > 0
    ? new Intl.NumberFormat(locale, { style: 'percent', maximumFractionDigits: 0 }).format(poolHashrate / networkHashrate) : '-';
  const paidTotal = stats?.pool?.paid_to_miners_total;

  const payoutEta = insights?.payout_eta;
  const recentBlocks = (insights?.luck_history ?? []).filter((row) => !row.orphaned).slice(0, 5);
  const payoutShortfall = payoutEta?.wallet_spendable == null ? 0 : Math.max(0, payoutEta.pending_total_amount - payoutEta.wallet_spendable);
  const payoutLiquidityConstrained = payoutEta != null && payoutEta.pending_total_amount > 0 && payoutShortfall > 0;

  return (
    <div id="page-dashboard" className="home">
      <section className="home-hero">
        <div className="home-hero-body">
          {showMiner ? (
            <MinerHero
              miner={returning.miner}
              balance={returning.balance}
              loading={returning.status === 'loading'}
              destination={destination}
              poolInfo={poolInfo}
              onForget={forgetMiner}
            />
          ) : (
            <div className="home-pitch">
              <h1 className="home-title">
                <span className="home-kicker">{t('m399')}</span>
                <span className="home-fee">{t('heroFee', [feeLabel])}</span>
              </h1>
              <div className="home-cta">
                <a className="home-btn" href={localizePath('/start')}>{t('m002')}</a>
                <a className="home-link" href={localizePath('/blog/') + 'estimate-blocknet-mining-rewards/#mining-calculator'}>
                  {t('rewardsCalculator')}<span aria-hidden="true"> ↗</span>
                </a>
              </div>
            </div>
          )}
          <div className="home-connect">
            <span className="home-label">{t('m402')}</span>
            <div className="home-field">
              <code id="stratum-url" dir="ltr">{poolUrl ?? '-'}</code>
              <button type="button" onClick={copyStratum} disabled={!poolUrl} aria-label={t('m387')}>
                <span role="status">{copyState === 'copied' ? t('m386') : t('m392')}</span>
              </button>
            </div>
            {copyState === 'error' && <p className="home-copy-error" role="alert">{t('copyUnavailable')}</p>}
            {showMiner && <p className="home-fee-row"><span>{t('poolFee')}</span><strong dir="ltr">{feeLabel}</strong></p>}
            {!showMiner && <>
            <label className="home-label" htmlFor="dashboard-lookup">{t('checkMiner')}</label>
            <form
              className="home-field"
              onSubmit={(event) => {
                event.preventDefault();
                const value = lookupInput.trim();
                // Keep an exchange deposit's memo when the address is unchanged.
                if (value) onJumpToStats(value, value === destination.address ? destination.memo : '');
              }}
            >
              <input
                id="dashboard-lookup"
                type="text"
                dir="ltr"
                autoComplete="off"
                spellCheck={false}
                placeholder={t('m486')}
                value={lookupInput}
                onChange={(event) => setLookupInput(event.target.value)}
              />
              <button type="submit">{t('m488')}</button>
            </form>
            </>}
          </div>
        </div>
      </section>

      <section className="home-band" aria-label={t('m403')}>
        <div><span className="home-num">{stats?.pool?.miners ?? '-'}</span><span className="home-cap">{t('bandMiners')}</span></div>
        <div>
          <span className="home-num" dir="ltr">{stats ? humanRate(poolHashrate) : '-'}</span>
          <span className="home-cap">{tx('bandHashrate', [<bdi key="share">{networkShare}</bdi>])}</span>
        </div>
        <div>
          <span className="home-num" dir="ltr">{networkHashrate > 0 ? humanRate(networkHashrate) : '-'}</span>
          <span className="home-cap">{t('bandNetwork')}</span>
        </div>
        <div>
          <span className="home-num" title={paidTotal != null ? formatCoins(paidTotal) : undefined}>{paidTotal != null ? formatCompactCoins(paidTotal) : '-'}</span>
          <span className="home-cap">{t('bandPaid')}</span>
        </div>
      </section>

      <div className="home-body">
        {!showMiner && (
          <section aria-labelledby="home-steps-title">
            <div className="home-head"><h2 id="home-steps-title">{t('m450')}</h2><a href={localizePath('/start')}>{t('setupGuide')}</a></div>
            <ol className="home-steps">
              <li><b aria-hidden="true">1</b><div><strong>{t('walletStep')}</strong>
                <span>{tx('walletStepBody', [<a key="wallet" href={`${localizePath('/start')}#wallet`}>{t('walletStepLink')}</a>])}</span></div></li>
              <li><b aria-hidden="true">2</b><div><strong>{t('m452')}</strong>
                <span>{tx('m453', [<a key="seine" href="https://seineminer.com/#download">seineminer.com</a>])}</span></div></li>
              <li><b aria-hidden="true">3</b><div><strong>{t('m454')}</strong>
                <span>{tx('launchBody', [<code key="exe" dir="ltr">seine.exe</code>, <code key="bin" dir="ltr">./seine</code>, <code key="at" dir="ltr">@name</code>, <code key="dollar" dir="ltr">$name</code>])}</span></div></li>
            </ol>
          </section>
        )}

        {payoutLiquidityConstrained && payoutEta && (
          <div className="card home-notice">
            {tx("m429", [<span className="mono" key="a">{formatCoins(payoutEta.wallet_spendable ?? 0)}</span>, <span className="mono" key="b">{formatCoins(payoutEta.wallet_pending ?? 0)}</span>, <span className="mono" key="c">{formatCoins(payoutEta.pending_total_amount)}</span>, <span className="mono" key="d">{formatCoins(payoutShortfall)}</span>])}
          </div>
        )}

        <div className="home-feeds">
          <section aria-labelledby="home-blocks-title">
            <div className="home-head"><h2 id="home-blocks-title">{t('recentBlocks')}</h2><a href={localizePath('/luck')}>{t('m431')}</a></div>
            <ol className="home-list">
              {!recentBlocks.length ? <li className="home-empty">{t('m436')}</li> : recentBlocks.map((row) => (
                <li key={`${row.block_height}-${row.block_hash}`}>
                  <ExplorerLink kind="block" value={row.block_hash}>{row.block_height}</ExplorerLink>
                  <MiningType solo={row.solo} />
                  <span className="home-dim" title={timestampTitle(row.timestamp)}>{timeAgo(row.timestamp)}</span>
                </li>
              ))}
            </ol>
          </section>
          <section aria-labelledby="home-payouts-title">
            <div className="home-head"><h2 id="home-payouts-title">{t('m437')}</h2><a href={localizePath('/payouts')}>{t('m431')}</a></div>
            <ol className="home-list">
              {!payouts.length ? <li className="home-empty">{t('m440')}</li> : payouts.map((payout, index) => (
                <li key={`${toUnixMs(payout.timestamp)}-${index}`}>
                  {payout.tx_hashes[0]
                    ? <ExplorerLink kind="tx" value={payout.tx_hashes[0]}>{formatCoins(payout.total_amount)}</ExplorerLink>
                    : <span>{formatCoins(payout.total_amount)}</span>}
                  <span className="home-recipients" title={t('m439')}>{POOL_ICON}<span className="sr-only">{t('m439')}: </span>{payout.recipient_count}</span>
                  <span className="home-dim" title={timestampTitle(payout.timestamp)}>{timeAgo(payout.timestamp)}</span>
                </li>
              ))}
            </ol>
          </section>
        </div>

        <section aria-labelledby="home-chart-title">
          <div className="home-head">
            <h2 id="home-chart-title">{t('m404')}</h2>
            <RangeTabs<Range> id="hashrate-ranges" options={['1h', '24h', '7d', '30d']} value={range} onChange={setRange} />
          </div>
          <HashrateChart data={history} range={range} theme={theme} />
        </section>

        <PoolGuide t={t} localizePath={localizePath} />
      </div>
    </div>
  );
}

interface MinerHeroProps {
  miner: MinerResponse | null;
  balance: MinerBalancePayload | null;
  loading: boolean;
  destination: SavedDestination;
  poolInfo: InfoResponse | null;
  onForget: () => void;
}

function MinerHero({ miner, balance, loading, destination, poolInfo, onForget }: MinerHeroProps) {
  const exchange = knownExchange(poolInfo, destination.address);
  const memoAccount = !!destination.memo;
  const who = exchange ? t('exchangeDeposit', [exchange.name]) : shortAddr(destination.address);
  const workers = miner?.workers ?? [];
  // Exchange (memo) accounts have their own, higher minimum; the balance API reports it.
  const accountMinimum = balance?.min_payout_amount ?? (memoAccount ? poolInfo?.memo_min_payout_amount : poolInfo?.min_payout_amount);
  const minimum = minimumPayoutAtoms(accountMinimum);
  const payout = balance ? heroPayout(minimum, balance.balance.pending_confirmed, balance.balance.pending_queued) : null;
  const lastPayout = miner?.payouts?.[0];
  const recentPayout = lastPayout && Date.now() - toUnixMs(lastPayout.timestamp) < RECENT_PAYOUT_MS ? lastPayout : null;

  return (
    <div className="home-pitch home-miner" aria-busy={loading}>
      <p className="home-me-kicker">
        <span>{t('yourMiner')} · <bdi dir="ltr">{who}</bdi></span>
        <button type="button" className="home-link-button" onClick={onForget}>{t('notYou')}</button>
      </p>
      <h1 className="home-me-hr" dir="ltr">{miner ? humanRate(miner.hashrate) : '-'}</h1>
      {miner && !workers.length && (
        <p className="home-me-sub">
          {tx('noWorkersHelp', [<a key="help" href={`${localizePath('/blog/')}track-your-miner-on-bntpool/#zero-hashrate`}>{t('minerHelp')}</a>])}
        </p>
      )}

      {payout && payout.state !== 'unavailable' && (
        <div className="home-pay">
          {payout.state === 'below' && <>
            <div className="home-pay-top"><span>{t('nextPayout')}</span>
              <span>{tx('payoutOf', [<strong key="have" dir="ltr">{formatCoinAmount(payout.confirmed)}</strong>, <bdi key="min" dir="ltr">{formatCoins(payout.minimum)}</bdi>])}</span></div>
            <div className="home-bar"><div style={{ width: `${payout.fillPct}%` }} /></div>
            {recentPayout && <span className="home-pay-sub">{t('lastPayout', [formatCoins(recentPayout.amount), timeAgo(recentPayout.timestamp)])}</span>}
          </>}
          {payout.state === 'ready' && <>
            <div className="home-pay-top"><span className="home-tag"><i aria-hidden="true" />{t('payoutReady')}</span>
              <strong dir="ltr">{formatCoins(payout.amount)}</strong></div>
            <div className="home-bar"><div style={{ width: '100%' }} /></div>
          </>}
          {payout.state === 'queued' && <>
            <div className="home-pay-top"><span className="home-tag"><i aria-hidden="true" />{t('payoutQueuedTag')}</span>
              <strong dir="ltr">{formatCoins(payout.amount)}</strong></div>
            <div className="home-bar"><div className="is-queued" style={{ width: '100%' }} /></div>
          </>}
        </div>
      )}
      {balance && payout?.state === 'unavailable' && <p className="home-me-sub">{t('statsPayoutBalanceUnavailable')}</p>}

      <div className="home-cta">
        <a className="home-btn" href={localizePath('/stats')}>{t('fullStats')}</a>
      </div>
    </div>
  );
}
