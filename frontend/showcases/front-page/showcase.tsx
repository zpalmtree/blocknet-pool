// Development-only: the real App, front page and Luck page, entirely synthetic API responses.
import { createRoot } from 'react-dom/client';
import { App } from '../../src/App';
import { initializeLanguage } from '../../src/i18n';
import { applyTheme } from '../../src/lib/theme';
import type { BlockItem, CheckpointsResponse, InfoResponse, LuckRound, MinerResponse, PayoutItem } from '../../src/types';
import '../../src/styles.css';

const STATES = {
  first: 'First visit: no saved lookup',
  below: 'Returning miner, below the 1 BNT minimum',
  ready: 'Returning miner, over the minimum',
  queued: 'Returning miner, payout queued',
  paid: 'Returning miner, just paid',
  'no-workers': 'Returning miner, no workers online',
  exchange: 'Exchange deposit, below its 10 BNT minimum',
  'exchange-ready': 'Exchange deposit, over its minimum',
  'miner-error': 'Saved lookup that fails: falls back to the first-visit view',
  loading: 'Pool data still loading',
  empty: 'New pool: no blocks or payouts yet',
  luck: 'Luck page with the Current Round card',
  stats: 'My Stats in the site-wide style',
  blocks: 'Blocks: effort and status columns',
  start: 'Get Started: the quick start steps',
  payouts: 'Payouts: recent pool payouts',
  checkpoints: 'Checkpoints: the published checkpoint file',
} as const;
type State = keyof typeof STATES;

const params = new URLSearchParams(location.search);
const state = (params.get('state') ?? 'first') in STATES ? params.get('state') as State : 'first';
const theme = params.get('theme') === 'dark' ? 'dark' : 'light';
localStorage.setItem('pool_theme', theme);
applyTheme(theme);

const WALLET = 'demo-miner-not-a-real-wallet';
const EXCHANGE = 'demo-shared-exchange-deposit-address';
const exchangeState = state === 'exchange' || state === 'exchange-ready';
localStorage.removeItem('pool_last_miner_lookup_address');
if (state === 'first' || state === 'loading' || state === 'empty' || state === 'luck' || state === 'blocks' || state === 'start' || state === 'payouts' || state === 'checkpoints') {
  localStorage.removeItem('pool_payout_destination_v1');
} else {
  localStorage.setItem('pool_payout_destination_v1', JSON.stringify({
    version: 1, address: exchangeState ? EXCHANGE : WALLET, memo: exchangeState ? 'demo-memo-1234' : '',
  }));
}

const BNT = 100_000_000;
const now = Math.floor(Date.now() / 1000);
const ts = (secondsAgo: number) => ({ secs_since_epoch: now - secondsAgo, nanos_since_epoch: 0 });

const info = {
  pool_name: 'bntpool.com', pool_url: 'https://bntpool.com', stratum_port: 3333, pool_fee_pct: 0,
  min_payout_amount: 1, payout_memos_enabled: true, memo_min_payout_amount: 10,
  shared_exchange_addresses: [{ address: EXCHANGE, name: 'SafeTrade' }],
  blocks_before_payout: 60, pplns_window_duration: '6h', hosted_wallet_enabled: false,
} satisfies InfoResponse;

const stats = {
  pool: { miners: 285, hashrate: 15_030, raw_hashrate: 15_030, blocks_found: 41_153, paid_to_miners_total: 2_325_395_859_240_390 },
  chain: { current_job_height: 71_573, network_hashrate: 54_200 },
};

const luck: LuckRound[] = [
  [71_570, 519.6, 6_197, 720, false, false], [71_552, 4.2, 48, 3_900, false, true], [71_551, 237.4, 2_917, 3_960, false, false],
  [71_546, 10.4, 149, 6_900, false, false], [71_545, 101.5, 1_427, 7_020, false, false], [71_540, 122.3, 1_658, 8_460, false, false],
].map(([height, effort, duration, ago, orphaned, solo]) => ({
  block_height: height as number, block_hash: `demo-block-${height}`, timestamp: ts(ago as number), effort_pct: effort as number,
  duration_seconds: duration as number, orphaned: orphaned as boolean, confirmed: false, solo: solo as boolean,
}));

const blocks: BlockItem[] = [
  [71_587, 75.4, 960, 2_460, 'pending', false], [71_585, 117.6, 1_680, 3_420, 'pending', false],
  [71_580, 175.9, 2_220, 3_900, 'pending', false], [71_570, 519.6, 6_197, 7_200, 'pending', false],
  [71_552, 4.2, 48, 10_800, 'pending', true], [71_551, 237.4, 2_917, 11_000, 'pending', false],
  [71_526, 58.0, 1_140, 21_600, 'confirmed', false], [71_521, 0.9, 15, 25_200, 'confirmed', false],
  [71_504, 1_043.2, 12_610, 28_800, 'confirmed', false], [71_494, 54.8, 780, 29_000, 'confirmed', false],
  [71_472, 6.3, 60, 36_000, 'orphaned', false], [71_471, 173.5, 2_340, 36_100, 'confirmed', false],
].map(([height, effort, duration, ago, status, solo]) => ({
  height: height as number, hash: `demo-block-${height}`, reward: Math.round(439.46 * BNT), timestamp: ts(ago as number),
  effort_pct: effort as number, duration_seconds: duration as number, confirmed: status === 'confirmed',
  orphaned: status === 'orphaned', solo: solo as boolean,
}));

const payouts: PayoutItem[] = [12.45, 16.68, 22.53, 33.43, 60.7].map((amount, index) => ({
  total_amount: Math.round(amount * BNT), total_fee: 285_610, recipient_count: index === 0 ? 12 : 15,
  tx_hashes: [`demo-payout-tx-${index}`], timestamp: ts(2_880 + index * 30), confirmed: true,
} as unknown as PayoutItem));

const checkpoints: CheckpointsResponse = {
  available: true, url: '/checkpoints.dat', entries: 71_500, latest_height: 71_500,
  latest_hash: 'demo-checkpoint-hash-71500', bytes: 2_288_000, sha256: 'demo-sha256-not-a-real-digest', updated_at: ts(1_800),
};

const hashrateHistory = Array.from({ length: 168 }, (_, hour) => ({
  timestamp: ts((168 - hour) * 3_600),
  hashrate: 9_000 + hour * 40 + 1_400 * Math.sin(hour / 9) + (hour > 96 && hour < 101 ? 12_000 : 0),
}));
// One miner's last hour, one point a minute.
const minerHistory = Array.from({ length: 60 }, (_, minute) => ({
  timestamp: ts((60 - minute) * 60),
  hashrate: 300 + 14 * Math.sin(minute / 5) + 6 * Math.cos(minute / 2),
}));

const worker = (name: string, rate: number, lastShare: number) => ({
  worker: name, hashrate: rate, accepted: 1_204, rejected: 4, last_share_at: ts(lastShare),
  miner_version: '0.3.1', backend: 'cpu', version_status: 'current' as const, solo: false,
});
const recentPayout = state === 'paid';
const miner: MinerResponse = {
  hashrate: state === 'no-workers' ? 0 : 312.4, solo_hashrate: 0, mining_since: ts(86_400 * 22),
  workers: state === 'no-workers' ? [] : [worker('rig-1', 201.1, 8), worker('laptop', 111.3, 21)],
  shares: [], blocks_found: 3, total_accepted: 1_859, total_rejected: 5, latest_miner_version: '0.3.1', verification_hold: null,
  payouts: [{ amount: Math.round(1.84 * BNT), fee: 24_000, tx_hash: 'demo-miner-payout', timestamp: ts(recentPayout ? 240 : 2 * 86_400), confirmed: true }],
};
const confirmed = {
  below: 0.42, stats: 0.42, ready: 1.84, queued: 1.84, paid: 0.03, 'no-workers': 0.42, exchange: 4.2, 'exchange-ready': 12.4,
}[state as string] ?? 0;
const balance = {
  address: exchangeState ? 'memo:demo-account' : WALLET, min_payout_amount: exchangeState ? 10 : 1,
  balance: { pending_confirmed: Math.round(confirmed * BNT), pending_queued: state === 'queued' ? Math.round(1.84 * BNT) : 0,
    paid: Math.round((recentPayout ? 38.04 : 36.2) * BNT) },
  pending_estimate: { estimated_pending: Math.round(0.08 * BNT), blocks: [] },
};

const insights = {
  round: { elapsed_seconds: 844, effort_pct: 70.5, expected_block_seconds: 1_082, timer_effort_pct: 78 },
  payout_eta: { next_sweep_at: ts(-600), pending_total_amount: 0, wallet_spendable: 29_686 * BNT, wallet_pending: 5_448 * BNT },
  avg_effort_pct: null, luck_history: state === 'empty' ? [] : luck,
  rejections: { accepted: 0, rejected: 0, by_reason: [], totals_by_reason: [] },
};

const json = (value: unknown) => Response.json(value);
const nativeFetch = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const url = new URL(String(input instanceof Request ? input.url : input), location.href);
  // Never reach a real pool, explorer or analytics endpoint from the showcase.
  if (url.origin !== location.origin) return new Response(null, { status: 204 });
  if (!url.pathname.startsWith('/api/')) return nativeFetch(input, init);
  const path = url.pathname;
  if (state === 'loading' && path !== '/api/info') return new Promise<Response>(() => {});
  if (path === '/api/info') return json(info);
  if (path === '/api/stats') return json(stats);
  if (path === '/api/stats/insights') return json(insights);
  if (path === '/api/stats/history') return json(hashrateHistory);
  if (path === '/api/payouts/recent') return json({ items: state === 'empty' ? [] : payouts, total: payouts.length });
  if (path === '/api/luck') return json({ items: luck, total: luck.length });
  if (path === '/api/blocks') return json({ items: blocks, total: 41_048 });
  if (path === '/api/checkpoints') return json(checkpoints);
  if (path === '/api/payout-destination') return json({ account: 'memo:demo-account', min_payout_amount: 10 });
  if (path.startsWith('/api/miner/')) {
    if (state === 'miner-error') return Response.json({ error: 'Demo: lookup unavailable' }, { status: 500 });
    if (path.endsWith('/balance')) return json(balance);
    if (path.endsWith('/hashrate')) return json(minerHistory);
    return json(miner);
  }
  return json({});
};

// The app routes on the path; the showcase picks it, then App renders as on the live site.
const ROUTES: Partial<Record<State, string>> = { luck: '/luck', stats: '/stats', blocks: '/blocks', start: '/start', payouts: '/payouts', checkpoints: '/checkpoints' };
history.replaceState(null, '', `${ROUTES[state] ?? '/'}${location.search}`);

function Bar() {
  return (
    <div lang="en" dir="ltr" style={{ display: 'flex', flexWrap: 'wrap', gap: '8px 18px', alignItems: 'center', padding: '8px 16px', fontSize: 13, background: '#f1f5ee', borderBottom: '1px solid #d8e3d2' }}>
      <span>Front page · synthetic data · <a href="/showcases/front-page/captures.html">Captures</a></span>
      <label>State{' '}
        <select value={state} onChange={(event) => {
          params.set('state', event.target.value);
          location.href = `/showcases/front-page/?${params}`;
        }}>{Object.entries(STATES).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select>
      </label>
      <label>Theme{' '}
        <select value={theme} onChange={(event) => {
          params.set('theme', event.target.value);
          location.href = `/showcases/front-page/?${params}`;
        }}><option value="light">Light</option><option value="dark">Dark</option></select>
      </label>
    </div>
  );
}

initializeLanguage().then(() => {
  const root = document.getElementById('root')!;
  // Captures: no scrollbar, so a full-page phone capture keeps its full 390 px width.
  if (params.get('chrome') === 'off') document.documentElement.style.scrollbarWidth = 'none';
  if (params.get('chrome') !== 'off') {
    const bar = document.createElement('div');
    root.before(bar);
    createRoot(bar).render(<Bar />);
  }
  createRoot(root).render(<App />);
});
