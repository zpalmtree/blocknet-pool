// Development-only: real StatsPage, BlocksPage and API client, entirely synthetic responses.
import { useState } from 'react';
import { createRoot } from 'react-dom/client';
import { createApiClient } from '../../src/api/client';
import { initializeLanguage } from '../../src/i18n';
import { BlocksPage } from '../../src/pages/BlocksPage';
import { LuckPage } from '../../src/pages/LuckPage';
import { StatsPage } from '../../src/pages/StatsPage';
import { LAST_MINER_LOOKUP_KEY } from '../../src/lib/storage';
import type { BlockItem, LuckRound, MinerResponse } from '../../src/types';
import '../../src/styles.css';

const states = ['solo', 'mixed', 'pplns', 'loading', 'error', 'empty', 'blocks', 'blocks-empty', 'luck'];
const params = new URLSearchParams(location.search);
const state = states.includes(params.get('state') || '') ? params.get('state')! : 'solo';
const theme = params.get('theme') === 'dark' ? 'dark' : 'light';
document.documentElement.dataset.theme = theme;
const address = 'demo-solo-miner-not-a-real-wallet';
localStorage.removeItem('pool_payout_destination_v1');
if (state === 'empty') localStorage.removeItem(LAST_MINER_LOOKUP_KEY);
else localStorage.setItem(LAST_MINER_LOOKUP_KEY, address);

const now = Math.floor(Date.now() / 1000);
const hashrate = 5_200_000;
const soloHashrate = state === 'solo' ? hashrate : state === 'mixed' ? 2_100_000 : 0;
const reward = 34_000_000_000;
const worker = (name: string, rate: number) => ({
  worker: name, version_status: 'current' as const, hashrate: rate, accepted: 412, rejected: 1,
  last_share_at: now - 40, miner_version: '0.3.1', backend: 'cpu',
});
const miner: MinerResponse = {
  hashrate, solo_hashrate: soloHashrate, mining_since: now - 86_400 * 3,
  blocks_found: state === 'pplns' ? 0 : 2, total_accepted: 824, total_rejected: 2, latest_miner_version: '0.3.1',
  workers: state === 'mixed'
    ? [worker('pool-rig', hashrate - soloHashrate), worker('solo-rig', soloHashrate)]
    : [worker('rig-1', hashrate)],
  shares: [{ job_id: 'demo-job', worker: 'rig-1', difficulty: 3200, status: 'verified', created_at: now - 40 }],
  payouts: state === 'pplns' ? [] : [{ amount: reward - 340_000_000, fee: 120_000, tx_hash: 'demo-solo-payout', timestamp: now - 86_400, confirmed: true }],
  verification_hold: null,
};
const soloPending = { height: 71_402, hash: 'demo-solo-block', solo: true, timestamp: now - 1_800,
  estimated_credit: reward - 340_000_000, credit_withheld: false, validation_state: 'ready',
  validation_detail: 'Solo block: its full reward, less the pool fee, goes to the address that found it.',
  confirmations_remaining: 54 };
const pplnsPending = { height: 71_399, hash: 'demo-pplns-block', solo: false, timestamp: now - 2_700,
  estimated_credit: 182_400_000, credit_withheld: false, validation_state: 'ready',
  validation_detail: '41 verified shares and 100.0% verified difficulty in this payout window.',
  confirmations_remaining: 51 };
const pendingBlocks = state === 'solo' ? [soloPending] : state === 'mixed' ? [soloPending, pplnsPending] : [pplnsPending];

const blocks: BlockItem[] = state === 'blocks-empty' ? [] : [
  { height: 71_402, hash: 'demo-a', reward, confirmed: false, orphaned: false, solo: true, timestamp: now - 1_800, effort_pct: 64.2, duration_seconds: 1_920 },
  { height: 71_399, hash: 'demo-b', reward, confirmed: false, orphaned: false, solo: false, timestamp: now - 2_700, effort_pct: 131.8, duration_seconds: 3_940 },
  { height: 71_377, hash: 'demo-c', reward, confirmed: true, orphaned: false, solo: false, timestamp: now - 9_400, effort_pct: 88.5, duration_seconds: 2_650 },
  { height: 71_350, hash: 'demo-d', reward, confirmed: true, orphaned: false, solo: true, timestamp: now - 17_300, effort_pct: 22.9, duration_seconds: 690 },
  { height: 71_331, hash: 'demo-e', reward, confirmed: false, orphaned: true, solo: false, timestamp: now - 23_100, effort_pct: 102.4, duration_seconds: 3_060 },
];

const luck: LuckRound[] = blocks.map((block) => ({
  block_height: block.height, block_hash: block.hash, timestamp: block.timestamp,
  effort_pct: block.effort_pct ?? 0, duration_seconds: block.duration_seconds ?? 0,
  orphaned: block.orphaned, confirmed: block.confirmed, solo: block.solo,
}));

const nativeFetch = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const path = String(input);
  if (!path.startsWith('/api/')) return nativeFetch(input, init);
  if (path === '/api/info') return Response.json({ min_payout_amount: 1, pool_fee_pct: 1 });
  if (path.startsWith('/api/blocks')) return Response.json({ items: blocks, total: blocks.length });
  if (path.startsWith('/api/luck')) return Response.json({ items: luck, total: luck.length });
  if (path.startsWith('/api/miner/')) {
    if (state === 'loading') return new Promise<Response>(() => {});
    if (state === 'error') return Response.json({ error: 'Demo: lookup unavailable' }, { status: 500 });
    if (path.includes('/hashrate')) return Response.json([]);
    if (path.includes('/balance')) return Response.json({
      address, min_payout_amount: 1,
      balance: { pending_confirmed: 0, pending_queued: 0, paid: state === 'pplns' ? 0 : reward - 340_000_000 },
      pending_estimate: {
        estimated_pending: pendingBlocks.reduce((sum, block) => sum + block.estimated_credit, 0),
        blocks: pendingBlocks,
      },
    });
    return Response.json(miner);
  }
  return Response.json({ round: {}, payout_eta: null, luck_history: [], rejections: {} });
};

function Showcase() {
  const [error, setError] = useState('');
  const [api] = useState(() => createApiClient(() => '', setError));
  const blocksPage = state.startsWith('blocks');
  return <>
    <header className="container" style={{ paddingBottom: 0 }} lang="en" dir="ltr">
      <p className="section-lead">Solo mining · Synthetic data · <a href="./captures.html">Captures & revision</a></p>
      <label>State{' '}<select value={state} onChange={(event) => {
        params.set('state', event.target.value); location.search = params.toString();
      }}>{states.map(value => <option key={value}>{value}</option>)}</select></label>
    </header>
    <main className="container">
      {error && <p role="alert">{error}</p>}
      {state === 'luck' ? <LuckPage api={api} liveTick={0} />
        : blocksPage ? <BlocksPage api={api} liveTick={0} />
        : <StatsPage api={api} theme={theme} liveTick={0} />}
    </main>
  </>;
}

initializeLanguage().then(() => createRoot(document.getElementById('root')!).render(<Showcase />));
