// Development-only: real StatsPage + API client, entirely synthetic responses.
import { useState } from 'react';
import { createRoot } from 'react-dom/client';
import { createApiClient } from '../../src/api/client';
import { initializeLanguage } from '../../src/i18n';
import { StatsPage } from '../../src/pages/StatsPage';
import { rememberDestination } from '../../src/lib/storage';
import { StartPage } from '../../src/pages/StartPage';
import type { InfoResponse } from '../../src/types';
import type { MinerResponse } from '../../src/types';
import '../../src/styles.css';

const states = ['regular', 'required', 'success', 'empty', 'loading', 'error', 'invalid', 'unknown', 'setup', 'disabled', 'rotated'];
const params = new URLSearchParams(location.search);
const state = states.includes(params.get('state') || '') ? params.get('state')! : 'success';
const theme = params.get('theme') === 'light' ? 'light' : 'dark';
document.documentElement.dataset.theme = theme;
const sharedAddress = 'ZJZqGfDPCVg78CRH5Z33CtHafg911Dp5Bauf1Dnxg5jgXx7Whqsvqka96FCV8W6QZXNDKzJefkFU6bwDEGBewqPrTWjij';
const rotatedAddress = '2'.repeat(90);
const address = state === 'rotated' ? rotatedAddress : state === 'regular' || state === 'unknown' ? '1'.repeat(90) : sharedAddress;
const memo = state === 'invalid' ? 'é'.repeat(63) : ['regular', 'required', 'empty', 'setup', 'disabled', 'rotated'].includes(state) ? '' : 'DEMO001';
rememberDestination(state === 'empty' ? null : { address, memo });
const info = {
  min_payout_amount: 0.1, memo_min_payout_amount: 10, payout_memos_enabled: state !== 'disabled',
  pool_name: 'Demo pool', pool_url: 'stratum+tcp://demo.invalid:3333', stratum_port: 3333,
  pool_fee_pct: 0, blocks_before_payout: 60, pplns_window_duration: '24h',
  shared_exchange_addresses: [{ address: state === 'rotated' ? rotatedAddress : sharedAddress, name: 'SafeTrade' }],
} as InfoResponse;
const now = Math.floor(Date.now() / 1000);
const lastShare = now - 120;
const accepted = 5;
const held = false;
const reward = 1250000000;
const confirmed = reward;
const estimated = 250000000;
const miner: MinerResponse = {
  hashrate: 0, mining_since: accepted ? lastShare - 3600 : null,
  blocks_found: 0, total_accepted: accepted, total_rejected: 0, latest_miner_version: '',
  workers: !accepted ? [] : [{
    worker: 'demo-cpu', version_status: 'current', hashrate: 0, accepted, rejected: 0,
    last_share_at: lastShare, miner_version: '0.2.38', backend: 'cpu',
  }],
  shares: accepted ? [{ job_id: 'demo-job', worker: 'demo-cpu', difficulty: 30, status: 'verified', created_at: lastShare }] : [],
  payouts: [],
  verification_hold: held ? {
    mode: 'verified_only', reason: 'Demo verification pending',
    started_at: now - 60, verified_only_until: now + 3600,
    quarantined_until: null,
    validation_hold_cause: 'provisional_backlog', validation_pending_provisional: 2,
  } : null,
};
const nativeFetch = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const path = String(input);
  if (!path.startsWith('/api/')) return nativeFetch(input, init);
  if (path === '/api/info') return Response.json(info);
  if (path === '/api/payout-destination') {
    const request = JSON.parse(String(init?.body));
    return Response.json({ account: `memo:fixture-${request.memo}`, min_payout_amount: 10 });
  }
  if (path.startsWith('/api/miner/')) {
    if (state === 'loading') return new Promise<Response>(() => {});
    if (state === 'error') {
      return Response.json({ error: 'Demo: lookup unavailable' }, { status: 500 });
    }
    const secondCustomer = decodeURIComponent(path.split('/')[3]).endsWith('DEMO002');
    if (path.includes('/hashrate')) return Response.json([]);
    if (path.includes('/balance')) return Response.json({
      address: decodeURIComponent(path.split('/')[3]), min_payout_amount: path.includes('memo') ? 10 : 0.1, balance: { pending_confirmed: secondCustomer ? 3450000000 : confirmed, pending_queued: 0, paid: reward },
      pending_estimate: { estimated_pending: estimated, blocks: reward || held ? [{
        height: 123456, hash: 'demo-block', timestamp: now - 60,
        estimated_credit: reward, credit_withheld: false,
        validation_state: held ? 'extra_verification' : 'normal',
        validation_detail: held ? 'Demo verification pending' : '', confirmations_remaining: 30,
      }] : [] },
    });
    return Response.json({ ...miner, workers: miner.workers.map(worker => ({ ...worker, worker: secondCustomer ? 'customer-two' : 'customer-one' })) });
  }
  return Response.json({ round: {}, payout_eta: null, luck_history: [], rejections: {} });
};

function Showcase() {
  const [error, setError] = useState('');
  const [api] = useState(() => createApiClient(() => '', setError));
  return <>
    <header className="container" style={{ paddingBottom: 0 }} lang="en" dir="ltr">
      <p className="section-lead">Exchange memos · Synthetic data · <a href="./captures.html">Captures & revision</a></p>
      <label>State{' '}<select value={state} onChange={(event) => {
        params.set('state', event.target.value); location.search = params.toString();
      }}>{states.map(value => <option key={value}>{value}</option>)}</select></label>
    </header>
    <main className="container">
      {error && <p role="alert">{error}</p>}
      {state === 'setup' || state === 'disabled' ? <StartPage poolInfo={info} /> : <StatsPage api={api} theme={theme} liveTick={0} />}
    </main>
  </>;
}

initializeLanguage().then(() => createRoot(document.getElementById('root')!).render(<Showcase />));
