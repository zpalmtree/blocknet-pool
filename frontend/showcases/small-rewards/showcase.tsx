// Development-only: real StatsPage + API client, entirely synthetic responses.
import { useState } from 'react';
import { createRoot } from 'react-dom/client';
import { createApiClient } from '../../src/api/client';
import { initializeLanguage } from '../../src/i18n';
import { StatsPage } from '../../src/pages/StatsPage';
import { LAST_MINER_LOOKUP_KEY } from '../../src/lib/storage';
import type { MinerResponse } from '../../src/types';
import '../../src/styles.css';

const states = ['small', 'one-atom', 'zero', 'no-shares', 'old-shares', 'hold', 'quarantined', 'loading', 'balance-error', 'error', 'empty', 'below-minimum', 'almost-paid', 'minimum-met', 'queued', 'settings-loading', 'settings-error', 'custom-minimum', 'zero-minimum'];
const params = new URLSearchParams(location.search);
const state = states.includes(params.get('state') || '') ? params.get('state')! : 'small';
const theme = params.get('theme') === 'dark' ? 'dark' : 'light';
document.documentElement.dataset.theme = theme;
const address = 'demo-small-miner-not-a-real-wallet';
if (state === 'empty') localStorage.removeItem(LAST_MINER_LOOKUP_KEY);
else localStorage.setItem(LAST_MINER_LOOKUP_KEY, address);
const now = Math.floor(Date.now() / 1000);
const lastShare = now - (state === 'old-shares' ? 86400 : 120);
const accepted = state === 'no-shares' ? 0 : 5;
const held = state === 'hold' || state === 'quarantined';
const reward = state === 'small' ? 1234 : state === 'one-atom' ? 1 : 0;
const confirmed = state === 'below-minimum' ? 379117
  : state === 'almost-paid' ? 99999999
  : state === 'minimum-met' || state === 'queued' ? 100000000
  : state === 'custom-minimum' ? 1 : reward;
const estimated = state === 'below-minimum' ? 73690 : state === 'almost-paid' ? 100000000 : reward;
const miner: MinerResponse = {
  hashrate: 0, mining_since: accepted ? lastShare - 3600 : null,
  blocks_found: 0, total_accepted: accepted, total_rejected: 0, latest_miner_version: '',
  workers: !accepted || state === 'old-shares' ? [] : [{
    worker: 'demo-cpu', version_status: 'current', hashrate: 0, accepted, rejected: 0,
    last_share_at: lastShare, miner_version: '0.2.38', backend: 'cpu',
  }],
  shares: accepted ? [{ job_id: 'demo-job', worker: 'demo-cpu', difficulty: 30, status: 'verified', created_at: lastShare }] : [],
  payouts: [],
  verification_hold: held ? {
    mode: state === 'quarantined' ? 'quarantined' : 'verified_only', reason: 'Demo verification pending',
    started_at: now - 60, verified_only_until: now + 3600,
    quarantined_until: state === 'quarantined' ? now + 3600 : null,
    validation_hold_cause: 'provisional_backlog', validation_pending_provisional: 2,
  } : null,
};
const nativeFetch = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const path = String(input);
  if (!path.startsWith('/api/')) return nativeFetch(input, init);
  if (path === '/api/info') {
    if (state === 'settings-loading') return new Promise<Response>(() => {});
    if (state === 'settings-error') return Response.json({ error: 'Demo: pool settings unavailable' }, { status: 500 });
    return Response.json({ min_payout_amount: state === 'custom-minimum' ? 5.12345678 : state === 'zero-minimum' ? 0 : 1 });
  }
  if (path.startsWith('/api/miner/')) {
    if (state === 'loading') return new Promise<Response>(() => {});
    if (state === 'error' || (state === 'balance-error' && path.includes('/balance'))) {
      return Response.json({ error: 'Demo: lookup unavailable' }, { status: 500 });
    }
    if (path.includes('/hashrate')) return Response.json([]);
    if (path.includes('/balance')) return Response.json({
      address, balance: { pending_confirmed: confirmed, pending_queued: state === 'queued' ? 100000000 : 0, paid: reward },
      pending_estimate: { estimated_pending: estimated, blocks: reward || held ? [{
        height: 123456, hash: 'demo-block', timestamp: now - 60,
        estimated_credit: reward, credit_withheld: state === 'quarantined',
        validation_state: held ? 'extra_verification' : 'normal',
        validation_detail: held ? 'Demo verification pending' : '', confirmations_remaining: 30,
      }] : [] },
    });
    return Response.json(miner);
  }
  return Response.json({ round: {}, payout_eta: null, luck_history: [], rejections: {} });
};

function Showcase() {
  const [error, setError] = useState('');
  const [api] = useState(() => createApiClient(() => '', setError));
  return <>
    <header className="container" style={{ paddingBottom: 0 }} lang="en" dir="ltr">
      <p className="section-lead">Small miner rewards · Synthetic data · <a href="./captures.html">Captures & revision</a></p>
      <label>State{' '}<select value={state} onChange={(event) => {
        params.set('state', event.target.value); location.search = params.toString();
      }}>{states.map(value => <option key={value}>{value}</option>)}</select></label>
    </header>
    <main className="container">
      {error && <p role="alert">{error}</p>}
      <StatsPage api={api} theme={theme} liveTick={0} />
    </main>
  </>;
}

initializeLanguage().then(() => createRoot(document.getElementById('root')!).render(<Showcase />));
