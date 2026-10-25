// Development-only entry: uses the real StatsPage and API client with synthetic responses.
import { useState } from 'react';
import { createRoot } from 'react-dom/client';
import { createApiClient } from '../../src/api/client';
import { StatsPage } from '../../src/pages/StatsPage';
import { LAST_MINER_LOOKUP_KEY } from '../../src/lib/storage';
import type { MinerResponse, WorkerVersionStatus } from '../../src/types';
import '../../src/styles.css';

const states = ['outdated', 'mixed', 'unknown', 'current', 'no-workers', 'no-version', 'loading', 'error', 'empty', 'long-version'];
const params = new URLSearchParams(location.search);
const state = states.includes(params.get('state') || '') ? params.get('state')! : 'outdated';
const theme = params.get('theme') === 'dark' ? 'dark' : 'light';
document.documentElement.dataset.theme = theme;
const address = 'demo-worker-address-not-a-real-wallet';
if (state === 'empty') localStorage.removeItem(LAST_MINER_LOOKUP_KEY);
else localStorage.setItem(LAST_MINER_LOOKUP_KEY, address);

const statuses: WorkerVersionStatus[] = state === 'mixed' ? ['outdated', 'outdated', 'unknown', 'current']
  : state === 'unknown' ? ['unknown'] : state === 'current' ? ['current']
  : state === 'no-workers' ? [] : ['outdated'];
const miner: MinerResponse = {
  hashrate: 1250, mining_since: null, blocks_found: 3, total_accepted: 12840, total_rejected: 2,
  latest_miner_version: state === 'no-version' ? '  ' : state === 'long-version' ? '0.2.23-preview.20261025-experimental' : '0.2.23',
  workers: statuses.map((version_status, i) => ({
    worker: `demo-rig-${i + 1}`, version_status, hashrate: 1250, accepted: 12840, rejected: 2,
    last_share_at: null,
    miner_version: version_status === 'unknown' ? null : version_status === 'current' ? '0.2.23' : '0.2.22',
    backend: 'cuda',
  })),
  shares: [], payouts: [], verification_hold: null,
};
const nativeFetch = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const url = String(input);
  if (!url.startsWith('/api/')) return nativeFetch(input, init);
  if (url.startsWith('/api/miner/')) {
    if (state === 'loading') return new Promise<Response>(() => {});
    if (state === 'error') return Response.json({ error: 'Demo: miner lookup unavailable' }, { status: 500 });
    if (url.includes('/hashrate')) return Response.json([]);
    if (url.includes('/balance')) return Response.json({
      address, balance: { pending_confirmed: 240000000, pending_queued: 0, paid: 720000000 },
      pending_estimate: { estimated_pending: 120000000, blocks: [] },
    });
    return Response.json(miner);
  }
  return Response.json({ round: {}, payout_eta: null, luck_history: [], rejections: {} });
};

function Showcase() {
  const [error, setError] = useState('');
  const [api] = useState(() => createApiClient(() => '', setError));
  return (
    <>
      <header className="container" style={{ paddingBottom: 0 }}>
        <p style={{ color: 'var(--muted)', fontSize: 12, marginBottom: 12 }}>
          Feature showcase · Synthetic data · {state} · {theme} · <a href="./captures.html">Reviewed captures & revision</a>
        </p>
        <label>State{' '}
          <select value={state} onChange={(event) => { params.set('state', event.target.value); location.search = params.toString(); }}>
            {states.map((value) => <option key={value}>{value}</option>)}
          </select>
        </label>{' '}
        <a href={`?state=${state}&theme=${theme === 'dark' ? 'light' : 'dark'}`}>Switch theme</a>
      </header>
      <main className="container">
        {error && <p role="alert" style={{ color: 'var(--bad)', marginBottom: 16 }}>{error}</p>}
        <StatsPage api={api} theme={theme} liveTick={0} />
      </main>
    </>
  );
}

createRoot(document.getElementById('root')!).render(<Showcase />);
