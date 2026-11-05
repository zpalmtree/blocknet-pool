// Development-only: the real App on My Stats, with synthetic API responses and an
// in-memory stand-in for the miner settings endpoints.
import { createRoot } from 'react-dom/client';
import { App } from '../../src/App';
import { initializeLanguage } from '../../src/i18n';
import { applyTheme } from '../../src/lib/theme';
import type { MinerResponse, MinerSettingsState } from '../../src/types';
import '../../src/styles.css';

const STATES = {
  unclaimed: 'No password yet',
  locked: 'Locked',
  unlocked: 'Unlocked, pool defaults',
  custom: 'Unlocked, 5 BNT minimum, solo, names hidden',
  'no-password': 'Taken back elsewhere, no password yet',
  'public-hidden': 'Stats tab as a visitor sees hidden worker names',
  loading: 'Settings still loading',
  error: 'Settings unavailable',
} as const;
type State = keyof typeof STATES;

const params = new URLSearchParams(location.search);
const state = ((params.get('state') ?? 'locked') in STATES ? params.get('state') : 'locked') as State;
const theme = params.get('theme') === 'dark' ? 'dark' : 'light';
localStorage.setItem('pool_theme', theme);
applyTheme(theme);

const WALLET = 'demo-miner-not-a-real-wallet';
const PASSWORD = 'correct horse';
const RIG_IP = '203.0.113.24';
const COOLING_IP = '192.0.2.1';
localStorage.removeItem('pool_last_miner_lookup_address');
localStorage.setItem('pool_payout_destination_v1', JSON.stringify({ version: 1, address: WALLET, memo: '' }));

const BNT = 100_000_000;
const now = Math.floor(Date.now() / 1000);
const ts = (secondsAgo: number) => ({ secs_since_epoch: now - secondsAgo, nanos_since_epoch: 0 });

// Settings server: same rules as the API, in memory.
const server = {
  claimed: state !== 'unclaimed',
  hasPassword: state !== 'unclaimed' && state !== 'no-password',
  unlocked: state === 'unlocked' || state === 'custom',
  changedAt: now - 3 * 3600,
  minPayout: state === 'custom' ? 5 : 1,
  solo: state === 'custom',
  hide: state === 'custom' || state === 'public-hidden',
  unlockTries: 5,
  takeBackTries: 5,
};

function settingsState(extra: Partial<MinerSettingsState> = {}): MinerSettingsState {
  return {
    account: WALLET,
    claimed: server.claimed,
    has_password: server.hasPassword,
    unlocked: server.unlocked,
    changed_at: server.claimed ? server.changedAt : null,
    pool_min_payout: 1,
    max_min_payout: 100,
    your_ip: RIG_IP,
    settings: server.unlocked ? { min_payout: server.minPayout, default_solo: server.solo, hide_workers: server.hide } : undefined,
    ...extra,
  };
}

const fail = (status: number, error: string, extra: Record<string, unknown> = {}) =>
  Response.json({ error, ...extra }, { status });

function settingsRoute(method: string, action: string, body: Record<string, unknown>): Response | Promise<Response> {
  if (state === 'loading') return new Promise<Response>(() => {});
  if (state === 'error') return fail(500, 'internal');
  const touch = () => { server.changedAt = Math.floor(Date.now() / 1000); };
  if (method === 'GET') return Response.json(settingsState());
  if (action === 'claim') {
    if (server.claimed) return fail(409, 'already_claimed');
    Object.assign(server, { claimed: true, hasPassword: true, unlocked: true });
    touch();
    return Response.json(settingsState());
  }
  if (action === 'unlock') {
    if (server.unlockTries <= 0) return fail(429, 'rate_limited', { retry_after: 2400 });
    if (!server.hasPassword || body.password !== PASSWORD) {
      server.unlockTries -= 1;
      return fail(401, 'wrong_password', { tries_left: server.unlockTries });
    }
    server.unlocked = true;
    return Response.json(settingsState());
  }
  if (action === 'take-back') {
    const ip = String(body.ip ?? '').trim();
    if (!/^[0-9a-f.:/]+$/i.test(ip)) return fail(400, 'invalid_ip');
    if (ip === COOLING_IP) return fail(429, 'cooling_down', { retry_after: 20 * 3600 });
    if (ip !== RIG_IP) {
      server.takeBackTries -= 1;
      return fail(403, 'not_enough_work', { tries_left: server.takeBackTries });
    }
    Object.assign(server, { hasPassword: false, unlocked: true });
    touch();
    return Response.json(settingsState({ share: 0.82 }));
  }
  if (action === 'password') {
    if (!server.unlocked) return fail(401, 'locked');
    server.hasPassword = true;
    touch();
    return Response.json(settingsState());
  }
  if (method === 'PUT') {
    if (!server.unlocked) return fail(401, 'locked');
    Object.assign(server, { minPayout: body.min_payout, solo: body.default_solo, hide: body.hide_workers });
    touch();
    return Response.json(settingsState());
  }
  return fail(404, 'not_found');
}

const info = {
  pool_name: 'bntpool.com', pool_url: 'https://bntpool.com', stratum_port: 3333, pool_fee_pct: 0,
  min_payout_amount: 1, payout_memos_enabled: true, memo_min_payout_amount: 10, payout_interval_seconds: 3600,
  shared_exchange_addresses: [], blocks_before_payout: 60, pplns_window_duration: '6h', hosted_wallet_enabled: false,
};

const minerHistory = Array.from({ length: 60 }, (_, minute) => ({
  timestamp: ts((60 - minute) * 60),
  hashrate: 300 + 14 * Math.sin(minute / 5) + 6 * Math.cos(minute / 2),
}));

const worker = (name: string, rate: number, lastShare: number) => ({
  worker: name, hashrate: rate, accepted: 1_204, rejected: 4, last_share_at: ts(lastShare),
  miner_version: '0.3.1', backend: 'cpu', version_status: 'current' as const, solo: false,
});

function miner(): MinerResponse {
  // A visitor sees numbered rigs when the owner hides names.
  const hidden = server.hide && !server.unlocked;
  return {
    hashrate: 312.4, solo_hashrate: 0, mining_since: ts(86_400 * 22),
    workers: [worker(hidden ? 'Rig 1' : 'laptop', 111.3, 21), worker(hidden ? 'Rig 2' : 'rig-1', 201.1, 8)],
    shares: [], blocks_found: 3, total_accepted: 1_859, total_rejected: 5, latest_miner_version: '0.3.1', verification_hold: null,
    payouts: [{ amount: Math.round(1.84 * BNT), fee: 24_000, tx_hash: 'demo-miner-payout', timestamp: ts(2 * 86_400), confirmed: true }],
  } as MinerResponse;
}

const balance = () => ({
  address: WALLET, min_payout_amount: server.minPayout,
  balance: { pending_confirmed: Math.round(0.42 * BNT), pending_queued: 0, paid: Math.round(36.2 * BNT) },
  pending_estimate: { estimated_pending: Math.round(0.08 * BNT), blocks: [] },
});

const json = (value: unknown) => Response.json(value);
const nativeFetch = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const url = new URL(String(input instanceof Request ? input.url : input), location.href);
  // Never reach a real pool, explorer or analytics endpoint from the showcase.
  if (url.origin !== location.origin) return new Response(null, { status: 204 });
  if (!url.pathname.startsWith('/api/')) return nativeFetch(input, init);
  const path = url.pathname;
  const method = init?.method ?? 'GET';
  if (path.startsWith('/api/miner-settings/')) {
    const action = path.split('/')[4] ?? '';
    const body = init?.body ? JSON.parse(String(init.body)) : {};
    await new Promise((resolve) => setTimeout(resolve, 120));
    return settingsRoute(method, action, body);
  }
  if (path === '/api/info') return json(info);
  if (path === '/api/stats') return json({ pool: { miners: 285, hashrate: 15_030 }, chain: { current_job_height: 71_573 } });
  if (path === '/api/stats/insights') {
    return json({ payout_eta: { next_sweep_at: ts(-600), pending_total_amount: 0, wallet_spendable: 29_686 * BNT, wallet_pending: 0 } });
  }
  if (path.startsWith('/api/miner/')) {
    if (path.endsWith('/balance')) return json(balance());
    if (path.endsWith('/hashrate')) return json(minerHistory);
    return json(miner());
  }
  return json({});
};

const tab = state === 'public-hidden' ? '' : '#settings';
history.replaceState(null, '', `/stats${location.search}${tab}`);

function Bar() {
  return (
    <div lang="en" dir="ltr" style={{ display: 'flex', flexWrap: 'wrap', gap: '8px 18px', alignItems: 'center', padding: '8px 16px', fontSize: 13, background: '#f1f5ee', borderBottom: '1px solid #d8e3d2' }}>
      <span>Miner settings · synthetic data · password <code>{PASSWORD}</code> · rig IP <code>{RIG_IP}</code> (cooling down: <code>{COOLING_IP}</code>)</span>
      <label>State{' '}
        <select value={state} onChange={(event) => {
          params.set('state', event.target.value);
          location.href = `/showcases/miner-settings/?${params}`;
        }}>{Object.entries(STATES).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select>
      </label>
      <label>Theme{' '}
        <select value={theme} onChange={(event) => {
          params.set('theme', event.target.value);
          location.href = `/showcases/miner-settings/?${params}`;
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
