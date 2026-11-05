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
  exchange: 'Exchange account: 10 BNT minimum, can be raised',
  'alerts-pending': 'Alert email waiting for its link',
  'alerts-on': 'Alert email confirmed, alerts on',
  'link-confirm': 'Confirmation link opened (/alerts)',
  'link-unsubscribe': 'Unsubscribe link opened (/alerts)',
  'link-expired': 'Expired confirmation link (/alerts)',
  'difficulty-set': 'Starting difficulty set to 50,000',
  'private-on': 'Unlocked, stats private',
  'public-private': 'Stats tab as a visitor sees private stats',
  'alerts-all': 'Email, Discord and Telegram connected, all alerts on',
  'discord-only': 'Discord only, no email',
  'telegram-pending': 'Telegram waiting for Start in the bot',
  'no-password': 'Taken back elsewhere, no password yet',
  'public-hidden': 'Stats tab as a visitor sees hidden worker names',
  loading: 'Settings still loading',
  error: 'Settings unavailable',
} as const;
type State = keyof typeof STATES;

const params = new URLSearchParams(location.search);
const state = ((params.get('state') ?? 'locked') in STATES ? params.get('state') : 'locked') as State;
const theme = params.get('theme') === 'dark' ? 'dark' : 'light';
// `save=slow` holds each settings save for 4 s; `save=fail` ends the session on the first save.
const saveMode = params.get('save');
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
  unlocked: ['unlocked', 'custom', 'exchange', 'alerts-pending', 'alerts-on', 'difficulty-set', 'private-on', 'alerts-all',
    'discord-only', 'telegram-pending'].includes(state),
  changedAt: now - 3 * 3600,
  minPayout: state === 'custom' ? 5 : state === 'exchange' ? 10 : 1,
  solo: state === 'custom',
  hide: state === 'custom' || state === 'public-hidden',
  unlockTries: 5,
  takeBackTries: 5,
  privateStats: state === 'private-on' || state === 'public-private',
  startDifficulty: state === 'difficulty-set' ? 50_000 : null as number | null,
  alertEmail: ['alerts-pending', 'alerts-on', 'alerts-all', 'telegram-pending'].includes(state) ? 'miner@example.com' : '',
  alertConfirmed: ['alerts-on', 'alerts-all', 'telegram-pending'].includes(state),
  discord: ['alerts-all', 'discord-only'].includes(state) ? 'discord.com/api/webhooks/1204…3qXw' : null as string | null,
  telegram: (state === 'alerts-all' ? { name: '@rigfarm_ops' } : state === 'telegram-pending'
    ? { pending: 'https://t.me/bntpool_alerts_bot?start=demo' } : null) as { name: string } | { pending: string } | null,
  alertChoices: state === 'alerts-all'
    ? { offline_minutes: 10, notify_payouts: true, notify_solo_blocks: true, hashrate_drop_minutes: 60, notify_updates: true, weekly_summary: true }
    : { offline_minutes: 30, notify_payouts: true, notify_solo_blocks: false, hashrate_drop_minutes: 0, notify_updates: false, weekly_summary: false },
};

function settingsState(extra: Partial<MinerSettingsState> = {}): MinerSettingsState {
  return {
    account: WALLET,
    claimed: server.claimed,
    has_password: server.hasPassword,
    unlocked: server.unlocked,
    changed_at: server.claimed ? server.changedAt : null,
    pool_min_payout: state === 'exchange' ? 10 : 1,
    default_min_payout: state === 'exchange' ? 10 : 1,
    max_min_payout: 10_000,
    your_ip: RIG_IP,
    settings: server.unlocked ? {
      min_payout: server.minPayout, default_solo: server.solo, hide_workers: server.hide,
      private_stats: server.privateStats, start_difficulty: server.startDifficulty,
    } : undefined,
    difficulty: { min: 60, max: 1_000_000_000, current: 9_400 },
    alerts_available: true,
    telegram_available: true,
    alerts: server.unlocked
      ? server.alertEmail || server.discord || server.telegram
        ? { email: server.alertEmail, confirmed: server.alertConfirmed, discord: server.discord, telegram: server.telegram, ...server.alertChoices }
        : null
      : undefined,
    ...extra,
  };
}

const fail = (status: number, error: string, extra: Record<string, unknown> = {}) =>
  Response.json({ error, ...extra }, { status });

let path4 = '';
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
  if (action === 'sign-out') {
    server.unlocked = false;
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
  if (action === 'alert-email') {
    if (!server.unlocked) return fail(401, 'locked');
    if (path4 === 'remove') {
      Object.assign(server, { alertEmail: '', alertConfirmed: false });
      return Response.json(settingsState());
    }
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(String(body.email ?? ''))) return fail(400, 'invalid_email');
    Object.assign(server, { alertEmail: String(body.email), alertConfirmed: false });
    return Response.json(settingsState());
  }
  if (action === 'discord') {
    if (!server.unlocked) return fail(401, 'locked');
    if (path4 === 'remove') server.discord = null;
    else server.discord = String(body.webhook).replace(/^https:\/\//, '').replace(/\/(\d{4})\d*\/([\w-]*)([\w-]{4})$/, '/$1…$3');
    return Response.json(settingsState());
  }
  if (action === 'telegram') {
    if (!server.unlocked) return fail(401, 'locked');
    server.telegram = path4 === 'remove' ? null : { pending: 'https://t.me/bntpool_alerts_bot?start=demo' };
    return Response.json(settingsState());
  }
  if (method === 'PUT') {
    if (!server.unlocked) return fail(401, 'locked');
    if (saveMode === 'fail') {
      server.unlocked = false;
      return fail(401, 'locked');
    }
    if (saveMode === 'slow') {
      return new Promise<Response>((resolve) => setTimeout(() => {
        Object.assign(server, { minPayout: body.min_payout, solo: body.default_solo, hide: body.hide_workers });
        touch();
        resolve(Response.json(settingsState()));
      }, 4000));
    }
    Object.assign(server, {
      minPayout: body.min_payout, solo: body.default_solo, hide: body.hide_workers,
      privateStats: body.private_stats ?? server.privateStats,
      startDifficulty: body.start_difficulty === undefined ? server.startDifficulty : body.start_difficulty,
    });
    if (body.alerts && (server.alertEmail || server.discord || server.telegram)) server.alertChoices = body.alerts;
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
  if (server.privateStats && !server.unlocked) {
    return { private_stats: true, hashrate: 0, mining_since: ts(0), workers: [], shares: [], blocks_found: 0,
      total_accepted: 0, total_rejected: 0, payouts: [] } as unknown as MinerResponse;
  }
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
    path4 = path.split('/')[5] ?? '';
    const body = init?.body ? JSON.parse(String(init.body)) : {};
    await new Promise((resolve) => setTimeout(resolve, 120));
    return settingsRoute(method, action, body);
  }
  if (path === '/api/alerts/confirm' || path === '/api/alerts/unsubscribe') {
    await new Promise((resolve) => setTimeout(resolve, 200));
    if (state === 'link-expired') return Response.json({ error: 'invalid_link' }, { status: 404 });
    return json({ ok: true, account: '8yAUM9…1bVm' });
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

const tab = state === 'public-hidden' || state === 'public-private' ? '' : '#settings';
const linkToken = 'c'.repeat(64);
if (state === 'link-confirm' || state === 'link-expired') {
  history.replaceState(null, '', `/alerts?confirm=${linkToken}&${params}`);
} else if (state === 'link-unsubscribe') {
  history.replaceState(null, '', `/alerts?unsubscribe=${linkToken}&${params}`);
} else {
  history.replaceState(null, '', `/stats${location.search}${tab}`);
}

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
