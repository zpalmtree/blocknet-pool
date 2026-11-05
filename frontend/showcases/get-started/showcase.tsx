// Development-only: the real App on Get Started with synthetic API responses.
// ?state= picks the pool info, ?os=windows|mac|linux the visitor's system; ?theme=dark and ?lang= work as on the site.
import { createRoot } from 'react-dom/client';
import { App } from '../../src/App';
import { initializeLanguage } from '../../src/i18n';
import { applyTheme } from '../../src/lib/theme';
import type { InfoResponse } from '../../src/types';
import '../../src/styles.css';

const STATES = {
  ready: 'Pool info loaded: solo available',
  'solo-off': 'Latest Seine predates solo: no Pool/Solo switch',
  loading: 'Pool info still loading',
  error: 'Pool info request fails',
} as const;
type State = keyof typeof STATES;

const params = new URLSearchParams(location.search);
const state: State = (params.get('state') ?? 'ready') in STATES ? params.get('state') as State : 'ready';
const theme = params.get('theme') === 'dark' ? 'dark' : 'light';
localStorage.setItem('pool_theme', theme);
applyTheme(theme);
localStorage.removeItem('pool_last_miner_lookup_address');
localStorage.removeItem('pool_payout_destination_v1');

const AGENTS: Record<string, string> = {
  windows: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
  mac: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5)',
  linux: 'Mozilla/5.0 (X11; Linux x86_64)',
};
const os = params.get('os');
if (os && AGENTS[os]) Object.defineProperty(navigator, 'userAgent', { get: () => AGENTS[os] });

const info = {
  pool_name: 'bntpool.com', pool_url: 'https://bntpool.com', stratum_port: 3333, pool_fee_pct: 0,
  min_payout_amount: 1, payout_memos_enabled: true, memo_min_payout_amount: 10,
  shared_exchange_addresses: [{ address: 'demo-shared-exchange-deposit-address', name: 'SafeTrade' }],
  blocks_before_payout: 60, pplns_window_duration: '6h', hosted_wallet_enabled: false,
  latest_miner_version: state === 'solo-off' ? '0.3.0' : '0.3.1',
} satisfies InfoResponse;

const nativeFetch = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const url = new URL(String(input instanceof Request ? input.url : input), location.href);
  // Never reach a real pool, explorer or analytics endpoint from the showcase.
  if (url.origin !== location.origin) return new Response(null, { status: 204 });
  if (!url.pathname.startsWith('/api/')) return nativeFetch(input, init);
  if (state === 'loading') return new Promise<Response>(() => {});
  if (url.pathname === '/api/info') {
    return state === 'error' ? Response.json({ error: 'Demo: pool info unavailable' }, { status: 503 }) : Response.json(info);
  }
  return Response.json({});
};

history.replaceState(null, '', `/start${location.search}${location.hash}`);

function Bar() {
  const go = (key: string, value: string) => { params.set(key, value); location.href = `/showcases/get-started/?${params}`; };
  return (
    <div lang="en" dir="ltr" style={{ display: 'flex', flexWrap: 'wrap', gap: '8px 18px', alignItems: 'center', padding: '8px 16px', fontSize: 13, background: '#f1f5ee', borderBottom: '1px solid #d8e3d2' }}>
      <span>Get Started · synthetic data</span>
      <label>State <select value={state} onChange={(event) => go('state', event.target.value)}>
        {Object.entries(STATES).map(([value, label]) => <option key={value} value={value}>{label}</option>)}
      </select></label>
      <label>System <select value={os ?? ''} onChange={(event) => go('os', event.target.value)}>
        <option value="">Browser</option><option value="windows">Windows</option><option value="mac">macOS</option><option value="linux">Linux</option>
      </select></label>
      <label>Theme <select value={theme} onChange={(event) => go('theme', event.target.value)}>
        <option value="light">Light</option><option value="dark">Dark</option>
      </select></label>
    </div>
  );
}

initializeLanguage().then(() => {
  const root = document.getElementById('root')!;
  // Captures: no scrollbar, so a full-page phone capture keeps its full 390 px width.
  if (params.get('chrome') === 'off') document.documentElement.style.scrollbarWidth = 'none';
  else {
    const bar = document.createElement('div');
    root.before(bar);
    createRoot(bar).render(<Bar />);
  }
  createRoot(root).render(<App />);
});
