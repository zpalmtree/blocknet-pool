import { useCallback, useEffect, useMemo, useState } from 'react';

import { createApiClient } from './api/client';
import { API_KEY_STORAGE_KEY, LAST_MINER_LOOKUP_KEY } from './lib/storage';
import { routeFromHash } from './lib/format';
import { AdminPage } from './pages/AdminPage';
import { BlocksPage } from './pages/BlocksPage';
import { DashboardPage } from './pages/DashboardPage';
import { LuckPage } from './pages/LuckPage';
import { PayoutsPage } from './pages/PayoutsPage';
import { StartPage } from './pages/StartPage';
import { StatusPage } from './pages/StatusPage';
import { StatsPage } from './pages/StatsPage';
import type { InfoResponse, Route } from './types';

const APP_TITLE = 'BNT Pool';

export function App() {
  const [route, setRoute] = useState<Route>(routeFromHash(window.location.hash || '#/'));
  const [errorMsg, setErrorMsg] = useState('');
  const [poolInfo, setPoolInfo] = useState<InfoResponse | null>(null);
  const [apiKey, setApiKey] = useState(localStorage.getItem(API_KEY_STORAGE_KEY) || '');
  const [apiKeyInput, setApiKeyInput] = useState(localStorage.getItem(API_KEY_STORAGE_KEY) || '');
  const [liveTick, setLiveTick] = useState(0);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);

  const showError = useCallback((msg: string) => {
    if (!msg) return;
    setErrorMsg(msg);
  }, []);

  useEffect(() => {
    if (!errorMsg) return;
    const t = window.setTimeout(() => setErrorMsg(''), 5000);
    return () => window.clearTimeout(t);
  }, [errorMsg]);

  const api = useMemo(() => createApiClient(() => apiKey, showError), [apiKey, showError]);

  const loadPoolInfo = useCallback(async () => {
    try {
      const info = await api.getInfo();
      setPoolInfo(info);
      document.title = APP_TITLE;
    } catch {
      // handled by api client
    }
  }, [api]);

  useEffect(() => {
    const onHashChange = () => setRoute(routeFromHash(window.location.hash || '#/'));
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, []);

  useEffect(() => {
    window.scrollTo(0, 0);
    setMobileNavOpen(false);
  }, [route]);

  useEffect(() => {
    void loadPoolInfo();
  }, [loadPoolInfo]);

  useEffect(() => {
    let mounted = true;
    let lastTickAt = Date.now();
    const source = new EventSource('/api/events');
    const onTick = () => {
      if (!mounted) return;
      lastTickAt = Date.now();
      setLiveTick((tick) => tick + 1);
    };
    source.addEventListener('tick', onTick);
    source.onerror = () => {
      // EventSource auto-reconnects.
    };

    // Fallback refresh for environments where SSE is delayed or blocked.
    const fallbackTimer = window.setInterval(() => {
      if (!mounted) return;
      if (document.visibilityState !== 'visible') return;
      if (Date.now() - lastTickAt < 15000) return;
      lastTickAt = Date.now();
      setLiveTick((tick) => tick + 1);
    }, 5000);

    return () => {
      mounted = false;
      window.clearInterval(fallbackTimer);
      source.removeEventListener('tick', onTick);
      source.close();
    };
  }, []);

  useEffect(() => {
    if (liveTick <= 0 || liveTick % 12 !== 0) return;
    void loadPoolInfo();
  }, [liveTick, loadPoolInfo]);

  const onSaveApiKey = useCallback(() => {
    const key = apiKeyInput.trim();
    setApiKey(key);
    localStorage.setItem(API_KEY_STORAGE_KEY, key);
  }, [apiKeyInput]);

  const onClearApiKey = useCallback(() => {
    setApiKey('');
    setApiKeyInput('');
    localStorage.removeItem(API_KEY_STORAGE_KEY);
  }, []);

  const onJumpToStats = useCallback((address: string) => {
    if (address) {
      localStorage.setItem(LAST_MINER_LOOKUP_KEY, address);
    }
    window.location.hash = '#/stats';
  }, []);

  return (
    <>
      <nav className={mobileNavOpen ? 'is-open' : ''}>
        <a
          href="#/"
          className="nav-brand"
          id="nav-brand"
          style={{ textDecoration: 'none' }}
          onClick={() => setMobileNavOpen(false)}
        >
          {poolInfo?.pool_name || 'Blocknet Pool'}
        </a>
        <button
          type="button"
          className={`nav-toggle${mobileNavOpen ? ' is-open' : ''}`}
          aria-label="Toggle navigation menu"
          aria-expanded={mobileNavOpen}
          aria-controls="site-nav-links"
          onClick={() => setMobileNavOpen((open) => !open)}
        >
          <span />
          <span />
          <span />
        </button>
        <div id="site-nav-links" className="nav-links">
          <a
            href="#/"
            data-nav="dashboard"
            className={route === 'dashboard' ? 'active' : ''}
            onClick={() => setMobileNavOpen(false)}
          >
            Dashboard
          </a>
          <a
            href="#/start"
            data-nav="start"
            className={route === 'start' ? 'active' : ''}
            onClick={() => setMobileNavOpen(false)}
          >
            Get Started
          </a>
          <a
            href="#/blocks"
            data-nav="blocks"
            className={route === 'blocks' ? 'active' : ''}
            onClick={() => setMobileNavOpen(false)}
          >
            Blocks
          </a>
          <a
            href="#/payouts"
            data-nav="payouts"
            className={route === 'payouts' ? 'active' : ''}
            onClick={() => setMobileNavOpen(false)}
          >
            Payouts
          </a>
          <a
            href="#/stats"
            data-nav="stats"
            className={route === 'stats' ? 'active' : ''}
            onClick={() => setMobileNavOpen(false)}
          >
            My Stats
          </a>
          <a
            href="#/admin"
            data-nav="admin"
            className={route === 'admin' ? 'active' : ''}
            onClick={() => setMobileNavOpen(false)}
          >
            Admin
          </a>
          <a
            href="#/status"
            data-nav="status"
            className={route === 'status' ? 'active' : ''}
            onClick={() => setMobileNavOpen(false)}
          >
            Status
          </a>
        </div>
      </nav>

      <div id="error-bar" style={{ display: errorMsg ? 'block' : 'none' }}>
        {errorMsg}
      </div>

      <div className="container">
        <DashboardPage active={route === 'dashboard'} api={api} poolInfo={poolInfo} liveTick={liveTick} />
        <StartPage active={route === 'start'} poolInfo={poolInfo} />
        <LuckPage active={route === 'luck'} api={api} liveTick={liveTick} />
        <BlocksPage active={route === 'blocks'} api={api} liveTick={liveTick} />
        <PayoutsPage active={route === 'payouts'} api={api} liveTick={liveTick} />
        <StatsPage active={route === 'stats'} api={api} liveTick={liveTick} />
        <AdminPage
          active={route === 'admin'}
          api={api}
          liveTick={liveTick}
          apiKey={apiKey}
          apiKeyInput={apiKeyInput}
          setApiKeyInput={setApiKeyInput}
          onSaveApiKey={onSaveApiKey}
          onClearApiKey={onClearApiKey}
          onJumpToStats={onJumpToStats}
        />
        <StatusPage active={route === 'status'} api={api} liveTick={liveTick} />
      </div>
    </>
  );
}
