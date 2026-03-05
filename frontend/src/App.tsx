import { useCallback, useEffect, useMemo, useState } from 'react';

import { createApiClient } from './api/client';
import { API_KEY_STORAGE_KEY, LAST_MINER_LOOKUP_KEY } from './lib/storage';
import { routeFromHash } from './lib/format';
import { AdminPage } from './pages/AdminPage';
import { BlocksPage } from './pages/BlocksPage';
import { DashboardPage } from './pages/DashboardPage';
import { PayoutsPage } from './pages/PayoutsPage';
import { StartPage } from './pages/StartPage';
import { StatsPage } from './pages/StatsPage';
import type { InfoResponse, Route } from './types';

export function App() {
  const [route, setRoute] = useState<Route>(routeFromHash(window.location.hash || '#/'));
  const [errorMsg, setErrorMsg] = useState('');
  const [poolInfo, setPoolInfo] = useState<InfoResponse | null>(null);
  const [apiKey, setApiKey] = useState(localStorage.getItem(API_KEY_STORAGE_KEY) || '');
  const [apiKeyInput, setApiKeyInput] = useState(localStorage.getItem(API_KEY_STORAGE_KEY) || '');

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

  useEffect(() => {
    const onHashChange = () => setRoute(routeFromHash(window.location.hash || '#/'));
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, []);

  useEffect(() => {
    api
      .getInfo()
      .then((info) => {
        setPoolInfo(info);
        document.title = info.pool_name || 'Blocknet Pool';
      })
      .catch(() => {
        // handled by api client
      });
  }, [api]);

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
      <nav>
        <a href="#/" className="nav-brand" id="nav-brand" style={{ textDecoration: 'none' }}>
          {poolInfo?.pool_name || 'Blocknet Pool'}
        </a>
        <a href="#/" data-nav="dashboard" className={route === 'dashboard' ? 'active' : ''}>
          Dashboard
        </a>
        <a href="#/start" data-nav="start" className={route === 'start' ? 'active' : ''}>
          Get Started
        </a>
        <a href="#/blocks" data-nav="blocks" className={route === 'blocks' ? 'active' : ''}>
          Blocks
        </a>
        <a href="#/payouts" data-nav="payouts" className={route === 'payouts' ? 'active' : ''}>
          Payouts
        </a>
        <a href="#/stats" data-nav="stats" className={route === 'stats' ? 'active' : ''}>
          My Stats
        </a>
        <a href="#/admin" data-nav="admin" className={route === 'admin' ? 'active' : ''}>
          Admin
        </a>
      </nav>

      <div id="error-bar" style={{ display: errorMsg ? 'block' : 'none' }}>
        {errorMsg}
      </div>

      <div className="container">
        <DashboardPage active={route === 'dashboard'} api={api} poolInfo={poolInfo} />
        <StartPage active={route === 'start'} poolInfo={poolInfo} />
        <BlocksPage active={route === 'blocks'} api={api} />
        <PayoutsPage active={route === 'payouts'} api={api} />
        <StatsPage active={route === 'stats'} api={api} />
        <AdminPage
          active={route === 'admin'}
          api={api}
          apiKey={apiKey}
          apiKeyInput={apiKeyInput}
          setApiKeyInput={setApiKeyInput}
          onSaveApiKey={onSaveApiKey}
          onClearApiKey={onClearApiKey}
          onJumpToStats={onJumpToStats}
        />
      </div>
    </>
  );
}
