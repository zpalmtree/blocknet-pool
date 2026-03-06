import { type MouseEvent, useCallback, useEffect, useLayoutEffect, useMemo, useState } from 'react';

import { createApiClient } from './api/client';
import { API_KEY_STORAGE_KEY, LAST_MINER_LOOKUP_KEY } from './lib/storage';
import { applyDocumentSeo } from './lib/seo';
import { applyTheme, getStoredTheme, setStoredTheme, type ThemeMode } from './lib/theme';
import { pathForRoute, pathFromLegacyHash, routeFromPathname } from './lib/routes';
import { AdminPage } from './pages/AdminPage';
import { BlocksPage } from './pages/BlocksPage';
import { DashboardPage } from './pages/DashboardPage';
import { LuckPage } from './pages/LuckPage';
import { PayoutsPage } from './pages/PayoutsPage';
import { StartPage } from './pages/StartPage';
import { StatusPage } from './pages/StatusPage';
import { StatsPage } from './pages/StatsPage';
import type { InfoResponse, Route } from './types';

function routeFromLocation(): Route {
  const legacyPath = pathFromLegacyHash(window.location.hash || '');
  if (legacyPath) {
    const nextUrl = `${legacyPath}${window.location.search}`;
    window.history.replaceState({}, '', nextUrl);
    return routeFromPathname(legacyPath);
  }
  return routeFromPathname(window.location.pathname);
}

function shouldHandleNavigation(event: MouseEvent<HTMLAnchorElement>): boolean {
  return !(
    event.defaultPrevented ||
    event.button !== 0 ||
    event.metaKey ||
    event.ctrlKey ||
    event.shiftKey ||
    event.altKey
  );
}

function SunIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="12" cy="12" r="4.5" />
      <path d="M12 2.75v2.5M12 18.75v2.5M4.75 12h-2.5M21.75 12h-2.5M5.96 5.96 4.2 4.2M19.8 19.8l-1.77-1.77M18.03 5.97 19.8 4.2M4.2 19.8l1.77-1.77" />
    </svg>
  );
}

function MoonIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M18.35 14.38A7.85 7.85 0 0 1 9.62 5.65 8.65 8.65 0 1 0 18.35 14.38Z" />
    </svg>
  );
}

export function App() {
  const [route, setRoute] = useState<Route>(() => routeFromLocation());
  const [errorMsg, setErrorMsg] = useState('');
  const [poolInfo, setPoolInfo] = useState<InfoResponse | null>(null);
  const [apiKey, setApiKey] = useState(localStorage.getItem(API_KEY_STORAGE_KEY) || '');
  const [apiKeyInput, setApiKeyInput] = useState(localStorage.getItem(API_KEY_STORAGE_KEY) || '');
  const [liveTick, setLiveTick] = useState(0);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [theme, setTheme] = useState<ThemeMode>(() => getStoredTheme());

  const showError = useCallback((msg: string) => {
    if (!msg) return;
    setErrorMsg(msg);
  }, []);

  useEffect(() => {
    if (!errorMsg) return;
    const t = window.setTimeout(() => setErrorMsg(''), 5000);
    return () => window.clearTimeout(t);
  }, [errorMsg]);

  useLayoutEffect(() => {
    applyTheme(theme);
    setStoredTheme(theme);
  }, [theme]);

  const api = useMemo(() => createApiClient(() => apiKey, showError), [apiKey, showError]);

  const loadPoolInfo = useCallback(async () => {
    try {
      const info = await api.getInfo();
      setPoolInfo(info);
    } catch {
      // handled by api client
    }
  }, [api]);

  useEffect(() => {
    const onPopState = () => setRoute(routeFromLocation());
    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
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

  useEffect(() => {
    applyDocumentSeo(route, poolInfo, theme);
  }, [poolInfo, route, theme]);

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
    const path = pathForRoute('stats');
    window.history.pushState({}, '', path);
    setRoute('stats');
  }, []);

  const onToggleTheme = useCallback(() => {
    setTheme((current) => (current === 'dark' ? 'light' : 'dark'));
  }, []);

  const navigateToRoute = useCallback((nextRoute: Route) => {
    const nextPath = pathForRoute(nextRoute);
    if (window.location.pathname !== nextPath) {
      window.history.pushState({}, '', nextPath);
    }
    setRoute(nextRoute);
  }, []);

  const onNavLinkClick = useCallback(
    (event: MouseEvent<HTMLAnchorElement>, nextRoute: Route) => {
      if (!shouldHandleNavigation(event)) return;
      event.preventDefault();
      navigateToRoute(nextRoute);
    },
    [navigateToRoute]
  );

  let currentPage: JSX.Element;
  if (route === 'start') {
    currentPage = <StartPage active poolInfo={poolInfo} theme={theme} />;
  } else if (route === 'luck') {
    currentPage = <LuckPage active api={api} liveTick={liveTick} />;
  } else if (route === 'blocks') {
    currentPage = <BlocksPage active api={api} liveTick={liveTick} />;
  } else if (route === 'payouts') {
    currentPage = <PayoutsPage active api={api} liveTick={liveTick} />;
  } else if (route === 'stats') {
    currentPage = <StatsPage active api={api} liveTick={liveTick} theme={theme} />;
  } else if (route === 'admin') {
    currentPage = (
      <AdminPage
        active
        api={api}
        liveTick={liveTick}
        apiKey={apiKey}
        apiKeyInput={apiKeyInput}
        setApiKeyInput={setApiKeyInput}
        onSaveApiKey={onSaveApiKey}
        onClearApiKey={onClearApiKey}
        onJumpToStats={onJumpToStats}
      />
    );
  } else if (route === 'status') {
    currentPage = <StatusPage active api={api} liveTick={liveTick} />;
  } else {
    currentPage = (
      <DashboardPage
        active
        api={api}
        poolInfo={poolInfo}
        liveTick={liveTick}
        theme={theme}
      />
    );
  }

  return (
    <>
      <nav className={mobileNavOpen ? 'is-open' : ''}>
        <a
          href={pathForRoute('dashboard')}
          className="nav-brand"
          id="nav-brand"
          style={{ textDecoration: 'none' }}
          onClick={(event) => onNavLinkClick(event, 'dashboard')}
        >
          {poolInfo?.pool_name || 'Blocknet Pool'}
        </a>
        <div id="site-nav-links" className="nav-links">
          <a
            href={pathForRoute('dashboard')}
            data-nav="dashboard"
            className={route === 'dashboard' ? 'active' : ''}
            onClick={(event) => onNavLinkClick(event, 'dashboard')}
          >
            Dashboard
          </a>
          <a
            href={pathForRoute('start')}
            data-nav="start"
            className={route === 'start' ? 'active' : ''}
            onClick={(event) => onNavLinkClick(event, 'start')}
          >
            Get Started
          </a>
          <a
            href={pathForRoute('blocks')}
            data-nav="blocks"
            className={route === 'blocks' ? 'active' : ''}
            onClick={(event) => onNavLinkClick(event, 'blocks')}
          >
            Blocks
          </a>
          <a
            href={pathForRoute('payouts')}
            data-nav="payouts"
            className={route === 'payouts' ? 'active' : ''}
            onClick={(event) => onNavLinkClick(event, 'payouts')}
          >
            Payouts
          </a>
          <a
            href={pathForRoute('stats')}
            data-nav="stats"
            className={route === 'stats' ? 'active' : ''}
            onClick={(event) => onNavLinkClick(event, 'stats')}
          >
            My Stats
          </a>
          <a
            href={pathForRoute('status')}
            data-nav="status"
            className={route === 'status' ? 'active' : ''}
            onClick={(event) => onNavLinkClick(event, 'status')}
          >
            Status
          </a>
          <a
            href={pathForRoute('admin')}
            data-nav="admin"
            className={route === 'admin' ? 'active' : ''}
            onClick={(event) => onNavLinkClick(event, 'admin')}
          >
            Admin
          </a>
        </div>
        <div className="nav-actions">
          <button
            type="button"
            className="theme-toggle"
            aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
            aria-pressed={theme === 'dark'}
            onClick={onToggleTheme}
          >
            <span className="theme-toggle-track" aria-hidden="true">
              <span className="theme-toggle-icon theme-toggle-icon-sun">
                <SunIcon />
              </span>
              <span className="theme-toggle-icon theme-toggle-icon-moon">
                <MoonIcon />
              </span>
              <span className="theme-toggle-thumb" />
            </span>
            <span className="sr-only">{theme === 'dark' ? 'Light mode' : 'Dark mode'}</span>
          </button>
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
        </div>
      </nav>

      <div id="error-bar" style={{ display: errorMsg ? 'block' : 'none' }}>
        {errorMsg}
      </div>

      <div className="container">
        {currentPage}
      </div>
    </>
  );
}
