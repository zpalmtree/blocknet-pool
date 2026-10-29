import { t, localizePath, locale } from './i18n';
import { type MouseEvent, useCallback, useEffect, useLayoutEffect, useMemo, useState } from 'react';

import { startSiteMetrics } from '../blog/site-metrics.mjs';
import { createApiClient } from './api/client';
import { LanguageControl } from './components/LanguageControl';
import { API_KEY_STORAGE_KEY, LAST_MINER_LOOKUP_KEY } from './lib/storage';
import { applyTheme, getStoredTheme, setStoredTheme, type ThemeMode } from './lib/theme';
import { metadataForPath, updatePageMetadata } from './lib/page-meta';
import { pathForRoute, routeFromPathname, titleForRoute } from './lib/routes';
import { BlocksPage } from './pages/BlocksPage';
import { CheckpointsPage } from './pages/CheckpointsPage';
import { DashboardPage } from './pages/DashboardPage';
import { LuckPage } from './pages/LuckPage';
import { PayoutsPage } from './pages/PayoutsPage';
import { StartPage } from './pages/StartPage';
import { StatusPage } from './pages/StatusPage';
import { AdminPage, StatsPage, WalletPage, PageBoundary, preloadPage } from './components/DeferredPages';
import type { InfoResponse, Route } from './types';

export function App() {
  const NAV_ITEMS: { route: Route; label: string }[] = [
    { route: 'dashboard', label: t("m001") },
    { route: 'start', label: t("m002") },
    { route: 'stats', label: t("m005") },
    { route: 'blocks', label: t("m003") },
    { route: 'payouts', label: t("m004") },
    { route: 'status', label: t("m006") },
  ];
  // Node operators and the pool operator need these, miners rarely do.
  const FOOTER_ITEMS: { route: Route; label: string }[] = [
    { route: 'checkpoints', label: t("m007") },
    { route: 'admin', label: t("m008") },
  ];
  const [route, setRoute] = useState<Route>(() => routeFromPathname(window.location.pathname));
  const isNotFound = route === 'not-found';
  const [errorMsg, setErrorMsg] = useState('');
  const [poolInfo, setPoolInfo] = useState<InfoResponse | null>(null);
  const [apiKey, setApiKey] = useState(localStorage.getItem(API_KEY_STORAGE_KEY) || '');
  const [apiKeyInput, setApiKeyInput] = useState(localStorage.getItem(API_KEY_STORAGE_KEY) || '');
  const [liveTick, setLiveTick] = useState(0);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [theme, setTheme] = useState<ThemeMode>(() => getStoredTheme());
  const [showNavKeyInput, setShowNavKeyInput] = useState(false);

  const showError = useCallback((msg: string) => {
    if (!msg) return;
    setErrorMsg(msg === 'network error' ? t('networkError') : msg === 'Temporarily busy; waiting before retrying' ? t('busyError') : msg);
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
    const onPopState = () => setRoute(routeFromPathname(window.location.pathname));
    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
  }, []);

  useEffect(() => {
    // React replaces the initial HTML; restore fragment navigation after mounting.
    let target: HTMLElement | null = null;
    try { target = document.getElementById(decodeURIComponent(window.location.hash.slice(1))); } catch { /* Invalid fragments fall back to the page top. */ }
    if (target) target.scrollIntoView();
    else window.scrollTo(0, 0);
    setMobileNavOpen(false);
  }, [route]);

  useEffect(() => {
    if (!isNotFound) void loadPoolInfo();
  }, [loadPoolInfo, isNotFound]);

  useEffect(() => {
    let mounted = true;
    const timer = window.setInterval(() => {
      if (!mounted) return;
      if (document.visibilityState !== 'visible') return;
      setLiveTick((tick) => tick + 1);
    }, 5000);

    return () => {
      mounted = false;
      window.clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    if (isNotFound || liveTick <= 0 || liveTick % 12 !== 0) return;
    void loadPoolInfo();
  }, [liveTick, loadPoolInfo, isNotFound]);

  useEffect(() => {
    const baseTitle = locale === 'en'
      ? metadataForPath(window.location.pathname, '').title
      : titleForRoute(route);
    const poolName = poolInfo?.pool_name.trim();
    updatePageMetadata(window.location.pathname, poolName ? `${baseTitle} | ${poolName}` : baseTitle, locale, t);
    document
      .querySelector<HTMLMetaElement>('meta[name="theme-color"]')
      ?.setAttribute('content', theme === 'dark' ? '#071114' : '#f6f8f2');
  }, [poolInfo, route, theme]);

  useEffect(() => {
    if (route !== 'dashboard' && route !== 'start') return;
    return startSiteMetrics(document.getElementById(`page-${route}`), route === 'dashboard' ? 'home' : 'pool-start', locale);
  }, [route]);

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
    if (window.location.pathname + window.location.search !== nextPath) {
      window.history.pushState({}, '', nextPath);
    }
    setRoute(nextRoute);
  }, []);

  const onNavLinkClick = useCallback(
    (event: MouseEvent<HTMLAnchorElement>, nextRoute: Route) => {
      if (
        event.defaultPrevented ||
        event.button !== 0 ||
        event.metaKey ||
        event.ctrlKey ||
        event.shiftKey ||
        event.altKey
      ) {
        return;
      }
      event.preventDefault();
      navigateToRoute(nextRoute);
    },
    [navigateToRoute]
  );

  let currentPage: JSX.Element;
  if (isNotFound) {
    currentPage = (
      <main id="page-not-found">
        <div className="page-header">
          <span className="page-kicker">404</span>
          <h1>{t('notFoundTitle')}</h1>
          <p className="page-intro">{t('notFoundDescription')}</p>
        </div>
        <div className="page-actions">
          <a className="btn btn-primary" href={pathForRoute('dashboard')}>{t('m001')}</a>
          <a className="btn btn-secondary" href={pathForRoute('start')}>{t('m002')}</a>
          <a className="btn btn-secondary" href={localizePath('/blog/')}>{t('m010')}</a>
        </div>
      </main>
    );
  } else if (route === 'wallet') {
    currentPage = <WalletPage />;
  } else if (route === 'start') {
    currentPage = <StartPage poolInfo={poolInfo} />;
  } else if (route === 'checkpoints') {
    currentPage = <CheckpointsPage api={api} liveTick={liveTick} />;
  } else if (route === 'luck') {
    currentPage = <LuckPage api={api} liveTick={liveTick} />;
  } else if (route === 'blocks') {
    currentPage = <BlocksPage api={api} liveTick={liveTick} />;
  } else if (route === 'payouts') {
    currentPage = <PayoutsPage api={api} liveTick={liveTick} />;
  } else if (route === 'stats') {
    currentPage = <StatsPage api={api} liveTick={liveTick} theme={theme} />;
  } else if (route === 'admin') {
    currentPage = (
      <AdminPage
        api={api}
        liveTick={liveTick}
        apiKey={apiKey}
        apiKeyInput={apiKeyInput}
        setApiKeyInput={setApiKeyInput}
        onSaveApiKey={onSaveApiKey}
        onJumpToStats={onJumpToStats}
      />
    );
  } else if (route === 'status') {
    currentPage = <StatusPage api={api} liveTick={liveTick} />;
  } else {
    currentPage = (
      <DashboardPage
        api={api}
        poolInfo={poolInfo}
        liveTick={liveTick}
        theme={theme}
        onJumpToStats={onJumpToStats}
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
          {poolInfo?.pool_name || t("m009")}
        </a>
        <div id="site-nav-links" className="nav-links">
          {[...NAV_ITEMS, ...(poolInfo?.hosted_wallet_enabled ? [{ route: 'wallet' as const, label: t('hostedWallet') }] : [])].map((item) => (
            <a
              key={item.route}
              href={pathForRoute(item.route)}
              data-nav={item.route}
              className={route === item.route ? 'active' : ''}
              onClick={(event) => onNavLinkClick(event, item.route)}
              onMouseEnter={() => preloadPage(item.route)}
              onFocus={() => preloadPage(item.route)}
            >
              {item.label}
            </a>
          ))}
          {/* The blog is static pages served by the API, so it's a plain link. */}
          <a href={localizePath("/blog/")}>{t("m010")}</a>
        </div>
        <div className="nav-actions">
          <LanguageControl />
          {route === 'admin' && apiKey ? (showNavKeyInput ? (<div className="nav-key-input">
                <input
                  type="password"
                  placeholder={t("m011")}
                  value={apiKeyInput}
                  onChange={(e) => setApiKeyInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      onSaveApiKey();
                      setShowNavKeyInput(false);
                    }
                    if (e.key === 'Escape') setShowNavKeyInput(false);
                  }}
                  autoFocus
                />
                <button
                  className="btn btn-primary btn-xs"
                  onClick={() => {
                    onSaveApiKey();
                    setShowNavKeyInput(false);
                  }}
                >{t("m012")}</button>
                <button
                  className="btn btn-secondary btn-xs"
                  onClick={() => {
                    onClearApiKey();
                    setShowNavKeyInput(false);
                  }}
                >{t("m013")}</button>
                <button
                  className="btn btn-secondary btn-xs"
                  onClick={() => setShowNavKeyInput(false)}
                >{t("m014")}</button>
              </div>) : (<button
                type="button"
                className="nav-key-badge"
                onClick={() => setShowNavKeyInput(true)}
                title={t("m015")}
              >
                <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                  <rect x="3" y="11" width="18" height="11" rx="2" ry="2" />
                  <path d="M7 11V7a5 5 0 0 1 10 0v4" />
                </svg>
              </button>)) : null}
          <button
            type="button"
            className="theme-toggle"
            aria-label={t("m016", [theme === 'dark' ? t('light') : t('dark')])}
            aria-pressed={theme === 'dark'}
            onClick={onToggleTheme}
          >
            <span className="theme-toggle-track" aria-hidden="true">
              <span className="theme-toggle-icon theme-toggle-icon-sun">
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <circle cx="12" cy="12" r="4.5" />
                  <path d="M12 2.75v2.5M12 18.75v2.5M4.75 12h-2.5M21.75 12h-2.5M5.96 5.96 4.2 4.2M19.8 19.8l-1.77-1.77M18.03 5.97 19.8 4.2M4.2 19.8l1.77-1.77" />
                </svg>
              </span>
              <span className="theme-toggle-icon theme-toggle-icon-moon">
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <path d="M18.35 14.38A7.85 7.85 0 0 1 9.62 5.65 8.65 8.65 0 1 0 18.35 14.38Z" />
                </svg>
              </span>
              <span className="theme-toggle-thumb" />
            </span>
            <span className="sr-only">{theme === 'dark' ? t("m017") : t("m018")}</span>
          </button>
          <button
            type="button"
            className={`nav-toggle${mobileNavOpen ? ' is-open' : ''}`}
            aria-label={t("m019")}
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

      <div className={route === 'admin' ? 'container container-wide' : 'container'}>
        <PageBoundary key={route} route={route}>{currentPage}</PageBoundary>
      </div>

      <footer className="site-footer">
        {FOOTER_ITEMS.map((item) => (
          <a
            key={item.route}
            href={pathForRoute(item.route)}
            className={route === item.route ? 'active' : ''}
            onClick={(event) => onNavLinkClick(event, item.route)}
            onMouseEnter={() => preloadPage(item.route)}
            onFocus={() => preloadPage(item.route)}
          >
            {item.label}
          </a>
        ))}
        <a href="https://seineminer.com/" target="_blank" rel="noopener">seineminer.com</a>
      </footer>
    </>
  );
}
