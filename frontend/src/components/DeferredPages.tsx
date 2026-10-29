import { Component, lazy, Suspense, type ReactNode } from 'react';
import { t } from '../i18n';
import { pathForRoute, titleForRoute } from '../lib/routes';
import type { Route } from '../types';

const loaders = {
  admin: () => import('../pages/AdminPage').then((module) => ({ default: module.AdminPage })),
  stats: () => import('../pages/StatsPage').then((module) => ({ default: module.StatsPage })),
  wallet: () => import('../pages/WalletPage').then((module) => ({ default: module.WalletPage })),
};
export const AdminPage = lazy(loaders.admin);
export const StatsPage = lazy(loaders.stats);
export const WalletPage = lazy(loaders.wallet);

export function preloadPage(route: Route) {
  // Keyboard focus and pointer intent can warm the page before navigation.
  if (route in loaders) void loaders[route as keyof typeof loaders]().catch(() => {});
}

export class PageBoundary extends Component<{ route: Route; children: ReactNode }, { failed: boolean }> {
  state = { failed: false };

  static getDerivedStateFromError() { return { failed: true }; }

  render() {
    if (this.state.failed) {
      // A deployment can replace a chunk while an older tab is still open.
      // Full navigation obtains the current entry point and its matching chunks.
      return <section className="card page-actions" role="alert">
        <p>{t('networkError')}</p>
        <a className="btn btn-primary" href={pathForRoute(this.props.route)}>{titleForRoute(this.props.route)}</a>
      </section>;
    }
    return <Suspense fallback={<section className="card" role="status">{t('m094')}</section>}>
      {this.props.children}
    </Suspense>;
  }
}
