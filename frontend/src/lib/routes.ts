import type { Route } from '../types';

export interface RouteConfig {
  route: Route;
  path: string;
  label: string;
  indexable: boolean;
}

export const ROUTES: RouteConfig[] = [
  { route: 'dashboard', path: '/', label: 'Dashboard', indexable: true },
  { route: 'start', path: '/start', label: 'Get Started', indexable: true },
  { route: 'luck', path: '/luck', label: 'Luck', indexable: true },
  { route: 'blocks', path: '/blocks', label: 'Blocks', indexable: true },
  { route: 'payouts', path: '/payouts', label: 'Payouts', indexable: true },
  { route: 'stats', path: '/stats', label: 'My Stats', indexable: false },
  { route: 'status', path: '/status', label: 'Status', indexable: true },
  { route: 'admin', path: '/admin', label: 'Admin', indexable: false },
];

const ROUTES_BY_PATH = new Map<string, Route>(
  ROUTES.map((entry) => [entry.path, entry.route])
);

const ROUTES_BY_NAME = new Map<Route, RouteConfig>(
  ROUTES.map((entry) => [entry.route, entry])
);

function normalizePathname(pathname: string): string {
  if (!pathname) return '/';
  const trimmed = pathname.replace(/\/+$/, '');
  if (!trimmed || trimmed === '/ui') return '/';
  return trimmed;
}

export function routeFromPathname(pathname: string): Route {
  return ROUTES_BY_PATH.get(normalizePathname(pathname)) || 'dashboard';
}

export function pathForRoute(route: Route): string {
  return ROUTES_BY_NAME.get(route)?.path || '/';
}

export function isIndexableRoute(route: Route): boolean {
  return ROUTES_BY_NAME.get(route)?.indexable ?? false;
}

export function routeFromHash(hash: string): Route | null {
  if (!hash.startsWith('#/')) return null;
  return routeFromPathname(hash.slice(1));
}

export function pathFromLegacyHash(hash: string): string | null {
  const route = routeFromHash(hash);
  if (!route) return null;
  return pathForRoute(route);
}
