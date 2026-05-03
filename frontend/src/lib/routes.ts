import type { Route } from '../types';

const ROUTES = {
  dashboard: ['/', 'Blocknet Mining Pool'],
  start: ['/start', 'Mine Blocknet With Seine'],
  luck: ['/luck', 'Blocknet Pool Luck'],
  blocks: ['/blocks', 'Recent Blocknet Blocks'],
  payouts: ['/payouts', 'Blocknet Pool Payouts'],
  stats: ['/stats', 'Blocknet Miner Stats'],
  status: ['/status', 'Blocknet Pool Status'],
  admin: ['/admin', 'Pool Admin Dashboard'],
} as const satisfies Record<Route, readonly [string, string]>;

const ROUTES_BY_PATH = Object.fromEntries(
  Object.entries(ROUTES).map(([route, [path]]) => [path, route])
) as Record<string, Route>;

function normalizePathname(pathname: string): string {
  if (!pathname) return '/';
  const trimmed = pathname.replace(/\/+$/, '');
  if (!trimmed) return '/';
  return trimmed;
}

export function routeFromPathname(pathname: string): Route {
  return ROUTES_BY_PATH[normalizePathname(pathname)] || 'dashboard';
}

export function pathForRoute(route: Route): string {
  return ROUTES[route][0];
}

export function titleForRoute(route: Route): string {
  return ROUTES[route][1];
}
