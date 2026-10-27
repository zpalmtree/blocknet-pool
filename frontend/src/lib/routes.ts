import { t, localizePath } from '../i18n';
import type { Route } from '../types';

const ROUTES = {
  wallet: ['/wallet', 'hostedWallet'],
  dashboard: ['/', 'm399'],
  start: ['/start', 'm611'],
  checkpoints: ['/checkpoints', 'm612'],
  luck: ['/luck', 'm613'],
  blocks: ['/blocks', 'm614'],
  payouts: ['/payouts', 'm615'],
  stats: ['/stats', 'm616'],
  status: ['/status', 'm617'],
  admin: ['/admin', 'm618'],
  'not-found': ['/404', 'notFoundTitle'],
} as const satisfies Record<Route, readonly [string, string]>;

const ROUTES_BY_PATH = Object.fromEntries(
  Object.entries(ROUTES).map(([route, [path]]) => [path, route])
) as Record<string, Route>;

export function routeFromPathname(pathname: string): Route {
  return ROUTES_BY_PATH[pathname.replace(/\/+$/, '') || '/'] || 'not-found';
}

export function pathForRoute(route: Route): string {
  return localizePath(ROUTES[route][0]);
}

export function titleForRoute(route: Route): string {
  return t(ROUTES[route][1]);
}
