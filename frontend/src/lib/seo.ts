import { stratumUrl } from './format';
import { isIndexableRoute, pathForRoute } from './routes';
import type { ThemeMode } from './theme';
import type { InfoResponse, Route } from '../types';

const DEFAULT_POOL_NAME = 'Blocknet Pool';
const DEFAULT_BASE_URL = 'https://bntpool.com';
const OG_IMAGE_PATH = '/og-image.svg';
const INDEXABLE_ROBOTS = 'index,follow,max-image-preview:large';
const PRIVATE_ROBOTS = 'noindex,nofollow,noarchive';

interface SeoSnapshot {
  title: string;
  description: string;
  canonicalUrl: string;
  robots: string;
  poolName: string;
  structuredData: object[];
}

function poolNameFor(poolInfo: InfoResponse | null): string {
  return poolInfo?.pool_name?.trim() || DEFAULT_POOL_NAME;
}

function baseUrlFor(poolInfo: InfoResponse | null): string {
  const origin =
    poolInfo?.pool_url?.trim() ||
    (typeof window !== 'undefined' && window.location.origin ? window.location.origin : '') ||
    DEFAULT_BASE_URL;
  return origin.replace(/\/+$/, '') || DEFAULT_BASE_URL;
}

function canonicalUrlFor(route: Route, poolInfo: InfoResponse | null): string {
  return `${baseUrlFor(poolInfo)}${pathForRoute(route)}`;
}

function descriptionFor(route: Route, poolInfo: InfoResponse | null): string {
  const name = poolNameFor(poolInfo);
  if (route === 'start') {
    const payoutScheme = (poolInfo?.payout_scheme || 'pplns').toUpperCase();
    const fee = poolInfo?.pool_fee_pct != null ? `${poolInfo.pool_fee_pct}% fee` : 'transparent fee model';
    return `Learn how to mine Blocknet with ${name}, connect to ${stratumUrl(poolInfo?.stratum_port, poolInfo?.pool_url)}, and understand ${payoutScheme} payouts, fees, and setup steps.`;
  }
  if (route === 'blocks') {
    return `Browse recently found Blocknet blocks from ${name}, including confirmed, pending, and orphaned rounds with effort and timing data.`;
  }
  if (route === 'payouts') {
    return `Review recent Blocknet mining pool payouts from ${name}, including payout totals, recipient counts, explorer transaction links, and payout timing.`;
  }
  if (route === 'luck') {
    return `Track ${name} luck history with round effort, round duration, and confirmation status for recent Blocknet blocks.`;
  }
  if (route === 'status') {
    return `Monitor ${name} uptime, daemon reachability, sync status, and incident history from the public Blocknet pool status page.`;
  }
  if (route === 'stats') {
    return `Look up a Blocknet wallet address on ${name} to inspect hashrate, balances, worker activity, recent payouts, and share history.`;
  }
  if (route === 'admin') {
    return `Administrative dashboard for ${name} with miners, payouts, fees, health checks, and daemon log streaming.`;
  }
  return `Mine Blocknet with ${name}. Track pool hashrate, round luck, recent blocks, payouts, and pool health from one public dashboard.`;
}

function titleFor(route: Route, poolInfo: InfoResponse | null): string {
  const name = poolNameFor(poolInfo);
  if (route === 'start') return `How To Start Mining Blocknet | ${name}`;
  if (route === 'blocks') return `Recent Blocknet Blocks | ${name}`;
  if (route === 'payouts') return `Recent Pool Payouts | ${name}`;
  if (route === 'luck') return `Pool Luck History | ${name}`;
  if (route === 'status') return `Pool Status | ${name}`;
  if (route === 'stats') return `Miner Stats Lookup | ${name}`;
  if (route === 'admin') return `Admin Dashboard | ${name}`;
  return `Blocknet Mining Pool Dashboard | ${name}`;
}

function pageSchemaType(route: Route): string {
  if (route === 'start') return 'HowTo';
  if (route === 'blocks' || route === 'payouts' || route === 'luck') return 'CollectionPage';
  return 'WebPage';
}

function buildHowToSchema(poolInfo: InfoResponse | null, canonicalUrl: string): object {
  return {
    '@context': 'https://schema.org',
    '@type': 'HowTo',
    name: 'How to start mining Blocknet with Seine',
    url: canonicalUrl,
    description: descriptionFor('start', poolInfo),
    step: [
      {
        '@type': 'HowToStep',
        name: 'Download Seine',
        text: 'Download the latest Seine release for your platform.',
      },
      {
        '@type': 'HowToStep',
        name: 'Enter your Blocknet wallet address',
        text: 'Launch Seine and provide your Blocknet payout address.',
      },
      {
        '@type': 'HowToStep',
        name: 'Connect to the pool stratum endpoint',
        text: `Use ${stratumUrl(poolInfo?.stratum_port, poolInfo?.pool_url)} as the pool URL.`,
      },
      {
        '@type': 'HowToStep',
        name: 'Start mining and monitor payouts',
        text: 'Run the miner, then use the pool dashboard and stats page to track hashrate and balances.',
      },
    ],
  };
}

function buildStructuredData(route: Route, poolInfo: InfoResponse | null, snapshot: Omit<SeoSnapshot, 'structuredData'>): object[] {
  const baseUrl = baseUrlFor(poolInfo);
  const pageType = pageSchemaType(route);
  const items: object[] = [
    {
      '@context': 'https://schema.org',
      '@type': 'WebSite',
      name: snapshot.poolName,
      url: baseUrl,
      description: descriptionFor('dashboard', poolInfo),
    },
    {
      '@context': 'https://schema.org',
      '@type': pageType,
      name: snapshot.title,
      url: snapshot.canonicalUrl,
      description: snapshot.description,
      isPartOf: {
        '@type': 'WebSite',
        name: snapshot.poolName,
        url: baseUrl,
      },
    },
  ];

  if (route === 'start') {
    items.push(buildHowToSchema(poolInfo, snapshot.canonicalUrl));
  }

  if (route !== 'dashboard') {
    items.push({
      '@context': 'https://schema.org',
      '@type': 'BreadcrumbList',
      itemListElement: [
        {
          '@type': 'ListItem',
          position: 1,
          name: snapshot.poolName,
          item: baseUrl,
        },
        {
          '@type': 'ListItem',
          position: 2,
          name: snapshot.title.replace(` | ${snapshot.poolName}`, ''),
          item: snapshot.canonicalUrl,
        },
      ],
    });
  }

  return items;
}

function buildSeoSnapshot(route: Route, poolInfo: InfoResponse | null): SeoSnapshot {
  const poolName = poolNameFor(poolInfo);
  const title = titleFor(route, poolInfo);
  const description = descriptionFor(route, poolInfo);
  const canonicalUrl = canonicalUrlFor(route, poolInfo);
  const robots = isIndexableRoute(route) ? INDEXABLE_ROBOTS : PRIVATE_ROBOTS;

  const snapshotBase = {
    title,
    description,
    canonicalUrl,
    robots,
    poolName,
  };

  return {
    ...snapshotBase,
    structuredData: buildStructuredData(route, poolInfo, snapshotBase),
  };
}

function ensureMeta(selector: string, attrs: Record<string, string>): HTMLMetaElement {
  let node = document.head.querySelector<HTMLMetaElement>(selector);
  if (!node) {
    node = document.createElement('meta');
    document.head.appendChild(node);
  }
  Object.entries(attrs).forEach(([key, value]) => node?.setAttribute(key, value));
  return node;
}

function ensureLink(selector: string, attrs: Record<string, string>): HTMLLinkElement {
  let node = document.head.querySelector<HTMLLinkElement>(selector);
  if (!node) {
    node = document.createElement('link');
    document.head.appendChild(node);
  }
  Object.entries(attrs).forEach(([key, value]) => node?.setAttribute(key, value));
  return node;
}

export function applyDocumentSeo(route: Route, poolInfo: InfoResponse | null, theme: ThemeMode): void {
  const snapshot = buildSeoSnapshot(route, poolInfo);
  const ogImageUrl = `${baseUrlFor(poolInfo)}${OG_IMAGE_PATH}`;
  const themeColor = theme === 'dark' ? '#071114' : '#f6f8f2';

  document.title = snapshot.title;

  ensureMeta('meta[name="description"]', {
    name: 'description',
    content: snapshot.description,
  });
  ensureMeta('meta[name="robots"]', {
    name: 'robots',
    content: snapshot.robots,
  });
  ensureMeta('meta[name="theme-color"]', {
    name: 'theme-color',
    content: themeColor,
  });
  ensureMeta('meta[property="og:type"]', {
    property: 'og:type',
    content: 'website',
  });
  ensureMeta('meta[property="og:site_name"]', {
    property: 'og:site_name',
    content: snapshot.poolName,
  });
  ensureMeta('meta[property="og:title"]', {
    property: 'og:title',
    content: snapshot.title,
  });
  ensureMeta('meta[property="og:description"]', {
    property: 'og:description',
    content: snapshot.description,
  });
  ensureMeta('meta[property="og:url"]', {
    property: 'og:url',
    content: snapshot.canonicalUrl,
  });
  ensureMeta('meta[property="og:image"]', {
    property: 'og:image',
    content: ogImageUrl,
  });
  ensureMeta('meta[property="og:image:alt"]', {
    property: 'og:image:alt',
    content: `${snapshot.poolName} Blocknet mining pool overview`,
  });
  ensureMeta('meta[name="twitter:card"]', {
    name: 'twitter:card',
    content: 'summary_large_image',
  });
  ensureMeta('meta[name="twitter:title"]', {
    name: 'twitter:title',
    content: snapshot.title,
  });
  ensureMeta('meta[name="twitter:description"]', {
    name: 'twitter:description',
    content: snapshot.description,
  });
  ensureMeta('meta[name="twitter:image"]', {
    name: 'twitter:image',
    content: ogImageUrl,
  });

  ensureLink('link[rel="canonical"]', {
    rel: 'canonical',
    href: snapshot.canonicalUrl,
  });

  let schemaNode = document.head.querySelector<HTMLScriptElement>('#structured-data');
  if (!schemaNode) {
    schemaNode = document.createElement('script');
    schemaNode.id = 'structured-data';
    schemaNode.type = 'application/ld+json';
    document.head.appendChild(schemaNode);
  }
  schemaNode.textContent = JSON.stringify(snapshot.structuredData);
}
