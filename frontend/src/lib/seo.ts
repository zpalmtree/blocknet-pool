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
  ogImageAlt: string;
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

function formatDecimal(value: number): string {
  return value.toFixed(2).replace(/\.?0+$/, '');
}

function formatBnt(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '0 BNT';
  return `${formatDecimal(value)} BNT`;
}

function poolFeeSummary(poolInfo: InfoResponse | null): string {
  const parts: string[] = [];
  if (poolInfo?.pool_fee_pct != null && poolInfo.pool_fee_pct > 0) {
    parts.push(`${formatDecimal(poolInfo.pool_fee_pct)}% fee`);
  }
  if (poolInfo?.pool_fee_flat != null && poolInfo.pool_fee_flat > 0) {
    parts.push(`${formatBnt(poolInfo.pool_fee_flat)} flat fee`);
  }
  return parts.length ? parts.join(' + ') : '0% fee';
}

function payoutRulesSummary(poolInfo: InfoResponse | null): string {
  const minPayout = formatBnt(poolInfo?.min_payout_amount);
  const confirmations = poolInfo?.blocks_before_payout ?? 0;
  return `${minPayout} minimum payout after ${confirmations} confirmations`;
}

function startFaqEntries(poolInfo: InfoResponse | null): Array<{ question: string; answer: string }> {
  return [
    {
      question: 'What miner should I use to mine Blocknet?',
      answer: 'Use Seine, then point it at the pool stratum endpoint and connect your Blocknet payout address.',
    },
    {
      question: 'What pool URL should I enter?',
      answer: `Use ${stratumUrl(poolInfo?.stratum_port, poolInfo?.pool_url)} as the pool URL in Seine or any compatible Blocknet mining configuration.`,
    },
    {
      question: 'How do payouts work on this Blocknet pool?',
      answer: `${(poolInfo?.payout_scheme || 'pplns').toUpperCase()} payouts are used here, with ${payoutRulesSummary(poolInfo)}.`,
    },
    {
      question: 'How can I verify pool activity before mining?',
      answer: 'Review the public dashboard, recent blocks, payout batches, and status page before directing any hashpower to the pool.',
    },
  ];
}

function descriptionFor(route: Route, poolInfo: InfoResponse | null): string {
  const name = poolNameFor(poolInfo);
  const stratum = stratumUrl(poolInfo?.stratum_port, poolInfo?.pool_url);
  if (route === 'start') {
    return `Start mining Blocknet with ${name}. Copy ${stratum}, mine with Seine, and review ${(poolInfo?.payout_scheme || 'pplns').toUpperCase()} payouts, ${poolFeeSummary(poolInfo)}, and ${payoutRulesSummary(poolInfo)}.`;
  }
  if (route === 'blocks') {
    return `Browse recent Blocknet blocks found by ${name}, including confirmed, pending, and orphaned rounds with explorer links, rewards, and timing.`;
  }
  if (route === 'payouts') {
    return `Review recent Blocknet pool payouts from ${name} with recipient counts, payout totals, network fees, and explorer transaction links.`;
  }
  if (route === 'luck') {
    return `Track ${name} pool luck with round effort, expected block timing, round duration, and recent confirmed or orphaned Blocknet rounds.`;
  }
  if (route === 'status') {
    return `Monitor ${name} uptime, daemon reachability, sync state, incident history, and current chain height from the public Blocknet pool status page.`;
  }
  if (route === 'stats') {
    return `Look up a Blocknet wallet address on ${name} to inspect hashrate, balances, worker activity, recent payouts, and share history.`;
  }
  if (route === 'admin') {
    return `Administrative dashboard for ${name} with miners, payouts, fees, health checks, and daemon log streaming.`;
  }
  return `Live Blocknet mining pool dashboard for ${name} with stratum URL ${stratum}, public blocks, payout batches, round luck, and real-time pool status.`;
}

function titleFor(route: Route, poolInfo: InfoResponse | null): string {
  const name = poolNameFor(poolInfo);
  if (route === 'start') return `Mine Blocknet With Seine | Setup, Stratum & Payouts | ${name}`;
  if (route === 'blocks') return `Recent Blocknet Blocks | Confirmed, Pending & Orphaned | ${name}`;
  if (route === 'payouts') return `Blocknet Pool Payouts | Recent Transactions | ${name}`;
  if (route === 'luck') return `Blocknet Pool Luck | Round Effort History | ${name}`;
  if (route === 'status') return `Blocknet Pool Status | Uptime & Daemon Health | ${name}`;
  if (route === 'stats') return `Blocknet Miner Stats Lookup | ${name}`;
  if (route === 'admin') return `Pool Admin Dashboard | ${name}`;
  return `Blocknet Mining Pool | Live Hashrate, Blocks & Payouts | ${name}`;
}

function ogImageAltFor(poolInfo: InfoResponse | null): string {
  return `${poolNameFor(poolInfo)} Blocknet mining pool card with stratum, fee, and payout details`;
}

function pageSchemaType(route: Route): string {
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
        text: 'Run the miner, then use the pool dashboard and payout pages to verify hashrate, rounds, and payment history.',
      },
    ],
  };
}

function buildFaqSchema(poolInfo: InfoResponse | null): object {
  return {
    '@context': 'https://schema.org',
    '@type': 'FAQPage',
    mainEntity: startFaqEntries(poolInfo).map((entry) => ({
      '@type': 'Question',
      name: entry.question,
      acceptedAnswer: {
        '@type': 'Answer',
        text: entry.answer,
      },
    })),
  };
}

function buildStructuredData(route: Route, poolInfo: InfoResponse | null, snapshot: Omit<SeoSnapshot, 'structuredData'>): object[] {
  const baseUrl = baseUrlFor(poolInfo);
  const websiteId = `${baseUrl}/#website`;
  const organizationId = `${baseUrl}/#organization`;
  const serviceId = `${baseUrl}/#service`;
  const pageId = `${snapshot.canonicalUrl}#webpage`;
  const breadcrumbId = `${snapshot.canonicalUrl}#breadcrumb`;
  const pageType = pageSchemaType(route);
  const items: object[] = [
    {
      '@context': 'https://schema.org',
      '@type': 'Organization',
      '@id': organizationId,
      name: snapshot.poolName,
      url: baseUrl,
      logo: {
        '@type': 'ImageObject',
        url: `${baseUrl}/favicon.svg`,
      },
    },
    {
      '@context': 'https://schema.org',
      '@type': 'WebSite',
      '@id': websiteId,
      name: snapshot.poolName,
      url: baseUrl,
      description: descriptionFor('dashboard', poolInfo),
      publisher: { '@id': organizationId },
      inLanguage: 'en-US',
    },
    {
      '@context': 'https://schema.org',
      '@type': 'Service',
      '@id': serviceId,
      name: `${snapshot.poolName} Blocknet mining pool`,
      serviceType: 'Cryptocurrency mining pool',
      provider: { '@id': organizationId },
      url: baseUrl,
      description: descriptionFor('dashboard', poolInfo),
      areaServed: 'Worldwide',
      offers: {
        '@type': 'Offer',
        description: `${(poolInfo?.payout_scheme || 'pplns').toUpperCase()} payouts, ${poolFeeSummary(poolInfo)}, ${payoutRulesSummary(poolInfo)}`,
      },
    },
    {
      '@context': 'https://schema.org',
      '@type': pageType,
      '@id': pageId,
      name: snapshot.title,
      url: snapshot.canonicalUrl,
      description: snapshot.description,
      inLanguage: 'en-US',
      isPartOf: {
        '@id': websiteId,
      },
      about: {
        '@id': serviceId,
      },
      primaryImageOfPage: {
        '@type': 'ImageObject',
        url: `${baseUrl}${OG_IMAGE_PATH}`,
      },
    },
  ];

  if (route === 'start') {
    items.push(buildHowToSchema(poolInfo, snapshot.canonicalUrl));
    items.push(buildFaqSchema(poolInfo));
  }

  if (route !== 'dashboard') {
    items.push({
      '@context': 'https://schema.org',
      '@type': 'BreadcrumbList',
      '@id': breadcrumbId,
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
  const ogImageAlt = ogImageAltFor(poolInfo);

  const snapshotBase = {
    title,
    description,
    canonicalUrl,
    robots,
    poolName,
    ogImageAlt,
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
  ensureMeta('meta[property="og:locale"]', {
    property: 'og:locale',
    content: 'en_US',
  });
  ensureMeta('meta[property="og:image"]', {
    property: 'og:image',
    content: ogImageUrl,
  });
  ensureMeta('meta[property="og:image:type"]', {
    property: 'og:image:type',
    content: 'image/svg+xml',
  });
  ensureMeta('meta[property="og:image:width"]', {
    property: 'og:image:width',
    content: '1200',
  });
  ensureMeta('meta[property="og:image:height"]', {
    property: 'og:image:height',
    content: '630',
  });
  ensureMeta('meta[property="og:image:alt"]', {
    property: 'og:image:alt',
    content: snapshot.ogImageAlt,
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
  ensureMeta('meta[name="twitter:image:alt"]', {
    name: 'twitter:image:alt',
    content: snapshot.ogImageAlt,
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
