// Pool announcements, newest first. Text lives in the message catalogs under each item's keys;
// `href` is a pool route (localized by the caller), a blog path or an external https link.
export interface NewsItem {
  /** Publication day, `YYYY-MM-DD` (UTC). */
  date: string;
  titleKey: string;
  bodyKey: string;
  linkKey: string;
  href: string;
  /** The tab this item announces; it carries a "new" dot until opened. */
  feature?: Feature;
}

export type Feature = 'settings';

export const NEWS: readonly NewsItem[] = [
  { date: '2026-11-08', titleKey: 'newsGiveawayTitle', bodyKey: 'newsGiveawayBody', linkKey: 'newsGiveawayLink',
    href: 'https://x.com/bntpool' },
  { date: '2026-11-04', titleKey: 'newsSettingsTitle', bodyKey: 'newsSettingsBody', linkKey: 'newsSettingsLink',
    href: '/stats#settings', feature: 'settings' },
  { date: '2026-11-04', titleKey: 'newsSoloTitle', bodyKey: 'newsSoloBody', linkKey: 'newsSoloLink',
    href: '/blog/solo-mine-blocknet-on-bntpool/' },
  { date: '2026-11-02', titleKey: 'newsMinPayoutTitle', bodyKey: 'newsMinPayoutBody', linkKey: 'newsMinPayoutLink',
    href: '/info#minimum-payout' },
];

const DAY_MS = 86_400_000;
/** Days an item stays in the news strip and keeps its "New" marks. */
export const NEWS_FRESH_DAYS = 14;
/** Most items the strip shows at once, one line each. */
export const STRIP_LIMIT = 2;

// The titleKeys of dismissed items, as a JSON array.
export const DISMISSED_KEY = 'pool_news_dismissed_v2';
const SEEN_KEY = (feature: Feature) => `pool_news_seen_${feature}_v1`;

export function newsTime(item: NewsItem): number {
  return Date.parse(`${item.date}T00:00:00Z`);
}

export function isFresh(item: NewsItem, now = Date.now()): boolean {
  const age = now - newsTime(item);
  return age >= 0 && age < NEWS_FRESH_DAYS * DAY_MS;
}

/** Items published by `now`, newest first. */
export function publishedNews(now = Date.now(), items: readonly NewsItem[] = NEWS): NewsItem[] {
  return items.filter((item) => newsTime(item) <= now);
}

/**
 * The items the news strip shows: fresh ones the visitor hasn't dismissed and that aren't older than
 * the newest dismissed item, newest first, at most STRIP_LIMIT. Dismissing never brings older news up.
 */
export function stripNews(now = Date.now(), dismissed: readonly string[] = readDismissed(),
  items: readonly NewsItem[] = NEWS): NewsItem[] {
  const newestDismissed = items.reduce<string | null>((latest, item) =>
    dismissed.includes(item.titleKey) && (latest == null || item.date > latest) ? item.date : latest, null);
  return publishedNews(now, items)
    .filter((item) => isFresh(item, now) && !dismissed.includes(item.titleKey)
      && (newestDismissed == null || item.date >= newestDismissed))
    .slice(0, STRIP_LIMIT);
}

function storageGet(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function storageSet(key: string, value: string) {
  try {
    localStorage.setItem(key, value);
  } catch {
    // Blocked storage: the strip or dot comes back on the next visit.
  }
}

function readDismissed(): string[] {
  try {
    const value: unknown = JSON.parse(storageGet(DISMISSED_KEY) ?? '[]');
    return Array.isArray(value) ? value.filter((key): key is string => typeof key === 'string') : [];
  } catch {
    return [];
  }
}

/**
 * Hides every item published so far, shown or not; only items published later appear. Keys of items
 * no longer in the catalog are dropped so the list stays short.
 */
export function dismissStripNews(now = Date.now(), items: readonly NewsItem[] = NEWS) {
  const known = new Set(items.map((item) => item.titleKey));
  const keys = new Set([...readDismissed().filter((key) => known.has(key)),
    ...publishedNews(now, items).map((item) => item.titleKey)]);
  storageSet(DISMISSED_KEY, JSON.stringify([...keys]));
}

/** Whether a tab should carry the "new" dot: a fresh item announces it and it hasn't been opened since. */
export function featureIsNew(feature: Feature, now = Date.now(), seen: string | null = storageGet(SEEN_KEY(feature)),
  items: readonly NewsItem[] = NEWS): boolean {
  const item = publishedNews(now, items).find((entry) => entry.feature === feature);
  return item != null && isFresh(item, now) && (seen == null || seen < item.date);
}

export function markFeatureSeen(feature: Feature, now = Date.now()) {
  storageSet(SEEN_KEY(feature), new Date(now).toISOString().slice(0, 10));
}
