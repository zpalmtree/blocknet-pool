// Pool announcements, newest first. Text lives in the message catalogs under each item's keys;
// `href` is a pool route (localized by the caller) or a blog path.
export interface NewsItem {
  /** Publication day, `YYYY-MM-DD` (UTC). */
  date: string;
  titleKey: string;
  bodyKey: string;
  /** One short sentence for the news strip under the menu. */
  shortKey: string;
  linkKey: string;
  href: string;
  /** The tab this item announces; it carries a "new" dot until opened. */
  feature?: Feature;
}

export type Feature = 'settings';

export const NEWS: readonly NewsItem[] = [
  { date: '2026-11-04', titleKey: 'newsSettingsTitle', bodyKey: 'newsSettingsBody', shortKey: 'newsSettingsShort', linkKey: 'newsSettingsLink',
    href: '/stats#settings', feature: 'settings' },
  { date: '2026-11-04', titleKey: 'newsSoloTitle', bodyKey: 'newsSoloBody', shortKey: 'newsSoloShort', linkKey: 'newsSoloLink',
    href: '/blog/solo-mine-blocknet-on-bntpool/' },
  { date: '2026-11-02', titleKey: 'newsMinPayoutTitle', bodyKey: 'newsMinPayoutBody', shortKey: 'newsMinPayoutShort', linkKey: 'newsMinPayoutLink',
    href: '/info#minimum-payout' },
];

const DAY_MS = 86_400_000;
/** Days an item stays in the news strip and keeps its "New" marks. */
export const NEWS_FRESH_DAYS = 30;

const DISMISSED_KEY = 'pool_news_dismissed_v1';
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
 * The item the news strip shows: the newest fresh one published after the day the visitor last
 * dismissed the strip, or null.
 */
export function stripNews(now = Date.now(), dismissedThrough: string | null = readDismissed(),
  items: readonly NewsItem[] = NEWS): NewsItem | null {
  return publishedNews(now, items)
    .find((item) => isFresh(item, now) && (dismissedThrough == null || item.date > dismissedThrough)) ?? null;
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
    // Blocked storage: the section or dot comes back on the next visit.
  }
}

function readDismissed(): string | null {
  const value = storageGet(DISMISSED_KEY);
  return value && /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : null;
}

/** Hides the strip until an item newer than the dismissed one is published. */
export function dismissStripNews(item: NewsItem) {
  storageSet(DISMISSED_KEY, item.date);
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
