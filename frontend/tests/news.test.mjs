import test from 'node:test';
import assert from 'node:assert/strict';
import { DISMISSED_KEY, NEWS, NEWS_FRESH_DAYS, STRIP_LIMIT, dismissStripNews, featureIsNew, isFresh, publishedNews, stripNews } from '../src/lib/news.ts';

const at = (day, hour = 12) => Date.parse(`${day}T${String(hour).padStart(2, '0')}:00:00Z`);
const item = (date, extra = {}) => ({ date, titleKey: `t${date}`, bodyKey: 'b', linkKey: 'l', href: '/', ...extra });
const ITEMS = [item('2026-12-20'), item('2026-12-10', { feature: 'settings' }), item('2026-12-05'), item('2026-11-01')];

test('the catalog is newest first with valid dates', () => {
  for (let index = 0; index < NEWS.length; index += 1) {
    assert.match(NEWS[index].date, /^\d{4}-\d{2}-\d{2}$/);
    assert.ok(!Number.isNaN(Date.parse(NEWS[index].date)));
    if (index > 0) assert.ok(NEWS[index - 1].date >= NEWS[index].date, NEWS[index].titleKey);
  }
});

test('items stay fresh for the window from their day', () => {
  const news = item('2026-12-01');
  assert.equal(isFresh(news, at('2026-11-30')), false, 'not yet published');
  assert.equal(isFresh(news, at('2026-12-01', 0)), true);
  assert.equal(isFresh(news, at('2026-12-14', 23)), true, 'last day');
  assert.equal(isFresh(news, at('2026-12-15', 0)), false, 'gone after two weeks');
  assert.equal(isFresh(news, Date.parse('2026-12-01T00:00:00Z') + NEWS_FRESH_DAYS * 86_400_000), false);
});

test('unpublished items are hidden everywhere', () => {
  assert.deepEqual(publishedNews(at('2026-12-07'), ITEMS).map((news) => news.date), ['2026-12-05', '2026-11-01']);
});

const dates = (items) => items.map((news) => news.date);

test('the strip shows the newest fresh items, up to the limit', () => {
  assert.equal(STRIP_LIMIT, 2);
  assert.deepEqual(dates(stripNews(at('2026-12-21'), [], ITEMS)), ['2026-12-20', '2026-12-10']);
  assert.deepEqual(dates(stripNews(at('2026-12-07'), [], ITEMS)), ['2026-12-05'], 'unpublished items skipped');
  assert.deepEqual(stripNews(at('2027-02-01'), [], ITEMS), [], 'nothing fresh: no strip');
});

test('dismissing never brings older news into the strip', () => {
  // Visitors who dismissed only the two shown items (before every published item was recorded).
  assert.deepEqual(stripNews(at('2026-12-21'), ['t2026-12-20', 't2026-12-10'], ITEMS), []);
  assert.deepEqual(stripNews(at('2026-12-12'), ['t2026-12-10'], ITEMS), [], 'older 12-05 stays hidden');
  const sameDay = [item('2026-12-20'), item('2026-12-10', { titleKey: 'later same day' }), item('2026-12-10')];
  assert.deepEqual(stripNews(at('2026-12-21'), ['t2026-12-10'], sameDay).map((news) => news.titleKey),
    ['t2026-12-20', 'later same day'], 'a new item on the dismissed day still shows');
});

test('dismissed items stay hidden and later ones still show', () => {
  const dismissed = ['t2026-12-10', 't2026-12-05'];
  assert.deepEqual(stripNews(at('2026-12-12'), dismissed, ITEMS), []);
  assert.deepEqual(dates(stripNews(at('2026-12-21'), dismissed, ITEMS)), ['2026-12-20']);
});

test('the shipped catalog shows its two newest items together', () => {
  const shown = stripNews(Date.parse(`${NEWS[0].date}T12:00:00Z`), [], NEWS);
  assert.deepEqual(shown.map((news) => news.titleKey), NEWS.slice(0, STRIP_LIMIT).map((news) => news.titleKey));
});

test('a feature is new while its item is fresh and until it is opened', () => {
  assert.equal(featureIsNew('settings', at('2026-12-09'), null, ITEMS), false, 'not announced yet');
  assert.equal(featureIsNew('settings', at('2026-12-11'), null, ITEMS), true);
  assert.equal(featureIsNew('settings', at('2026-12-11'), '2026-12-10', ITEMS), false, 'opened on the day');
  assert.equal(featureIsNew('settings', at('2026-12-11'), '2026-11-20', ITEMS), true, 'opened before the announcement');
  assert.equal(featureIsNew('settings', at('2027-01-20'), null, ITEMS), false, 'stale');
});

test('dismissing records every published item, shown or not', () => {
  const store = new Map();
  globalThis.localStorage = { getItem: (key) => store.get(key) ?? null, setItem: (key, value) => store.set(key, String(value)) };
  try {
    dismissStripNews(at('2026-12-21'), ITEMS);
    const dismissed = JSON.parse(store.get(DISMISSED_KEY));
    assert.deepEqual(dismissed.sort(), ITEMS.map((news) => news.titleKey).sort());
    assert.deepEqual(stripNews(at('2026-12-22'), dismissed, ITEMS), []);
    const later = [item('2026-12-22'), ...ITEMS];
    assert.deepEqual(dates(stripNews(at('2026-12-23'), dismissed, later)), ['2026-12-22']);
  } finally {
    delete globalThis.localStorage;
  }
});
