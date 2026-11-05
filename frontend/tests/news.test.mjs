import test from 'node:test';
import assert from 'node:assert/strict';
import { NEWS, NEWS_FRESH_DAYS, featureIsNew, isFresh, publishedNews, stripNews } from '../src/lib/news.ts';

const at = (day, hour = 12) => Date.parse(`${day}T${String(hour).padStart(2, '0')}:00:00Z`);
const item = (date, extra = {}) => ({ date, titleKey: `t${date}`, bodyKey: 'b', shortKey: 's', linkKey: 'l', href: '/', ...extra });
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
  assert.equal(isFresh(news, at('2026-12-30', 23)), true);
  assert.equal(isFresh(news, Date.parse('2026-12-01T00:00:00Z') + NEWS_FRESH_DAYS * 86_400_000), false);
});

test('unpublished items are hidden everywhere', () => {
  assert.deepEqual(publishedNews(at('2026-12-07'), ITEMS).map((news) => news.date), ['2026-12-05', '2026-11-01']);
});

test('the strip shows the newest fresh item', () => {
  assert.equal(stripNews(at('2026-12-21'), null, ITEMS)?.date, '2026-12-20');
  assert.equal(stripNews(at('2026-12-07'), null, ITEMS)?.date, '2026-12-05', 'unpublished items skipped');
  assert.equal(stripNews(at('2027-02-01'), null, ITEMS), null, 'nothing fresh: no strip');
});

test('dismissing hides the strip until a newer item arrives', () => {
  assert.equal(stripNews(at('2026-12-12'), '2026-12-10', ITEMS), null, 'older fresh items stay hidden');
  assert.equal(stripNews(at('2026-12-21'), '2026-12-10', ITEMS)?.date, '2026-12-20');
});

test('a feature is new while its item is fresh and until it is opened', () => {
  assert.equal(featureIsNew('settings', at('2026-12-09'), null, ITEMS), false, 'not announced yet');
  assert.equal(featureIsNew('settings', at('2026-12-11'), null, ITEMS), true);
  assert.equal(featureIsNew('settings', at('2026-12-11'), '2026-12-10', ITEMS), false, 'opened on the day');
  assert.equal(featureIsNew('settings', at('2026-12-11'), '2026-11-20', ITEMS), true, 'opened before the announcement');
  assert.equal(featureIsNew('settings', at('2027-01-20'), null, ITEMS), false, 'stale');
});
