import test from 'node:test';
import assert from 'node:assert/strict';
import { metadataForPath } from '../src/lib/page-meta.ts';

test('each public route has distinct metadata and a normalized canonical', () => {
  const paths = ['/', '/start', '/blocks', '/payouts', '/stats', '/luck', '/status', '/checkpoints'];
  const titles = new Set(), descriptions = new Set();
  for (const path of paths) {
    const meta = metadataForPath(path === '/' ? path : path + '/', 'https://bntpool.com');
    assert.equal(meta.canonical, 'https://bntpool.com' + path);
    assert.equal(meta.robots, 'index, follow');
    assert.ok(meta.guides.length);
    titles.add(meta.title); descriptions.add(meta.description);
  }
  assert.equal(titles.size, paths.length);
  assert.equal(descriptions.size, paths.length);
});

test('private and unknown routes are noindex; returning to a public page restores indexing', () => {
  for (const path of ['/admin', '/made-up', '/toString', '/__proto__']) {
    assert.equal(metadataForPath(path, 'https://bntpool.com').robots, 'noindex, follow');
  }
  const publicPage = metadataForPath('/start', 'https://bntpool.com');
  assert.equal(publicPage.robots, 'index, follow');
  assert.equal(publicPage.imageUrl, 'https://bntpool.com/ui-assets/og-start.png');
});
