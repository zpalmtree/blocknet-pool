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

test('all public language canonicals and reciprocal alternates exclude unrelated queries', async () => {
  const { LOCALES } = await import('../src/i18n/locales.mjs');
  const { readFileSync } = await import('node:fs');
  const pages = JSON.parse(readFileSync(new URL('../src/lib/page-meta.json', import.meta.url), 'utf8'));
  for (const locale of LOCALES) {
    const messages = JSON.parse(readFileSync(new URL(`../src/i18n/messages/${locale.code}.json`, import.meta.url), 'utf8'));
    const translate = key => messages[key];
    for (const [path, source] of Object.entries(pages).filter(([, meta]) => meta.index)) {
      const meta = metadataForPath(path, 'https://pool.example', locale.code, translate);
      const canonical = `https://pool.example${path}${locale.code === 'en' ? '' : `?lang=${locale.code}`}`;
      assert.equal(meta.canonical, canonical);
      assert.equal(meta.title, locale.code === 'en' ? source.title : messages[source.titleKey]);
      assert.equal(meta.description, locale.code === 'en' ? source.description : messages[source.descriptionKey]);
      assert.equal(meta.alternates.length, 21);
      assert.equal(meta.alternates.find(item => item.language === locale.htmlLang).url, canonical);
      assert.equal(meta.alternates.find(item => item.language === 'x-default').url, `https://pool.example${path}`);
    }
    for (const path of ['/admin', '/unknown']) {
      const meta = metadataForPath(path, 'https://pool.example', locale.code, translate);
      assert.equal(meta.robots, 'noindex, follow');
      assert.deepEqual(meta.alternates, []);
      assert.equal(meta.canonical, `https://pool.example${path}`);
    }
  }
});
