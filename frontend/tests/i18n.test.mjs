import assert from 'node:assert/strict';
import { test } from 'node:test';
import { globSync, readFileSync } from 'node:fs';
import { LOCALES } from '../src/i18n/locales.mjs';
import { interpolate, languageUrl, matchLanguage, preferredLanguage } from '../src/i18n/core.ts';

test('browser language matching follows preference order and handles regional variants', () => {
  assert.equal(preferredLanguage(null, ['es-MX', 'en']), 'es');
  assert.equal(preferredLanguage(null, ['en-GB', 'es']), 'en');
  assert.equal(preferredLanguage('en', ['ar']), 'en');
  assert.equal(preferredLanguage('fr', ['ja']), 'fr');
  assert.equal(preferredLanguage('invalid', ['unsupported', 'de-AT']), 'de');
  assert.equal(preferredLanguage(null, ['unsupported']), null);
  for (const code of ['zh-TW', 'zh-HK', 'zh-Hant-TW', 'zh-MO']) assert.equal(matchLanguage(code), 'zh-Hant');
  for (const code of ['zh-CN', 'zh-SG', 'zh-Hans']) assert.equal(matchLanguage(code), 'zh-Hans');
  assert.equal(matchLanguage('pt-PT'), 'pt-BR');
});

test('language switches preserve the route, other query parameters and fragment', () => {
  assert.equal(languageUrl('/stats?worker=demo&lang=de#miner-workers', 'ar'), '/stats?worker=demo&lang=ar#miner-workers');
  assert.equal(languageUrl('/stats?lang=de&worker=demo#miner-workers', 'en'), '/stats?worker=demo#miner-workers');
  assert.equal(languageUrl('/', 'zh-Hant'), '/?lang=zh-Hant');
});

test('translated interpolation can reorder values without evaluating HTML or dollar substitutions', () => {
  assert.equal(interpolate('{1} / {0} / {1}', ['$&<script>', 123]), '123 / $&<script> / 123');
  assert.equal(interpolate('Missing {0}'), 'Missing {0}');
});

test('every advertised language has a complete catalog with matching placeholders', async () => {
  assert.equal(LOCALES.length, 20);
  await import('../scripts/check-i18n.mjs');
});

test('public application translation references exist in the English catalog', () => {
  const english = JSON.parse(readFileSync(new URL('../src/i18n/messages/en.json', import.meta.url), 'utf8'));
  const files = globSync('src/**/*.{ts,tsx}', { cwd: new URL('../', import.meta.url) });
  for (const file of files) {
    const source = readFileSync(new URL(`../${file}`, import.meta.url), 'utf8');
    for (const [, key] of source.matchAll(/\b(?:t|tx)\(['"]([^'"]+)['"]/g)) {
      assert.ok(key in english, `${file}: unknown translation ${key}`);
    }
  }
});
