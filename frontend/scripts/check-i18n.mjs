// Keep the pool's advertised languages complete and interpolation-safe.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, readdirSync } from 'node:fs';
import { LOCALES } from '../src/i18n/locales.mjs';

const root = new URL('../src/i18n/', import.meta.url);
const dir = new URL('messages/', root);
const source = readFileSync(new URL('en.json', dir), 'utf8');
const english = JSON.parse(source);
const revision = JSON.parse(readFileSync(new URL('translation-source.json', root), 'utf8'));
assert.equal(createHash('sha256').update(source).digest('hex'), revision.sha256,
  'English changed: update the translated catalogs and translation-source.json together.');
assert.deepEqual(readdirSync(dir).filter((file) => file.endsWith('.json')).sort(), LOCALES.map(({ code }) => `${code}.json`).sort());
for (const { code } of LOCALES) {
  const catalog = JSON.parse(readFileSync(new URL(`${code}.json`, dir), 'utf8'));
  assert.deepEqual(Object.keys(catalog).sort(), Object.keys(english).sort(), code);
  for (const [key, message] of Object.entries(catalog)) {
    assert.equal(typeof message, 'string', `${code}:${key}`);
    assert.ok(message.trim(), `${code}:${key}`);
    assert.deepEqual(message.match(/\{\d+\}/g)?.sort() ?? [], english[key].match(/\{\d+\}/g)?.sort() ?? [], `${code}:${key}`);
    assert.ok(!/<script|<iframe|javascript:/i.test(message), `${code}:${key}`);
    if (code !== 'en' && english[key].length > 25) {
      assert.notEqual(message, english[key], `${code}:${key}: sentence left in English`);
    }
    const protectedTerms = english[key].match(/\b(?:Blocknet|seine|PPLNS|API|CPU|GPU|SHA-256|TUI|BNT)\b/gi) ?? [];
    for (const term of protectedTerms) {
      assert.ok(message.toLowerCase().includes(term.toLowerCase()), `${code}:${key}: missing ${term}`);
    }
  }
}
console.log(`Pool translations: ${Object.keys(english).length} messages across ${LOCALES.length} languages OK.`);
