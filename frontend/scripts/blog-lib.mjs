// Shared pieces of the bntpool.com blog: locales, interface strings, post loading.
// Posts live in frontend/blog/content/<slug>/index.md (English) with one <locale>.md translation
// per locale beside them; the format matches the seineminer.com blog (see blog/README.md).
import { createHash } from 'node:crypto';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { parse } from 'yaml';

export const BLOG_DIR = new URL('../blog/', import.meta.url);
export const CONTENT_DIR = new URL('content/', BLOG_DIR);
export const BASE_URL = process.env.BLOG_BASE_URL || 'https://bntpool.com';

// The same 20 locales as seineminer.com and Sol Incinerator. Adding one: add it here, add
// blog/messages/<code>.json, and translate every published post.
export { LOCALES } from '../src/i18n/locales.mjs';
import { LOCALES } from '../src/i18n/locales.mjs';
export const TRANSLATED = LOCALES.filter((l) => l.path);

/** Site path of a blog page: "/blog/", "/blog/<slug>/", "/blog/zh-hans/<slug>/". */
export function blogPath(locale, slug = '') {
  const prefix = LOCALES.find((l) => l.code === locale).path;
  return `/blog/${prefix ? `${prefix}/` : ''}${slug ? `${slug}/` : ''}`;
}

// Interface strings: blog/messages/<code>.json, every file with exactly en.json's keys.
const CATALOGS = Object.fromEntries(LOCALES.map(({ code }) => {
  const url = new URL(`messages/${code}.json`, BLOG_DIR);
  if (!existsSync(url)) throw new Error(`missing blog/messages/${code}.json`);
  return [code, JSON.parse(readFileSync(url, 'utf8'))];
}));
for (const [code, catalog] of Object.entries(CATALOGS)) {
  const missing = Object.keys(CATALOGS.en).filter((k) => !catalog[k]);
  const extra = Object.keys(catalog).filter((k) => !(k in CATALOGS.en));
  if (missing.length || extra.length) throw new Error(`blog/messages/${code}.json: missing [${missing.join(', ')}] extra [${extra.join(', ')}]`);
}

export function t(locale, key, vars = {}) {
  return CATALOGS[locale][key].replace(/\{(\w+)\}/g, (_, name) => String(vars[name] ?? `{${name}}`));
}

// The draft flag is left out so publishing a post doesn't make its translations stale.
export const sourceHash = (raw) => createHash('sha256').update(raw.replace(/^draft:.*\n/m, '')).digest('hex').slice(0, 12);

/** Read one post file; null when it doesn't exist. */
export function readPost(slug, file) {
  const url = new URL(`${slug}/${file}`, CONTENT_DIR);
  if (!existsSync(url)) return null;
  const raw = readFileSync(url, 'utf8');
  const m = raw.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
  if (!m) return { slug, file, raw, error: 'missing front matter' };
  try {
    return { slug, file, raw, data: parse(m[1]) ?? {}, body: m[2] };
  } catch (e) {
    return { slug, file, raw, error: `front matter is not valid YAML: ${e.message}` };
  }
}

export function listSlugs() {
  if (!existsSync(CONTENT_DIR)) return [];
  return readdirSync(CONTENT_DIR, { withFileTypes: true }).filter((d) => d.isDirectory()).map((d) => d.name).sort();
}

// Scripts written without spaces between words are counted by character.
const CJK = /[\u3040-\u30ff\u3400-\u9fff\uf900-\ufaff\uac00-\ud7af]/g;
const THAI = /[\u0e00-\u0e7f]/g;
export function readingMinutes(body) {
  const text = body.replace(/```[\s\S]*?```/g, ' ').replace(/<[^>]+>/g, ' ');
  const cjk = (text.match(CJK) ?? []).length;
  const thai = (text.match(THAI) ?? []).length;
  const words = (text.replace(CJK, ' ').replace(THAI, ' ').match(/[\p{L}\p{N}][\p{L}\p{N}\p{M}'’.-]*/gu) ?? []).length;
  return Math.max(1, Math.round(words / 230 + cjk / 400 + thai / 1000));
}
