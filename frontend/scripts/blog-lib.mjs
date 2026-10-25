// Shared pieces of the bntpool.com blog: locales, interface strings, post loading.
// Posts live in frontend/blog/content/<slug>/index.md (English) with zh-Hans.md and zh-Hant.md
// translations beside them; the format matches the seineminer.com blog (see blog/README.md).
import { createHash } from 'node:crypto';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { parse } from 'yaml';

export const BLOG_DIR = new URL('../blog/', import.meta.url);
export const CONTENT_DIR = new URL('content/', BLOG_DIR);
export const BASE_URL = process.env.BLOG_BASE_URL || 'https://bntpool.com';

export const LOCALES = [
  { code: 'en', path: null, htmlLang: 'en', ogLocale: 'en_US', short: 'EN', endonym: 'English' },
  { code: 'zh-Hans', path: 'zh-hans', htmlLang: 'zh-Hans', ogLocale: 'zh_CN', short: '简', endonym: '简体中文' },
  { code: 'zh-Hant', path: 'zh-hant', htmlLang: 'zh-Hant', ogLocale: 'zh_TW', short: '繁', endonym: '繁體中文' },
];
export const TRANSLATED = LOCALES.filter((l) => l.path);

/** Site path of a blog page: "/blog/", "/blog/<slug>/", "/blog/zh-hans/<slug>/". */
export function blogPath(locale, slug = '') {
  const prefix = LOCALES.find((l) => l.code === locale).path;
  return `/blog/${prefix ? `${prefix}/` : ''}${slug ? `${slug}/` : ''}`;
}

const en = {
  navDashboard: 'Dashboard', navStart: 'Get started', navBlog: 'Blog', navStats: 'My stats', navLanguage: 'Language',
  blogTitle: 'Blog', blogSub: 'Guides to mining Blocknet on bntpool.com.',
  blogMetaTitle: 'Blocknet mining pool guides · bntpool.com blog',
  blogMetaDescription: 'Guides to mining Blocknet on bntpool.com: getting started, PPLNS payouts, your miner stats, and the Blocknet coin itself.',
  blogEmpty: 'No posts yet.', breadcrumb: 'Breadcrumb', updated: 'Updated', minutes: '{n} min read',
  faq: 'Frequently asked questions', related: 'Related reading',
  ctaTitle: 'Start mining on bntpool.com', ctaStart: 'Get started', ctaSeine: 'Download seine',
  answer: 'Short answer', note: 'Note', warning: 'Warning', rssTitle: 'bntpool.com blog',
  footer: 'Blocknet mining pool',
};
const zhHans = {
  navDashboard: '仪表盘', navStart: '快速开始', navBlog: '博客', navStats: '我的统计', navLanguage: '语言',
  blogTitle: '博客', blogSub: '在 bntpool.com 上挖 Blocknet 的指南。',
  blogMetaTitle: 'Blocknet 矿池指南 · bntpool.com 博客',
  blogMetaDescription: '在 bntpool.com 上挖 Blocknet 的指南：快速开始、PPLNS 收益、矿工统计，以及 Blocknet 本身。',
  blogEmpty: '暂无文章。', breadcrumb: '面包屑导航', updated: '更新于', minutes: '{n} 分钟阅读',
  faq: '常见问题', related: '相关阅读',
  ctaTitle: '在 bntpool.com 开始挖矿', ctaStart: '快速开始', ctaSeine: '下载 seine',
  answer: '简短回答', note: '说明', warning: '警告', rssTitle: 'bntpool.com 博客',
  footer: 'Blocknet 矿池',
};
const zhHant = {
  navDashboard: '儀表板', navStart: '快速開始', navBlog: '部落格', navStats: '我的統計', navLanguage: '語言',
  blogTitle: '部落格', blogSub: '在 bntpool.com 上挖 Blocknet 的指南。',
  blogMetaTitle: 'Blocknet 礦池指南 · bntpool.com 部落格',
  blogMetaDescription: '在 bntpool.com 上挖 Blocknet 的指南：快速開始、PPLNS 收益、礦工統計，以及 Blocknet 本身。',
  blogEmpty: '尚無文章。', breadcrumb: '導覽路徑', updated: '更新於', minutes: '{n} 分鐘閱讀',
  faq: '常見問題', related: '相關閱讀',
  ctaTitle: '在 bntpool.com 開始挖礦', ctaStart: '快速開始', ctaSeine: '下載 seine',
  answer: '簡短回答', note: '說明', warning: '警告', rssTitle: 'bntpool.com 部落格',
  footer: 'Blocknet 礦池',
};
const CATALOGS = { en, 'zh-Hans': zhHans, 'zh-Hant': zhHant };
for (const [code, catalog] of Object.entries(CATALOGS)) {
  const missing = Object.keys(en).filter((k) => !catalog[k]);
  if (missing.length) throw new Error(`blog strings ${code}: missing ${missing.join(', ')}`);
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

const CJK = /[㐀-鿿豈-﫿]/g;
export function readingMinutes(body) {
  const text = body.replace(/```[\s\S]*?```/g, ' ').replace(/<[^>]+>/g, ' ');
  const cjk = (text.match(CJK) ?? []).length;
  const words = (text.replace(CJK, ' ').match(/[A-Za-z0-9][\w'’.-]*/g) ?? []).length;
  return Math.max(1, Math.round(words / 230 + cjk / 400));
}
