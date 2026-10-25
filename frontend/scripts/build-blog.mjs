// Pre-render the bntpool.com blog into frontend/dist/blog. Runs after `vite build` (which empties
// dist). The API embeds these files and serves them under /blog/ (apps/blocknet-pool-api/src/blog.rs),
// so crawlers get real HTML. The app's CSP forbids inline scripts and styles, so pages load
// /blog/blog.css and /blog/theme.js; JSON-LD data blocks are not executed and are unaffected.
import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { Marked } from 'marked';
import {
  BASE_URL, BLOG_DIR, LOCALES, TRANSLATED, blogPath, listSlugs, readPost, readingMinutes, t,
} from './blog-lib.mjs';

const DIST = new URL('../dist/blog/', import.meta.url);
const OG_IMAGE = `${BASE_URL}/ui-assets/og-start.png`;
const ORG = { '@type': 'Organization', '@id': `${BASE_URL}/#org`, name: 'bntpool.com', url: `${BASE_URL}/` };

// Editorial and translation checks must pass before anything is written.
execFileSync(process.execPath, [new URL('./check-blog.mjs', import.meta.url).pathname], { stdio: 'inherit' });

const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const jsonLd = (d) => `<script type="application/ld+json">${JSON.stringify(d).replace(/</g, '\\u003c')}</script>`;
const iso = (d) => new Date(d).toISOString().slice(0, 10);
const nice = (d, lang) => new Intl.DateTimeFormat(lang, { year: 'numeric', month: 'long', day: 'numeric', timeZone: 'UTC' }).format(new Date(d));
const def = (code) => LOCALES.find((l) => l.code === code);

function renderMarkdown(body, locale) {
  // <Answer>, <Note> and <Warning> use the same syntax as the seineminer.com blog.
  const labelled = body.replace(/<(Answer|Note|Warning)>([\s\S]*?)<\/\1>/g, (_, kind, inner) => {
    const k = kind.toLowerCase();
    return `<div class="block ${k}"><div class="block-label">${esc(t(locale, k))}</div>\n\n${inner.trim()}\n\n</div>`;
  });
  const used = new Map();
  const marked = new Marked({ gfm: true });
  marked.use({
    renderer: {
      heading({ tokens, depth }) {
        const text = this.parser.parseInline(tokens);
        const base = text.replace(/<[^>]+>/g, '').toLowerCase().replace(/[^\p{L}\p{N}]+/gu, '-').replace(/^-|-$/g, '') || 'section';
        const n = used.get(base) ?? 0;
        used.set(base, n + 1);
        return `<h${depth} id="${n ? `${base}-${n}` : base}">${text}</h${depth}>\n`;
      },
    },
  });
  return marked.parse(labelled);
}

function page({ locale, slug = '', title, description, ogType = 'website', schema = [], head = '', body }) {
  const l = def(locale);
  const canonical = BASE_URL + blogPath(locale, slug);
  const alternates = LOCALES.map((x) => `<link rel="alternate" hreflang="${x.htmlLang}" href="${BASE_URL}${blogPath(x.code, slug)}">`).join('\n')
    + `\n<link rel="alternate" hreflang="x-default" href="${BASE_URL}${blogPath('en', slug)}">`;
  const langs = LOCALES.map((x) => `<a href="${blogPath(x.code, slug)}" hreflang="${x.htmlLang}" lang="${x.htmlLang}" title="${esc(x.endonym)}"${x.code === locale ? ' aria-current="true"' : ''}>${x.short}</a>`).join('');
  return `<!DOCTYPE html>
<html lang="${l.htmlLang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${esc(title)}</title>
<meta name="description" content="${esc(description)}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="${canonical}">
${alternates}
<meta property="og:type" content="${ogType}">
<meta property="og:site_name" content="bntpool.com">
<meta property="og:title" content="${esc(title)}">
<meta property="og:description" content="${esc(description)}">
<meta property="og:url" content="${canonical}">
<meta property="og:locale" content="${l.ogLocale}">
<meta property="og:image" content="${OG_IMAGE}">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="${esc(title)}">
<meta name="twitter:description" content="${esc(description)}">
<meta name="twitter:image" content="${OG_IMAGE}">
<link rel="alternate" type="application/rss+xml" title="${esc(t(locale, 'rssTitle'))}" href="${blogPath(locale)}rss.xml">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=Manrope:wght@400;500;600;700&display=swap">
<link rel="stylesheet" href="/blog/blog.css">
<script src="/blog/theme.js"></script>
${head}${schema.map(jsonLd).join('\n')}
</head>
<body>
<header class="nav"><div class="wrap">
  <a class="brand" href="/">bntpool.com</a>
  <nav aria-label="Main">
    <a class="hide-sm" href="/">${esc(t(locale, 'navDashboard'))}</a>
    <a class="hide-sm" href="/start">${esc(t(locale, 'navStart'))}</a>
    <a class="hide-sm" href="/stats">${esc(t(locale, 'navStats'))}</a>
    <a href="${blogPath(locale)}">${esc(t(locale, 'navBlog'))}</a>
    <span class="langs" role="group" aria-label="${esc(t(locale, 'navLanguage'))}">${langs}</span>
  </nav>
</div></header>
${body}
<footer><div class="wrap">
  <span>bntpool.com · ${esc(t(locale, 'footer'))}</span>
  <nav aria-label="Footer"><a href="/">${esc(t(locale, 'navDashboard'))}</a><a href="/start">${esc(t(locale, 'navStart'))}</a><a href="${blogPath(locale)}">${esc(t(locale, 'navBlog'))}</a><a href="https://seineminer.com">seine</a><a href="https://blocknetcrypto.com">Blocknet</a></nav>
</div></footer>
</body>
</html>
`;
}

function cta(locale) {
  const seine = locale === 'en' ? 'https://seineminer.com/#download' : `https://seineminer.com/${def(locale).path}/#download`;
  return `<aside class="cta"><b>${esc(t(locale, 'ctaTitle'))}</b><span><a class="btn primary" href="/start">${esc(t(locale, 'ctaStart'))}</a><a class="btn" href="${seine}">${esc(t(locale, 'ctaSeine'))}</a></span></aside>`;
}

function loadPosts(locale) {
  return listSlugs().map((slug) => {
    const src = readPost(slug, 'index.md');
    if (!src?.data || src.data.draft) return null;
    const entry = locale === 'en' ? src : readPost(slug, `${locale}.md`);
    const d = entry.data;
    return {
      slug, locale, title: d.title, seoTitle: d.seoTitle, description: d.description, tags: d.tags ?? [], faq: d.faq ?? [],
      date: src.data.date, updated: src.data.updated ?? src.data.date, related: src.data.related ?? [],
      body: entry.body, url: BASE_URL + blogPath(locale, slug), path: blogPath(locale, slug),
    };
  }).filter(Boolean).sort((a, b) => new Date(b.date) - new Date(a.date));
}

function postPage(p, bySlug) {
  const lang = def(p.locale).htmlLang;
  const blog = blogPath(p.locale);
  const meta = [`<time datetime="${iso(p.date)}">${nice(p.date, lang)}</time>`];
  if (iso(p.updated) !== iso(p.date)) meta.push(`${esc(t(p.locale, 'updated'))} <time datetime="${iso(p.updated)}">${nice(p.updated, lang)}</time>`);
  meta.push(esc(t(p.locale, 'minutes', { n: readingMinutes(p.body) })));
  const faq = p.faq.length ? `<section class="faq" id="faq"><h2>${esc(t(p.locale, 'faq'))}</h2>${p.faq.map((f) => `<h3>${esc(f.q)}</h3><p>${esc(f.a)}</p>`).join('')}</section>` : '';
  const rel = p.related.map((s) => bySlug.get(s)).filter(Boolean);
  const related = rel.length ? `<nav class="related" aria-label="${esc(t(p.locale, 'related'))}"><h2>${esc(t(p.locale, 'related'))}</h2><ul>${rel.map((r) => `<li><a href="${r.path}">${esc(r.title)}</a></li>`).join('')}</ul></nav>` : '';
  const schema = [
    { '@context': 'https://schema.org', '@type': 'BlogPosting', headline: p.title, description: p.description, datePublished: iso(p.date),
      dateModified: iso(p.updated), image: OG_IMAGE, inLanguage: lang, mainEntityOfPage: { '@type': 'WebPage', '@id': p.url },
      author: ORG, publisher: ORG, ...(p.tags.length ? { keywords: p.tags.join(', ') } : {}) },
    { '@context': 'https://schema.org', '@type': 'BreadcrumbList', itemListElement: [
      { '@type': 'ListItem', position: 1, name: 'bntpool.com', item: `${BASE_URL}/` },
      { '@type': 'ListItem', position: 2, name: t(p.locale, 'blogTitle'), item: BASE_URL + blog },
      { '@type': 'ListItem', position: 3, name: p.title, item: p.url }] },
  ];
  if (p.faq.length) schema.push({ '@context': 'https://schema.org', '@type': 'FAQPage', '@id': `${p.url}#faq`, inLanguage: lang,
    mainEntity: p.faq.map((f) => ({ '@type': 'Question', name: f.q, acceptedAnswer: { '@type': 'Answer', text: f.a } })) });
  const head = `<meta property="article:published_time" content="${iso(p.date)}">\n<meta property="article:modified_time" content="${iso(p.updated)}">\n`;
  return page({
    locale: p.locale, slug: p.slug, title: p.seoTitle ?? `${p.title} · bntpool.com`, description: p.description, ogType: 'article', schema, head,
    body: `<main class="wrap">
<header class="page-head">
  <nav class="crumbs" aria-label="${esc(t(p.locale, 'breadcrumb'))}"><a href="/">bntpool.com</a> / <a href="${blog}">${esc(t(p.locale, 'blogTitle'))}</a></nav>
  <h1>${esc(p.title)}</h1>
  <div class="meta">${meta.join(' · ')}</div>
</header>
<article class="prose">
${renderMarkdown(p.body, p.locale)}
${faq}${related}${cta(p.locale)}
</article>
</main>`,
  });
}

function indexPage(locale, posts) {
  const lang = def(locale).htmlLang;
  const url = BASE_URL + blogPath(locale);
  const items = posts.map((p) => `<li><a href="${p.path}"><time datetime="${iso(p.date)}">${nice(p.date, lang)}</time><h2>${esc(p.title)}</h2><p>${esc(p.description)}</p></a></li>`).join('\n')
    || `<li><p>${esc(t(locale, 'blogEmpty'))}</p></li>`;
  const schema = [{ '@context': 'https://schema.org', '@type': 'Blog', '@id': `${url}#blog`, name: t(locale, 'rssTitle'), url, inLanguage: lang, publisher: ORG,
    blogPost: posts.map((p) => ({ '@type': 'BlogPosting', headline: p.title, url: p.url, datePublished: iso(p.date) })) }];
  return page({
    locale, title: t(locale, 'blogMetaTitle'), description: t(locale, 'blogMetaDescription'), schema,
    body: `<main class="wrap">
<header class="page-head">
  <nav class="crumbs" aria-label="${esc(t(locale, 'breadcrumb'))}"><a href="/">bntpool.com</a> / ${esc(t(locale, 'blogTitle'))}</nav>
  <h1>${esc(t(locale, 'blogTitle'))}</h1>
  <p class="lead">${esc(t(locale, 'blogSub'))}</p>
</header>
<ul class="posts">
${items}
</ul>
</main>`,
  });
}

function rss(locale, posts) {
  const items = posts.map((p) => `<item><title>${esc(p.title)}</title><link>${p.url}</link><guid>${p.url}</guid><pubDate>${new Date(p.date).toUTCString()}</pubDate><description>${esc(p.description)}</description></item>`).join('');
  return `<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>${esc(t(locale, 'rssTitle'))}</title><link>${BASE_URL}${blogPath(locale)}</link><description>${esc(t(locale, 'blogMetaDescription'))}</description><language>${def(locale).htmlLang}</language>${items}</channel></rss>\n`;
}

function write(rel, content) {
  const url = new URL(rel, DIST);
  mkdirSync(new URL('./', url), { recursive: true });
  writeFileSync(url, content);
}

rmSync(DIST, { recursive: true, force: true });
mkdirSync(DIST, { recursive: true });
copyFileSync(new URL('blog.css', BLOG_DIR), new URL('blog.css', DIST));
copyFileSync(new URL('theme.js', BLOG_DIR), new URL('theme.js', DIST));
const sitemapPaths = [];
for (const l of LOCALES) {
  const posts = loadPosts(l.code);
  const bySlug = new Map(posts.map((p) => [p.slug, p]));
  const prefix = l.path ? `${l.path}/` : '';
  write(`${prefix}index.html`, indexPage(l.code, posts));
  write(`${prefix}rss.xml`, rss(l.code, posts));
  sitemapPaths.push(blogPath(l.code));
  for (const p of posts) {
    write(`${prefix}${p.slug}/index.html`, postPage(p, bySlug));
    sitemapPaths.push(p.path);
  }
}
// The API lists these in /sitemap.xml.
write('sitemap-paths.txt', sitemapPaths.join('\n') + '\n');
console.log(`blog: ${sitemapPaths.length} pages in ${LOCALES.length} locales (${TRANSLATED.length} translated)`);
