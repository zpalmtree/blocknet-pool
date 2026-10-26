// Use the same translated metadata as React and the existing article translations.
// The API injects the configured origin into head tags at request time.
import { readFileSync, writeFileSync } from 'node:fs';
import { metadataForPath } from '../src/lib/page-meta.ts';
import pages from '../src/lib/page-meta.json' with { type: 'json' };
import { LOCALES, blogPath, readPost, t as blogText } from './blog-lib.mjs';

const esc = value => String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const publicPaths = Object.keys(pages).filter(path => pages[path].index);
const result = LOCALES.map(locale => {
  const messages = JSON.parse(readFileSync(new URL(`../src/i18n/messages/${locale.code}.json`, import.meta.url), 'utf8'));
  const t = key => {
    if (!messages[key]) throw new Error(`Missing ${locale.code}:${key}`);
    return messages[key];
  };
  const url = path => `${path}${locale.code === 'en' ? '' : `?lang=${encodeURIComponent(locale.code)}`}`;
  const rich = (key, values) => esc(t(key)).replace(/\{(\d+)\}/g, (_, index) => values[index]);
  const localizedPages = Object.fromEntries(publicPaths.map(path => {
    const meta = metadataForPath(path, '', locale.code, t);
    const guides = meta.guides.map(guide => {
      const slug = guide.href.slice('/blog/'.length).split('/')[0];
      const post = readPost(slug, locale.code === 'en' ? 'index.md' : `${locale.code}.md`);
      if (!post?.data?.title || post.data.draft) throw new Error(`Missing published guide ${locale.code}:${slug}`);
      return `<li><a href="${esc(blogPath(locale.code, slug) + new URL(guide.href, 'https://bntpool.com').hash)}">${esc(post.data.title)}</a></li>`;
    }).join('');
    const start = path === '/start' ? `<section class="card section"><h2>${esc(t('m450'))}</h2><p>${esc(t('m451'))}</p><ol>
<li><strong>${esc(t('m452'))}</strong><p>${rich('m453', [`<a href="https://seineminer.com/${locale.path ? `${locale.path}/` : ''}#download">seineminer.com</a>`])}</p></li>
<li><strong>${esc(t('m454'))}</strong><p>${rich('m455', ['<code dir="ltr">./seine</code>', '<code dir="ltr">@name</code>', '<code dir="ltr">$name</code>'])}</p></li>
<li><strong>${esc(t('m458'))}</strong><p>${rich('m459', [`<a href="${esc(url('/stats'))}">${esc(t('m005'))}</a>`])}</p></li></ol><div role="navigation" aria-label="${esc(t('browseTopics'))}"><p><a href="${blogPath(locale.code)}#getting-started">${esc(t('topicGettingStarted'))}</a></p><p><a href="${blogPath(locale.code)}#troubleshooting">${esc(t('topicTroubleshooting'))}</a></p></div></section>` : '';
    const menu = `<details class="pool-languages"><summary aria-label="${esc(t('language'))}">${esc(locale.short)}</summary><ul>${LOCALES.map(other => `<li><a href="${esc(path + (other.code === 'en' ? '' : `?lang=${encodeURIComponent(other.code)}`))}" lang="${other.htmlLang}" dir="${other.dir}" hreflang="${other.htmlLang}"${other.code === locale.code ? ' aria-current="true"' : ''}>${esc(other.endonym)}</a></li>`).join('')}</ul></details>`;
    const body = `<div id="root"><nav aria-label="${esc(blogText(locale.code, 'footer'))}"><a class="nav-brand" href="${esc(url('/'))}">bntpool.com</a><div class="nav-actions">${menu}</div></nav><main class="container" id="public-page"><div class="page-header"><h1>${esc(meta.title)}</h1><p class="page-intro">${esc(meta.description)}</p></div>${start}<section class="card section"><h2>${esc(blogText(locale.code, 'related'))}</h2><ul>${guides}</ul><a href="${blogPath(locale.code)}">${esc(t('m010'))}</a></section><section class="card section"><h2>${esc(blogText(locale.code, 'footer'))}</h2><div class="page-actions">${publicPaths.map(route => `<a class="btn btn-secondary" href="${esc(url(route))}">${esc(t(pages[route].labelKey))}</a>`).join('')}</div></section></main></div>`;
    return [path, { title: meta.title, description: meta.description, body }];
  }));
  return { ...locale, pages: localizedPages };
});
writeFileSync(new URL('../dist/public-pages.json', import.meta.url), JSON.stringify(result));
console.log(`public pages: ${publicPaths.length * LOCALES.length} localized HTML bodies and metadata`);
