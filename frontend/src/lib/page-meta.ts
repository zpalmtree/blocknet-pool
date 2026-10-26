import pages from './page-meta.json' with { type: 'json' };
import { LOCALES } from '../i18n/locales.mjs';

type PageMetadata = {
  title: string; description: string; image: string; index: boolean; label: string;
  guides: { href: string; label: string }[];
  titleKey?: string; descriptionKey?: string; labelKey?: string;
};

/** Only the language belongs in public canonicals; lookup/tracking queries do not. */
export function metadataForPath(pathname: string, baseUrl: string, code = 'en', translate?: (key: string) => string) {
  const path = pathname.replace(/\/+$/, '') || '/';
  const meta: PageMetadata = Object.prototype.hasOwnProperty.call(pages, path) ? pages[path as keyof typeof pages] : pages['/404'];
  const locale = LOCALES.find(item => item.code === code) ?? LOCALES[0];
  const url = (language: string) => `${baseUrl}${path}${language === 'en' ? '' : `?lang=${encodeURIComponent(language)}`}`;
  const localized = meta.index && locale.code !== 'en' && translate && meta.titleKey && meta.descriptionKey;
  return {
    ...meta,
    title: localized ? translate(meta.titleKey) : meta.title,
    description: localized ? translate(meta.descriptionKey) : meta.description,
    canonical: url(meta.index ? locale.code : 'en'),
    alternates: meta.index ? [...LOCALES.map(item => ({ language: item.htmlLang, url: url(item.code) })), { language: 'x-default', url: url('en') }] : [],
    ogLocale: locale.ogLocale,
    imageUrl: `${baseUrl}/ui-assets/${meta.image}`,
    robots: meta.index ? 'index, follow' : 'noindex, follow',
  };
}

/** Keep route changes, including Back/Forward and recovery from 404, in sync. */
export function updatePageMetadata(pathname: string, title: string, code = 'en', translate?: (key: string) => string) {
  const canonical = document.querySelector<HTMLLinkElement>('link[rel="canonical"]');
  if (!canonical) return;
  // Preserve the server-configured public origin, never a preview/request host.
  const meta = metadataForPath(pathname, new URL(canonical.href).origin, code, translate);
  canonical.href = meta.canonical;
  document.title = title;
  document.querySelectorAll('link[rel="alternate"][hreflang]').forEach(link => link.remove());
  for (const alternate of meta.alternates) {
    const link = document.createElement('link');
    link.rel = 'alternate';
    link.hreflang = alternate.language;
    link.href = alternate.url;
    document.head.append(link);
  }
  const fields = {
    'name:description': meta.description,
    'name:robots': meta.robots,
    'property:og:url': meta.canonical,
    'property:og:title': title,
    'property:og:description': meta.description,
    'property:og:image': meta.imageUrl,
    'property:og:locale': meta.ogLocale,
    'name:twitter:title': title,
    'name:twitter:description': meta.description,
    'name:twitter:image': meta.imageUrl,
  };
  for (const [field, content] of Object.entries(fields)) {
    const split = field.indexOf(':');
    const attr = field.slice(0, split), value = field.slice(split + 1);
    document.querySelector<HTMLMetaElement>(`meta[${attr}="${value}"]`)?.setAttribute('content', content);
  }
}
