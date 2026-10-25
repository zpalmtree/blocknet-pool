import { Fragment, type ReactNode } from 'react';
import { LOCALES } from './locales.mjs';
import { interpolate, languageUrl, matchLanguage, LANGUAGE_PREFERENCE_KEY } from './core';

export { LOCALES, languageUrl, LANGUAGE_PREFERENCE_KEY };

// Eager catalogs keep all translations in app.js, the API's existing embedded asset.
const catalogs = import.meta.glob('./messages/*.json', { eager: true, import: 'default' }) as Record<string, Record<string, string>>;
const search = typeof window === 'undefined' ? '' : window.location.search;
export const locale = matchLanguage(new URLSearchParams(search).get('lang') || 'en') || 'en';
export const localeInfo = LOCALES.find((item) => item.code === locale)!;

function message(key: string): string {
  return catalogs[`./messages/${locale}.json`]?.[key] ?? catalogs['./messages/en.json'][key] ?? key;
}

export function t(key: string, values: readonly (string | number)[] = []): string {
  return interpolate(message(key), values);
}

// Values remain React nodes: translated text never becomes raw HTML.
export function tx(key: string, values: readonly ReactNode[]): ReactNode {
  return message(key).split(/(\{\d+\})/g).map((part, index) => (
    <Fragment key={index}>{/^\{\d+\}$/.test(part) ? values[Number(part.slice(1, -1))] : part}</Fragment>
  ));
}

export function rememberLanguage(code: string) {
  try { localStorage.setItem(LANGUAGE_PREFERENCE_KEY, code); } catch { /* Selection still works without storage. */ }
}

export function localizePath(path: string): string {
  if (path === '/blog/') return locale === 'en' ? path : `/blog/${localeInfo.path}/`;
  return languageUrl(path, locale);
}

if (typeof document !== 'undefined') {
  document.documentElement.lang = locale;
  document.documentElement.dir = localeInfo.dir;
  const requested = new URLSearchParams(search).get('lang');
  if (requested && matchLanguage(requested)) rememberLanguage(locale);
}
