import { Fragment, type ReactNode } from 'react';
import english from './messages/en.json';
import { LOCALES } from './locales.mjs';
import { interpolate, languageUrl, matchLanguage, LANGUAGE_PREFERENCE_KEY } from './core';

export { LOCALES, languageUrl, LANGUAGE_PREFERENCE_KEY };

// Vite emits hashed JSON assets; only the selected language is downloaded.
// English remains in app.js, both for the default page and missing-key fallbacks.
const catalogUrls = import.meta.glob(['./messages/*.json', '!./messages/en.json'], {
  eager: true, query: '?url', import: 'default',
}) as Record<string, string>;
let catalog: Record<string, string> = english;
const search = typeof window === 'undefined' ? '' : window.location.search;
export const locale = matchLanguage(new URLSearchParams(search).get('lang') || 'en') || 'en';
export const localeInfo = LOCALES.find((item) => item.code === locale)!;

function message(key: string): string {
  return catalog[key] ?? english[key] ?? key;
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

export async function initializeLanguage() {
  if (locale !== 'en') {
    const response = await fetch(catalogUrls[`./messages/${locale}.json`], { signal: AbortSignal.timeout(10000) });
    if (!response.ok) throw new Error(`Language download failed: ${response.status}`);
    const messages: unknown = await response.json();
    if (!messages || typeof messages !== 'object' || Array.isArray(messages) ||
        !Object.keys(english).every(key => typeof (messages as Record<string, unknown>)[key] === 'string')) {
      throw new Error('Invalid language catalog');
    }
    catalog = messages as Record<string, string>;
  }
  document.documentElement.lang = locale;
  document.documentElement.dir = localeInfo.dir;
  const requested = new URLSearchParams(search).get('lang');
  if (requested && matchLanguage(requested)) rememberLanguage(locale);
}
