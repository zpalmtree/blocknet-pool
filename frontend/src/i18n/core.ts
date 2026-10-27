import { LOCALES } from './locales.mjs';

// v1 was also written by any visit to a ?lang= URL, so old values may not be real choices.
export const LANGUAGE_PREFERENCE_KEY = 'bntpool:language:preference:v2';

export function matchLanguage(language: string): string | null {
  const normalized = language.toLowerCase();
  const exact = LOCALES.find((item) => item.code.toLowerCase() === normalized);
  if (exact) return exact.code;
  const base = normalized.split('-')[0];
  if (base === 'zh') return /(?:^|-)(?:hant|tw|hk|mo)(?:-|$)/.test(normalized) ? 'zh-Hant' : 'zh-Hans';
  if (base === 'pt') return 'pt-BR';
  return LOCALES.find((item) => item.code === base)?.code ?? null;
}

export function preferredLanguage(saved: string | null, languages: readonly string[]): string | null {
  if (saved && matchLanguage(saved)) return matchLanguage(saved);
  for (const language of languages) {
    const match = matchLanguage(language);
    if (match) return match;
  }
  return null;
}

export function languageUrl(path: string, code: string): string {
  const url = new URL(path, 'https://bntpool.com');
  if (code === 'en') url.searchParams.delete('lang');
  else url.searchParams.set('lang', code);
  return `${url.pathname}${url.search}${url.hash}`;
}

export function interpolate(message: string, values: readonly (string | number)[] = []): string {
  return message.replace(/\{(\d+)\}/g, (token, index) => values[Number(index)] == null ? token : String(values[Number(index)]));
}
