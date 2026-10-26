import { useEffect, useRef, useState } from 'react';
import { LOCALES, LANGUAGE_PREFERENCE_KEY, languageUrl, locale, localeInfo, rememberLanguage, t } from '../i18n';
import { preferredLanguage } from '../i18n/core';

// Keep the native-language offer copy; discard unrelated blog translations.
const prompts = import.meta.glob('../../blog/messages/*.json', { eager: true, import: 'suggestPrompt' }) as Record<string, string>;
const actions = import.meta.glob('../../blog/messages/*.json', { eager: true, import: 'suggestAction' }) as Record<string, string>;

export function LanguageControl() {
  const menu = useRef<HTMLDetailsElement>(null);
  useEffect(() => {
    const close = (event: PointerEvent) => {
      if (!menu.current?.contains(event.target as Node)) menu.current?.removeAttribute('open');
    };
    const escape = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && menu.current?.open) {
        menu.current.removeAttribute('open');
        menu.current.querySelector('summary')?.focus();
      }
    };
    document.addEventListener('pointerdown', close);
    document.addEventListener('keydown', escape);
    return () => {
      document.removeEventListener('pointerdown', close);
      document.removeEventListener('keydown', escape);
    };
  }, []);
  return (
    <details className="pool-languages" ref={menu}>
      <summary aria-label={`${t('language')}: ${localeInfo.endonym}`}>{localeInfo.short}</summary>
      <ul aria-label={t('languageOfferLabel')}>
        {LOCALES.map((item) => (
          <li key={item.code}>
            <a href={languageUrl(location.pathname + location.search + location.hash, item.code)}
              lang={item.htmlLang} dir={item.dir} hrefLang={item.htmlLang}
              aria-current={item.code === locale ? 'true' : undefined}
              onClick={() => rememberLanguage(item.code)}>{item.endonym}</a>
          </li>
        ))}
      </ul>
    </details>
  );
}

export function LanguageOffer() {
  const [target, setTarget] = useState(() => {
    if (locale !== 'en') return null;
    let saved: string | null = null;
    try { saved = localStorage.getItem(LANGUAGE_PREFERENCE_KEY); } catch { /* Browser language still works. */ }
    const preferred = preferredLanguage(saved, navigator.languages || [navigator.language]);
    return preferred && preferred !== 'en' ? preferred : null;
  });
  if (!target) return null;
  const item = LOCALES.find((entry) => entry.code === target)!;
  const key = `../../blog/messages/${target}.json`;
  return (
    <aside className="pool-language-offer" dir="ltr" aria-label={t('languageOfferLabel')}>
      <span lang={item.htmlLang} dir={item.dir}>{prompts[key]}</span>
      <div className="pool-language-offer__actions">
        <a className="btn btn-primary" lang={item.htmlLang} dir={item.dir}
          href={languageUrl(location.pathname + location.search + location.hash, target)}
          onClick={() => rememberLanguage(target)}>{actions[key]}</a>
        <button className="btn btn-secondary" type="button" onClick={() => { rememberLanguage('en'); setTarget(null); }}>
          {t('keepEnglish')}
        </button>
      </div>
    </aside>
  );
}
