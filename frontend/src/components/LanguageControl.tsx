import { useEffect, useRef } from 'react';
import { LOCALES, languageUrl, locale, localeInfo, rememberLanguage, t } from '../i18n';

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
