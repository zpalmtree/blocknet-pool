import { useState } from 'react';

import { locale, localizePath, t, tx } from '../i18n';
import { dismissStripNews, isFresh, newsTime, publishedNews, stripNews, type NewsItem } from '../lib/news';

const SOLO = <code key="solo" className="mono inline-code" dir="ltr">solo:</code>;

function newsHref(href: string): string {
  return href.startsWith('/blog/') ? localizePath('/blog/') + href.slice('/blog/'.length) : localizePath(href);
}

function newsDate(item: NewsItem, withYear: boolean): string {
  return new Intl.DateTimeFormat(locale, { month: 'short', day: 'numeric', year: withYear ? 'numeric' : undefined, timeZone: 'UTC' })
    .format(newsTime(item));
}

const body = (item: NewsItem) => tx(item.bodyKey, [SOLO]);

const CLOSE = <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="M4 4l8 8M12 4l-8 8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" /></svg>;

/** One line under the menu with the newest fresh item; hidden once nothing is fresh or the visitor dismisses it. */
export function NewsStrip() {
  const [item, setItem] = useState(() => stripNews());
  if (!item) return null;
  return (
    <div className="news-strip" role="region" aria-label={t('newsNav')}>
      <div className="news-strip-inner">
        <span className="news-strip-tag">{t('newsNew')}</span>
        <span className="news-strip-text"><strong>{t(item.titleKey)}</strong><span className="news-strip-short"><span aria-hidden="true"> · </span>{t(item.shortKey)}</span></span>
        <a href={newsHref(item.href)}>{t(item.linkKey)}</a>
        <a className="news-strip-all" href={localizePath('/news')}>{t('newsAll')}</a>
        <button type="button" className="news-strip-close" aria-label={t('newsDismiss')} title={t('newsDismiss')}
          onClick={() => { dismissStripNews(item); setItem(null); }}>{CLOSE}</button>
      </div>
    </div>
  );
}

export function NewsPage() {
  const now = Date.now();
  const items = publishedNews(now);
  return (
    <div id="page-news">
      <div className="page-header">
        <h1>{t('newsNav')}</h1>
      </div>
      {!items.length ? <p className="section-lead">{t('newsEmpty')}</p> : (
        <ol className="news-timeline card">
          {items.map((item) => (
            <li key={item.titleKey}>
              <time className="news-date" dateTime={item.date}>{newsDate(item, true)}</time>
              <div className="news-entry">
                <h2>{t(item.titleKey)}{isFresh(item, now) && <span className="news-chip">{t('newsNew')}</span>}</h2>
                <p>{body(item)}</p>
                <a href={newsHref(item.href)}>{t(item.linkKey)}</a>
              </div>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}
