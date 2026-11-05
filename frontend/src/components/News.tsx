import { useState } from 'react';

import { locale, localizePath, t, tx } from '../i18n';
import { dismissFrontPageNews, frontPageNews, isFresh, newsTime, publishedNews, type NewsItem } from '../lib/news';

const SOLO = <code key="solo" className="mono inline-code" dir="ltr">solo:</code>;

function newsHref(href: string): string {
  return href.startsWith('/blog/') ? localizePath('/blog/') + href.slice('/blog/'.length) : localizePath(href);
}

function newsDate(item: NewsItem, withYear: boolean): string {
  return new Intl.DateTimeFormat(locale, { month: 'short', day: 'numeric', year: withYear ? 'numeric' : undefined, timeZone: 'UTC' })
    .format(newsTime(item));
}

const body = (item: NewsItem) => tx(item.bodyKey, [SOLO]);

/** Recent news at the top of the front page; hidden once nothing is fresh or the visitor dismisses it. */
export function NewsSection() {
  const [items, setItems] = useState(() => frontPageNews());
  if (!items.length) return null;
  return (
    <section aria-labelledby="home-news-title">
      <div className="home-head">
        <h2 id="home-news-title">{t('newsNav')}</h2>
        <div className="news-head-actions">
          <a href={localizePath('/news')}>{t('newsAll')}</a>
          <button type="button" className="news-dismiss" aria-label={t('newsDismiss')} title={t('newsDismiss')}
            onClick={() => { dismissFrontPageNews(items); setItems([]); }}>
            <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="M4 4l8 8M12 4l-8 8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" /></svg>
          </button>
        </div>
      </div>
      <ol className="news-rows card">
        {items.map((item) => (
          <li key={item.titleKey}>
            <time className="news-date" dateTime={item.date}>{newsDate(item, false)}</time>
            <div className="news-text"><strong>{t(item.titleKey)}</strong><span>{body(item)}</span></div>
            <a href={newsHref(item.href)}>{t(item.linkKey)}</a>
          </li>
        ))}
      </ol>
    </section>
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
