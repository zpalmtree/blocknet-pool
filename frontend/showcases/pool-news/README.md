# Pool news

Run `npm --prefix frontend run showcase:front` from the repository root. Open
http://localhost:5201/showcases/front-page/?state=first&news=on for the front page with news, `state=below&news=on`
for a returning miner, `state=stats&news=on` for the Settings tab dot and `state=news` for the archive. Add
`theme=dark`, `lang=ar` or `chrome=off` as usual. Every state uses synthetic data.

- Front page: a What's new section at the top of the page body lists up to three items from the last 30 days,
  newest first, each with its date, a one-line summary and a link. All news opens `/news`; the dismiss button
  hides everything shown, and the section returns only for an item published after the dismissed ones. With
  nothing from the last 30 days the section is gone.
- `/news`: every published item, newest first, with a New chip on items from the last 30 days. It is linked
  from the footer (not the main menu), listed in the sitemap and prerendered in every language.
- Settings tab: an item can name the tab it announces. That tab carries a dot while the item is fresh, until
  the miner opens it once in that browser.

Items live in `src/lib/news.ts` (date, link, message keys); their text is in the message catalogs, so every
item needs all 20 languages. Dismissals and opened tabs are stored per browser; blocked storage shows the news
again on the next visit.

The 14 captures in `captures/` cover the front page (first visit, returning miner, after dismissing, dark,
Arabic, phone), the archive (light, dark, Arabic, phone) and My Stats (dot, phone, after opening Settings),
at 1280 px and 390 px. Source: blocknet-pool branch pool-news on master 4bc10f3. Captured 2026-11-04 through the
shared Playwright browser.
