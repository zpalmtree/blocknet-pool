# Pool news

Run `npm --prefix frontend run showcase:front` from the repository root. Open
http://localhost:5201/showcases/front-page/?state=first&news=on for the front page with the news strip,
`state=blocks&news=on` for an inner page, `state=stats&news=on` for the Settings tab dot and `state=news` for
the archive. Add `theme=dark`, `lang=ar` or `chrome=off` as usual. Every state uses synthetic data.

- News strip: under the menu on every page except `/news`, Admin and the wallet. It shows the two newest items
  from the last 14 days, each as an outlined chip (New tag, title, arrow) linking to the item, then All news and
  a dismiss button. Phones drop All news and let the chips wrap. Dismissing hides the
  items shown; later items still appear. With nothing from the last 14 days there is no strip.
- `/news`: every published item, newest first, with a New chip on items from the last 14 days. It is linked
  from the strip and the footer (not the main menu), listed in the sitemap and prerendered in every language.
- Settings tab: an item can name the tab it announces. That tab carries a dot while the item is fresh, until
  the miner opens it once in that browser.

Items live in `src/lib/news.ts` (date, link, message keys); their text is in the message catalogs, so every
item needs all 20 languages. Dismissals and opened tabs are stored per browser; blocked storage shows the news
again on the next visit.

The captures in `captures/` cover the strip (front page, returning miner, Blocks, after dismissing, dark,
Arabic, phone in English, Arabic and German), the archive (light, dark, Arabic, phone) and My Stats (dot, phone, after opening Settings), at
1280 px and 390 px. Captured 2026-11-04 through the shared Playwright browser.
