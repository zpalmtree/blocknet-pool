# Public pool languages

The public pool UI and blog share the 20 locales in `locales.mjs`, including native
language names and Arabic RTL direction. Administrative diagnostics remain in
English; server-supplied incident/rejection details retain their original text.

Pool URLs use `?lang=es` (English uses the existing URL). This works with the API's
existing SPA routes and embedded `app.js`; the catalogs are bundled eagerly, so
no new server routes or assets are needed. The blog keeps its existing localized
paths, and its pool links carry the selected language.

The header offers every language. A pool URL without `lang` opens in the saved
language choice, otherwise in the first supported browser language: the app adds
`?lang=` to the address before its first render, without a reload. English blog
pages do the same by redirecting to the page's translation when one exists. A
choice from either language menu (including English) is saved and wins over the
browser language. Nothing else is saved: opening a `?lang=` link or being switched
automatically leaves the preference alone. Both surfaces use
`bntpool:language:preference:v2` (v1 was also written by `?lang=` visits, so it is
ignored); blocked browser storage falls back to the browser language. A language switch preserves the route, query and fragment.
Internal pool navigation preserves the chosen language.

## Updating copy

- `messages/en.json` is the source. Stable message IDs are referenced with `t()`
  (text) or `tx()` (React-node placeholders). Neither evaluates translated HTML.
- Translate complete sentences, preserving every numbered placeholder, product
  name, command, filename, address and unit. Keep commands in the component and
  interpolate them as nodes. Avoid English-only plural suffixes.
- Update all other catalogs when changing the source, then set `sha256` in
  `translation-source.json` to the SHA-256 of the exact English file bytes.
  The metadata records machine-assisted translation without independent
  linguistic review; it does not claim human certification.
- `npm run i18n:check` checks source freshness, locale/key coverage, nonempty
  values, placeholder parity, protected names/units and sentences left in
  English. It also runs before every production build.
- `npm test` exercises regional language matching, browser preference order,
  route preservation, safe interpolation and catalog references.

## Preview

From the repository root: `npm --prefix frontend run showcase:languages`.
Open `http://localhost:5184/stats?lang=es` or another public route. Use
`demo=empty`, `demo=loading`, or `demo=error` for synthetic states. The Vite
fixture intercepts API requests; it does not connect to a real pool or wallet.
