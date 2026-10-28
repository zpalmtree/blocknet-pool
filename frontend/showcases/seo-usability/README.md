# Pool dashboard SEO: usability review

The existing dashboard remains first. The guide follows recent payouts and is collapsed by default. Loading/error captures include disabled Copy and Lookup controls; empty means no chart, round or payout history. Expanded English, Arabic and dark-mode views are included.

Open `index.html` for reviewed captures, with exact source hashes in
`source-revision.json`. These are real components with synthetic pool fixtures
or the built Seine site. No private wallet data is included.

Run `npm --prefix frontend run showcase:seo` from the repository root and open
http://localhost:5192/?demo=farm&lang=en. Use `demo=empty`, `loading`, or `error`.
Open http://localhost:5192/showcases/seo-usability/index.html for the gallery.

Verified: upper dashboard layout unchanged with guide hidden/open; no page-wide
overflow at either width; native keyboard expansion; Copy feedback; My Stats
lookup; payout-details fragment navigation; English/Arabic and light/dark.
API test suite: 127 pass, one opt-in export test; frontend: 22 pass.
Production frontend build and all translation checks pass.
The only extra runtime request on a production landing/setup page is a small
best-effort aggregate event; local previews send none. No mining API polls added.

Initial-HTML captures use the real Rust HTML export with app scripts omitted to
simulate JavaScript disabled. The opt-in export test also passes (128 total).
