# Solo mining

Run `npm --prefix frontend run showcase:solo` from the repository root.
Open http://localhost:5199/showcases/solo-mining/?state=solo&lang=en.

The real StatsPage, BlocksPage and API client use synthetic responses. States:
`solo` (all hashrate solo, a pending solo block), `mixed` (one PPLNS and one solo
rig, pending blocks of both kinds), `pplns` (no solo card or badge), `loading`,
`error`, `empty`, `blocks` (solo badges beside block status) and `blocks-empty`.
The 17 captures in `captures/` cover each state at 1280 px and 390 px, plus the
mobile blocks table scrolled to its Status column.

Source: blocknet-pool master 12e3258 plus the solo mining change that added this
showcase. Captured 2026-11-03 through the shared Playwright browser.
