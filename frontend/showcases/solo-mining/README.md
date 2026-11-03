# Solo mining

Run `npm --prefix frontend run showcase:solo` from the repository root.
Open http://localhost:5199/showcases/solo-mining/?state=solo&lang=en.

The real StatsPage, BlocksPage, LuckPage, StartPage and API client use synthetic
responses. States: `solo` (all hashrate solo, a pending solo block), `mixed` (one
PPLNS and one solo rig, pending blocks of both kinds), `pplns` (no solo badges),
`loading`, `error`, `empty`, `blocks` (solo badges beside block status),
`blocks-empty`, `luck` (solo badges on the luck history, which the dashboard's
recent blocks share) and `start` (Get Started; open "Mine solo" to see the solo
section). Solo rigs carry a SOLO badge in the Workers table. The 23 captures in
`captures/` cover these states at 1280 px and 390 px; the Get Started captures
show only the Quick Start card with "Mine solo" open and the rewards card.

Source: blocknet-pool master 12e3258 plus the solo mining change that added this
showcase. Captured 2026-11-03 through the shared Playwright browser.
