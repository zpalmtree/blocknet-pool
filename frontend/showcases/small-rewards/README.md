# Small miner rewards

Start from the repository root with `npm --prefix frontend run showcase:rewards`.
Open http://localhost:5197/showcases/small-rewards/?state=small&lang=en.
The state picker renders the real StatsPage through the real API client with
synthetic responses; it never contacts the pool. Add `theme=dark` or `lang=de`
(any supported language) to preview other appearances.

The [capture gallery](./captures.html) contains 26 reviewed desktop (1366px) and
mobile (390px) captures. `source-revision.json` records the base commit and exact
source hashes. States cover small rewards, one atomic unit, zero rewards, no
shares, old shares without active workers, verification holds, quarantine,
loading, failed balance lookup, failed miner lookup and empty lookup. Disabled
controls, dark mode, German decimal formatting and Arabic RTL are also covered.

Amounts below 1 BNT retain up to eight decimals, with unnecessary trailing zeros
removed (at least two decimals remain). Larger values keep their existing
formatting. The same formatter serves balance cards, tooltips and credit tables.
The status uses the API's accepted count and most recent submission timestamp
from workers or recent shares. A submission can be rejected, so it is deliberately
not labeled the last accepted share. Recorded work does not assert current payout
eligibility, an active connection or a guaranteed reward. Verification holds
appear before balances, and failed balance requests retain the existing unknown
display instead of showing zero. No accounting or API changes are required.

Validation: 29 frontend tests, production build (including type checks, all 20
catalogs and static content), and the shared Windows Chrome Playwright MCP.
Checked small amounts in cards and block rows, clear/relookup behavior, unknown
balances, holds, no document overflow, and both themes. The only intermittent
console error was from the shared browser's wallet extension, outside the app.

For a standalone build, run Vite with this config and a dedicated `--outDir`,
then copy `captures.html`, `source-revision.json` and `captures/` beside the emitted
`showcases/small-rewards/index.html`. Serve that output directory with
`python3 -m http.server 5197 --directory <output-directory>`.

Deploy through the normal API-only deployment path when requested. This change
does not require a Stratum or daemon restart.
