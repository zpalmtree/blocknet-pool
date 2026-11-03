# Front page

Run `npm --prefix frontend run showcase:front` from the repository root.
Open http://localhost:5201/showcases/front-page/?state=first&lang=en. Add `theme=dark` for dark mode
and `chrome=off` to hide the state picker.

The real App, front page and Luck page run against synthetic API responses; nothing reaches a real
pool, explorer or analytics endpoint. States:

- `first`: no saved lookup. The deep green hero leads with the pool fee, the Stratum address and a
  miner lookup, followed by the number band, Quick Start, recent blocks and payouts, the hashrate
  chart and the mining guide.
- `below`, `ready`, `queued`, `paid`: a saved lookup turns the hero into the visitor's miner, with
  hashrate, workers, last share and the next payout. Below the minimum the bar fills toward it; over
  it the bar is full and a tick marks the minimum; a queued payout shows a striped bar; just after a
  payout the line names it.
- `no-workers`: a saved miner with nothing online links to the zero-hashrate help.
- `exchange`, `exchange-ready`: an exchange deposit with a memo uses its own 10 BNT minimum from the
  balance API and notes where personal wallets start.
- `miner-error`: a saved lookup that fails falls back to the first-visit view.
- `loading`, `empty`: pool data pending, and a pool with no blocks or payouts yet.
- `luck`: the Luck page, which now carries the Current Round card the old dashboard showed.

The 23 captures in `captures/` (see `captures.html`) cover these states at 1280 px and 390 px, dark
mode and Arabic. Source: blocknet-pool master 10f56ea plus the front page change. Captured
2026-11-03 through the shared Playwright browser.
