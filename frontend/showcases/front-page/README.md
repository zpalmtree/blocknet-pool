# Front page

Run `npm --prefix frontend run showcase:front` from the repository root.
Open http://localhost:5201/showcases/front-page/?state=first&lang=en. Add `theme=dark` for dark mode
and `chrome=off` to hide the state picker.

The real App, front page and Luck page run against synthetic API responses; nothing reaches a real
pool, explorer or analytics endpoint. States:

- `first`: no saved lookup. The deep green hero leads with the pool fee, the Stratum address and a
  miner lookup, followed by the number band (miners, pool hashrate, network hashrate, paid; two by two
  under 1100 px, stacked on phones), Quick Start, recent blocks and payouts, the hashrate
  chart and the mining guide.
- `below`, `ready`, `queued`, `paid`: a saved lookup turns the hero into the visitor's miner, with
  hashrate, one next-payout bar and Full stats; the side panel keeps Stratum and the pool fee. Below the minimum
  the bar fills toward it; over it the bar is full; a queued payout shows a striped bar; just after a
  payout a line names it.
- `no-workers`: a saved miner with nothing online links to the zero-hashrate help.
- `exchange`, `exchange-ready`: an exchange deposit with a memo uses its own 10 BNT minimum from the
  balance API.
- `miner-error`: a saved lookup that fails falls back to the first-visit view.
- `loading`, `empty`: pool data pending, and a pool with no blocks or payouts yet.
- `luck`: the Luck page, which now carries the Current Round card the old dashboard showed.
- `stats`: My Stats: one lookup row (memo and Clear as links), then a summary card in the hero's
  shape (hashrate, next-payout bar, estimated, confirmed and paid), the miner's hashrate chart,
  workers, payouts and, when present, blocks still maturing. Clear it for the empty state.
- `stats-new`: My Stats for a miner that is hashing but has no estimated rewards yet: a ? beside
  Estimated Rewards opens a short note on why it is 0.
- `blocks`: Blocks, where effort and status badges share one width per column.
- `start`: Get Started, with one row per quick start step and its action at the end, and Seine in
  Action drawn as text from seine's own renderer (setup prompts and the mining dashboard).
- `payouts`: Payouts, with the public data table layout.
- `checkpoints`: Checkpoints, with a synthetic checkpoint file.
- `news`: What's new, the news archive linked from the footer, newest first, with a New chip on items
  from the last 14 days.

Add `news=on` to any state to see pool news as a visitor who hasn't dismissed it: a strip under the menu on
every page except the archive, with the two newest items from the last 14 days (one linked chip each),
All news and a dismiss button, and a dot on the My Stats Settings tab until Settings is opened once. Without it every state
shows the page as it looks once the news is dismissed. News captures live in `../pool-news/captures/`.

Every page shares the front page's style: a Mint page under the dark green nav and a deep green
title band whose bottom edge the page rises into as one sheet (slightly curved inside corners on
wide screens, a straight edge under 1136 px), deep green buttons and tabs, a green footer, flat
square cards, number groups as one box split by hairlines, centred public tables with equal-width
badges, and labels and tables without all caps.

The 37 captures in `captures/` (see `captures.html`) cover these states at 1280 px and 390 px, dark
mode and Arabic. Every capture uses synthetic data. `chrome=off` also hides the scrollbar, so a
full-page phone capture keeps its full 390 px width. Source: blocknet-pool branch front-page-redesign.
Captured 2026-11-03 through the shared Playwright browser.
