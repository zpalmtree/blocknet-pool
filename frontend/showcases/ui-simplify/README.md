# Simpler pool UI showcase

Run `npm --prefix frontend run showcase:ui` from the repository root, then open
<http://localhost:5186/start>. The fixture is the pool-languages one plus
`demo=farm` (47 rigs, 26 payouts, a payout sweep that is due). No live pool or
wallet is used; addresses and hashes are synthetic.

What it shows:

- Get Started: a wallet step first, a "Get a Blocknet wallet" section with the
  official desktop and command-line wallets, then help links and pool details.
- Navigation: Checkpoints and Admin move to the footer; My Stats sits next to
  Get Started; the menu stays expanded down to 1100px.
- Dashboard: a My Stats lookup beside the Stratum address; a past sweep time
  reads "Due now". The Luck & Health row (Average Luck, Unique Orphans, and
  Orphan Rate) is removed, so Blocks flows directly into Round Progress.
- My Stats: a hint for new miners before a lookup; workers page at 20 and
  payouts at 10; the pool-wide rejects panel moves to Status.
- Language: a URL without `lang` opens in the saved or browser language.

`captures/` holds desktop (1366px) and phone (390px) captures taken with the
Windows Playwright MCP from the fixture: `/start`, `/start` auto-selected German,
`/?demo=farm`, `/stats?demo=empty`, `/stats?demo=farm` and `/status`.

Dashboard captures refreshed on October 27, 2026 (EDT), from base revision
`7ea431f` with the Luck & Health removal. Dashboard source blob:
`703e2226148e37f9a8b0e77cd25b0b28fa92d0e6`.
Open <http://localhost:5186/?demo=farm&lang=en> for populated data and a due
payout sweep; use `demo=loading`, `demo=empty`, or `demo=error` for the other
captured states. The loading/error captures include disabled Copy and Lookup
controls. All eight dashboard captures were reviewed at 1366px and 390px;
the removed cards are absent, Round Progress and Pool Luck History remain,
and the page has no horizontal overflow.

Validation: production frontend build passed; 25 of 26 frontend tests passed.
The existing rewards calculator test expects release v0.2.39 while the release
metadata on the base revision is v0.2.42. Browser console inspection found only
an unrelated wallet-extension injection error.
