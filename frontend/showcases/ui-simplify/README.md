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
  reads "Due now".
- My Stats: a hint for new miners before a lookup; workers page at 20 and
  payouts at 10; the pool-wide rejects panel moves to Status.
- Language: a URL without `lang` opens in the saved or browser language.

`captures/` holds desktop (1366px) and phone (390px) captures taken with the
Windows Playwright MCP from the fixture: `/start`, `/start` auto-selected German,
`/?demo=farm`, `/stats?demo=empty`, `/stats?demo=farm` and `/status`.
