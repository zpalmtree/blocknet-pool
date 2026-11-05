# Miner settings

Run `npm --prefix frontend run showcase:settings` from the repository root.
Open http://localhost:5204/showcases/miner-settings/?state=locked. Add `theme=dark` for dark mode
and `chrome=off` to hide the state picker.

The real App opens My Stats on its Settings tab against synthetic API responses and an
in-memory stand-in for `/api/miner-settings/*` that follows the API's rules. Nothing reaches a
real pool. The demo password is `correct horse`; the rig IP that wins a take-back is
`203.0.113.24` (filled in as "your network"), any other IP has done too little work, and
`192.0.2.1` answers as cooling down. States:

- `unclaimed`: no password yet; anyone can set the first one.
- `locked`: enter the password, or take the address back from a rig. Five wrong passwords an
  hour lock out further tries.
- `unlocked`: minimum payout (pool minimum up to what one payout may send, 10,000 BNT on bntpool), mining mode for logins without
  `solo:`, and hiding worker names, each with a ? note for the first two linking to Pool info.
  Saving a new minimum updates the payout bar on the Stats tab.
- `custom`: unlocked with a 5 BNT minimum, solo and hidden names.
- `exchange`: an exchange (memo) account, 10 BNT by default, which may be lowered to the
  regular 1 BNT minimum.
- `no-password`: another browser took the address back and has not set a password yet.
- `public-hidden`: the Stats tab as a visitor sees it when names are hidden (Rig 1, Rig 2).
- `loading`, `error`: settings still loading, and the settings API failing.

Take-backs are clicked through from `locked`: Take it back from a rig, a refused IP, then
`203.0.113.24`, then a new password.

The captures in `captures/` were taken through the shared Playwright browser on 2026-11-04 (EST) at
1280 px and 390 px and in dark mode, all with synthetic data. Source: blocknet-pool branch
`miner-settings`.
