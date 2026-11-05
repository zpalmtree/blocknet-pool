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
  Each change saves on its own: Pool/Solo and the checkbox at once, the minimum when the field
  loses focus, on Enter, or after a short pause in typing. An out-of-range minimum is not sent.
  Saving a new minimum updates the payout bar on the Stats tab.
- `custom`: unlocked with a 5 BNT minimum, solo and hidden names.
- `exchange`: an exchange (memo) account, whose 10 BNT minimum can be raised but not
  lowered, so the exchange is not sent many small deposits.
- `no-password`: another browser took the address back and has not set a password yet.
- `public-hidden`: the Stats tab as a visitor sees it when names are hidden (Rig 1, Rig 2).
- `loading`, `error`: settings still loading, and the settings API failing.

- `alerts-pending`, `alerts-on`: an alert email waiting for its confirmation link, and
  confirmed with alerts on (rig stopped for 10/30/60 minutes, payout sent, solo block found).
  From `unlocked`, typing an email and pressing Send link shows the waiting state.
- `link-confirm`, `link-unsubscribe`, `link-expired`: the `/alerts` page that email links open.
- `difficulty-set`: starting difficulty set to 50,000 (the floor is the pool's normal start).
- `private-on`, `public-private`: stats kept private, in Settings and as a visitor sees the Stats tab.
- `alerts-all`: email, Discord and Telegram connected with every alert on (hashrate drop,
  old Seine, weekly summary included).
- `discord-only`: Discord connected without an email, which is enough for alerts.
- `telegram-pending`: Connect Telegram pressed, waiting for Start in the bot (the page checks
  every few seconds).

`emails/` holds every alert email (and its chat text for Discord and Telegram) rendered by the real templates
(`ALERT_SAMPLES_DIR=$PWD/frontend/showcases/miner-settings/emails cargo test -p
blocknet-pool-api-app -- write_sample_mails_when_asked`), as HTML and plain text.

Add `save=slow` to hold each save for 4 s (shows "Saving…" and lets changes queue), or
`save=fail` to end the session on the first save (the "locked again" path).

Take-backs are clicked through from `locked`: Take it back from a rig, a refused IP, then
`203.0.113.24`, then a new password.

The captures in `captures/` were taken through the shared Playwright browser on 2026-11-04 (EST) at
1280 px and 390 px and in dark mode, all with synthetic data. Source: blocknet-pool branch
`miner-settings`. The unlocked-form captures (`unlocked-*`, `saved`, `saving`, `help-notes`,
`exchange`, `take-back-done`, `min-payout-invalid`, `locked-again`) were retaken on 2026-11-05
(EST) for auto-save, from branch `settings-autosave`.

The alert captures (`alerts-*`) and the unlocked forms that now show the alert rows
(`unlocked-*`, `saved`, `exchange`, `min-payout-invalid`) were retaken on 2026-11-05 (EST) from
branch `email-alerts`. `alerts-saved` and `alerts-saving` show an alert choice saving on its own;
`alerts-locked-again` is `save=fail`.
