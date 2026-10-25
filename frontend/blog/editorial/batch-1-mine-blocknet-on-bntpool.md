# Batch 1 notes: `mine-blocknet-on-bntpool`

Writer notes, 2026-10-25 (EDT). Pool code: this worktree, `blocknet-pool-blog` at 02232a5 (branch
`claude/pool-blog`, up to date with `origin/master`). seine: `seine@master` d20f253, read with `git show`.

## Intent

A reader who has seine (or is about to install it) wants to join bntpool.com: the pool address, how
login works with an address or a Blocknet ID `@name`, how to name and run several rigs, and how to
confirm on the pool's stats page that shares are arriving. Nearest owner is seineminer.com's
`/blog/how-to-mine-blocknet/`, which owns installing seine and first shares on any OS; this post
links to it for installation and adds the pool-side procedure (name resolution and name changes,
rig names and the difficulty memory, headless first start, stats-page checks, what `/start` shows).

## Keyword-map row (proposed)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| bntpool.com, bntpool stratum address, Blocknet mining pool | `/blog/mine-blocknet-on-bntpool/` | Join and use bntpool.com with seine: pool address, address or `@name` login, rig names, confirming shares on My Stats, the Get Started page | stratum+tcp://bntpool.com:3333, mine to @name, Blocknet pool worker name, several rigs one address, bntpool 矿池地址 |

## Facts used (not already in the fact sheet, or re-verified)

- Live pool settings (2026-10-25 01:41 EDT, `https://bntpool.com/api/info`): `stratum_port` 3333,
  `min_payout_amount` 0.1, `blocks_before_payout` 60, `pplns_window_duration` "6h"
  (`pool_fee_pct` 2.5 deliberately not mentioned). Live `/api/stats` at the same time: 246 miners,
  ~5,270 H/s pool, height 69,016; not used in the post.
- First-run prompts, verbatim: "Enter your Blocknet address or shortname (@name / $name): " and
  "Enter pool URL [default: stratum+tcp://bntpool.com:3333]: ". `seine@master:src/config.rs:2315`, `:2335`.
- Non-interactive first start needs `--address` and `--pool-url`. `seine@master:src/config.rs:2307-2331`.
- Seine auto-generates the rig name `seine-NNNN` (four digits) on first run and saves it with the
  address and pool URL in the config. `seine@master:src/config.rs:2343-2348`, `:2452-2459`,
  `:2398-2412`. Override flag `--pool-worker` (`src/config.rs:319-321`; `docs/guides/configuration.md:39`).
  Note: `configuration.md:11` says the first run "asks for ... worker name", but the code generates
  it without a prompt; the post follows the code.
- Saved config at `seine-data/seine.config.json`; reset by editing/deleting it or overriding with
  `--address`, `--pool-url`, `--pool-worker`. `blocknet-pool-blog:frontend/src/pages/StartPage.tsx:107-111`;
  `seine@master:docs/guides/configuration.md:27-28`.
- Seine resolves a shortname at startup, logs the resolved address, keeps it fixed for the run
  including reconnects, and resolves saved names again on the next run.
  `seine@master:docs/guides/configuration.md:78-88`; `StartPage.tsx:102-104`.
- Shortnames need seine v0.2.16+. `seine@master:docs/guides/configuration.md:19`.
- Pool login resolves `@name`/`$name` (case-insensitive, 1-64 chars of a-z 0-9 `_` `-` `.`) via
  `https://blocknet.id/api/v1/resolve/{handle}`, verifies the Ed25519 signature against
  `/.well-known/blocknet-id.json`, validates the address, then logs in with the full address.
  `crates/pool-runtime/src/shortname.rs:16-35`, `:78-104`; `crates/pool-runtime/src/stratum.rs:681-700`.
  Address fixed per connection; new login resolves again; "a name change never redirects an
  existing balance". `README.md:280`.
- Shell quoting: "Keep the quotes around shortnames so your shell passes them correctly."
  `StartPage.tsx:89-91`; `configuration.md:82`.
- Rig (worker) name: login carries `worker`; pool trims it and keeps up to 64 chars (blank becomes
  "default"). `crates/pool-common/src/protocol.rs:51-58`, `:79-86`; stored with the connection in
  `stratum.rs:703-711`.
- Vardiff difficulty cached per `address+worker`, reused on reconnect when fresh (1 h TTL).
  `README.md:286`. Per-connection vardiff that adjusts while mining: `README.md:284`, `:287`
  (defaults 60 / 10 shares per 5 min in `crates/pool-runtime/src/config.rs:85`, `:110-111`). The
  post says only "within minutes", derived from that target.
- Balances and payouts keyed by the resolved address. `README.md:280`.
- Each submit carries `miner_version`/`backend` (`protocol.rs:69-77`); the stats page shows an
  "update available" badge linking to github.com/zpalmtree/seine/releases for rigs not on the
  latest version. `frontend/src/components/WorkerVersionBadge.tsx:3-28`; `StatsPage.tsx:574-582`.
- Stats page: input placeholder "Wallet address, @name or $name" and **Lookup** button
  (`StatsPage.tsx:352-368`); resolves names at blocknet.id and shows the resolved address badge
  (`StatsPage.tsx:150-163`, `:388-392`; `frontend/src/api/client.ts:174-175`); remembers the last
  lookup in localStorage (`StatsPage.tsx:162`, `frontend/src/lib/storage.ts:1`). Cards and tables used:
  Shares Accepted (`:452`), per-rig table with Hashrate/Accepted/Rejected/Last Share (`:556-595`),
  Recent Shares with rig, difficulty, status (`:714-745`), Estimated/Confirmed Rewards text (`:409-440`).
- Nav labels "Get Started" and "My Stats". `frontend/src/App.tsx:18-27`.
- Get Started page contents: Pool Information table (Stratum with click-to-copy, Fee, Payout Scheme,
  Min Payout, Block Confirmations), three-step Quick Start linking `https://seineminer.com/#download`,
  copyable command `./seine --pool-url <url> --address YOUR_BLOCKNET_ADDRESS`, notes, "How Rewards
  Are Counted", "Seine in Action" screenshots. `StartPage.tsx:15`, `:31-115`, `:118-216`.
- No account system for miners: public endpoints and pages need no key; only admin routes use an
  API key. `README.md:218-246`; `App.tsx:18-27`.

## Choices

- "worker" fails `--strict` as a CAUTION word, so the prose says "rig name" and the
  `--pool-worker` flag appears only inside a fenced code block. The pool UI's "Workers" table is
  described, not named.
- Fees left out per the writing guide; the Get Started bullet lists the table without the fee row.
- Payout mechanics kept to two sentences (owned by `how-pplns-payouts-work`); the stats-page tour
  kept to the three checks a new miner needs (owned by `track-your-miner-on-bntpool`).
- Translations link seineminer.com in the same locale as the brief asks. `check-blog.mjs`
  requires translations' external links to match the source exactly, so once the post is
  published these links will fail the check (tested on a scratch copy: the only errors). Either
  the check or the brief rule needs to change before publishing.
