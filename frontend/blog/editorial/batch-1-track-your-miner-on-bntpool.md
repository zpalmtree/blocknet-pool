# Batch 1 research notes: track-your-miner-on-bntpool

Writer notes for `/blog/track-your-miner-on-bntpool/` (2026-10-25). Code read from this worktree at
`02232a5` (branch `claude/pool-blog`, level with `origin/master`); seine read with
`git -C seine show master:<path>`. Live values fetched 2026-10-25 about 01:41 EDT.

## Intent

A miner already hashing on bntpool.com wants to read their stats (hashrate, shares, balance,
payouts), understand the public Blocks, Payouts, Luck and Status pages, and pull the same data
into scripts or a home dashboard through the public API. Nearest owners: `mine-blocknet-on-bntpool`
(joining, confirming the miner shows up) and `how-pplns-payouts-work` (payout mechanics, luck as a
payout concept); this post adds the full field-by-field tour, the hashrate-difference explanation,
and the API reference with curl, jq and Home Assistant examples.

## Keyword-map row (proposed)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| bntpool stats, Blocknet miner stats, bntpool API | `/blog/track-your-miner-on-bntpool/` | Read your miner's stats and the pool's public pages, and query the public API | check Blocknet hashrate on pool, bntpool.com API miner endpoint, pool hashrate differs from miner, Blocknet pool luck/effort page, bntpool status uptime, Home Assistant mining sensor; zh: 矿池统计 / 礦池統計, bntpool API, 查看矿机算力 |

Boundary: payouts are described only as page fields (Estimated / Confirmed / Paid); PPLNS, the
60-block maturity and the minimum payout belong to `how-pplns-payouts-work`.

## Facts used (not in the fact sheet) with sources

My Stats page (`frontend/src/pages/StatsPage.tsx`):
- Heading "My Stats"; search placeholder "Wallet address, @name or $name" (`:349`, `:354`);
  Lookup / Clear buttons (`:370-389`).
- Names resolved client-side via `https://blocknet.id/api/v1/resolve/<name>`
  (`frontend/src/api/client.ts:175`, `StatsPage.tsx:140-146`); after lookup the input is set to the
  resolved full address and saved in localStorage (`StatsPage.tsx:157-163`).
- Balance cards: Estimated Rewards "Your share of unmatured blocks", Confirmed Rewards "Matured
  balance awaiting payout", Paid Balance "Already sent to this address" (`:410-432`); explanatory
  note "for a new miner this can take a few rounds of mining" (`:434-438`).
- Mining cards: Hashrate, Blocks Found, Mining Since (`:442-448`); Shares Accepted, Shares
  Rejected, Reject Rate, Avg Difficulty (`:451-456`).
- "Estimated From Recent Blocks" table: Height, Estimated Credit, Confirms Left, Time (`:509-540`).
- Hashrate History ranges 1h / 24h / 7d / 30d (`:548-553`).
- Per-rig table (UI title "Workers"): Worker, Hashrate, Accepted, Rejected, Last Share (`:556-595`).
  The post avoids the word "worker" because `check-blog.mjs` flags it; the integrator may prefer to
  name the table.
- "update available" badge linking to `https://github.com/zpalmtree/seine/releases`
  (`frontend/src/components/WorkerVersionBadge.tsx:3`, `:15-26`).
- Recent Payouts: Amount, Fee, Tx (explorer link), Status queued/unconfirmed/confirmed, Time
  (`StatsPage.tsx:598-638`); Fee column deliberately not mentioned.
- Recent Shares: Job, Worker, Difficulty, Status, Time (`:714-750`); 20 shares
  (`apps/blocknet-pool-api/src/api.rs:98` `MINER_DETAIL_SHARE_LIMIT = 20`).
- Refresh: app tick every 5 s while the tab is visible (`frontend/src/App.tsx:82-93`); miner details
  every 6th tick = 30 s (`StatsPage.tsx:263`).
- Explorer links go to `https://explorer.blocknetcrypto.com/...`
  (`frontend/src/components/ExplorerLink.tsx:7`).

Pool hashrate estimate:
- Miner detail hashrate = `db_miner_hashrate`, accepted share difficulty over `HASHRATE_WINDOW`
  = 1 h (`api.rs:75`, `:120-140`, used at `:4929`).
- Denominator is the observed span clamped between the 5-minute warmup (60 s for a brand-new
  miner) and 1 h (`crates/pool-runtime/src/hashrate.rs:76-129`), hence "averages over the minutes
  it has been mining ... after an hour the full one-hour window applies".
- Blocks Found = blocks where the miner is `finder` (`crates/pool-runtime/src/pgdb.rs:5660-5662`).
- ~10 shares per 5 minutes vardiff target: fact sheet §3.

Public pages:
- Blocks: columns Height, Reward, Effort, Round Time, Status, Time; filter All / Confirmed /
  Pending / Orphaned (`frontend/src/pages/BlocksPage.tsx:36-56`).
- Payouts: Amount, Miners Paid, Network Fee, Transaction, Status, Time; "appear here immediately
  as unconfirmed and flip to confirmed shortly after the first block confirmation"
  (`PayoutsPage.tsx:31-45`).
- Luck: Block, Effort, Round Time, Status, Found (`LuckPage.tsx:34-40`); route `/luck`
  (`frontend/src/lib/routes.ts:7`); dashboard "Pool Luck History" with "View All" to `/luck`
  (`DashboardPage.tsx:316-318`); dashboard "Average Luck" card (`DashboardPage.tsx:196`).
- Effort = round accepted work / block difficulty × 100 (`api.rs:3939-3943`, `:3767`).
- Status: Pool + Public HTTP, API, Stratum, Database, Daemon cards; Template Refresh, Refresh Lag,
  Template Age, Sync State, Chain Height; Historical Uptime "Local samples come from the on-box
  monitor. External samples come from the Cloudflare public probe."; Incident History
  (`StatusPage.tsx:17-23`, `:84-120`, `:123-128`, `:159-161`).
- Uptime windows 10m / 6h / 24h / 7d and the dated snapshot (pool_up_pct 100.0 at 24h,
  99.99669 at 7d): live `https://bntpool.com/api/status`, 2026-10-25 ~01:41 EDT. Volatile.
- Dashboard cards: Connected Miners, Pool Hashrate, Network Hashrate, Blocks Found, Average Luck
  (`DashboardPage.tsx:153-196`).

API:
- Public routes (no key): `apps/blocknet-pool-api/src/api.rs:937-969` and `README.md:218-234`.
  `/api/stats` and `/api/miner/{address}[/balance]` sit behind a per-IP limiter: 12 requests per
  5 s window, 429 with `Retry-After: 5` (`api.rs:92-96`, `:5733-5760`).
- Paging: `limit` default 25, max 200, `offset` (`api.rs:6001-6005`, test `:8105-8109`); responses
  `{items, total}` (live).
- Ranges `1h`, `24h` (default), `7d`, `30d` for `/api/stats/history` and
  `/api/miner/{address}/hashrate` (`api.rs:1466-1482`).
- `/api/blocks?status=` accepts confirmed / orphaned / pending (`api.rs:2680-2697`).
- Miner response fields (hashrate, total_accepted, total_rejected, workers, shares, payouts,
  blocks_found, mining_since) (`api.rs:755-768`); balance fields pending_confirmed,
  pending_queued, paid plus pending_estimate (`api.rs:404-409`, `:718-723`). Confirmed live:
  `/api/miner/YOUR_ADDRESS` returns 404 with the empty structure; `/balance` returns 200.
- `/api/stats/insights` keys: round, payout_eta, avg_effort_pct, luck_history, rejections (live).
- Amounts in atomic units, 1 BNT = 1e8 (fact sheet §1; `frontend/src/lib/format.ts:93-95`;
  live `/api/blocks` reward 46767460036).
- The miner API takes a full address; name resolution happens only in the browser (see above).
- Home Assistant snippet uses the documented RESTful integration (`rest:` with `resource`,
  `scan_interval`, `sensor` list, `value_template`, `unit_of_measurement`). Not run against a live
  HA instance.

seine:
- Pool worker name prompt on first run and `--pool-worker rig-01`
  (`seine@master:docs/guides/configuration.md:11`, `:39`; `src/config.rs:319-321`).
- Control API: fact sheet §4; `--api-server` mode in `seine@master:docs/API.md:29-37`. Linked doc:
  `https://github.com/zpalmtree/seine/blob/master/docs/API.md`.

## Left out on purpose

Pool fee, payout Fee / Network Fee columns, verification holds, the liquidity notice, the Pool
Rejects panel, the admin-only `/api/miners`, and `latest_miner_version` (live value 0.2.15 lags
seine v0.2.22).
