# Batch 1 notes: how-pplns-payouts-work

Writer notes for `/blog/how-pplns-payouts-work/` (2026-10-25). Pool code read from this worktree
(`blocknet-pool-blog`, branch `claude/pool-blog`, up to date with `origin/master` at 02232a5).
Live values fetched 2026-10-25 at 01:41 EDT.

## Intent

A bntpool.com miner wants to know how the pool turns their shares into payouts: what PPLNS over a
six-hour window means, why rewards wait 60 confirmations, when the 0.1 BNT minimum triggers a
payment, how to read Estimated/Confirmed/Paid on the stats page, and what effort and luck mean.
Nearest owners: seineminer.com `/blog/how-to-mine-blocknet/` has a three-sentence "How long until
you get paid?" section (links here would deepen it); the batch's `track-your-miner-on-bntpool`
owns the stats-page tour, so this post covers only the balance cards and the luck/effort meaning.
It adds the actual allocation procedure from the pool code and a worked example.

Suggested keyword-map row:

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| bntpool PPLNS, Blocknet pool payouts | `/blog/how-pplns-payouts-work/` | How bntpool.com splits block rewards and when it pays | PPLNS 6-hour window, 60 confirmations, 0.1 BNT minimum payout, estimated vs confirmed rewards, pool effort/luck, 矿池 PPLNS 收益支付 |

## Facts used and sources

Fact sheet (§1, §2, §3): PPLNS with a 6h window, 0.1 BNT minimum, 60 blocks before payout
(live `/api/info`); coinbase maturity 60 blocks (`blocknet-core@pool:block.go:56`); 5-minute
target (`block.go:53-54`); difficulty ≈ expected hashes per block (`crypto-rs/src/pow.rs:163-164`);
start-page wording and tentative rewards shown separately (`StartPage.tsx:147-195`).

Verified for this post:

- Live `https://bntpool.com/api/info` (2026-10-25 01:41 EDT): `"min_payout_amount":0.1,
  "blocks_before_payout":60,"pplns_window_duration":"6h"`. Handler:
  `apps/blocknet-pool-api/src/api.rs:1025-1046`. Config fields and defaults:
  `crates/pool-runtime/src/config.rs:59-61`, `:117-119`.
- Window is the 6 hours ending at the block (block timestamp, or the latest share recorded for
  that block hash if later): `crates/pool-runtime/src/payout.rs:694-705`, `reward_window_end` at
  `:2326-2331`.
- Shares weighted by difficulty (verified difficulty plus aged provisional difficulty, capped):
  `weight_shares`, `payout.rs:3051-3114`. Each address gets `amount × weight / total_weight`
  (remainder to the largest weight): `payout.rs:775-797`.
- Block marked confirmed when depth ≥ `blocks_before_payout` and its hash still matches the
  node's block at that height; a mismatch past 60 + 6 grace blocks is marked orphaned:
  `payout.rs:564-617`. Only confirmed, non-orphaned, unpaid blocks are credited:
  `pgdb.rs:1927-1933`, `distribute_rewards` at `payout.rs:620`.
- A payout is queued only when the confirmed balance is at least `min_payout_amount`:
  `payout.rs:1123`. Paid automatically: stats-page text "being paid out automatically"
  (`frontend/src/pages/StatsPage.tsx:435-438`).
- Stats balance cards "Estimated Rewards" ("Your share of unmatured blocks"), "Confirmed Rewards"
  ("Matured balance awaiting payout"), "Paid Balance" ("Already sent to this address"):
  `StatsPage.tsx:411-432`; per-block estimate table "Estimate only. These amounts can still move
  until those blocks confirm or are orphaned.": `StatsPage.tsx:509-542`.
- Vardiff per connection (stated in the post only as "a steady stream of shares"):
  `blocknet-pool-blog/README.md:285-287`.
- Effort = accepted round work ÷ block difficulty × 100: `api.rs:3939-3943` (luck rows),
  `api.rs:3762-3770` (backfill), current round `api.rs:5153-5157`. Luck page columns Block,
  Effort, Round Time, Status, Found: `frontend/src/pages/LuckPage.tsx:35-39`. Status badges
  pending/confirmed/orphaned: `frontend/src/components/BlockStatusBadge.tsx`. Dashboard "Average
  Luck", "Round Effort", "Luck is probabilistic. Rounds above 100% are normal":
  `frontend/src/pages/DashboardPage.tsx:196`, `:217`, `:244`.
- "Over many rounds effort averages around 100%": derived; effort's expected value is 100% because
  difficulty is the expected work per block (fact sheet §1, `pow.rs:163-164`).
- 40,873 blocks found: live `https://bntpool.com/api/stats` `pool.blocks_found` on 2026-10-25
  01:41 EDT; dated in the post. Volatile; refresh or drop when publishing.

Worked-example numbers (1,000,000 / 20,000 / 400 BNT / 30 minutes / 100) are invented round
numbers, labelled as an example in a `<Note>`; 400 BNT is "what the pool shares out", not a
claimed block reward.

## Deliberate omissions

- Pool fee (2.5%) not mentioned per the favorable rule; the allocation is described as shares of
  "the reward"/"what the pool shares out", which stays accurate in proportion.
- Payout sweep interval (production value unverified, fact sheet §7) not stated; the post says
  "automatically".
- Trust policy details (minimum verified shares, provisional cap) and network transaction fees on
  payouts not covered.
- Live average luck not quoted (volatile).

## Translation note

Per the brief, the translations point seineminer.com links at `/zh-hans/` and `/zh-hant/`. The
check script's translation rule ("external links must match the source") will flag those two
links once the post is published (drafts skip translation checks). A scratch copy with draft
removed and translations marked reviewed reported only those two errors. Either the script
should treat locale-matched seineminer.com URLs as equal, or the links should revert to English.
