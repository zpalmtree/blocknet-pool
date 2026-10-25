# Batch 2 notes: estimate-blocknet-mining-rewards

Writer notes for `/blog/estimate-blocknet-mining-rewards/` (2026-10-25). Pool code read from this
worktree (`blocknet-pool-blog`, branch `claude/pool-blog`, HEAD 673c3e7). Live values fetched
2026-10-25 at 02:03 EDT.

## Intent

A miner wants to know how much BNT their hardware will mine per day: the formula, where each
input sits on the bntpool.com dashboard (and in `/api/stats`), a dated worked example, and how
pool mining (PPLNS) changes the timing of rewards. Nearest owners: `blocknet-block-reward-schedule`
owns the reward table (linked, not copied; this post quotes only months 7 and 8);
`pool-vs-solo-mining-blocknet` owns the pool-or-solo choice and blocks-per-day framing (this post
uses blocks/day only as a step toward BNT/day and mentions the solo timing once);
`how-pplns-payouts-work` owns the split mechanics (linked); seineminer.com
`blocknet-mining-hardware-hashrate` owns hardware results (linked; the table here repeats the five
best-per-generation figures as calculation inputs).

Suggested keyword-map row:

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| blocknet mining calculator, blocknet mining rewards estimate | `/blog/estimate-blocknet-mining-rewards/` | Estimate BNT per day from your hashrate, the network hashrate and the block reward, with inputs read from the pool dashboard | how much BNT per day, Blocknet network hashrate, BNT per day per H/s, expected block time, 挖矿收益计算 / 挖礦收益計算 |

## Facts used and sources

Fact sheet (`seine-blog-batch2/site/editorial/fact-sheet.md`):
- 5-minute target, 288 blocks/day, 8,640 blocks per month (§1, §2; `blocknet-core@pool:block.go:53-54`, `miner.go:32`).
- LWMA difficulty adjusts every block (§1, `block.go:63-68`).
- Difficulty ≈ expected hashes per block (§1, `crypto-rs/src/pow.rs:163-164`).
- Expected blocks/day = 288 × (your H/s ÷ network H/s); × reward = gross BNT/day; pool income
  PPLNS-smoothed, minus pool fee and seine dev fee (§2, derived).
- Reward formula and month = height ÷ 8,640 (§1, `miner.go:383-397`); month 7 = 467.67 BNT,
  month 8 = 439.46 BNT starting at height 69,120 (§2).
- PPLNS 6h, 60 blocks before payout, 0.1 BNT minimum (§3, live `/api/info`; re-fetched
  2026-10-25 02:03 EDT, unchanged).
- Best-per-generation hashrates: Zen 5 49.4 H/s @ 32 (owner-approved update), Zen 4 35.7 @ 24,
  M4 Max 30.9 @ 16, Graviton4 21.5 @ 16, Xeon 8488C 13.1 @ 12 (§5).
- seine mines NVIDIA GPUs alongside the CPU; huge pages; update check every 12 h; Faraday
  ~2027-02-16 planned PoW change (§0, §4).
- Effort averages around 100% over many rounds (derived, as in batch-1 PPLNS notes).

Verified for this post:
- Live snapshot, `https://bntpool.com/api/stats` 2026-10-25 02:03 EDT:
  `chain.current_job_height` 69020, `chain.network_hashrate` 62329.14, `pool.hashrate` 5240.13,
  `pool.miners` 247. Volatile; labelled as a snapshot in a `<Note>`. (The fact sheet's 00:26 EDT
  snapshot had 50,057 H/s network; the 10-block sample moves a lot, which the post says.)
- Live `/api/blocks?limit=3` (same time): `reward` 46767460036 (= 467.67460036 BNT) on heights
  69002-69015, the full gross block reward; the Blocks page shows it in its Reward column
  (`frontend/src/pages/BlocksPage.tsx:51`, `:67`); unconfirmed rows filled by
  `estimated_block_reward(height)` (`apps/blocknet-pool-api/src/api.rs:161-164`,
  `crates/pool-runtime/src/rewards.rs:8-18`).
- Dashboard cards "Network Hashrate", "Current Block", "Pool Hashrate":
  `frontend/src/pages/DashboardPage.tsx:153-181`. Displayed with `humanRate`, which switches to
  KH/s at 1,000 H/s with two decimals: `frontend/src/lib/format.ts:81-91`.
- Network hashrate = observed work ÷ observed time over the most recent 10 positive block
  intervals, matching the daemon explorer: `api.rs:5446-5530`, `EXPLORER_HASHRATE_SAMPLE_COUNT = 10`
  at `api.rs:71`.
- "Expected Block Time" in the "Round Progress" panel = 300 × network ÷ pool hashrate:
  `api.rs:5161-5166` (`ROUND_TARGET_SECONDS = 300.0`, `api.rs:78`); labels at
  `DashboardPage.tsx:209`, `:225-226`. Snapshot: 300 × 62,329 ÷ 5,240 = 3,568 s, about an hour.
- My Stats "Hashrate" card: `frontend/src/pages/StatsPage.tsx:445`; hourly average per batch-1
  track-your-miner notes. Balance cards "Confirmed Rewards", "Paid Balance": `StatsPage.tsx:421-428`.
- New miner's PPLNS portion grows as shares fill the 6 h window: batch-1 PPLNS notes
  (`crates/pool-runtime/src/payout.rs:694-705`, `:775-797`).

Arithmetic (all from the snapshot, month 7 = 467.67460036 BNT, month 8 = 439.4608 BNT):
- Zen 5: 49.4 ÷ 62,329.14 = 0.0793%; × 288 = 0.2283 blocks/day; × 467.67 = 106.75; × 439.46 = 100.31; 1 ÷ 0.2283 = 4.38 days/block.
- Zen 4 35.7: 0.0573%, 77.15, 72.49. M4 Max 30.9: 0.0496%, 66.77, 62.75. Graviton4 21.5: 0.0345%, 46.46, 43.66. Xeon 13.1: 0.0210%, 28.31, 26.60.
- Pool: 5,240.13 ÷ 62,329.14 = 8.41%; × 288 = 24.21 blocks/day. 49.4 ÷ 5,240.13 = 0.943%; 0.00943 × 24.21 × 467.67 = 106.75 (same as the direct formula).
- 69,020 ÷ 8,640 = 7.99 → month 7; 69,120 − 69,020 = 100 blocks to month 8.

## Deliberate choices

- No price, fiat, profit or electricity-cost figures. Results are gross BNT; one plain sentence
  says the pool fee and seine's dev fee come out of the gross figure, with no percentages.
- The 9950X3D desktop figure and GPU hashrates are left out (fact sheet: best per generation only;
  no GPU H/s).
- `related:` is filled with three published posts (`how-pplns-payouts-work`,
  `blocknet-block-reward-schedule`, `track-your-miner-on-bntpool`) instead of `[]`, because
  `--strict` warns "list 2-4 related posts" now that five posts are published. Swap in batch-2
  slugs (`pool-vs-solo-mining-blocknet`) when the integrator links the batch.
- Snapshot numbers will age; refresh the `<Note>` example (or keep it as an explicitly dated
  example) at publish time. Month 8 began about 8 hours after the snapshot, so by publication the
  439.46 column is the current-month one.

## Checks

- `node scripts/check-blog.mjs estimate-blocknet-mining-rewards --strict`: ok.
- Overlap: 2.0% of 8-word phrases with `pool-vs-solo-mining-blocknet`, 1.3% with
  `blocknet-block-reward-schedule`, 0.4% with `how-pplns-payouts-work`.
- A scratch copy with `draft` removed and translations marked reviewed reported only the missing
  keyword-map row (translation structure, links and hash all pass).
- Translations: `sourceHash: 43fc4321890d`, `translation: draft`; no separate review pass yet.
