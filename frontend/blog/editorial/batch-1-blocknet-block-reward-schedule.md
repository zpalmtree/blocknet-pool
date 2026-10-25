# Batch 1 notes: blocknet-block-reward-schedule

Written 2026-10-25. Post: `content/blocknet-block-reward-schedule/` (draft; zh-Hans and zh-Hant at
`translation: draft`, `sourceHash` 204feed46f0b).

## Intent

A miner or holder searching "Blocknet block reward" or "Blocknet emission schedule" wants the
reward per block for each month, when it changes, the supply before and after the tail, and what
that means for mining. Nearest existing owner: seineminer.com `/blog/what-is-blocknet/` has one
summary paragraph (line 77) and its keyword map leaves "the reward schedule" to bntpool.com. This
post adds the full month-by-month table, the formula, cumulative supply and the yearly split.

## Suggested keyword-map row

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| blocknet block reward, blocknet emission schedule | `/blog/blocknet-block-reward-schedule/` | Look up the reward per block by month, the tail, supply before the tail | blocknet supply, blocknet tail emission, BNT per block, blocks per day, 区块奖励时间表 / 區塊獎勵時程表 |

Boundary: the fair-launch post owns no premine/genesis story; this post mentions the genesis block
only to explain why month 0 has 8,639 paid blocks. Payout mechanics stay with
`how-pplns-payouts-work` (this post only says the pool waits 60 blocks).

## Facts used and sources

From the fact sheet (`seine-blog-batch1/site/editorial/fact-sheet.md`): 5-minute blocks, 288/day,
8,640/month, formula and constants, tail from month 48 / height 414,720, 98,561,905.08 BNT before
the tail, "~100M over 4 years" label, 576 BNT/day and ~210,240 BNT/year tail, coinbase maturity 60,
LWMA 60-block window, 1 BNT = 1e8 atomic units, pool waits 60 blocks, seine default pool,
`@name` login, Faraday ~2027-02-16 per blocknetcrypto.com, seine update check every 12 h.

Verified in code (`git -C blocknet-core show pool:<path>`, pool = c646196, 2026-10-15):

- `miner.go:18-21` InitialReward 72,325,093,035, TailEmission 200,000,000, MonthsToTail 48,
  DecayRate 0.75; `miner.go:32` BlocksPerMonth = 2,592,000 / 300 = 8,640.
- `miner.go:383-397` GetBlockReward: month = height / 8640; month >= 48 returns the tail;
  otherwise `uint64((Initial - Tail) × exp(-0.75 × month/12) + Tail)` (truncated to atomic units).
- `miner.go:405-419` TotalEmittedSupply sums from height 1 (genesis has no coinbase).
- `block.go:54` BlockIntervalSec 300; `block.go:56` CoinbaseMaturity 60.
- `block.go:1016-1017` computeLWMA runs "for a given height" (difficulty recomputed per block).

Derived (scratch script replicating GetBlockReward in Python, integer truncation, output checked
against the fact sheet's 98,561,905.08 total and its sample rows for months 0, 1, 12, 24, 36, 47):

- Full monthly table, months 0-47, rewards rounded half-up to 2 decimals; cumulative totals
  include month 0 as 8,639 blocks.
- Yearly totals: months 0-11 54,475,840.37 (55.3%); 12-23 25,842,316.64 (26.2%); 24-35
  12,316,456.10 (12.5%); 36-47 5,927,291.98 (6.0%) of 98,561,905.08.
- e^-0.75 = 0.4724 (above-tail part after a year); e^-0.0625 = 0.9394 per month; month 12 reward
  is 47.4% of month 0.
- Tail adds 17,280 BNT per month (8,640 × 2).

Pool frontend (this worktree, master 02232a5): the dashboard `/` shows "Network Hashrate", pool
hashrate and "Current Block" (`frontend/src/pages/DashboardPage.tsx:171-181`); `/stats` is the
per-miner page (`frontend/src/lib/routes.ts:10`).

Live, dated: `https://bntpool.com/api/stats` on 2026-10-25 01:40 EDT returned
`current_job_height` 69,016 (month 7, 467.67 BNT; month 8 at 69,120, 439.46 BNT). The post states
this with the date only.

## Deliberately left out

- Price, profit or "current reward" as an evergreen number.
- Fees in the coinbase / burned fees (unverified per fact sheet §7).
- Faraday effects on emission (not public); only the dated upgrade notice with attribution.
- Calendar date of the tail (fact sheet says the 2030-01-26 projection is unreliable); the post
  uses "1,440 days of five-minute blocks, roughly four years" and the block height.
