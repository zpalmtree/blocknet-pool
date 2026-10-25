# Batch 3 notes: what-is-hashrate

Writer notes for `/blog/what-is-hashrate/` (2026-10-25). Pool code read with
`git show master:<path>` (master 9bf4eb6); seine read from `seine@master` (36f40e6). Live values
fetched 2026-10-25 at about 02:26 EDT.

## Intent

A general searcher wants to know what hashrate is, how to read the units (H/s, kH/s, MH/s...),
how their own hashrate relates to a pool's and the network's, and why each figure moves; the post
explains the concept and uses Blocknet's H/s scale and the bntpool.com dashboard as the worked
example. Nearest owners: `estimate-blocknet-mining-rewards` owns turning hashrate into BNT/day
(linked; this post keeps one share-of-blocks sentence and a 0.1% example);
`track-your-miner-on-bntpool` owns the My Stats tour and pool-vs-seine hashrate differences
(linked; this post gives the one-hour share estimate in two sentences); seineminer.com
`blocknet-mining-hardware-hashrate` owns per-machine results and buying advice (linked; the
best-per-generation table appears only to show the scale) and `argon2id-proof-of-work-explained`
owns the algorithm (linked).

Suggested keyword-map row:

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| what is hashrate, hash rate explained | `/blog/what-is-hashrate/` | Explain hashrate in general: units, your vs pool vs network hashrate, how network hashrate is calculated, why it fluctuates, with Blocknet and the bntpool.com dashboard as the example | H/s kH/s MH/s meaning, KH/s to H/s, network hashrate difficulty ÷ 300, why hashrate fluctuates, Blocknet hashrate H/s, 什么是算力 / 什麼是算力, 算力单位 |

## Facts used and sources

Fact sheet (`seine-blog-batch3/site/editorial/fact-sheet.md`):
- Argon2id, 2 GiB, 32-byte output; nonce = password, header without nonce = salt (§1, `pow.rs:11-17`, `:96-106`).
- H/s = completed hashes per second (§5 heading).
- A hash writes about 2 GiB and reads about 2 GiB (§9, argon2id notes); 2 GiB per thread, RAM caps thread count (§2, §4).
- Difficulty ≈ expected hashes per block; network hashrate ≈ difficulty ÷ 300 (§1, §2, `pow.rs:163-164`).
- 300 s target, 288 blocks/day; LWMA adjusts every block; longer solvetimes lower the next difficulty per the formula (§1, `block.go:53-54`, `:63-68`, `:1018-1046`).
- Vardiff aims for about 10 shares per 5 minutes (§3, README:285-287); seine tunes thread count and kernel on real mining work (§4).
- Best per generation: Zen 5 49.4 @ 32 (owner-approved update), Zen 4 35.7 @ 24, M4 Max 30.9 @ 16, Graviton4 21.5 @ 16, Xeon 8488C 13.1 @ 12 (§5).
- Faraday ~2027-02-16 planned PoW change; seine update check at start and every 12 h (§0, §4).
- Network hashrate snapshot 50,057 H/s at 00:26 EDT (§2 live snapshot).

Verified for this post:
- Dashboard units: `humanRate` steps H/s, KH/s, MH/s, GH/s, TH/s, PH/s at each 1,000, two decimals (`frontend/src/lib/format.ts:81-91`).
- Dashboard cards "Pool Hashrate" ("Primary value is estimated from credited share work") and "Network Hashrate" (`frontend/src/pages/DashboardPage.tsx:155-173`); "Round Progress" / "Expected Block Time" (`:209`, `:225`), computed as 300 × network ÷ pool (`apps/blocknet-pool-api/src/api.rs:5224-5229`).
- Miner and pool hashrate = accepted share difficulty over `HASHRATE_WINDOW` = 1 h (`api.rs:74`, `:122-160`), with a 5-minute warmup and 60 s floor for a brand-new miner (`api.rs:75-76`; `crates/pool-runtime/src/hashrate.rs:76`). Dashboard pool value `raw_hashrate` = `db_pool_hashrate` (`api.rs:4261`, `:4282`).
- Network hashrate = observed work ÷ observed time over the 10 most recent positive block intervals, "Match the daemon explorer" (`api.rs:70`, `:5557-5600`).
- seine dashboard shows "Current" and "Average" hashrate (`seine@master:src/miner/tui.rs:420-421`).
- Live 2026-10-25 02:26 EDT: `https://bntpool.com/api/stats` `chain.network_hashrate` 44,440.32, `pool.hashrate` 5,210.8, height 69,023; explorer `https://explorer.blocknetcrypto.com` "next difficulty 16943147 retargets every block" and "hashrate 44440.32 H/s recent blocks". 62,329 H/s at 02:03 EDT is from the batch-2 estimate notes' `/api/stats` snapshot. All volatile; the post dates each one.
- Bitcoin wiki (https://en.bitcoin.it/wiki/Hash_per_second, fetched 2026-10-25): hash/s counts double SHA-256 computations; "In 2014, a miner's performance was generally measured in Ghash/s, or billions of hashes per second."

Arithmetic:
- 16,943,147 ÷ 300 = 56,477 H/s; 16.9 million hashes per block.
- 49.4 H/s ÷ 32 threads = 1.54 hashes per thread per second.
- 10 shares / 5 min × 12 = about 120 shares per hour.
- 0.1% × 288 = 0.288 blocks per day.
- 44,440.32 H/s = 44.44 KH/s.

## Deliberate choices

- No fees, prices or profit figures; rewards appear only as share-of-blocks.
- The difficulty-vs-measured gap is explained neutrally (slow recent blocks read lower; LWMA brings them together), with no claim about which is "right".
- Bitcoin mentioned once, factually, with a dated wiki source; no comparison of coins.
- Units table "typical use" for MH/s is phrased generally ("faster proof-of-work algorithms") to avoid naming coins without sources.
- `related:` holds three published posts instead of `[]` because `--strict` warns "list 2-4 related posts" (same choice as batch 2). Swap in `what-is-a-mining-pool` when the integrator links the batch.
- Hardware table repeats five figures used in the estimate post; overlap with that post is 5.2% of 8-word phrases (table plus the shared Faraday note).

## Checks

- `node scripts/check-blog.mjs what-is-hashrate --strict`: ok (English and both translations).
- Overlap: 5.2% with `estimate-blocknet-mining-rewards`, 2.0% with `pool-vs-solo-mining-blocknet`, 1.3% with `blocknet-block-reward-schedule`.
- English body about 1,425 words; Answers 56 and 52 words; FAQ 60, 58, 56, 57 words; description 158 characters.
- Translations: `sourceHash: 475f24178b18`, `translation: draft`; no separate review pass yet.
