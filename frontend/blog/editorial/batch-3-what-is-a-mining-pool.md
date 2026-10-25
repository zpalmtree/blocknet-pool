# Batch 3 notes: what-is-a-mining-pool

Writer notes for `/blog/what-is-a-mining-pool/` (2026-10-25). Pool code read from this worktree
(`blocknet-pool-blog`, branch `claude/pool-blog` at 6e1444c). Fact sheet:
`seine-blog-batch3/site/editorial/fact-sheet.md`. Live values fetched 2026-10-25 at 02:25 EDT.

## Intent

Someone searching "what is a mining pool" wants the concept explained: what a pool does, why
miners join one, how shares and payout schemes (PPLNS, PPS, solo) work, and what to check before
choosing a pool. The post answers that generically and uses bntpool.com as the worked example
throughout. Nearest owners: `/blog/how-pplns-payouts-work/` owns bntpool.com's payout mechanics
(this post gives one paragraph and links there); `/blog/pool-vs-solo-mining-blocknet/` owns the
pool-vs-solo decision and solo setup (this post reuses its hypothetical 50 / 5,000 / 50,000 H/s
example in brief and links there). It adds the generic definition, the share explanation, the
PPLNS/PPS/solo comparison table and the "what to look for" checklist.

Suggested keyword-map row:

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| what is a mining pool, how mining pools work | `/blog/what-is-a-mining-pool/` | Understand what a mining pool is, why miners pool, shares, PPLNS vs PPS vs solo, and what to look for in a pool, with bntpool.com as the example | mining pool explained, what is a share in a mining pool, PPLNS vs PPS, pool payout schemes, how to choose a mining pool, 什么是矿池 / 什麼是礦池 |

Boundary to record with seineminer.com: the seine batch-3 worktree has a draft
`how-crypto-mining-works` with a "Why do most miners join a pool?" section and an FAQ "What is a
share in a mining pool?". That post owns how mining works overall; this one owns pools in depth
(shares, payout schemes, choosing a pool). Suggest the seine post link here for its pool section.

## Facts used and sources

From the fact sheet:

- 5-minute target, 288 blocks/day, expected solo blocks/day = 288 × (your H/s ÷ network H/s),
  difficulty ≈ expected hashes per block (§1 `block.go:53-54`, `crypto-rs/src/pow.rs:163-164`; §2).
- Coinbase maturity 60 blocks (§1 `block.go:56`).
- PPLNS, 6h window, 60 blocks before payout, 0.1 BNT minimum, Stratum port 3333 (§3, live
  `/api/info`; re-fetched 2026-10-25 02:25 EDT, unchanged). Stratum URL
  `stratum+tcp://bntpool.com:3333`, seine's default pool (§3).
- Vardiff per connection, ~10 shares per 5 minutes, starting difficulty 60 (§3, `README.md:285-287`).
- Login with address or Blocknet ID `@name` (§3). seine tunes threads, kernels, memory (§4).
- seine solo mode `./seine --mode daemon` (§4).

Verified for this post:

- The pool fetches block templates from its own daemon (`/api/mining/blocktemplate`) and submits
  solved blocks (`/api/mining/submitblock`): `crates/pool-runtime/src/node.rs:386-391`, `:490`;
  `jobs.rs:980`.
- Each share is checked against the share target, then against the network target to flag a block
  candidate: `crates/pool-runtime/src/validation.rs:866-876`, `:889`.
- Vardiff difficulty cached per `address+worker` and reused on reconnect/restart with a 1h TTL
  ("remembers each rig's difficulty for an hour"): `README.md:286`.
- Stratum is the pool protocol; login requires protocol v2: `README.md:279-282`.
- Dashboard cards Connected Miners, Pool Hashrate, Network Hashrate, Blocks Found, Round Effort:
  `frontend/src/pages/DashboardPage.tsx:153`, `:155`, `:171`, `:189`, `:217`.
- Get Started page lists Fee, Payout Scheme, Min Payout, Block Confirmations and the PPLNS window:
  `frontend/src/pages/StartPage.tsx:147-191`. Fee is not mentioned in the post.
- PPLNS split by share difficulty over the six hours before the block; shares stay in the window
  six hours: published `how-pplns-payouts-work` and its batch-1 notes (`payout.rs:694-705`,
  `:775-797`).
- Blocks, payouts, luck, status, stats pages: fact sheet §3 public pages; `frontend/src/pages/`.

General definitions (industry-standard, no project-specific claim): PPLNS = pay per last N shares;
PPS = pay per share, a fixed payment per accepted share at its expected value (share difficulty ÷
network difficulty × block reward), with the operator absorbing block-finding variance; solo =
mining against your own node. No other pools or projects are named.

Live snapshot used in the post, dated (`https://bntpool.com/api/stats`, 2026-10-25 02:25 EDT):
pool hashrate 5,232.98 H/s, network hashrate 44,440.32 H/s, 246 miners, job height 69,023.
Arithmetic: 5,233 ÷ 44,440 = 11.8% (written "about 12%"); 288 × 0.1178 = 33.9 blocks/day
("around 34"); 1,440 min ÷ 33.9 = 42.5 min ("roughly every 42 minutes"). Not used: blocks_found,
paid_to_miners_total, orphan figures (semantics not verified; orphan data off-message).

Hypothetical example (labeled in the post): 50 H/s on a 50,000 H/s network = 0.1%, 0.288
blocks/day, ~3.5 days between blocks; a 5,000 H/s pool finds 28.8/day, one per 50 min; 50 H/s is
1% of that pool.

## Notes for the integrator

- `related: []` per the brief, so `--strict` reports only "list 2-4 related posts". Suggested:
  `[how-pplns-payouts-work, pool-vs-solo-mining-blocknet, what-is-hashrate]` (or
  `mine-blocknet-on-bntpool` in place of the last).
- Natural in-body link to add: `what-is-hashrate` on the first "hashrate" mention in "Why do miners
  join a mining pool?".
- Body links already include `pool-vs-solo-mining-blocknet`, `how-pplns-payouts-work`,
  `track-your-miner-on-bntpool`, `estimate-blocknet-mining-rewards`, `mine-blocknet-on-bntpool`.
- No fees are mentioned. The live snapshot paragraph is dated; refresh or drop it on later edits.
- Translations are `translation: draft` with `sourceHash: e58ae54a6c8f`; they need a separate review.
