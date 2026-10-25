# Batch 2 notes: pool-vs-solo-mining-blocknet

Writer notes for `/blog/pool-vs-solo-mining-blocknet/` (2026-10-25). Pool code read from this
worktree (`blocknet-pool-blog`, branch `claude/pool-blog` at 673c3e7, on top of `origin/master`
02232a5). blocknet-core read with `git show pool:<path>` (c646196). seine read from `master`
(d20f253). Live values fetched 2026-10-25 at 02:03 EDT.

## Intent

A miner deciding between bntpool.com and solo mining wants to know how often their hashrate would
find a block alone, why a pool turns that into steady payouts, and how to set up solo mining with
seine if their operation is large enough. Nearest owners: seineminer.com
`/blog/how-to-mine-blocknet/` has a three-sentence "Can you mine Blocknet solo?" section (this post
expands it and links back); `/blog/how-pplns-payouts-work/` owns payout mechanics (this post gives
one paragraph and links there); batch-2 `estimate-blocknet-mining-rewards` owns BNT estimates with
live values and the reward (this post uses only blocks per day with hypothetical numbers and never
computes BNT). It adds the blocks-per-day comparison table and the solo setup steps.

Suggested keyword-map row:

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| Blocknet solo mining, pool vs solo mining Blocknet | `/blog/pool-vs-solo-mining-blocknet/` | Decide between bntpool.com and solo mining: expected solo blocks per day, why a pool pays steadily, solo setup with seine `--mode daemon` | how often find a block solo, seine daemon mode, mine against your own node, Blocknet solo vs pool, 独立挖矿还是矿池 / 單獨挖礦還是礦池 |

Boundary: payout mechanics stay with `how-pplns-payouts-work`; BNT/day estimates and live
snapshots stay with `estimate-blocknet-mining-rewards`; checkpoint use stays with
`blocknet-checkpoints-fast-sync` (this post mentions `/checkpoints` in one sentence).

## Facts used and sources

From the fact sheet (`seine-blog-batch2/site/editorial/fact-sheet.md`):

- 5-minute target, 288 blocks/day, expected solo blocks/day = 288 × (your H/s ÷ network H/s),
  difficulty ≈ expected hashes per block (§1, §2: `block.go:53-54`, `crypto-rs/src/pow.rs:163-164`).
- Reward constant within each 30-day month (§1, `miner.go:383-397`).
- PPLNS 6h window, 60 blocks before payout, 0.1 BNT minimum (§3, live `/api/info`; re-fetched
  2026-10-25 02:03 EDT, unchanged).
- AMD EPYC Zen 5 (EPYC 9R45) 49.4 H/s at 32 threads, used as "about 50 H/s" for one rig (§5 update).
- seine solo mode `./seine --mode daemon` with the node's API cookie; pool is the default (§4).
- Faraday note wording and seine update check every 12 h (§0, §4), matching the seineminer.com
  guide's note.

Verified for this post:

- Solo setup steps, cookie path, sync to 100%, custom `--api-url`/`--cookie` command, wallet
  password options: `seine@master:docs/guides/solo-mining.md:3`, `:11-12`, `:16`, `:20`, `:29`,
  `:35`, `:39-45`.
- seine resubmits the full solved block if the daemon forgot the template:
  `seine@master:docs/guides/solo-mining.md:58-59`.
- In pool mode with a local daemon, seine's wallet panel shows the local wallet balance next to
  the pool balance: `seine@master:docs/guides/solo-mining.md:52-54`.
- The daemon's block template pays the coinbase to the node's loaded wallet unless an `address`
  is passed, and refuses templates while far behind ("node is syncing"):
  `blocknet-core@pool:api_handlers.go:2532-2560`. seine adds `?address=` only when given one:
  `seine@master:src/daemon_api.rs:263-266`.
- The pool fetches templates from its daemon and submits solved blocks (workspace `AGENTS.md`,
  "How They Fit Together").
- Dashboard cards "Pool Hashrate", "Network Hashrate", "Blocks Found":
  `frontend/src/pages/DashboardPage.tsx:155`, `:171`, `:189`; live values come from `/api/stats`
  `pool.hashrate` and `chain.network_hashrate`.
- Checkpoints page: "Pool daemon checkpoints for fast initial sync and trusted chain pinning.",
  download `checkpoints.dat`: `frontend/src/pages/CheckpointsPage.tsx:60`, `:79-80`.
- Several rigs to one address and `@name` login: published post
  `content/mine-blocknet-on-bntpool/index.md:66`, `:92` (fact sheet §3 for names).
- PPLNS split in proportion to share difficulty over the six hours before the block, shares stay
  in the window six hours: published `how-pplns-payouts-work` and its batch-1 notes
  (`payout.rs:694-705`, `:775-797`).

Live snapshot, for context only and not in the post (`/api/stats`, 2026-10-25 02:03 EDT): chain
height 69,020, network ≈ 62,329 H/s, pool ≈ 5,240 H/s, 247 miners. The post's example uses
hypothetical round numbers (network 50,000 H/s; 50 / 500 / 5,000 H/s), labeled as such.

Arithmetic in the example: 288 × 50/50,000 = 0.288 blocks/day → 24 h ÷ 0.288 ≈ 83.3 h;
288 × 0.01 = 2.88/day → 8.3 h; 288 × 0.1 = 28.8/day → 1,440 min ÷ 28.8 = 50 min. A 50 H/s rig on a
5,000 H/s pool holds 1% of its work; 1% × 28.8 = 0.288 blocks of work per day.

## Notes for the integrator

- `related: []` per the brief, so `--strict` reports only "list 2-4 related posts". Suggested:
  `[how-pplns-payouts-work, estimate-blocknet-mining-rewards, mine-blocknet-on-bntpool]`
  (or `blocknet-checkpoints-fast-sync` in place of the last).
- Natural in-body links to add: `estimate-blocknet-mining-rewards` after the worked example, and
  `blocknet-checkpoints-fast-sync` on the `/checkpoints` sentence in the solo setup steps.
- No fees are mentioned anywhere in the post.
- Translations are `translation: draft` with `sourceHash: 58de5d26fdb3`; they need a separate review.
