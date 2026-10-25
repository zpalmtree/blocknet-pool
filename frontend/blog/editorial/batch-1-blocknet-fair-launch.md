# batch-1 research notes: blocknet-fair-launch

Writer notes for `content/blocknet-fair-launch/` (English, zh-Hans, zh-Hant drafts), 2026-10-25.

## Intent

A reader asking whether Blocknet had a premine, dev fund or treasury gets the answer, the genesis
block's contents, the consensus rules that enforce it, a way to verify both on the explorer and in
the code, and a path to mining BNT with seine on bntpool.com. Nearest existing owner: seineminer.com
`/blog/what-is-blocknet/` (one FAQ and one short section on the fair launch); its keyword map
(`seine-blog-batch1/site/editorial/keyword-map.md:48`) leaves "the fair launch" to bntpool.com.
This post adds the field-by-field genesis table, the coinbase checks, and reproducible
verification steps (explorer pages, an OpenSSL hash of the genesis message, function names in the
public repo). The emission table is left to `blocknet-block-reward-schedule`.

## Keyword-map row (proposed)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| blocknet fair launch | `/blog/blocknet-fair-launch/` | Show that Blocknet had no premine, dev fund or treasury, how to verify it, and how to mine it | Blocknet premine, Blocknet genesis block, Blocknet dev fund, Blocknet no premine, Blocknet 公平启动, Blocknet 有预挖吗 |

Boundary: `/blog/what-is-blocknet/` (seineminer.com) keeps the one-paragraph overview and links
here for depth; `blocknet-block-reward-schedule` owns reward amounts over time.

## Facts used and sources

Code refs: `blocknet-core@pool` = c646196 (2026-10-15), read with
`git -C /home/zp/Code/blocknet/blocknet-core show pool:<path>`. Public repo
`blocknetprivacy/core` = `upstream/master` 514a8cf; same rules there (line numbers differ, noted).

| Claim in post | Source |
|---|---|
| Genesis message "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything"; code cites the Wired article | `pool:block.go:2967` (source URL comment), `:2968` (constant). Fact sheet says 2967 for the constant; it is 2968. upstream: `block.go:2928` |
| Genesis timestamp 1771207118 = 2026-02-15 20:58:38 EST = 2026-02-16 01:58:38 UTC | `pool:block.go:2974`; converted with `TZ=America/New_York date -d @1771207118` |
| Previous hash = SHA3-256(message) | `pool:block.go:2994-2997`, `:3007` |
| SHA3-256(message) = `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df` | computed with `openssl dgst -sha3-256` (OpenSSL 3.0.13) and Python `hashlib.sha3_256`, 2026-10-25; matches explorer block 0 "previous" |
| Genesis: height 0, difficulty MinDifficulty (4), nonce 0, no transactions, zero merkle root | `pool:block.go:3000-3020`; MinDifficulty 4 in fact sheet (`block.go:63-68`) |
| Comment quote "Genesis has no transactions - no coinbase, no burned coins" / "First real block at height 1 will have the first coinbase" | `pool:block.go:3001-3002` |
| Validation rejects a block 0 with transactions or a non-zero merkle root | `pool:block.go:629-661` (`validateGenesisBlock`), tx check `:650-652`, merkle `:653-655`; called at `:466`. upstream: `block.go:507` |
| First tx must be coinbase; no other tx may be coinbase | `pool:block.go:284-299` (`ValidateBlock`, from `:239`), `:559-571` (`validateBlockWithContext`, from `:447`) |
| Coinbase: no inputs, fee zero, exactly one output | `pool:transaction.go:1135-1144` (`validateCoinbase`), dispatched at `:970-973` |
| Coinbase output must commit to exactly `GetBlockReward(height)`; blinding derived from tx public key and height (public data) | `pool:block.go:663-686` (`validateCoinbaseConsensus`), applied at `:316-318` and `:605-610`; comment `:688-690`. upstream: `block.go:541-549` |
| Coinbase memo must be empty | `pool:block.go:688-694` |
| Ordinary transactions balance: sum(pseudo-outputs) = sum(outputs) + fee, on commitments | `pool:transaction.go:1058-1062`, `:1074-1132` (`verifyCommitmentBalance`) |
| Every output carries a Bulletproofs range proof | `pool:transaction.go:1046-1056`; fact sheet (Bulletproofs, `crypto-rs/src/rangeproof.rs:8`) |
| No premine/treasury/dev-fund strings in code | `git grep -i 'premine\|treasury\|devfund\|dev_fund\|founder' pool -- '*.go' '*.rs'` returned nothing (2026-10-25); fact sheet agrees |
| Block 1 reward 723.25093035 BNT, single coinbase, 0 in / 1 out | explorer `https://explorer.blocknetcrypto.com/block/1` fetched 2026-10-25 ~01:45 EDT; `InitialReward` `pool:miner.go:18` |
| Explorer block 0 shows 0 transactions, merkle root of zeros, nonce 0, difficulty 4, previous `a94a7f2b…`, and a "reward" line | `https://explorer.blocknetcrypto.com/block/0` fetched 2026-10-25 ~01:45 EDT. The reward line is `GetBlockReward(height)` computed for display (`pool:explorer.go:453`, template `:1078`), which is why the post explains it |
| Public repo and file URLs resolve | `curl` 200 for `https://github.com/blocknetprivacy/core` and `.../blob/master/block.go`, 2026-10-25 |
| Argon2id 2 GiB, "designed to resist ASICs" | fact sheet §1 Proof of work, §6 |
| seine flow, default pool `stratum+tcp://bntpool.com:3333`, NVIDIA on Windows/Linux, `@name` | fact sheet §3-4; example post `how-to-mine-blocknet` |
| /stats is the miner lookup, /blocks lists found blocks | `frontend/src/lib/routes.ts:10`, `frontend/src/pages/StatsPage.tsx:47`, `BlocksPage.tsx:29` |

## Deliberately left out

- "Relaunch" wording, `ChainID 0x20260215`, the `block.go:2973` comment and any earlier chain
  (assignment rule).
- Block 1's timestamp (explorer: 2026-02-20 19:29:20 UTC); not needed, and it invites questions
  outside this post's job.
- Where transaction fees go (fact sheet marks "burned" as unverified).
- The explorer's "coins emitted" stat: it is computed from the schedule
  (`pool:explorer.go:146-149`, `miner.go:405`), so it isn't independent evidence.
- Any fee figures; seine's dev fee is not mentioned and "no dev fund" is said of the protocol only.
- Live pool numbers (`/api/stats` 2026-10-25 01:41 EDT: height 69,016, 246 miners) not used.

## Notes for the integrator

- Translations point seineminer.com links at the same locale (`https://seineminer.com/zh-hans/...`,
  `/zh-hant/...`) as the brief asks. `check-blog.mjs` `checkTranslation` requires external links to
  match the source exactly, so it will flag these once the post is published; either keep English
  seineminer.com URLs in translations or relax that check.
- Suggested related slugs: `blocknet-block-reward-schedule`, `mine-blocknet-on-bntpool`,
  `how-pplns-payouts-work`.
- Translations are `translation: draft`, sourceHash `ffbad58137b7`; no independent review yet.
