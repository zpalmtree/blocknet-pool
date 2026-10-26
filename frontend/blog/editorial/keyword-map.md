# bntpool.com blog keyword map

Which bntpool.com blog URL owns which search job. The build fails if a published post has no row.
Same rules as the seineminer.com keyword map (seine repo, `site/editorial/keyword-map.md`); never
target a search job a seineminer.com post already owns.

## Main site and blog boundaries

- `/` owns **bntpool / Blocknet mining pool**: the pool dashboard and entry point.
- `/start` owns the quick connection workflow; the setup article owns the detailed
  walkthrough, worker names and troubleshooting links.
- `/stats`, `/blocks`, `/payouts`, `/luck` and `/status` own their live lookups and
  histories. Articles explain how to interpret those values and link back to the tools.
- `/checkpoints` supplies the current download; the checkpoint article owns the
  verification and sync procedure.

These are distinct reader jobs, not evidence of current query rankings. The eight public app pages use self-canonical `?lang=<code>` URLs for non-English
languages, with translated initial HTML and reciprocal hreflang links. English keeps
the query-free path. Tracking and lookup parameters are excluded from canonicals.
Admin and unknown routes stay noindex and outside the language clusters. Localized
blog pages retain their existing path URLs and hreflang clusters.

## Current ownership

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| join bntpool.com, bntpool stratum address, connect Seine to bntpool | `/blog/mine-blocknet-on-bntpool/` | Join and use bntpool.com with seine: pool address, address or `@name` login, rig names, confirming shares on My Stats, the Get Started page | stratum+tcp://bntpool.com:3333, mine to @name, Blocknet pool worker name, several rigs one address, bntpool 矿池地址 |
| mining pool luck, pool effort explained | `/blog/mining-pool-luck-explained/` | Interpret difficulty-weighted effort, expected block time, long rounds and bntpool luck history | 100% effort, pool block overdue, Average Luck, pool luck vs payouts, 矿池幸运值 / 礦池幸運值 |
| bntpool PPLNS, Blocknet pool payouts | `/blog/how-pplns-payouts-work/` | How bntpool.com splits block rewards and when it pays | PPLNS 6-hour window, 60 confirmations, 0.1 BNT minimum payout, estimated vs confirmed rewards, pool effort and luck, 矿池 PPLNS 收益支付 |
| bntpool stats, Blocknet miner stats, bntpool API | `/blog/track-your-miner-on-bntpool/` | Read your miner's stats and the pool's public pages, and query the public API | check Blocknet hashrate on pool, bntpool.com API miner endpoint, pool hashrate differs from miner, Blocknet pool luck/effort page, bntpool status page, Home Assistant mining sensor, 矿池统计 / 礦池統計, 查看矿机算力 |
| blocknet block reward, blocknet emission schedule | `/blog/blocknet-block-reward-schedule/` | Look up the reward per block by month, the tail, supply before the tail | blocknet supply, blocknet tail emission, BNT per block, blocks per day, 区块奖励时间表 / 區塊獎勵時程表 |
| blocknet fair launch | `/blog/blocknet-fair-launch/` | Show that Blocknet had no premine, dev fund or treasury, how to verify it, and how to mine it | Blocknet premine, Blocknet genesis block, Blocknet dev fund, Blocknet no premine, Blocknet 公平启动, Blocknet 有预挖吗 |
| blocknet checkpoints, blocknet fast sync, checkpoints.dat | `/blog/blocknet-checkpoints-fast-sync/` | Understand bntpool.com's checkpoints and use them to sync a Blocknet node fast | sync Blocknet node, Blocknet full sync, bntpool checkpoints API, verify checkpoints.dat SHA-256, load-checkpoints, Blocknet 检查点快速同步 / Blocknet 檢查點快速同步 |
| Blocknet solo mining, pool vs solo mining Blocknet | `/blog/pool-vs-solo-mining-blocknet/` | Decide between bntpool.com and solo mining: expected solo blocks per day, why a pool pays steadily, solo setup with seine `--mode daemon` | how often find a block solo, seine daemon mode, mine against your own node, Blocknet solo vs pool, 矿池挖矿还是单独挖矿 / 礦池挖礦還是單獨挖礦 |
| blocknet mining calculator, blocknet mining rewards estimate | `/blog/estimate-blocknet-mining-rewards/` | Estimate BNT per day from your hashrate, the network hashrate and the block reward, with inputs read from the pool dashboard | how much BNT per day, Blocknet network hashrate, BNT per day per H/s, expected block time, 挖矿收益计算 / 挖礦收益計算 |
| what is a mining pool, how mining pools work | `/blog/what-is-a-mining-pool/` | Understand what a mining pool is, why miners pool, shares, PPLNS vs PPS vs solo, and what to look for in a pool, with bntpool.com as the example | mining pool explained, what is a share in a mining pool, PPLNS vs PPS, pool payout schemes, how to choose a mining pool, 什么是矿池 / 什麼是礦池 |
| what is hashrate, hash rate explained | `/blog/what-is-hashrate/` | Explain hashrate in general: units, your vs pool vs network hashrate, how network hashrate is calculated, why it fluctuates, with Blocknet and the bntpool.com dashboard as the example | H/s kH/s MH/s meaning, KH/s to H/s, network hashrate from difficulty, why hashrate fluctuates, Blocknet hashrate H/s, 什么是算力 / 什麼是算力, 算力单位 |

## Boundaries with seineminer.com

| seineminer.com owner | bntpool.com owner | Boundary |
|---|---|---|
| `/blog/how-to-mine-blocknet/` (install seine, first shares on any OS) | `/blog/mine-blocknet-on-bntpool/` | The seineminer.com guide owns installing the miner; the pool post owns joining and using the pool and links to the guide for installation. |
| `/blog/blocknet-mining-hardware-hashrate/` (per-machine results, hardware choice) | `/blog/what-is-hashrate/` | The seineminer.com post owns hardware results and buying advice; the pool post shows the best-per-generation table only to illustrate Blocknet's H/s scale and links there. |
| `/blog/how-crypto-mining-works/` (draft: how mining works overall) | `/blog/what-is-a-mining-pool/` | The seineminer.com post owns mining in general and keeps a short pool section; the pool post owns pools in depth (shares, payout schemes, choosing a pool). |

Expanded 2026-10-25: `track-your-miner-on-bntpool` also owns zero pool hashrate,
address/pool mismatches, and connection/reconnect error checks (`#zero-hashrate`).

Expanded 2026-10-25: `estimate-blocknet-mining-rewards` now contains the live
20-language calculator at `#mining-calculator`. It remains the owner of the mining
calculator intent, linked from the pool dashboard and Seine hardware guide. No
competing calculator landing page is created.

Added 2026-10-26: the luck guide owns the detailed probability and effort explanation. PPLNS remains the accounting/payment owner; My Stats remains the lookup/troubleshooting owner; the calculator owns reward estimation. `/luck` remains the live history. Existing pages keep short context and link to the new owner; no redirects.

## Share outcomes (October 26, 2026)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| accepted rejected stale mining shares | `/blog/accepted-rejected-stale-shares/` | Interpret submission outcomes, vardiff and rejection reasons, then diagnose repeated errors on one rig | duplicate share, low difficulty share, nonce range, rejection rate |

The tracking guide owns navigating My Stats and zero-hashrate/connection diagnosis. The PPLNS guide owns reward eligibility and payment timing. This article owns submitted-share outcomes and difficulty-weighted interpretation; it links to those owners for the next stage.

## Mining difficulty (October 26, 2026)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| mining difficulty explained; network difficulty vs share difficulty | `/blog/mining-difficulty-explained/` | Explain target thresholds, Blocknet's per-block LWMA adjustment and difficulty-to-time arithmetic | difficulty adjustment, vardiff meaning, 挖矿难度 |

Hashrate owns speed and units, luck owns round variance, share outcomes owns rejection diagnosis, and the calculator owns reward estimates. This guide owns difficulty as a threshold and adjustment mechanism, linking to those existing owners for their tasks.

## Tuning and Stratum (October 26, 2026)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| what is Stratum; mining pool protocol | `/blog/stratum-mining-protocol/` | Follow bntpool login, job assignment, share submission and reply stages | Blocknet mining, seine, bntpool |

Pool overview owns pooling and payout models; setup owns connection instructions; difficulty owns target arithmetic; share outcomes owns rejection diagnosis. This article owns the wire conversation. No URL is retargeted.

## Benchmarks and confirmations (October 26, 2026)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| block confirmations; immature mining rewards | `/blog/block-confirmations-mining-rewards/` | Follow chain depth and block identity from pending reward through maturity to a separate payout | Blocknet mining, seine, bntpool |

PPLNS owns allocation, balance threshold and sending rules. This article owns depth counting, maturity timing and block identity, with a worked height timeline. Tracking owns the full account interface. No URL is retargeted.

## CPU usage and mining fees (October 26, 2026)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| mining pool fees vs miner developer fees | `/blog/mining-pool-fees-vs-miner-fees/` | Identify the two fee stages, select gross versus already-adjusted inputs and avoid double subtraction | Blocknet mining, seine, bntpool |

The calculator owns numerical estimates and PPLNS owns allocation and payout timing. This guide owns fee-layer interpretation and input provenance. The user explicitly approved this fee topic. No URL is retargeted.

## Windows startup and multiple miners (October 26, 2026)

| Primary keyword cluster | URL | Search job |
|---|---|---|
| mine multiple computers one wallet; Blocknet multiple rigs | `/blog/mine-multiple-computers-one-wallet/` | Add another computer with a distinct rig name and verify work under a shared receiving address |

Pool setup owns first connection; stats owns dashboard fields; PPLNS owns reward allocation. No existing URL is retargeted.
