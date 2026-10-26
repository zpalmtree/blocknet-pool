---
title: 'Mining difficulty explained: network and shares'
description: 'Learn how mining difficulty sets the block target, how Blocknet adjusts it, and why network difficulty and pool share difficulty affect different mining stats.'
date: 2026-10-26
tags: ['Mining basics', 'Difficulty', 'Blocknet']
faq:
  - q: 'Does higher mining difficulty reduce my hashrate?'
    a: 'Difficulty changes the target a result must satisfy. It does not directly change how many hashes your hardware computes each second. With the same hashrate, a higher network difficulty means a longer expected interval between block discoveries. Check miner performance separately from changes in block or share frequency.'
  - q: 'Does a higher share difficulty pay more?'
    a: 'A higher-difficulty share represents more expected hashing work, but the miner submits such shares less often at the same hashrate. Compare difficulty-weighted work over the same period. On bntpool.com, actual rewards also depend on accepted blocks and the pool''s PPLNS accounting, rather than the raw share count.'
  - q: 'How often does Blocknet adjust difficulty?'
    a: 'Blocknet derives the next block''s difficulty from recent chain history after each block. Its current rule uses a linear weighted moving average over 60 solve intervals and targets 300 seconds per block. Recent intervals carry more weight. Individual blocks can still arrive sooner or later than that target.'
related: [what-is-hashrate, mining-pool-luck-explained, accepted-rejected-stale-shares, estimate-blocknet-mining-rewards]
---

Mining difficulty connects the work your hardware performs with the blocks the network accepts. This guide explains the target, Blocknet's adjustment rule and the separate difficulty assigned to pool shares. Use it to understand a changing difficulty number before comparing hashrate, expected block time or the [mining rewards calculator](/blog/estimate-blocknet-mining-rewards/#mining-calculator).

## What is mining difficulty?

<Answer>
Mining difficulty describes how much hashing work is expected to find a result that meets a target. Higher difficulty makes qualifying results less frequent at the same hashrate. For Blocknet, network difficulty is approximately the expected number of hashes per block; pool share difficulty measures a separate submission target.
</Answer>

Each hash produces a number. A result qualifies when that number meets the target threshold. A smaller target accepts a smaller fraction of possible results, so its difficulty is higher. Blocknet converts difficulty into a 256-bit target by dividing the largest 256-bit value by difficulty. This relationship explains why doubling difficulty approximately doubles expected work while leaving the hashing operation itself unchanged.

## How does Blocknet adjust network difficulty?

Blocknet derives the next difficulty after each block. The current mainnet rule uses a linear weighted moving average, or LWMA, over 60 recent solve intervals. Newer intervals receive more weight. Its target is 300 seconds per block. The calculation combines past difficulty with weighted timing, so sustained faster arrivals tend to raise difficulty and sustained slower arrivals tend to lower it.

The implementation bounds the solve intervals used in the calculation and applies a minimum difficulty. The 60-interval window needs 61 block timestamps; it does not mean one adjustment every 60 blocks. These rules aim to keep the chain near its target pace as hashing power changes. A five-minute target describes an average, and an individual block can arrive much sooner or later.

## Network difficulty versus share difficulty

A pool gives miners a share target so it can observe their contributed work between block discoveries. That target is generally easier to satisfy than the network block target. Both numbers describe expected work, but they answer different questions and are set by different parts of the system:

| Difficulty | What it controls |
|---|---|
| Network | The work threshold for a candidate block, derived from the chain's consensus rules. |
| Share | The submission threshold assigned by the pool to a mining connection or job. |

bntpool.com uses variable share difficulty, often called vardiff, to adjust submission frequency. At the same hashrate, a higher share difficulty generally produces fewer shares, each representing more expected work. It does not change network difficulty. A submitted result that also satisfies the network target can become a block candidate; the daemon still validates the block. See the [share outcomes guide](/blog/accepted-rejected-stale-shares/) for acceptance and rejection messages.

## How does difficulty affect expected block time?

For Blocknet, divide network difficulty by the hashrate doing the work to estimate the mean discovery interval in seconds. Use H/s for hashrate and compare values from the same period. The following example holds difficulty and hashrate constant solely to show the arithmetic. Its figures are illustrative, rather than current network measurements or a forecast for a particular miner.

**Expected seconds per block ≈ network difficulty ÷ hashrate in H/s**

| Network difficulty | Hashrate | Expected interval |
|---|---|---|
| 30,000 | 100 H/s | 300 s |
| 60,000 | 100 H/s | 600 s |
| 60,000 | 200 H/s | 300 s |

At difficulty 30,000 and 100 H/s, the mean interval is 300 seconds. Doubling difficulty to 60,000 with the same hashrate makes it 600 seconds. If hashrate also doubles to 200 H/s, the mean returns to 300 seconds. These are averages across repeated trials. The [pool luck guide](/blog/mining-pool-luck-explained/) explains why an actual round can run above or below its expected duration.

## Why can rewards change when my hashrate is steady?

With your hashrate fixed, higher network difficulty reduces your expected block discoveries per unit of time while that difficulty holds. If total network hashrate grows and difficulty follows, your share of the network's work becomes smaller. The reward per block is another input: Blocknet's [reward schedule](/blog/blocknet-block-reward-schedule/) changes that amount independently. Difficulty does not set the number of BNT in the scheduled block reward.

Use the rewards calculator with fresh network inputs to explore expected BNT amounts. Check whether the entered hashrate comes from the miner or the pool so the miner fee is handled correctly, and use the fee for your release. Estimates describe an average under their assumptions. Your actual pool payment also depends on found blocks, confirmations and [PPLNS accounting](/blog/how-pplns-payouts-work/); a difficulty reading alone cannot tell you when a payout will arrive.

## Which difficulty number should you check?

Start with the question you are trying to answer. For block discovery, use network difficulty and the hashrate of the miner or pool being considered. For the frequency of submitted shares, use the difficulty assigned to those jobs. For a balance or payment, open [My Stats](/stats) and [payout history](/payouts). Keep the timestamp, scope and units alongside each value when comparing two readings.

The [pool dashboard](/) and [block history](/blocks) supply current context. Network hashrate there is an estimate from recent work and elapsed block time, so it need not equal next difficulty divided by the 300-second target. The [hashrate guide](/blog/what-is-hashrate/) explains that distinction. Keep difficulty, hashrate and luck separate when diagnosing a change: each describes a different part of mining.

<CTA target="pool" title="Connect your miner to bntpool.com" text="Use the setup page, then follow your shares and mining activity on My Stats." />
