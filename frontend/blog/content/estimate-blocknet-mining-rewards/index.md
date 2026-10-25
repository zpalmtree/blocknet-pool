---
title: How to estimate your Blocknet mining rewards
description: Estimate how much BNT your hardware mines per day with one formula, where to read each input on the bntpool.com dashboard, and a dated worked example.
date: 2026-10-25
tags: [Mining rewards, Mining guide, Hashrate, bntpool]
faq:
  - q: How do you calculate Blocknet mining rewards?
    a: Divide your hashrate by the network hashrate, multiply by 288, the number of blocks Blocknet produces a day, and multiply by the current block reward. The result is your expected gross BNT per day. Read the network hashrate on the bntpool.com dashboard and your own hashrate in seine or on My Stats.
  - q: Where can I see the Blocknet network hashrate?
    a: The bntpool.com dashboard shows it in the Network Hashrate card, usually in KH/s, so multiply by 1,000 to get H/s. The same value is available in H/s from the network_hashrate field of the pool API at bntpool.com/api/stats. It is measured over the most recent ten blocks and updates with every block.
  - q: Does the Blocknet block reward change over time?
    a: Yes. The reward is fixed within each 30-day month of 8,640 blocks and steps down at the start of the next month, until it reaches a permanent 2.0 BNT per block at month 48. To find the current reward, divide the block height by 8,640, round down, and look up that month in the reward schedule.
  - q: Why do my daily rewards on the pool vary from the estimate?
    a: The estimate is an average. On any one day the pool may find more or fewer blocks than expected, and the network hashrate moves as miners join. Over a week or more of steady mining, the day-to-day swings average out, and you can follow your credited rewards on the My Stats page.
  - q: Does mining on a pool change how much BNT I can expect?
    a: Your expected share of the network's rewards stays the same, because it depends only on your share of the network hashrate. What a pool changes is the timing. Instead of waiting days for a whole block, you receive a portion of every block bntpool.com finds, so rewards arrive in small, regular amounts.
related: [blocknet-block-reward-schedule, how-pplns-payouts-work, pool-vs-solo-mining-blocknet]
---

This guide shows you how to estimate how much BNT your hardware mines per day. You'll learn the formula, where to read each input on the [bntpool.com dashboard](/), and how to run the numbers with a dated example built from the pool's live data and seine's best measured hashrates.

## How much BNT will your hardware mine per day?

<Answer>
Expected BNT per day equals your hashrate divided by the network hashrate, times 288 blocks a day, times the current block reward. Your hashrate comes from seine or My Stats, the network hashrate from the bntpool.com dashboard, and the reward from the current block height and Blocknet's reward schedule.
</Answer>

Written as a formula:

```
BNT per day = (your H/s ÷ network H/s) × 288 × block reward
```

Each part has a simple reason behind it:

- **288 blocks a day.** Blocknet targets one block every five minutes, and 86,400 seconds in a day divided by 300 seconds per block is 288. The difficulty adjusts after every block, so the chain stays close to that pace as miners join.
- **Your share of the hashrate is your share of the blocks.** Blocknet's difficulty is about the expected number of hashes needed to find a block. Every hash has the same chance of solving it, so a machine doing 1% of the network's hashes can expect 1% of the blocks over time.
- **The block reward is set by height.** Every block in the same 30-day month of 8,640 blocks pays the same reward, so for any day inside a month the reward is a known constant.

The result is your expected gross reward, averaged over time.

## Where do you find each input?

### Your hashrate

Seine shows your hashrate on its terminal dashboard as soon as it starts. Give it a few minutes: seine tunes thread count and CPU kernel on real mining work, and the figure settles once it has found the best settings for your machine.

Once you are mining on the pool, the **Hashrate** card on [My Stats](/stats) shows your hashrate as the pool sees it, averaged over the last hour. For an estimate, use seine's steady figure or the pool's 24-hour chart.

### Network hashrate

The dashboard's **Network Hashrate** card shows the whole network's hashrate. It is displayed in KH/s, so a reading of 62.33 KH/s means 62,330 H/s. Convert it to H/s before you divide, or your result will be off by a factor of a thousand.

The pool measures this figure from the work and timestamps of the most recent ten blocks, so it moves from block to block; [what is hashrate?](/blog/what-is-hashrate/) explains this calculation and why the figure moves. For a steadier input, check it a few times over a day and use the average. You can also read it in H/s from the API:

```sh
curl -s https://bntpool.com/api/stats
```

The `chain` section of the response has `network_hashrate` in H/s and `current_job_height`, the block the pool is working on.

### Block reward

The dashboard's **Current Block** card shows the height the network is working on. Divide it by 8,640 and round down to get the month, then find that month in the [Blocknet block reward schedule](/blog/blocknet-block-reward-schedule/). The [blocks page](/blocks) is a quick cross-check: its Reward column shows the full reward of each block the pool has found.

## A worked example from October 25, 2026

<Note>
This example is a snapshot of live values from https://bntpool.com/api/stats at 02:03 EDT on October 25, 2026. Network hashrate and the block reward change, so plug in today's values from the dashboard before you rely on the result.
</Note>

| Input | Value at the snapshot |
|---|---|
| Current block | 69,020 (month 7, because 69,020 ÷ 8,640 = 7.99) |
| Block reward | 467.67 BNT |
| Network hashrate | 62,329 H/s (shown as 62.33 KH/s) |
| bntpool.com hashrate | 5,240 H/s |

Take the AMD EPYC Zen 5 server from seine's benchmarks, at 49.4 H/s:

1. Share of the network: 49.4 ÷ 62,329 = 0.0793%.
2. Expected blocks per day: 0.000793 × 288 = 0.2283.
3. Expected BNT per day: 0.2283 × 467.67 = about 106.8 BNT.

Month 8 starts at height 69,120, about 100 blocks after the snapshot, and pays 439.46 BNT per block. At the same network hashrate, the same server's estimate becomes 0.2283 × 439.46 = about 100.3 BNT per day. This is why the reward is worth checking at the start of each month.

Here is the same example calculation, at the snapshot's network hashrate, for the best result seine has measured on each hardware generation:

| Hardware | Threads | Hashrate | Share of network | BNT/day at 467.67 | BNT/day at 439.46 |
|---|---|---|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49.4 H/s | 0.0793% | 106.8 | 100.3 |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35.7 H/s | 0.0573% | 77.1 | 72.5 |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30.9 H/s | 0.0496% | 66.8 | 62.7 |
| Arm server (AWS Graviton4) | 16 | 21.5 H/s | 0.0345% | 46.5 | 43.7 |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13.1 H/s | 0.0210% | 28.3 | 26.6 |

Your own machine's number is the one that counts. Run seine for a while, take its steady hashrate and use it in place of the table. The guide to [Blocknet mining hardware and hashrate](https://seineminer.com/blog/blocknet-mining-hardware-hashrate/) explains why memory bandwidth and RAM set these figures, and the full results are on seine's [performance page](https://github.com/zpalmtree/seine/blob/master/docs/PERFORMANCE.md).

## How does mining on bntpool.com change the estimate?

Your expected share stays the same on the pool. What changes is how the reward reaches you. bntpool.com pays with PPLNS: each block's reward is shared among the shares miners submitted in the six hours before it, in proportion to their difficulty. [How PPLNS payouts work](/blog/how-pplns-payouts-work/) walks through the split.

You can run the same estimate from the pool's side, and it gives the same answer. At the snapshot, the pool's 5,240 H/s was 8.4% of the network, so it could expect 288 × 5,240 ÷ 62,329 = about 24.2 blocks a day, one roughly every hour. The dashboard shows this as **Expected Block Time** in the Round Progress panel. A 49.4 H/s server is 0.94% of the pool's hashrate, and 0.94% of 24.2 blocks at 467.67 BNT is again about 106.8 BNT a day. The pool's hashrate cancels out.

The difference is timing. Mining alone, that Zen 5 server would expect one whole block about every 4.4 days; [pool vs solo mining for Blocknet](/blog/pool-vs-solo-mining-blocknet/) compares the two in more detail. On bntpool.com it receives a portion of each of the roughly 24 blocks the pool finds a day, so its balance grows in small steps around the clock. The pool fee and seine's dev fee come out of the gross figure the formula gives you.

Two pool details shape your first day:

- **The window fills over six hours.** A new miner's portion of each block grows as their shares fill the six-hour PPLNS window, then holds at the full rate for as long as they keep mining.
- **Rewards mature before payout.** Each block's reward moves to Confirmed Rewards after 60 confirmations, about five hours, and the pool sends your balance automatically once it reaches 0.1 BNT.

## How do you check the estimate against real results?

The estimate is an average, and individual days move around it as the pool's luck varies from round to round. Over many rounds, effort averages out around 100%, so a week of steady mining is a good period to compare. Note the total of your Confirmed Rewards and Paid Balance on [My Stats](/stats) at the start of a week and again at the end. The increase is what the pool credited you from blocks that matured that week; set it beside your estimate for the same days, using the reward and network hashrate from those days.

Re-run the formula whenever one of its inputs changes:

- **At each new month**, when the reward steps down. Month boundaries fall every 8,640 blocks, about every 30 days.
- **When the network hashrate moves**, as miners join the network.
- **When your hashrate changes**, after adding a machine, reserving huge pages on Linux or adding an NVIDIA GPU, which seine mines on alongside the CPU.

<Note>
The hashrates in this guide describe Blocknet's current Argon2id proof of work. Blocknet plans a network upgrade called Faraday around 2027-02-16, according to [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), and it is planned to change the proof of work. Keep seine up to date: it checks for new releases when it starts and every 12 hours.
</Note>

<CTA target="seine" title="Measure your own hashrate" text="Download seine, let it tune itself for a few minutes, and use its hashrate in the formula with today's dashboard values." />
