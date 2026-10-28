---
title: Blocknet block reward schedule
description: Blocknet's block reward by month, from 723.25 BNT at launch to a permanent 2.0 BNT tail at month 48, with 288 blocks a day and 98.6M BNT before the tail.
date: 2026-10-25
updated: 2026-10-28
tags: [Blocknet, Block reward, Emission, Mining]
faq:
  - q: What is the current Blocknet block reward?
    a: It depends on the month the chain is in, because the reward steps down every 8,640 blocks. On October 25, 2026, the chain was in month 7, paying 467.67 BNT per block, and month 8 starts at height 69,120 with 439.46 BNT. Find the current block height on the bntpool.com dashboard and look it up in the table.
  - q: How many Blocknet blocks are mined per day?
    a: Blocknet targets one block every five minutes, which is 288 blocks a day and 8,640 blocks in each 30-day month. The difficulty adjusts every block, using a weighted average of the last 60 block times, so the average stays close to five minutes as miners join or leave the network.
  - q: What is the total supply of Blocknet?
    a: About 98.56 million BNT is mined before the tail begins at month 48. The code and the official site round this to about 100 million. After that, every block pays 2.0 BNT forever, which adds 576 BNT a day, or about 210,240 BNT a year, so the supply keeps growing slowly.
  - q: When does Blocknet's tail emission start?
    a: The tail starts at month 48, at block height 414,720. That is 48 months of 30 days, or 1,440 days of five-minute blocks after launch, which is roughly four years. The exact calendar date depends on real block times. From that height, every block pays 2.0 BNT.
  - q: How long before mined Blocknet coins can be spent?
    a: A block reward becomes spendable after 60 blocks, which is about five hours at Blocknet's five-minute block time. This is called coinbase maturity. The bntpool.com pool also waits 60 blocks before it pays out a block's reward, so it only pays out rewards from blocks that have matured.
related: [blocknet-fair-launch, how-mining-secures-blocknet-payments, how-pplns-payouts-work]
---

Follow newly issued BNT from a block reward to spendable coins and later payments. This page explains that currency flow, then lists every month of the reward schedule and shows how to read the figures as a miner or coin holder.

## What is the Blocknet block reward?

<Answer>
The Blocknet block reward started at 723.25 BNT per block and steps down once every 30-day month of 8,640 blocks, following a smooth decay curve. It stays the same for every block within a month. From month 48, at height 414,720, every block pays a permanent tail of 2.0 BNT.
</Answer>

Blocknet targets one block every five minutes (300 seconds). That gives 288 blocks a day and 8,640 blocks in each 30-day month. The difficulty adjusts after every block, based on a weighted average of the last 60 block times, so the pace stays close to five minutes as the network's hashrate changes.

The reward for any block depends only on which month its height falls in. The month is the block height divided by 8,640, rounded down, so heights 0 to 8,639 are month 0, heights 8,640 to 17,279 are month 1, and so on.

## How does a block reward become usable BNT?

A mined block creates its scheduled reward in a coinbase output. That output belongs to the destination chosen for the block and first passes through coinbase maturity. Once spendable, the owner can use it in ordinary transactions. If a pool mined the block, its accounting divides eligible rewards among participants and its later payout transactions send existing BNT to their wallets.

A payout therefore does not create a second reward. Nor does receiving a transfer require you to mine new coins. Issuance describes the origin of BNT; transfers describe how ownership changes afterward. The [confirmation guide](/blog/block-confirmations-mining-rewards/) follows the pool stages, and the [Blocknet supply guide](https://seineminer.com/blog/blocknet-supply/) explains the broader monetary schedule.

## How is the reward calculated?

The daemon computes each month's reward with one formula:

```
reward(month) = (723.25093035 - 2.0) × e^(-0.75 × month / 12) + 2.0
```

The first term is the part of the reward above the 2.0 BNT tail. It shrinks by a factor of e^-0.75 each year, so after twelve months it is about 47% of its starting size. Spread over the months, the above-tail part of each month's reward is about 93.9% of the month before, so the reward moves down in small, regular steps.

The result is rounded down to whole atomic units (1 BNT is 100,000,000 atomic units). The table below shows each reward rounded to two decimals.

## Blocknet reward table by month

| Month | First block | Reward per block (BNT) | Total mined at month end (million BNT) |
|---|---|---|---|
| 0 | 0 | 723.25 | 6.25 |
| 1 | 8,640 | 679.55 | 12.12 |
| 2 | 17,280 | 638.50 | 17.64 |
| 3 | 25,920 | 599.94 | 22.82 |
| 4 | 34,560 | 563.71 | 27.69 |
| 5 | 43,200 | 529.68 | 32.27 |
| 6 | 51,840 | 497.71 | 36.57 |
| 7 | 60,480 | 467.67 | 40.61 |
| 8 | 69,120 | 439.46 | 44.40 |
| 9 | 77,760 | 412.96 | 47.97 |
| 10 | 86,400 | 388.06 | 51.33 |
| 11 | 95,040 | 364.67 | 54.48 |
| 12 | 103,680 | 342.69 | 57.44 |
| 13 | 112,320 | 322.05 | 60.22 |
| 14 | 120,960 | 302.66 | 62.83 |
| 15 | 129,600 | 284.45 | 65.29 |
| 16 | 138,240 | 267.33 | 67.60 |
| 17 | 146,880 | 251.26 | 69.77 |
| 18 | 155,520 | 236.16 | 71.81 |
| 19 | 164,160 | 221.97 | 73.73 |
| 20 | 172,800 | 208.64 | 75.53 |
| 21 | 181,440 | 196.12 | 77.23 |
| 22 | 190,080 | 184.36 | 78.82 |
| 23 | 198,720 | 173.31 | 80.32 |
| 24 | 207,360 | 162.93 | 81.73 |
| 25 | 216,000 | 153.18 | 83.05 |
| 26 | 224,640 | 144.02 | 84.29 |
| 27 | 233,280 | 135.42 | 85.46 |
| 28 | 241,920 | 127.33 | 86.56 |
| 29 | 250,560 | 119.74 | 87.60 |
| 30 | 259,200 | 112.61 | 88.57 |
| 31 | 267,840 | 105.91 | 89.49 |
| 32 | 276,480 | 99.61 | 90.35 |
| 33 | 285,120 | 93.70 | 91.16 |
| 34 | 293,760 | 88.14 | 91.92 |
| 35 | 302,400 | 82.92 | 92.63 |
| 36 | 311,040 | 78.02 | 93.31 |
| 37 | 319,680 | 73.41 | 93.94 |
| 38 | 328,320 | 69.09 | 94.54 |
| 39 | 336,960 | 65.02 | 95.10 |
| 40 | 345,600 | 61.20 | 95.63 |
| 41 | 354,240 | 57.62 | 96.13 |
| 42 | 362,880 | 54.25 | 96.60 |
| 43 | 371,520 | 51.08 | 97.04 |
| 44 | 380,160 | 48.11 | 97.45 |
| 45 | 388,800 | 45.31 | 97.85 |
| 46 | 397,440 | 42.69 | 98.21 |
| 47 | 406,080 | 40.22 | 98.56 |
| 48 onward | 414,720 | 2.00 | +0.017 per month |

The genesis block at height 0 carries no reward, so month 0 has 8,639 paid blocks. Every BNT comes from these block rewards, with no premine, as the [Blocknet fair launch](/blog/blocknet-fair-launch/) page explains. On October 25, 2026, the pool was working on block 69,016, near the end of month 7. The [bntpool.com dashboard](/) shows the current block height, so you can find its row above, and the [blocks page](/blocks) lists the blocks the pool has found.

## What happens at month 48?

<Answer>
Month 48 starts at block height 414,720, and from there every Blocknet block pays a fixed 2.0 BNT forever. This is the tail emission. It keeps a reward on every block for as long as the chain runs, so miners are always paid for securing the network.
</Answer>

The decay formula applies to months 0 through 47, ending at 40.22 BNT per block in month 47. From month 48 the daemon uses the tail value directly, so the reward becomes 2.0 BNT and stays there. At 288 blocks a day, the tail adds 576 BNT a day, or about 210,240 BNT a year.

Month 48 begins 1,440 days of five-minute blocks after launch, which is roughly four years. The calendar date depends on real block times, so the block height is the reliable marker.

## How much BNT is mined before the tail?

Adding up every block from height 1 to 414,719 gives 98,561,905.08 BNT, about 98.6 million. The code and the official site describe the schedule as about 100 million coins over four years plus the tail, and the exact sum lands just under that round figure.

The cumulative totals below measure scheduled issuance from accepted blocks. They are not a count of currently spendable wallet balances or a measurement of trading activity. Moving existing BNT between holders does not add to these issuance totals.

Most of it is mined early. Grouped by year of the schedule:

| Months | BNT mined | Share of the pre-tail total |
|---|---|---|
| 0-11 (year 1) | 54.48 million | 55.3% |
| 12-23 (year 2) | 25.84 million | 26.2% |
| 24-35 (year 3) | 12.32 million | 12.5% |
| 36-47 (year 4) | 5.93 million | 6.0% |

## When can you spend mined BNT?

A block's reward can be spent once 60 more blocks have been added on top of it, about five hours later. This rule is called coinbase maturity. The bntpool.com pool waits the same 60 blocks before paying out a block's reward, and the [payouts page](/payouts) lists what it has sent.

## What does the schedule mean for miners?

Rewards are larger in earlier months. A block found in month 12 pays 342.69 BNT, about 47% of the 723.25 BNT paid in month 0, and a block in month 24 pays 162.93 BNT. Each new month pays less per block than the one before until the tail.

Your share of those rewards follows your share of the network's hashrate. For solo mining, the expected number of blocks you find per day is 288 × (your hashrate ÷ network hashrate), and each one pays the reward for its month. On a pool, the same arithmetic applies to the pool's hashrate, and each block's reward is shared among the miners who contributed work, so you earn from every block the pool finds instead of waiting to find a whole block yourself. The [bntpool.com dashboard](/) shows the current network and pool hashrate, and the [stats page](/stats) shows your own miner's hashrate once you are mining.

To start mining now:

1. Download seine from [seineminer.com](https://seineminer.com/#download). It runs on Windows, macOS and Linux and tunes itself to your hardware.
2. Run it and paste your Blocknet address or `@name`.
3. Accept the default pool, `stratum+tcp://bntpool.com:3333`.

The [guide to mining on bntpool.com](/blog/mine-blocknet-on-bntpool/) covers each step on the pool side, the [Get Started page](/start) lists the pool settings, and the [seine mining guide](https://seineminer.com/blog/how-to-mine-blocknet/) walks through the setup step by step. To see what different processors achieve, read [Blocknet mining hardware and hashrate](https://seineminer.com/blog/blocknet-mining-hardware-hashrate/).

<Note>
The figures on this page come from the reward code in the Blocknet daemon, checked on October 25, 2026. Blocknet plans a network upgrade called Faraday around 2027-02-16, according to [blocknetcrypto.com](https://blocknetcrypto.com/#faraday). Keep seine up to date: it checks for new releases when it starts and every 12 hours.
</Note>

<CTA target="seine" title="Mine Blocknet with seine" text="seine is free for Windows, macOS and Linux and mines on bntpool.com by default." />
