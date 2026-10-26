---
title: How PPLNS payouts work on bntpool.com
description: bntpool.com pays Blocknet miners with PPLNS over a six-hour share window. See how rewards are split, why blocks need 60 confirmations, and the 0.1 BNT minimum.
date: 2026-10-25
tags: [Payouts, PPLNS, bntpool, Mining guide]
faq:
  - q: How long is the PPLNS window on bntpool.com?
    a: The window is six hours. When the pool finds a block, it looks back over the six hours before that block and shares the reward among everyone who submitted shares in that time, in proportion to the difficulty of their shares. The current value is listed on the pool's Get Started page.
  - q: What is the minimum payout on bntpool.com?
    a: The minimum payout is 0.1 BNT. Once your confirmed balance reaches that amount, the pool sends it to your Blocknet address automatically, with nothing to request or claim. A smaller balance stays on your account and grows with each confirmed block you have shares in until it crosses the threshold.
  - q: Why are my Estimated Rewards different from my Confirmed Rewards?
    a: Estimated Rewards are your share of blocks the pool has found that are still collecting their 60 confirmations, so the figure can still move. Confirmed Rewards come from blocks that are 60 blocks deep and are waiting to be paid. As each block matures, its amount moves from the first card to the second.
  - q: Do my rewards stop as soon as I stop mining?
    a: No. Each share stays in the PPLNS window for six hours after you submit it, so blocks the pool finds during that time still credit you. Rewards that are already confirmed stay in your balance, and the pool pays them out once the balance reaches the 0.1 BNT minimum.
  - q: What does an effort above 100% mean on the luck page?
    a: Effort compares the work the pool did during a round with the block's difficulty, which is the expected amount of work. An effort above 100% means that round took longer than average, and below 100% means it was quicker. Both are normal, and over many rounds effort averages out around 100%.
related: [track-your-miner-on-bntpool, mining-pool-luck-explained, blocknet-block-reward-schedule, mining-pool-fees-vs-miner-fees]
---

This guide explains how bntpool.com turns the shares your miner submits into Blocknet payouts. By the end you'll know how a block's reward is split, why it waits about five hours before it can be paid, when your balance is sent, and how to read the balance cards on the [stats page](/stats) and the [luck page](/luck).

If you're new to pool mining, [what is a mining pool?](/blog/what-is-a-mining-pool/) covers the basics. If you haven't started mining yet, the [seine mining guide](https://seineminer.com/blog/how-to-mine-blocknet/) gets you from download to first shares in a few minutes.

## What is PPLNS?

<Answer>
PPLNS means "pay per last N shares". When bntpool.com finds a block, it shares the reward among the shares miners submitted in the six hours before that block, in proportion to each share's difficulty. The more steady work you contribute inside that window, the larger your portion of each block.
</Answer>

On bntpool.com, the "last N" is a length of time: six hours. The pool's [Get Started page](/start) shows the payout scheme, the window, the minimum payout and the number of confirmations, and the same values are published at `https://bntpool.com/api/info`.

| Setting | Value on bntpool.com |
|---|---|
| Payout scheme | PPLNS |
| Share window | 6 hours |
| Confirmations before payout | 60 blocks |
| Minimum payout | 0.1 BNT |

Shares are counted by difficulty. The pool gives each miner connection its own share difficulty and adjusts it so every miner sends a steady stream of shares. A faster machine gets harder shares, so each of its shares carries more weight. What decides your portion is the total work you did, which is a fair measure for a laptop and a server alike.

## How does bntpool.com split a block reward?

When the pool finds a block, the reward goes through the same steps every time:

1. **The block is recorded.** It appears on the [blocks page](/blocks) with a pending status.
2. **The window is measured.** The pool takes the six hours that end at the block and adds up the difficulty of every accepted share from each address in that period.
3. **The reward is divided.** Each address receives the same fraction of the reward as its fraction of the window's total work.
4. **The block matures.** Your portion shows as an estimate until the block is 60 blocks deep, then it moves into your confirmed balance.
5. **The balance is paid.** When your confirmed balance reaches 0.1 BNT, the pool sends it to your address.

## A worked example

<Note>
This example uses made-up round numbers to show the arithmetic. Real totals depend on the pool's hashrate, your hashrate and the [block reward](/blog/blocknet-block-reward-schedule/) at the time.
</Note>

Say that in the six hours before a block, all miners on the pool submitted shares with a combined difficulty of 1,000,000, and your shares add up to 20,000. Your fraction of the window is 20,000 ÷ 1,000,000 = 2%. If the pool shares out 400 BNT for that block, your credit is 400 × 0.02 = 8 BNT.

Now picture a second miner with exactly your hashrate who connected 30 minutes before the block. Thirty minutes is one-twelfth of six hours, so they have about 1,667 difficulty in the window and receive about 0.67 BNT from this block. Their work keeps counting, though: each of those shares stays in the window for the next six hours and earns a portion of every block the pool finds in that time.

| Miner (example) | Work in window | Fraction | Credit from a 400 BNT payout |
|---|---|---|---|
| You, mining all six hours | 20,000 | 2% | 8 BNT |
| Same hashrate, joined 30 minutes before the block | 1,667 | about 0.17% | about 0.67 BNT |
| Small miner, mining all six hours | 100 | 0.01% | 0.04 BNT |

The small miner in the last row earns 0.04 BNT per block, so their confirmed balance crosses the 0.1 BNT minimum after three such blocks and is then paid out.

## Why does PPLNS reward miners who mine steadily?

Every share you submit counts toward every block the pool finds in the following six hours. A miner who runs around the clock always has a full window of work behind them, so each new block credits them at their full rate. Their income tracks their hashrate closely from block to block.

This also makes PPLNS forgiving. A reboot, a driver update or a short network drop doesn't reset your position, because the shares you already sent stay in the window for six hours. Work you add after a block is found goes toward the blocks that follow, so extra hashrate is always put to use.

For the steadiest results, leave your miner running and let it tune itself. Seine keeps adjusting thread count and CPU kernel while it mines without reconnecting to the pool, so your share stream stays continuous. You can [download seine](https://seineminer.com/#download) for Windows, macOS or Linux.

## Why does a block's reward wait for 60 confirmations?

Blocknet makes newly mined coins wait 60 blocks before they can be spent. The daemon's rules call this coinbase maturity, about five hours at Blocknet's five-minute block time (60 × 5 minutes = 300 minutes). bntpool.com uses the same 60 blocks before it counts a reward as confirmed, so every payout comes from coins the pool can actually send.

While a block matures, the pool checks that it is still part of the main chain. Once it is 60 blocks deep, the block is marked confirmed and its reward is added to each miner's confirmed balance. In the rare case that the network settles on a different block at that height, the block is marked orphaned and its estimated rewards are removed.

## When do bntpool.com payouts arrive?

<Answer>
After a block has 60 confirmations, about five hours at Blocknet's five-minute block time, your portion moves into Confirmed Rewards on the stats page. When your confirmed balance reaches 0.1 BNT, bntpool.com sends it to your Blocknet address automatically. There is nothing to request or claim.
</Answer>

For a new miner, the first estimate appears once the pool finds a block while your shares are in the window. The first payment follows after that block matures and your balance reaches the minimum. Sent payments are listed on the [payouts page](/payouts) and in your own history on the stats page.

## How are tentative rewards shown?

Look up your address or `@name` on the [stats page](/stats) and you'll see three balance cards:

- **Estimated Rewards:** your share of blocks that haven't matured yet. These are tentative and can still move until each block confirms or is orphaned.
- **Confirmed Rewards:** your matured balance, waiting to be paid.
- **Paid Balance:** everything already sent to your address.

The pool keeps tentative rewards separate so your confirmed balance only ever holds amounts that are final. Below the cards, the stats page lists each unconfirmed block with your estimated portion, so you can watch it move toward confirmation. The [guide to tracking your miner](/blog/track-your-miner-on-bntpool/) covers the rest of the stats page.

## What do the luck and effort pages tell you?

Effort measures how much work the pool did to find a block. It is the accepted work during a round divided by the block's difficulty, shown as a percentage. Blocknet's difficulty equals the expected number of hashes per block, so 100% effort means the block took exactly the average amount of work. Below 100% means the pool found it faster than average; above 100% means it took longer.

The [luck page](/luck) lists every round with its block height, effort, round time, status and when it was found. The [pool dashboard](/) shows the current round's effort as it builds and the pool's average luck. Individual rounds swing above and below 100%, which is normal for any proof-of-work pool, and over many rounds effort averages out around 100%.

Luck affects how often blocks arrive. Your fraction of each block depends only on your share of the window's work, and steady mining keeps that fraction full whether a round is quick or long.

<CTA target="pool" title="Join the pool" text="Mine steadily on bntpool.com and your share of every block is paid automatically once you reach 0.1 BNT." />
