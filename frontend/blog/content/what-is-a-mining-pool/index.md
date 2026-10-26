---
title: What is a mining pool?
description: A mining pool combines many miners' hashrate and shares each block reward among them. Learn how pools work, PPLNS, PPS and solo, and what to look for.
date: 2026-10-25
tags: [Mining basics, Mining pools, PPLNS, bntpool]
faq:
  - q: Does joining a mining pool increase how much I mine?
    a: A pool leaves your hashrate and your share of the network's work exactly as they are. It changes how rewards reach you. Mining alone, you wait for whole blocks at random intervals. On a pool, you earn a small portion of every block the pool finds, so payments arrive often and in amounts that track your hashrate.
  - q: What is the difference between PPLNS and PPS?
    a: PPLNS pays you a portion of each block the pool actually finds, split by the work miners submitted in a recent window. PPS pays a fixed amount for every accepted share, worked out from the share's expected value, whether or not the pool finds a block. bntpool.com uses PPLNS with a six-hour window.
  - q: Do I need to run my own node to mine on a pool?
    a: No. The pool runs the node, builds the block templates, checks shares and submits the blocks it finds. On bntpool.com you only need the seine miner and a Blocknet address or Blocknet ID name. Run seine, paste your address, accept the default pool, and your portion of each block is paid to that address.
  - q: How do I start mining on bntpool.com?
    a: Download seine for Windows, macOS or Linux from seineminer.com, run it and paste your Blocknet address or @name when it asks. bntpool.com is seine's default pool, so you can accept it and start mining. Look up your address on the pool's stats page to see your hashrate, shares and balance.
related: [how-pplns-payouts-work, pool-vs-solo-mining-blocknet, what-is-hashrate, stratum-mining-protocol]
---

This guide explains what a mining pool is, why most miners join one, and how a pool decides what each miner earns. You'll learn the common payout schemes, PPLNS, PPS and solo mining, and what to check before you point your miner at a pool. bntpool.com, the Blocknet mining pool, serves as the worked example throughout.

## What is a mining pool?

<Answer>
A mining pool is a server that combines the [hashrate](/blog/what-is-hashrate/) of many miners so they find blocks together. The pool hands every miner work on the same block, counts the work each one submits, and shares each block reward among them in proportion to that work. Miners earn small, frequent payments in place of rare whole blocks.
</Answer>

In a proof-of-work coin, miners compete to find the next block by hashing its header with different nonces until one result falls below the network's target. Every hash is an independent try with the same small chance of success, and whoever finds the winning hash collects the block reward.

A pool turns that competition into a team effort. Everyone connected to bntpool.com works on blocks that the pool builds, and when any one of them finds a winning hash, the block's reward is shared among all the miners whose work went into finding it.

## Why do miners join a mining pool?

<Answer>
Miners join a pool to get paid steadily. Alone, a typical miner holds a tiny fraction of the network's hashrate and may wait days between blocks, at random intervals. In a pool, the combined hashrate finds blocks often, and each miner earns a portion of every one, so income arrives regularly and tracks their hashrate.
</Answer>

The arithmetic is easy to check on Blocknet. The network targets one block every five minutes, which is 288 blocks a day, so a miner working alone can expect 288 × (their hashrate ÷ network hashrate) blocks per day.

Take round, hypothetical numbers: a 50 H/s rig on a 50,000 H/s network holds 0.1% of the network and finds 0.288 blocks a day on average, or one block about every three and a half days. That is only an average. Block finding is random, so the actual gap between that rig's blocks can be much shorter or much longer.

Put the same rig on a pool running 5,000 H/s in total and the pool finds about 28.8 blocks a day, one every 50 minutes on average. The rig did 1% of the pool's work, so it earns 1% of each of those blocks. The work it contributes is the same; what changes is that the reward arrives in small portions many times a day. The guide to [pool vs solo mining for Blocknet](/blog/pool-vs-solo-mining-blocknet/) works through this comparison for larger operations.

## How does a mining pool work?

Every pool follows the same basic loop. Here is how it runs on bntpool.com:

1. **The pool builds a block template.** bntpool.com runs its own Blocknet node and fetches a block template from it, so miners never need to run or sync a node themselves.
2. **It sends work to miners.** Miners connect over Stratum, the protocol pools use to send work to miners, at `stratum+tcp://bntpool.com:3333`. The pool sends each connection a job based on the current template.
3. **Miners hash and submit shares.** A share is a hash that meets an easier target set by the pool. Shares prove how much work each miner is doing.
4. **A share that also meets the network target is a block.** The pool checks every share against both targets. When one is good enough for the network, the pool submits the block to its node.
5. **The reward is split and paid.** The pool divides the block reward according to its payout scheme and sends each miner their portion once it is confirmed and above the minimum payout.

## What is a share in a mining pool?

A share is a hash that clears a lower bar than a real block. A block needs a hash below the network's target; a share needs only a hash below the pool's much easier share target. Most shares never become blocks, but they show the pool, at a steady pace, how much hashing each miner does.

Shares are weighted by difficulty. On Blocknet, a difficulty of D means about D hashes are expected before one meets the target, so a share at difficulty 60 stands for about 60 hashes of work. bntpool.com sets a share difficulty for each connection and adjusts it as it goes, aiming for about 10 shares every five minutes and starting new connections at difficulty 60. A fast server gets harder shares and a laptop gets easier ones, and both are credited for the work they actually did. The pool also remembers each rig's difficulty for an hour, so a quick restart picks up where it left off.

## Common mining pool payout schemes

The payout scheme is the rule a pool uses to turn shares into earnings. These are the ones you will see most often.

| Scheme | What you are paid for | How it works |
|---|---|---|
| PPLNS (pay per last N shares) | Blocks the pool finds | Each block's reward is split among the shares submitted in a recent window, by share difficulty |
| PPS (pay per share) | Every accepted share | The pool pays a fixed amount per share, based on the share's expected value, whether or not it finds a block |
| Solo | Blocks you find yourself | You mine against your own node and keep the whole reward of each block you find |

**PPLNS** looks back from each block the pool finds. On bntpool.com, the "last N shares" is a window of six hours: the pool adds up the difficulty of every accepted share in the six hours before the block and gives each miner the same fraction of the reward as their fraction of that work. Each share counts toward every block found while it is in the window, so a miner who runs steadily always has a full window behind them, and a reboot doesn't reset anything. [How PPLNS payouts work on bntpool.com](/blog/how-pplns-payouts-work/) goes through the split step by step.

**PPS** pays for each share as it arrives, using the share's expected value: its difficulty divided by the network difficulty, times the block reward. The pool operator takes on the ups and downs of block finding and pays miners from its own balance in between.

**Solo mining** skips the pool. Your own node builds the templates and each block you find pays its full reward to your wallet. It suits operations large enough to find blocks often on their own. The seine miner supports both: it mines on bntpool.com by default, and `./seine --mode daemon` mines against your own synced Blocknet node.

## What should you look for in a mining pool?

When you compare pools, these are the details that decide how you are paid and how easily you can check it:

- **A clear payout scheme.** The pool should state its scheme and its settings, such as the PPLNS window. bntpool.com lists them on its [Get Started page](/start) and publishes them at `https://bntpool.com/api/info`.
- **Confirmations and minimum payout.** Rewards usually wait until the block is deep enough in the chain, and payments go out once your balance passes a minimum. On bntpool.com, a block's reward is confirmed after 60 blocks, matching Blocknet's coinbase maturity, and balances of 0.1 BNT or more are paid automatically.
- **Public, checkable numbers.** You should be able to see every block the pool found and every payment it sent. bntpool.com shows them on its [blocks](/blocks) and [payouts](/payouts) pages, round-by-round effort on the [luck page](/luck), and service health on the [status page](/status).
- **Your own stats.** Look up your address to see your hashrate, shares and balances. On bntpool.com that's the [stats page](/stats); the guide to [tracking your miner](/blog/track-your-miner-on-bntpool/) explains each card.
- **Easy setup.** The fewer settings you need, the sooner you're mining. bntpool.com is the default pool in seine, which tunes threads, kernels and memory by itself, and you can log in with a [Blocknet ID name](https://seineminer.com/blog/blocknet-id-pay-by-name/) such as `@name` in place of a full address.

## bntpool.com at a glance

| Setting | bntpool.com |
|---|---|
| Payout scheme | PPLNS |
| Share window | 6 hours |
| Confirmations before payout | 60 blocks |
| Minimum payout | 0.1 BNT |
| Stratum address | `stratum+tcp://bntpool.com:3333` |

The [pool dashboard](/) shows the live picture: connected miners, pool hashrate, network hashrate, blocks found and the current round's effort. In a snapshot taken on October 25, 2026, at 2:25 a.m. EDT, the pool ran about 5,200 H/s of a network of about 44,400 H/s, with 246 miners connected. That is about 12% of the network, enough to expect around 34 blocks a day, one roughly every 42 minutes. These figures change constantly, so check the dashboard for current values, and use [the rewards estimate guide](/blog/estimate-blocknet-mining-rewards/) to turn them into BNT for your own hashrate.

To join, [download seine](https://seineminer.com/#download), run it, paste your Blocknet address or `@name`, and accept the default pool. The [seine mining guide](https://seineminer.com/blog/how-to-mine-blocknet/) covers each step, and [mining Blocknet on bntpool.com](/blog/mine-blocknet-on-bntpool/) covers rig names and checking your first shares.

<CTA target="pool" title="Join bntpool.com" text="Point seine at the pool and earn a share of every block it finds, sent to your address automatically once you reach 0.1 BNT." />
