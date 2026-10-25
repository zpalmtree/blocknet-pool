---
title: Pool vs solo mining for Blocknet
description: Choose pool or solo mining for Blocknet. Work out your expected blocks per day, see how bntpool.com turns them into steady payouts, and set up solo mining.
date: 2026-10-25
tags: [Mining guide, Solo mining, bntpool, PPLNS]
faq:
  - q: How often would I find a Blocknet block mining solo?
    a: On average you find 288 × (your hashrate ÷ network hashrate) blocks a day, because Blocknet targets one block every five minutes. With 50 H/s on a network of 50,000 H/s, that is 0.288 blocks a day, or about one block every three and a half days. Read the live network hashrate on the bntpool.com dashboard.
  - q: Do I need to run a Blocknet node to mine on bntpool.com?
    a: No. The pool runs its own Blocknet node, builds the block templates and submits the blocks it finds. You only need seine and a Blocknet address or Blocknet ID name. Run seine, paste your address, accept the default pool, and the pool pays your portion of each block to that address.
  - q: Can seine mine Blocknet solo?
    a: Yes. Start seine with ./seine --mode daemon and it mines directly against your own synced Blocknet node, using the node's API cookie to connect. Each block you find pays its full reward to the node's wallet. The solo mining guide in the seine repository covers custom node layouts and wallet passwords.
  - q: Can I switch between pool and solo mining later?
    a: Yes. Seine uses the same download for both. Run it normally to mine on bntpool.com, or add --mode daemon to mine against your own node, and restart it whenever you want to change. Shares you already sent to the pool stay in its six-hour PPLNS window and keep earning while it lasts.
related: [estimate-blocknet-mining-rewards, how-pplns-payouts-work, mine-blocknet-on-bntpool]
---

This guide helps you decide whether to mine Blocknet on bntpool.com or solo against your own node. You'll work out how many blocks your hashrate finds on its own, see how the pool turns that into a steady stream of payouts, and learn how to set up either one with seine.

## Should you mine Blocknet on a pool or solo?

<Answer>
Both follow the same arithmetic: your share of the network's hashrate sets your share of its blocks. On bntpool.com you earn a portion of every block the pool finds, so payouts arrive steadily as the pool finds blocks. Solo, you keep the whole reward of each block you find, which suits large operations running their own node.
</Answer>

For most miners, bntpool.com is the easy place to start. It is seine's default pool, so there is nothing to set up beyond your address, and you don't need to run or sync a node. Solo mining is fully supported too: seine has a daemon mode that mines straight against your own Blocknet node. The rest of this guide shows how to tell which one fits your hardware.

## How many blocks would you find solo?

<Answer>
Blocknet targets one block every five minutes, which is 288 blocks a day. Mining alone, you can expect to find 288 × (your hashrate ÷ network hashrate) of them each day. Divide one day by that number to get the average time between your blocks. Read the live network hashrate on the bntpool.com dashboard.
</Answer>

The formula works because Blocknet adjusts its difficulty so that the whole network finds a block every 300 seconds on average, and a block's difficulty is about the number of hashes the network is expected to try before one succeeds. Every hash has the same chance, so the fraction of the network's hashes that come from your machine is the fraction of blocks you can expect to find.

You need two numbers:

- **Your hashrate.** Seine shows it on its dashboard while it mines, and the pool's [stats page](/stats) shows it once you have mined there.
- **The network hashrate.** The [bntpool.com dashboard](/) shows it in the Network Hashrate card, next to the Pool Hashrate card. Both change over time, so check them when you do the sum.

Each block you find pays the block reward for that month. The [block reward schedule](/blog/blocknet-block-reward-schedule/) lists the reward by month.

## A worked example

<Note>
The numbers below are hypothetical round figures chosen to show the arithmetic. They are not live values. Use the current network hashrate from the dashboard for your own estimate.
</Note>

Suppose the network runs at 50,000 H/s. A single rig at 50 H/s, about what seine measured on one AMD EPYC Zen 5 server, holds 0.1% of the network. Here is what that rig and two larger operations would find mining alone:

| Your hashrate (example) | Share of the network | Expected blocks per day | Average time between blocks |
|---|---|---|---|
| 50 H/s | 0.1% | 0.288 | about 83 hours (3.5 days) |
| 500 H/s | 1% | 2.88 | about 8.3 hours |
| 5,000 H/s | 10% | 28.8 | about 50 minutes |

Now put the 50 H/s rig on a pool that also runs at 5,000 H/s in total. The pool finds about 28.8 blocks a day, one every 50 minutes on average, and the rig's 1% share of the pool's work earns it 1% of each of those blocks. The work counted is the same 0.288 blocks a day. What changes is how it arrives: a small portion roughly every 50 minutes, where solo it comes as one whole block every few days.

The table also shows why solo mining suits large operations. At 5,000 H/s, a solo miner finds blocks as often as a pool of that size, so its income is steady on its own.

To turn blocks per day into BNT for your own hardware, with a dated example from live pool data, see [how to estimate your Blocknet mining rewards](/blog/estimate-blocknet-mining-rewards/).

## Why does a pool give steadier payouts?

bntpool.com combines the hashrate of everyone mining on it, so it finds blocks far more often than any one rig. Each block's reward is shared among the miners whose work helped find it, using PPLNS: the pool looks back over the six hours before the block and divides the reward in proportion to the difficulty of each miner's shares.

That has a few practical results for you:

- **Earnings from every block.** As long as your shares are in the six-hour window, you earn a portion of each block the pool finds. You can watch them arrive on the [blocks page](/blocks).
- **Automatic payouts.** After a block is 60 blocks deep, your portion moves into your confirmed balance, and the pool sends it to your address once it reaches 0.1 BNT. Sent payments are listed on the [payouts page](/payouts).
- **Continuity through restarts.** Shares you already submitted stay in the window for six hours, so a reboot or driver update doesn't reset your position.
- **Nothing to run but the miner.** The pool runs the node, builds block templates and submits blocks. You can mine from several rigs to one address or to a Blocknet ID `@name`.

The [PPLNS payouts guide](/blog/how-pplns-payouts-work/) walks through the split step by step, with its own worked example. The pool's [Get Started page](/start) lists the current payout settings.

## Why mine Blocknet solo?

Solo mining puts every part of the process under your control. Your own node builds each block template and checks every block itself, and each block you find pays its full reward straight to your node's wallet.

It fits best when your hashrate finds blocks often. In the example above, an operation with 10% of the network finds a block about every 50 minutes, so solo payouts come as regularly as a pool's would. If you already run a Blocknet node for your wallet or for other services, solo mining adds the miner on top of it.

Seine is built for this mode as well. It mines against the node's block templates, and if the node has forgotten a template, for example after a restart, seine resubmits the full solved block so a found block still reaches the chain.

## How do you set up solo mining with seine?

1. **Install and sync a Blocknet node.** With the `blocknet` wrapper, run:

   ```sh
   blocknet install latest
   blocknet start mainnet
   ```

   This writes the node's API cookie to `~/.config/bnt/data/mainnet/api.cookie`. To shorten the first sync, bntpool.com publishes a `checkpoints.dat` file for fast initial sync on its [checkpoints page](/checkpoints); [syncing a Blocknet node with checkpoints](/blog/blocknet-checkpoints-fast-sync/) explains how the node uses it.
2. **Wait for the sync to finish.** The node's `[sync] progress` messages reach 100%. The node serves block templates only once it has caught up with the chain.
3. **Start seine in daemon mode.**

   ```sh
   ./seine --mode daemon
   ```

   For a custom node layout, point seine at the API and cookie yourself:

   ```sh
   ./seine --mode daemon --api-url http://127.0.0.1:8332 --cookie /path/to/api.cookie
   ```

The [solo mining guide](https://github.com/zpalmtree/seine/blob/master/docs/guides/solo-mining.md) covers wallet passwords and custom setups, and the [seine mining guide](https://seineminer.com/blog/how-to-mine-blocknet/) covers downloading seine and getting your first shares. You can [download seine](https://seineminer.com/#download) for Windows, macOS or Linux.

## Which should you choose?

Run the formula with your own hashrate and the current network hashrate from the [dashboard](/), then look at the average time between blocks.

- **Start on bntpool.com** if you mine on a desktop, a laptop or a handful of servers, or if you'd like steady payouts without running a node. Run seine, paste your address or `@name`, and accept the default pool.
- **Mine solo** if your operation holds a large share of the network, so blocks arrive often on their own, and you already run or want to run your own synced node.

Seine makes the choice easy to revisit. Pool mining and daemon mode use the same download, so you can switch with one flag and a restart as your hardware grows. If you run a local node while mining on the pool, seine also shows your node's wallet balance next to your pool balance.

<Note>
Blocknet plans a network upgrade called Faraday around 2027-02-16, according to [blocknetcrypto.com](https://blocknetcrypto.com/#faraday). It is planned to change the proof of work. Keep seine up to date: it checks for new releases when it starts and every 12 hours.
</Note>

<CTA target="pool" title="Start mining on bntpool.com" text="Run seine, paste your address and earn a portion of every block the pool finds, paid automatically from 0.1 BNT." />
