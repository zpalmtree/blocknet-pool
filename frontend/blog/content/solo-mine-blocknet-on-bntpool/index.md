---
title: Solo mine Blocknet on bntpool.com
description: 'Solo mine Blocknet without running a node. Put solo: in front of your address in seine and bntpool.com pays you the whole reward of every block you find.'
date: 2026-11-04
tags: [Mining guide, Solo mining, bntpool, seine]
faq:
  - q: 'How do I solo mine Blocknet on bntpool.com?'
    a: 'Update seine to v0.3.1 or newer and put solo: in front of your payout address, for example ./seine --address solo:YOUR_ADDRESS. Seine connects to bntpool.com as usual, and the pool pays you the whole reward of every block your rig finds through its normal payouts. You don''t need to run a node.'
  - q: 'Do I need my own Blocknet node to solo mine?'
    a: 'No. With the solo: prefix, bntpool.com runs the node, builds the block templates and submits the blocks you find. Your rig only runs seine. Seine''s daemon mode is the other way to mine solo, against your own synced node, and it suits miners who already run one.'
  - q: 'Can I use a Blocknet ID name for solo mining?'
    a: 'Yes. Use solo:@name or solo:$name in place of solo:ADDRESS, and quote it on the command line so your shell passes it through unchanged. The pool resolves the name to its full address, then credits your solo blocks and pays them to that address like any other payout.'
  - q: 'How do I switch back to pool rewards?'
    a: 'Remove the solo: prefix from your address and restart seine. Your rig goes back to sharing PPLNS rewards from every block the pool finds. Each rig picks its mode when it connects, so you can run some rigs solo and others on shared rewards with the same address.'
related: [pool-vs-solo-mining-blocknet, mine-blocknet-on-bntpool, track-your-miner-on-bntpool, block-confirmations-mining-rewards]
---

This guide shows you how to solo mine Blocknet on bntpool.com: add one prefix to your address in seine, and the pool pays you the whole reward of every block your rigs find. You don't install or sync a node. You'll set it up, see how solo blocks are paid, and learn where to follow them on the pool.

## What is solo mining on bntpool.com?

<Answer>
Solo mining on bntpool.com pays you only for the blocks your own rigs find, and each one pays its whole block reward to your address. The pool still runs the node, builds the block templates and submits your blocks, so you get solo rewards with the same setup as pool mining. You turn it on by putting solo: in front of your address.
</Answer>

On normal pool mining, bntpool.com shares every block among the miners whose shares fall in its six-hour PPLNS window, so you earn a portion of each block the pool finds. A solo rig keeps the whole reward of its own blocks instead and earns nothing from blocks found by others. Both use the same server, the same address and the same payouts. Only the login changes.

Solo mining through the pool gives you solo rewards without the work of a node:

- **No node to run.** The pool's node follows the chain, serves fresh templates and submits your solved blocks.
- **Automatic payouts.** Your blocks are paid to your address or Blocknet ID name like any other bntpool.com payout.
- **Live stats.** Your rigs, hashrate and blocks show up on the pool's [stats page](/stats).
- **One prefix to switch.** Remove `solo:` and restart seine to go back to shared rewards.

Seine's daemon mode is the other way to mine solo, against your own synced Blocknet node. [Pool vs solo mining for Blocknet](/blog/pool-vs-solo-mining-blocknet/) compares all three options and shows how often your hashrate finds a block.

## How do you start solo mining with seine?

<Answer>
Install seine v0.3.1 or newer, then start it with solo: in front of your Blocknet address, for example ./seine --address solo:YOUR_ADDRESS on macOS or Linux. Seine connects to bntpool.com, its default pool, and the dashboard labels your account Solo. You can also type solo: and your address when seine asks for a payout address on its first run.
</Answer>

1. **Update seine.** Solo logins need seine v0.3.1 or newer. [Download seine](https://seineminer.com/#download), or let automatic updates install it.
2. **Start seine with the prefix.** On macOS or Linux:

   ```sh
   ./seine --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   On Windows, in PowerShell:

   ```powershell
   .\seine.exe --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   Seine mines on bntpool.com by default. The pool's [Get Started page](/start) shows the same command with the pool address written out.
3. **Check the dashboard.** The balance panel labels your account **Solo**, and the log shows the pool's confirmation that you are paid for the blocks this address finds.

The prefix works wherever seine takes an address: the first-run prompt, the saved config, the local control API and farm proxy rigs. Seine saves it with your address, so later runs keep mining solo.

## Can you solo mine with a Blocknet ID name?

Yes. A Blocknet ID name works the same way as a full address. Write `solo:@name` or `solo:$name`, and quote it so your shell passes it through unchanged:

```sh
./seine --address 'solo:@yourname'
```

The pool resolves the name at blocknet.id and pays your solo blocks to the address behind it. To name a rig, add it to a full address after a dot, as in `solo:YOUR_ADDRESS.rig1`, or pass `--pool-worker rig1` with any address or name. The prefix accepts any letter case, so `SOLO:` works too.

## How are solo blocks paid?

When your rig finds a block, the pool records it as a solo block. After it is 60 blocks deep, about five hours, the pool credits its whole block reward, less the pool fee, to your balance. The fee is 0% as of November 4, 2026. Your balance is then paid to your address through the pool's normal payouts.

Payouts follow the same rules as pool mining. Each block waits for 60 confirmations so a short chain reorganization can't undo a reward that has already been paid, and [block confirmations and mining rewards](/blog/block-confirmations-mining-rewards/) explains why. The pool's `/api/info` endpoint reported a 1 BNT minimum payout on November 4, 2026, and one Blocknet block reward is far above that, so a confirmed solo block is paid on the pool's normal payout schedule. The [payouts page](/payouts) lists every payment the pool sends.

If you mine to an exchange deposit address with a payout memo, solo works the same way. The memo and its own minimum payout apply to your solo blocks just as they do to shared rewards.

Your rig's work still counts toward the pool's hashrate, round effort and luck, and a solo block ends the pool's round like any other block. Solo shares never enter the PPLNS window, so other miners earn nothing from your blocks, and you earn nothing from theirs.

## How often will a solo rig find a block?

Blocknet targets one block every five minutes, or 288 blocks a day. A solo rig finds 288 × (your hashrate ÷ network hashrate) of them each day on average. Read the network hashrate on the [bntpool.com front page](/) and your own hashrate on seine's dashboard, then divide one day by the result to get the average time between your blocks.

Solo rewards arrive as whole blocks, so they suit miners whose hashrate finds blocks often: large farms, server fleets and anyone with a big share of the network. Smaller rigs earn the same amount on average with shared PPLNS rewards, paid in steady portions from every block the pool finds. [Pool vs solo mining for Blocknet](/blog/pool-vs-solo-mining-blocknet/) works through examples at three hashrates.

## Where do you see your solo rigs and blocks?

Look up your address on the [stats page](/stats). You can paste it with or without the `solo:` prefix. The page then marks your solo work:

- **Workers** lists each rig with a Type column that reads **Solo** for rigs that sent solo shares in the hashrate window.
- **Estimated rewards** marks your solo blocks while they wait for confirmations.
- **Blocks** and **Luck** show a Type column, Pool or Solo, before each block's status. The [blocks page](/blocks) lists every block the pool has found, newest first.

[Track your miner on bntpool.com](/blog/track-your-miner-on-bntpool/) walks through the rest of the stats page.

## Can you mix solo and pool rigs?

Yes. Each rig chooses its mode when it connects, so one address can run solo rigs and pool rigs at the same time. A farm can solo mine on its largest servers and keep desktops on shared rewards, all paid to one balance. The pool keeps the two kinds of work apart: a share found on a solo connection only counts for that address's solo blocks.

To switch a rig back, remove `solo:` from its address, in your command or saved config, and restart seine. Shares the rig already sent in solo mode stay solo, and new shares join the PPLNS window right away.

<CTA target="pool" title="Solo mine on bntpool.com" text="Add solo: to your address in seine and keep the whole reward of every block your rigs find, with no node to run." />
