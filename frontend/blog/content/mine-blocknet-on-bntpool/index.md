---
title: Mine Blocknet on bntpool.com
description: Join bntpool.com, seine's default Blocknet pool. Connect to stratum+tcp://bntpool.com:3333 with your address or @name, name your rigs and check your stats.
date: 2026-10-25
tags: [Mining guide, bntpool, Blocknet ID, seine]
faq:
  - q: What is the bntpool.com Stratum address?
    a: The address is stratum+tcp://bntpool.com:3333, the pool seine suggests on its first run. Press Enter at the pool prompt to accept it, or pass it with the --pool-url flag. Your Blocknet address or Blocknet ID name is your login, and the pool pays your rewards to that address.
  - q: Can I mine on bntpool.com with a Blocknet ID name?
    a: Yes. Enter @name or $name wherever seine or the pool asks for an address. The pool checks the signed mapping at blocknet.id, then credits shares and payouts to the full address behind the name. You need seine v0.2.16 or newer, and you should quote the name on the command line so your shell leaves it alone.
  - q: Do I need an account to mine on bntpool.com?
    a: No. There is no sign-up, password or email. Your Blocknet address, or a Blocknet ID name that points to it, is your login and your payout destination. Start seine with it, then look it up on the My Stats page to see your hashrate, shares, rigs, balance and payouts.
  - q: Can I mine to one address from several computers?
    a: Yes. Run seine on each computer with the same address or name and give each one its own rig name. The pool counts shares, balance and payouts per address, so every rig adds to one balance and one payout, and the My Stats page lists each rig on its own row.
related: [how-pplns-payouts-work, track-your-miner-on-bntpool, blocknet-fair-launch]
---

This guide covers joining the bntpool.com pool with seine: the pool address, logging in with a Blocknet address or a Blocknet ID `@name`, naming each rig, and checking on the stats page that your shares are arriving. If seine isn't installed yet, follow the [seine install guide](https://seineminer.com/blog/how-to-mine-blocknet/) first, then come back here.

## What is the bntpool.com pool address?

<Answer>
The bntpool.com Stratum address is `stratum+tcp://bntpool.com:3333`. Seine offers it as the default pool on its first run, so pressing Enter at the pool prompt connects you. You log in with your Blocknet address or a Blocknet ID `@name`. There is no account to create, and rewards go to that address.
</Answer>

These are the pool's settings, as reported by its public `/api/info` endpoint on October 25, 2026:

| Setting | Value |
|---|---|
| Stratum address | `stratum+tcp://bntpool.com:3333` |
| Login | Blocknet address, `@name` or `$name` |
| Payout scheme | PPLNS over a 6-hour window |
| Confirmations before payout | 60 blocks |
| Minimum payout | 0.1 BNT |

You can see the same values at any time on the pool's [Get Started](/start) page.

## Why is bntpool.com seine's default pool?

When you run seine for the first time, it asks for a pool and fills in `stratum+tcp://bntpool.com:3333` as the default. The pool and the miner fit together in a few practical ways:

- **One protocol.** bntpool.com runs the Stratum protocol seine speaks (version 2), so seine connects with nothing to configure beyond your address.
- **Names work on both sides.** Seine and the pool both accept Blocknet ID names and verify them against the same signed record at blocknet.id.
- **Version checks.** Each rig reports its seine version with its shares, and the stats page marks a rig running an older seine with an "update available" badge that links to the seine releases page.
- **A seine setup guide.** The pool's Get Started page walks through downloading and running seine, with a command you can copy.

## How do you connect seine to bntpool.com?

The first time you start seine, it asks two questions:

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

Paste your address or type your name, then press Enter to accept bntpool.com. Seine saves both answers to `seine-data/seine.config.json`, so every later start goes straight to mining. It also picks a rig name and saves it in the same file (more on that below).

On a headless rig, in a service or in a script, there is no one to answer the prompts. Pass both values as flags on the first start, which is the same command the Get Started page gives you:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

To change pools or addresses later, edit or delete `seine-data/seine.config.json`, or override the saved values with `--address` and `--pool-url` on the command line.

## Can you mine to a Blocknet ID @name?

<Answer>
Yes. bntpool.com accepts a Blocknet ID shortname, written `@name` or `$name`, anywhere it accepts an address. The pool checks the signed mapping from blocknet.id, validates the address the name points to, and credits shares, balance and payouts to that full address for the whole connection.
</Answer>

Names are case-insensitive, so `@Alice` and `@alice` are the same login. Name support arrived in seine v0.2.16; any current release has it. Before mining starts, seine prints the address the name resolved to, so you can confirm it matches your wallet. On the command line, put the name in single quotes. Without them, a shell would try to expand `$name` as a variable:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

If you later point your name at a different wallet, restart seine. Seine resolves a saved name each time it starts and keeps that address for the whole run, and the pool does the same for each connection. A balance you have already earned stays with the address it was earned for and is paid there, so moving a name never moves your coins.

You can register a name at [blocknet.id](https://blocknet.id). The [Blocknet ID guide](https://seineminer.com/blog/blocknet-id-pay-by-name/) explains how paying by name works.

## How do you name your rigs?

Every connection logs in with a rig name alongside your address. On its first run, seine picks one for you, `seine-` followed by four digits, and saves it in its config file, so the same machine keeps the same name across restarts. To use a name you'll recognize, set it on the command line:

```sh
./seine --pool-worker rig-01
```

The pool keeps up to 64 characters of the name. A stable name also helps after a restart: the pool remembers each rig's share difficulty for an hour, keyed by your address and the rig name, so a rig that reconnects picks up at the difficulty it had already settled on.

## Can you mine to one address from several rigs?

Yes. Start seine on each machine with the same address or `@name` and a different rig name. The pool counts shares, balances and payouts per address, so all of your rigs add up to one balance and one payout. On the stats page, each rig gets its own row, which makes it easy to see that every machine is mining.

## How do you check that your miner is working?

Seine's dashboard shows your hashrate and accepted shares on the machine itself. To see the pool's view, open [My Stats](/stats), enter your address, `@name` or `$name`, and press **Lookup**. If you enter a name, the page shows the address it resolved to next to the search box. The page also remembers your last lookup in that browser, and the [guide to tracking your miner](/blog/track-your-miner-on-bntpool/) covers every card on it.

Look for three things:

- **Hashrate and Shares Accepted.** The hashrate card shows what the pool sees from you, and the accepted count climbs as your rigs submit work.
- **A row for each rig.** Each row shows the rig's name, hashrate, accepted and rejected shares, and the time of its last share. A last share from a moment ago means that rig is connected and working.
- **Recent Shares.** The latest shares are listed with the rig that sent them, their difficulty and their status.

The pool sets share difficulty separately for each connection and adjusts it as you mine, so even a small CPU sends shares steadily and your first accepted shares appear within minutes of connecting. For the hashrate to expect from your hardware, see the [Blocknet mining hardware guide](https://seineminer.com/blog/blocknet-mining-hardware-hashrate/).

Rewards come next. Estimated Rewards appear once the pool finds a block while your shares are inside the six-hour window. After that block has 60 confirmations, your share moves to Confirmed Rewards, and the pool sends it automatically once you have at least 0.1 BNT. [How PPLNS payouts work](/blog/how-pplns-payouts-work/) explains how each block's reward is split. Pool-wide history is on the [Blocks](/blocks) and [Payouts](/payouts) pages.

## What does the Get Started page cover?

The pool's [Get Started](/start) page has everything on one screen:

- **Pool information.** The Stratum address (click it to copy), the payout scheme, the minimum payout and the number of block confirmations before payout, read live from the pool.
- **Quick start in three steps.** Download seine from [seineminer.com](https://seineminer.com/#download), run it once and enter your address or name and the pool URL, then start mining and follow your hashrate and balance on My Stats.
- **A command to copy.** The full `./seine` command with the pool URL filled in. Swap in your address, or `'@yourname'` in quotes.
- **Setup notes.** Where seine saves its config, how to reset it, and the flags that override it.
- **How rewards are counted.** A short explanation of PPLNS: the shares you submitted before a block was found earn its reward, and work you add afterward counts toward the blocks that follow. It also notes that rewards from unconfirmed blocks are shown separately on My Stats until those blocks confirm.
- **Screenshots** of seine's first-run setup and its mining dashboard, so you know what to expect before you start.

<CTA target="pool" title="Start mining on bntpool.com" text="Point seine at bntpool.com with your address or @name and watch your shares arrive on My Stats." />
