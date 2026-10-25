---
title: What is hashrate in crypto mining?
description: Hashrate is the number of hashes a miner computes per second. Learn its units, how your hashrate, the pool's and the network's differ, and why each one moves.
date: 2026-10-25
tags: [Hashrate, Mining basics, Blocknet, bntpool]
faq:
  - q: What hashrate can a computer reach mining Blocknet?
    a: In seine's measurements, the best result per hardware generation is 49.4 H/s for an AMD EPYC Zen 5 server, 35.7 H/s for AMD EPYC Zen 4, 30.9 H/s for an Apple M4 Max laptop and 21.5 H/s for an AWS Graviton4 Arm server. Your own figure depends mostly on memory bandwidth and how many 2 GB threads your RAM holds.
  - q: Why is Blocknet hashrate measured in H/s instead of MH/s or TH/s?
    a: Each Blocknet hash is one Argon2id computation that writes about 2 GiB of memory and reads it back, so even a large server completes only tens of hashes per second. Proofs of work built on fast hashes such as SHA-256 reach far bigger numbers, which is why they use larger units. Compare hashrates only within the same algorithm.
  - q: Why does the pool show a different hashrate than my miner?
    a: Your miner counts every hash it completes. The pool sees only the shares you submit and estimates your hashrate from their difficulty over the last hour. Shares arrive at random moments, so the pool figure moves more and settles over time. Compare the 24-hour chart on My Stats with your miner's average for a fair match.
  - q: Does a higher hashrate mean more mining rewards?
    a: Yes, in proportion. Every hash has the same chance of finding a block, so your expected share of the network's blocks equals your share of the network hashrate. Doubling your hashrate doubles your expected rewards, while a rising network hashrate shrinks your share. On bntpool.com, that share arrives as a portion of every block the pool finds.
related: [what-is-a-mining-pool, estimate-blocknet-mining-rewards, track-your-miner-on-bntpool]
---

Hashrate is the number every miner, pool and block explorer shows first. This guide explains what it measures, how to read the units from H/s upward, how your own hashrate relates to the pool's and the network's, and why each figure moves from minute to minute, using Blocknet and the bntpool.com dashboard as the worked example.

## What is hashrate?

<Answer>
Hashrate is how many hashes a miner computes per second. In proof-of-work mining, each hash is one guess at a block solution, and every guess has the same small chance of winning. A higher hashrate means more guesses per second, so your expected share of blocks grows in proportion to your share of the total hashrate.
</Answer>

A proof-of-work chain asks miners to find a block header whose hash falls below a target. There is no shortcut: a miner changes a number in the header called the nonce, hashes the result, checks it against the target, and repeats. Hashrate counts how many of those attempts the machine completes each second.

On Blocknet, one attempt is one Argon2id hash. The nonce goes in as the password, the rest of the block header is the salt, and the function works through 2 GiB of memory before it produces its 32-byte answer. Seine and the pool both report hashrate in completed hashes per second, so 1 H/s means one full Argon2id computation per second.

## What do H/s, kH/s and MH/s mean?

Hashrate units follow the usual metric prefixes, each step a thousand times the one before:

| Unit | Hashes per second | Typical use |
|---|---|---|
| H/s | 1 | A single computer on Blocknet |
| kH/s (KH/s) | 1,000 | Blocknet pool and network totals |
| MH/s | 1,000,000 | Faster proof-of-work algorithms |
| GH/s | 1,000,000,000 | Fast hashes such as SHA-256 |
| TH/s and up | 1,000,000,000,000 and more | Network totals for fast hashes |

The bntpool.com dashboard writes the thousands unit as KH/s and switches up a unit each time a value passes 1,000, so 44.44 KH/s means 44,440 H/s. When you divide one hashrate by another, convert both to H/s first.

## Why are Blocknet hashrates so small?

Blocknet's proof of work is memory-hard. Every hash writes about 2 GiB of memory and reads about 2 GiB back, far more than any processor cache holds, so the speed of the memory system sets the pace. Each mining thread needs its own 2 GiB, which is why RAM decides how many threads a machine can run.

Here are the best results seine has measured on each hardware generation:

| Hardware | Threads | Hashrate |
|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49.4 H/s |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35.7 H/s |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30.9 H/s |
| Arm server (AWS Graviton4) | 16 | 21.5 H/s |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13.1 H/s |

Even the Zen 5 server, with 32 threads, completes about 1.5 hashes per thread each second. Algorithms built on fast hashes produce much bigger numbers: the Bitcoin wiki notes that Bitcoin miners, which run double SHA-256, were already rated in billions of hashes per second in 2014. The two numbers measure different amounts of work per hash, so a hashrate only means something next to others from the same algorithm. [Argon2id proof of work explained](https://seineminer.com/blog/argon2id-proof-of-work-explained/) covers the algorithm in depth, and the [Blocknet mining hardware guide](https://seineminer.com/blog/blocknet-mining-hardware-hashrate/) shows how memory bandwidth and RAM set these results.

## Your hashrate, the pool's and the network's

Three hashrates matter to anyone mining on a [pool](/blog/what-is-a-mining-pool/), and each is measured a different way.

### Your hashrate

Seine counts every hash your machine finishes and shows two figures on its dashboard: **Current**, your rate right now, and **Average**, the rate across the session. For a stable reading of what a machine can do, use the average after seine has had a few minutes to tune its thread count and CPU kernel on real mining work.

The pool cannot see your hashes. It sees the shares you submit, which are hashes that beat an easier target set for your connection. bntpool.com estimates your hashrate from the total difficulty of your accepted shares over the last hour and shows it on [My Stats](/stats). [Track your miner on bntpool.com](/blog/track-your-miner-on-bntpool/) walks through that page.

### Pool hashrate

The dashboard's **Pool Hashrate** card adds up the share work of every connected miner over the same one-hour window. It tells you how much of the network's work bntpool.com is doing, and with it how often the pool can expect to find a block, which the dashboard shows as **Expected Block Time**.

### Network hashrate

No one can count every hash on the network, so the network hashrate is worked out from blocks. Difficulty is the expected number of hashes needed to find a block, and Blocknet targets one block every 300 seconds.

## How is network hashrate calculated?

<Answer>
Network hashrate is estimated from difficulty and block times. On Blocknet, difficulty is about the number of hashes expected per block, and blocks target 300 seconds, so network hashrate is roughly difficulty divided by 300. The bntpool.com dashboard works it out from the actual work and timestamps of the ten most recent blocks.
</Answer>

Here is that arithmetic with live values read on October 25, 2026 at 02:26 EDT. The [Blocknet explorer](https://explorer.blocknetcrypto.com) showed a next difficulty of 16,943,147, so the network was set to need about 16.9 million hashes per block. Divided by 300 seconds, that is about 56,477 H/s.

At the same moment, the bntpool.com dashboard showed a network hashrate of 44.44 KH/s, or 44,440 H/s. Both are estimates of the same thing from different angles. The difficulty figure is the rate that would produce blocks exactly on the five-minute target. The dashboard figure divides the actual work of the last ten blocks by the actual time they took, so when recent blocks take longer than five minutes, it reads lower. Blocknet adjusts difficulty after every block, which keeps the two close over time.

## Why does hashrate fluctuate?

Each of the three figures moves for its own reasons.

- **Your machine.** In the first minutes, seine compares thread counts and kernels while it mines and settles on the fastest, so the rate climbs as it tunes. Afterward, anything else using the computer's memory or cores can take a share of the hashing time.
- **The pool's view of you.** Shares are random events. bntpool.com sets each connection's share difficulty to aim for about 10 shares every five minutes, so an hour holds roughly 120 shares, and a lucky or quiet stretch moves the estimate. A rig that just connected is averaged over the minutes it has been mining until the full hour fills.
- **The network.** Block times are random too, and a ten-block sample is short. On October 25, 2026, the dashboard's network hashrate read about 50,057 H/s at 00:26 EDT, 62,329 H/s at 02:03 and 44,440 H/s at 02:26. Miners joining and leaving add real changes on top of that noise.

For decisions, use averages: seine's session average, the 24-hour chart on My Stats, and the network figure checked a few times over a day.

## What does your hashrate mean for rewards?

Because every hash has the same chance, your share of the network hashrate is your expected share of blocks. Blocknet produces 288 blocks a day, so a miner with 0.1% of the network can expect about 0.29 blocks' worth of rewards per day over time. [How to estimate your Blocknet mining rewards](/blog/estimate-blocknet-mining-rewards/) turns this into BNT per day with inputs read from the [dashboard](/).

On bntpool.com, that expected share reaches you as a portion of every block the pool finds, credited by your accepted shares, so rewards arrive in small steps around the clock. If you haven't started yet, the [seine install guide](https://seineminer.com/blog/how-to-mine-blocknet/) gets you from download to your first shares in a few minutes.

<Note>
The hashrates in this guide describe Blocknet's current Argon2id proof of work. Blocknet plans a network upgrade called Faraday around 2027-02-16, according to [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), and it is planned to change the proof of work. Keep seine up to date: it checks for new releases when it starts and every 12 hours.
</Note>

<CTA target="seine" title="See your own hashrate" text="Download seine, let it tune itself for a few minutes, and watch your hashrate on its dashboard and on My Stats." />
