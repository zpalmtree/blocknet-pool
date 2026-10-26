---
title: 'Accepted, rejected and stale mining shares explained'
description: 'Understand accepted, rejected and stale mining shares on bntpool.com, read common rejection messages, and see how share difficulty affects your mining stats.'
date: 2026-10-26
tags: ['Mining guide', 'Shares', 'Troubleshooting']
faq:
  - q: 'Does an accepted share mean I found a block?'
    a: 'An accepted share means the pool accepted a submitted result under its share-processing rules. A network block must meet a separate network target and be accepted by the daemon. Pool rewards follow block discovery, validation and the payout process; a share acknowledgment is not a payment notification.'
  - q: 'Why does the pool reject a stale share?'
    a: 'A stale job response means the submission refers to an assignment that is no longer eligible, such as an expired assignment or an older template beyond its grace period. Check for fresh jobs and renewed accepted shares. Repeated stale responses call for checking connectivity and the miner version.'
  - q: 'Does a higher share difficulty reduce my reward?'
    a: 'Difficulty changes how much work each eligible share represents and how often shares are expected to arrive. Compare difficulty-weighted work over comparable periods, rather than raw share counts. Your actual reward also depends on the pool''s PPLNS window, eligible work, block discoveries and validation rules.'
related: [track-your-miner-on-bntpool, how-pplns-payouts-work, what-is-hashrate]
---

Use the share result in your miner's log to decide whether to keep mining, check the connection, or investigate a repeated rejection. This guide explains bntpool.com's share responses and the difficulty behind its counters. If you have not connected yet, start with the [pool setup guide](/blog/mine-blocknet-on-bntpool/); if an address shows no activity, use the [tracking guide](/blog/track-your-miner-on-bntpool/#zero-hashrate).

## What is an accepted mining share?

<Answer>
A share is a mining result submitted against a pool-assigned target. An accepted response confirms that the pool accepted the submission into its share-processing flow. It is evidence that work is reaching the pool. Block discovery, share verification and payout eligibility are separate steps, so acceptance alone is not a payment.
</Answer>

The miner tries many hashes between submitted shares. Most hashes do not meet the share target and never become submissions. The accepted counter therefore counts reported share outcomes, not all hashes your processor or GPU calculated. The [hashrate guide](/blog/what-is-hashrate/) explains the difference between local hash counting and the pool's estimate from submitted work.

## How are shares different from blocks?

The pool gives your connection jobs and a share difficulty. A result meeting the share target demonstrates work at that difficulty. A block candidate must also meet the network target and pass the daemon's block acceptance checks. Finding a share does not imply that a new block has been added to the chain.

bntpool.com records verified and provisional share states as part of validation. An accepted submission can be provisional, and payout weighting applies validation and eligibility rules. Read acceptance as a useful connection signal, then use [My Stats](/stats) for your account's recorded work and rewards. The [PPLNS guide](/blog/how-pplns-payouts-work/) covers the reward sequence after a pool block.

## What do rejected and stale shares mean?

A rejected submission did not pass the applicable check. The reason is more useful than the counter alone. A stale job is one rejection category: the assignment is no longer eligible. Read the exact message and compare it with what happened around the same time, such as a reconnect or a new job.

| Message | Meaning and next check |
|---|---|
| `stale job` | The assignment is missing, expired, superseded beyond its grace period, or tied to an unavailable old template. Check that fresh jobs and accepted shares resume. |
| `duplicate share` | The same template and nonce have already been submitted. Check for repeated submissions; sending the same work again does not add another contribution. |
| `low difficulty share` | The submitted hash does not satisfy the assigned share target. Check the miner version and job/difficulty handling if the message repeats. |
| `nonce out of assigned range` | The submitted nonce is outside the range assigned to that job. Check the miner version and any custom proxy or mining integration. |

A stale share and an orphaned block describe different stages. The former concerns submitted work for an assignment; the latter concerns a block's place in the chain. Keep the share log and the [block list](/blocks) separate when diagnosing a result. A stale-job message alone does not say that the pool found an orphaned block.

## Why does share difficulty change?

<Answer>
Variable difficulty, often called vardiff, adjusts the target for a connection as its observed share activity changes. Higher difficulty generally means fewer shares, each representing more work. Lower difficulty generally means more shares, each representing less work. Compare difficulty-weighted eligible work over the same period instead of comparing share counts alone.
</Answer>

For a simple arithmetic example, ten eligible shares at difficulty 100 represent the same total difficulty as one eligible share at difficulty 1,000: both sum to 1,000. This illustrates weighting, not a promised reward or an exact arrival schedule. Real shares arrive at variable intervals, and validation rules determine which work is eligible for accounting.

The assignment's difficulty matters when a share is submitted. Do not compare an older result only with the latest difficulty shown on screen. The pool keeps assignment information for submission checks. A normal difficulty adjustment is not itself an error, and forcing easier shares does not create additional computational work.

## How should you read a rejection rate?

Compare accepted and rejected submissions over the same time window and scope. A miner's counters may cover one process session, while the pool page may cover a different period or aggregate several rigs. Restarting the miner changes the local comparison window; it does not erase the pool's history for your address.

As an arithmetic example, 99 accepted submissions and one rejected submission make a 1% rejection rate by count. That count-based percentage is not automatically the percentage of lost reward: difficulties can differ, and accounting has eligibility rules. Look at the rejection reasons, their timing and whether accepted work continues before changing settings.

## What should you do when rejections repeat?

Start with one affected rig. Note its miner version, the exact rejection message, when it started, and whether new jobs still arrive. Check [Status](/status) for a pool incident, then check the machine's connection for interruptions. Use the official [seine releases](https://github.com/zpalmtree/seine/releases) when updating, and compare behavior after one change at a time.

If you use a custom proxy or integration, verify that it preserves the assigned job, nonce range and difficulty when forwarding submissions. If the issue continues, provide a short log excerpt and the affected rig's details through the project's support channel. Remove secrets and private account details from anything you share; a wallet recovery phrase is never needed to diagnose shares.

## How do you confirm that mining is working again?

Look for fresh jobs followed by accepted shares, then open [My Stats](/stats) with the same receiving address used by the miner. Confirm recent activity for the affected rig. Let the pool's hashrate estimate accumulate more work before comparing averages. Once work is arriving normally, follow estimated rewards, balance and payouts as separate stages in the PPLNS guide.

<CTA target="pool" title="Connect to bntpool.com" text="Use the setup guide, confirm accepted shares, and track your mining activity by receiving address." />
