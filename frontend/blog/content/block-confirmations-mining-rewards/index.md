---
title: 'Block confirmations and immature mining rewards explained'
description: 'Follow a Blocknet pool block from pending to mature rewards and payment. Learn how bntpool counts block depth and why confirmations are measured in blocks.'
date: 2026-10-26
tags: ['Mining rewards', 'Confirmations', 'Blocknet']
faq:
  - q: 'How many confirmations does bntpool require for a block reward?'
    a: 'At the October 26, 2026 check, bntpool.com publishes a 60-block requirement. Its confirmation check uses the current chain height minus the found block''s height and verifies the block hash against the node. Check Get Started or the public pool-info endpoint for the current setting.'
  - q: 'Does a 60-block requirement mean an exact five-hour wait?'
    a: 'No. Sixty intervals at Blocknet''s five-minute target equal 300 minutes, or five hours, as a planning estimate. Actual progress follows blocks added to the chain, whose arrival times vary. Reward maturity, your payout threshold and the later payment transaction are separate stages.'
  - q: 'Do I need to keep mining for an existing reward to mature?'
    a: 'An already found block gains depth as the network adds blocks, independently of whether your own miner stays connected. Your eligibility for that reward comes from the pool''s accounting rules. Stopping does not speed up or reset the block''s maturity; use My Stats to follow the recorded reward.'
related: [how-pplns-payouts-work, track-your-miner-on-bntpool, mining-pool-luck-explained, mining-difficulty-explained]
---

Follow a found Blocknet block through confirmation, reward maturity and a later payment to your wallet. This guide explains the chain-depth stage behind bntpool.com's reward cards. For how the pool divides a reward among miners, use the [PPLNS guide](/blog/how-pplns-payouts-work/); for submitting work in the first place, start with the [pool setup guide](/blog/mine-blocknet-on-bntpool/).

## What are confirmations and immature mining rewards?

<Answer>
Confirmations describe a block's place beneath the current chain tip. An immature mining reward belongs to a recently found block that has not yet reached its required depth. bntpool checks that depth and the block's identity before confirming the reward; balance eligibility and the later payout transaction follow as separate steps.
</Answer>

*A found block gains depth as later blocks arrive, reaches the maturity checkpoint, and then proceeds through balance eligibility to a separate wallet payment.*

## How does bntpool count a block's depth?

The pool's confirmation check subtracts the found block's height from the current chain height. Its published requirement was 60 blocks when checked on October 26, 2026. The found block starts at depth zero under this convention. A page elsewhere may count the containing block as its first confirmation, so compare the counting rule as well as the number shown.

For an arithmetic example, a pool block at height 70,000 has depth zero when the tip is 70,000, depth one at 70,001 and depth 60 at 70,060. These are illustrative heights, not a live block report. The pool also checks the block hash against its node before confirming it. The [pool implementation](https://github.com/zpalmtree/blocknet-pool/blob/b002c96/crates/pool-runtime/src/payout.rs) records that depth and identity check.

## Why is five hours only an estimate?

Blocknet targets a five-minute interval between network blocks. Sixty intervals multiplied by five minutes gives 300 minutes, or five hours. That arithmetic helps you plan, but the maturity condition is a block count. The relevant observation is the chain advancing from the found height toward the required depth, rather than a wall-clock timer reaching a fixed deadline.

Blocks found anywhere on the accepted chain advance that depth; they do not all have to come from bntpool. Your own miner's speed does not create a private confirmation clock. Network block arrival times vary, so elapsed time can differ from the target-based estimate. The [difficulty guide](/blog/mining-difficulty-explained/) explains the network's adjustment mechanism, while the [luck guide](/blog/mining-pool-luck-explained/) explains variation in pool rounds.

## Read pending, confirmed and paid as different stages

A newly recorded pool block appears as pending while its reward is collecting depth. Your estimated portion can appear on [My Stats](/stats) when you have eligible work. The underlying block reward creates new coins; the Blocknet wallet's coinbase-maturity rule uses a 60-block depth before those outputs are spendable. The pool's matching wait lets its accounting follow that maturation process.

After the block passes the required depth and identity checks, the pool can mark it confirmed and distribute the eligible reward to balances. This does not mean every address receives a payment at that instant. The payout process evaluates confirmed balances and its sending conditions separately. Keep the block status, your confirmed balance and your payment history distinct when reading the public pages; each describes a different part of the same flow.

## Why does the pool check the block hash?

A height identifies a position in the chain, while a hash identifies a particular block. Nodes can learn of competing blocks, and the accepted chain determines which block occupies that position. The pool therefore asks its node for the block at the recorded height and compares the hash. Depth alone would not establish that the originally recorded block is still the one on the accepted chain.

A block that is no longer on that chain can be marked orphaned, and its tentative reward is no longer eligible as a reward from the accepted chain. The pool's checks handle that status separately from confirmation. Use the hash link on the [blocks page](/blocks) to inspect the specific block, rather than assuming that two entries at the same height describe the same event. An estimated reward remains tied to its block.

## How is a payout different from the mined reward?

The mined reward first belongs to the block's coinbase output. A pool payment is a later transaction sending an eligible balance to your receiving address. At the checked date, bntpool's published minimum payout is 0.1 BNT. Reaching block maturity and reaching that balance threshold answer different questions; the [PPLNS guide](/blog/how-pplns-payouts-work/) owns the allocation and payout rules in detail.

After a payment is sent, follow its own transaction record and your wallet's receipt and spendability status. Its confirmation history belongs to the payment transaction, not to the original pool block. You do not need to create a new receiving address or reconnect the miner just because a block is maturing. Use the intended address consistently when comparing the pool's [payout history](/payouts) with your wallet.

## How do you follow a reward from start to finish?

Start on [Blocks](/blocks) and identify the found height, hash and current status. Then open [My Stats](/stats) with the payout address used by your miner and inspect the estimated and confirmed reward sections. Once a payment exists, follow its transaction link from the payout history. This sequence gives each observation a place: block, account balance and wallet payment, rather than treating them as interchangeable counters.

Confirm current settings on [Get Started](/start) or `https://bntpool.com/api/info`, and use [pool status](/status) for service context. If a stage needs investigation, record the address, block or transaction identifier, and the observation time; never share wallet recovery words. The [tracking guide](/blog/track-your-miner-on-bntpool/) explains the remaining stats. Existing blocks continue gaining depth while your miner is offline; their eligibility follows the recorded work and chain state.

<CTA target="pool" title="Follow your mining rewards" text="Connect to bntpool.com and use My Stats to follow estimated rewards, confirmed balances and payments." />
