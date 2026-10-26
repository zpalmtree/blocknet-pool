---
title: 'Mining pool fees vs miner developer fees'
description: 'Understand pool fees and miner developer fees in Blocknet mining. See where each applies, how to combine them and how to avoid subtracting the same fee twice.'
date: 2026-10-26
tags: ['Mining guide', 'Pool fees', 'Mining rewards']
faq:
  - q: 'Are pool fees and miner developer fees the same charge?'
    a: 'No. A pool fee applies in the pool''s reward accounting, while a miner developer fee belongs to the mining software. seine v0.2.36 assigns a 2% share of billable mining time to development after its startup grace period. bntpool.com''s separately published pool fee was 0% when checked on October 26, 2026.'
  - q: 'How do I combine a pool fee and a miner fee?'
    a: 'For a gross reward estimate, apply each retained fraction in sequence: multiply by one minus the miner-fee fraction, then by one minus the pool-fee fraction. Use the fee for your actual miner release and the pool''s current setting. This estimates retained rewards; it is not a guaranteed payout or a calculation of cash profit.'
  - q: 'Should I subtract the miner fee from pool-reported hashrate?'
    a: 'First identify what that hashrate represents. If it already estimates work credited to your receiving address after developer-fee time, applying the miner fee again would count it twice. The calculator lets you enter zero for that already-reflected adjustment. A recorded confirmed balance or payment should not have either fee deducted again.'
related: [estimate-blocknet-mining-rewards, how-pplns-payouts-work, track-your-miner-on-bntpool, block-confirmations-mining-rewards]
---

Read the two fee layers in a Blocknet mining estimate and follow each to the stage where it applies. This guide uses seine v0.2.36 and bntpool.com's public settings checked on October 26, 2026. It helps you choose the right inputs for the [mining calculator](/blog/estimate-blocknet-mining-rewards/#mining-calculator) and understand the balances you see on [My Stats](/stats).

## What is the difference between a pool fee and a miner fee?

<Answer>
A pool fee is part of the pool's reward distribution. A miner developer fee is part of the software's mining schedule. They have different owners and apply at different stages. Check both settings, identify whether your input is gross or already adjusted, and apply each relevant adjustment once when estimating rewards.
</Answer>

*Mining work passes through a miner-time adjustment and a separate pool-reward adjustment before the estimated retained amount reaches the account.*

## What does the pool fee apply to?

The pool collects eligible mining work and divides the distributable reward according to its accounting rules. In bntpool's implementation, a configured pool fee is calculated from the recorded block reward before the remainder is assigned through PPLNS. That makes it a reward-distribution step. The [PPLNS guide](/blog/how-pplns-payouts-work/) explains which work participates and how balances are credited.

At the October 26, 2026 check, `https://bntpool.com/api/info` publishes `pool_fee_pct` as 0.0, meaning a 0% pool fee. Read the current value on [Get Started](/start) or that endpoint whenever you update an estimate. A zero pool fee is a specific statement about this pool setting; the mining software's own policy remains a separate input.

## How does seine's developer fee work?

The public v0.2.36 release documents a flat 2% developer fee on every pool and in solo mode. It is scheduled as mining time for the developer address rather than a later withdrawal from your wallet. The first 10 minutes of a run are fee-free, and the tracker accounts for time after that grace period. The [release configuration guide](https://github.com/zpalmtree/seine/blob/v0.2.36/docs/guides/configuration.md) describes this policy.

The scheduler works in rounds, so the fraction in a short observation window need not equal exactly 2%. Treat the published percentage as the schedule's long-run target after the grace period, not a promise about every minute or every block reward. Keep the miner version with your notes: a historical guide or another program may describe a different policy. The pool fee cannot substitute for that software-specific information.

## How do you combine the percentages?

Use retained fractions. If the miner fee is m and the pool fee is p, expressed as fractions, the estimate keeps `(1 - m) * (1 - p)` of the gross amount. This is the calculation used by bntpool's [reward estimator](/blog/estimate-blocknet-mining-rewards/). Adding percentage labels is not the general rule because the second adjustment applies to the amount remaining after the first.

For a purely arithmetic example, start with a hypothetical 100 BNT gross estimate, use a 2% miner adjustment and the checked 0% pool setting. The calculation is 100 × 0.98 × 1.00 = 98 BNT. This illustrates the steady-state adjustment only; it is not a measured machine result, a promised reward, or a short-session simulation of the startup grace period.

## Choose the correct hashrate input

A local miner or benchmark rate describes work at the machine. Before applying a developer-fee factor, establish whether that rate includes all mining time or already represents only work for your receiving address. Read the source and time window of the number. The [hashrate guide](/blog/what-is-hashrate/) explains why local, pool and network rates are different observations.

A pool-reported rate for your address can already reflect the time when work was credited elsewhere. If your chosen input already includes that miner adjustment, enter zero for the additional miner fee in the calculator. This changes the arithmetic input, not the software's actual fee policy. The pool fee still applies when estimating gross eligible rewards; check what each displayed figure already includes.

## Why shouldn't you deduct fees from a balance again?

A confirmed account balance is an accounting result, not an untouched gross estimate. The pool has already assigned the relevant credits. Subtracting the same fee again from that balance, or from a payment recorded in history, would change the number without representing another fee event. Use the displayed amount when reconciling the account, and inspect its stage before doing a new estimate.

A pending reward, confirmed balance and sent payment also differ in timing. Block maturity and the payout threshold explain movement between those stages; they are not extra percentage fees. Follow the [confirmation guide](/blog/block-confirmations-mining-rewards/) for maturity and [payout history](/payouts) for an actual sent transaction. A wallet transaction's own fee, if applicable, is a separate transaction field rather than either mining percentage.

## Keep an estimate traceable to its inputs

Save the date, miner version, fee settings, hashrate source and measurement window with your estimate. The calculator combines live pool/network inputs with your hashrate and miner-fee entry. Refresh its network data when revisiting the result. Actual credited rewards also follow eligible shares, found blocks and PPLNS accounting, so a formula result and a later account entry answer different questions.

Use [My Stats](/stats) and the [tracking guide](/blog/track-your-miner-on-bntpool/) to follow recorded work and rewards. If an amount needs explanation, compare the same address, period and stage before adjusting any fee field. The calculator estimates BNT rewards; electricity, equipment costs and a coin's cash price are outside this two-fee calculation. No change to your wallet address is needed to interpret the percentages.

<CTA target="pool" title="Follow rewards on bntpool.com" text="Use the calculator for estimates and My Stats for your recorded work, balances and payments." />
