---
title: 'How mining secures Blocknet payments'
description: 'Learn how Blocknet uses proof of work to select payment history, why confirmations add work behind a payment, and what nodes verify before accepting a block.'
date: 2026-10-28
tags: ['Blocknet', 'Payment security', 'Proof of work']
faq:
  - q: 'Can more mining power authorize a payment from my wallet?'
    a: 'Mining power helps produce competing blocks. Spending funds still requires valid authorization, and nodes check transaction proofs and accounting before accepting a block. A heavier branch must satisfy those rules. Proof of work does not replace the private keys needed to authorize spending your outputs.'
  - q: 'Does Blocknet follow whichever chain has the most blocks?'
    a: 'Released core v0.22.0 tracks cumulative work by adding each block’s difficulty. A valid branch with greater accumulated work can become the selected chain, subject to the reorganization rules. Block count alone is insufficient because blocks can have different difficulty values; their work and validity both matter.'
  - q: 'Are payment confirmations and pool shares the same thing?'
    a: 'A pool share records mining work submitted to the pool. A payment confirmation concerns a transaction’s inclusion in the selected blockchain and the blocks built after it. Shares help allocate pool rewards, while network blocks advance payment history. Increasing your share count does not directly add confirmations to a particular payment.'
related: [block-confirmations-mining-rewards, mining-difficulty-explained, accept-blocknet-payments]
---

Follow a BNT payment from inclusion in a block to a history supported by later mining work. This guide explains why the network needs a way to choose between competing histories, what confirmations tell you, and how that choice works alongside private transaction validation. The implementation described here is released core v0.22.0, checked on October 28, 2026.

## What does mining protect when you send BNT?

<Answer>
Mining helps the network choose a shared, ordered payment history. Each accepted block links to its predecessor and contributes proof of work. Nodes check proposed blocks and transactions, then compare valid branches by accumulated work under their reorganization rules. Later blocks add work behind an included payment, giving its confirmation history practical meaning.
</Answer>

A valid payment needs both authorization and a place in the ledger. Your wallet supplies the authorization needed to spend your funds. Mining and chain selection help nodes agree which valid transactions belong to the current history. These jobs work together: a block’s proof of work does not make an otherwise invalid payment acceptable.

The [rule-enforcement guide](https://seineminer.com/blog/who-controls-blocknet/) explains who develops, proposes and validates. Here the focus is the payment itself: how its inclusion becomes part of the history that your wallet and other nodes follow.

## How do blocks commit to a payment history?

A block header includes the previous block’s hash and a Merkle root derived from its transactions. The root commits to the block’s contents, while the previous hash links it to earlier history. Changing a transaction changes the root and the header being mined. Descendant blocks must then refer to that changed history through their own links.

Consider a payment included in a block, followed by several more accepted blocks. Replacing that part of the history requires a competing branch with valid linked blocks and sufficient accumulated work, within the node’s reorganization rules. Editing a transaction in a local file does not produce that branch. The relevant work belongs to a specific sequence of headers and transactions.

The [released block code](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go) defines these header fields, Merkle checks and chain-selection paths. The [difficulty guide](/blog/mining-difficulty-explained/) explains how the network determines the work requirement for each new block.

## Why does accumulated work matter more than block count?

Two miners can find different valid blocks extending the same parent. Each proposal has its own hash, and nodes may hear about them in different orders. This can briefly produce competing branches. The network needs a selection rule that follows the work supporting a history rather than the order in which a particular peer advertised it.

Core v0.22.0 computes cumulative work by adding a block’s difficulty to its parent’s cumulative work. After validation, a branch with strictly greater work can trigger a change of the selected chain. An equal-work alternative does not trigger that change through this comparison. Difficulty must itself be valid for the branch’s parent context; a miner cannot simply write a larger number into the header.

For example, compare two branches sharing an earlier prefix. Their shared prefix contributes the same work to both totals. The deciding difference is the work accumulated after they diverge, together with validity and reorganization checks. Counting their blocks without considering difficulty can miss that difference. This is a conceptual comparison, not a report of a live fork.

## How do confirmations help you read a payment’s progress?

Once a payment is included on the selected chain, later blocks build on that history. Its depth grows as those blocks arrive. Replacing its inclusion would require a qualifying alternative history from before the relevant block, while the current branch continues accumulating work. This is why checking inclusion and subsequent depth is more informative than seeing a transaction broadcast.

A height identifies a position; a hash identifies a particular block. When following a payment, check the transaction record and its containing block, then confirm that block remains on the selected chain. If the selected history changes, inclusion and depth can change too. Confirmation counters may include the containing block or count only later blocks, so read the interface’s convention.

Core v0.22.0 also enforces a reorganization boundary: it rejects a switch whose common ancestor is below the current tip height minus 100. This is a rule in the running implementation, not a universal promise attached to a displayed counter. The [transfer-time guide](https://seineminer.com/blog/blocknet-transfer-time/) covers ordinary wallet spendability; [mining-reward confirmations](/blog/block-confirmations-mining-rewards/) covers coinbase maturity and pool accounting.

## How can a private payment be checked for repeated spending?

Blocknet’s released transaction format uses a key image bound to the spending proof. Validation checks that binding and rejects a key image already spent in the relevant chain state. This lets nodes detect conflicting use of an output while the ring signature hides which eligible ring member is the real input. Private amounts still undergo balance and range checks.

Suppose an owner authorizes conflicting transactions spending the same output. A valid accepted history cannot include both spends of that output. Chain selection matters because competing histories can contain different choices; transaction validation matters because each history must obey the spending rules. More work cannot substitute for a valid signature or make an unbalanced transaction pass the accounting check.

These checks appear in the [released transaction validator](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/transaction.go). For the wider privacy mechanisms, use the [Blocknet privacy guide](https://seineminer.com/blog/blocknet-privacy-features/). Payment security combines valid spending, consistent history and the cryptography protecting what that history reveals.

## Where does pool mining fit into payment security?

A pool coordinates miners’ search for a block and submits a solved candidate to its upstream node. A share meeting the pool’s easier target records contributed work; only a solution meeting the network target can become a network block after validation. The [pool explanation](/blog/what-is-a-mining-pool/) separates these two targets and their purposes.

Your miner can therefore contribute to the process that extends payment history while the pool accounts for your work separately. A private pool balance, a share count and a confirmed wallet payment describe different things. Start with the [pool connection guide](/blog/mine-blocknet-on-bntpool/) to participate, and use your wallet’s transaction record when following a particular BNT payment.

<CTA target="pool" title="Contribute work to Blocknet" text="Connect seine to bntpool.com and follow your mining work, rewards and payments through the pool’s public tools." />
