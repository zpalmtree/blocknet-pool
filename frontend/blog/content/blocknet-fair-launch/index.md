---
title: "Blocknet's fair launch: no premine, every coin mined"
description: Check Blocknet’s empty genesis block, first mined reward and issuance rules. Learn how BNT entered circulation and how mined coins reach other holders.
date: 2026-10-25
updated: 2026-10-28
tags: [Blocknet, Fair launch, Genesis block, Mining, bntpool]
faq:
  - q: Did Blocknet have a premine?
    a: No. Blocknet's genesis block, created on February 15, 2026, contains no transactions at all, so no coins existed before mining began. The first coins came from the block reward of block 1, and every BNT since then has been paid to whoever mined the block it came from.
  - q: Does Blocknet have a dev fund or treasury?
    a: The released rules have no protocol-mandated dev fund or treasury allocation. Each post-genesis block has one coinbase output equal to the scheduled reward. This describes new issuance, rather than the identity of later holders. Anyone who owns BNT can make ordinary transfers, including voluntary payments to a developer.
  - q: When was Blocknet's genesis block created?
    a: The genesis block's timestamp is February 15, 2026 at 20:58:38 EST, which is 01:58:38 UTC on February 16. It carries the message "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything", and its previous-block hash is the SHA3-256 hash of that message.
  - q: How can I verify Blocknet's fair launch myself?
    a: Open block 0 on the Blocknet explorer and you will see zero transactions and an all-zero merkle root. Then read the open-source daemon code, where the genesis block is hard-coded with an empty transaction list and the coinbase check accepts one output equal to the block reward.
  - q: How do I start mining Blocknet?
    a: Download the seine miner for Windows, macOS or Linux, run it, and paste your Blocknet address or Blocknet ID shortname when it asks. Press Enter to accept the default pool, bntpool.com, and seine starts mining right away. You can then look up your address on the bntpool.com stats page.
related: [blocknet-block-reward-schedule, how-mining-secures-blocknet-payments, mine-blocknet-on-bntpool]
---

See how BNT entered circulation: an empty genesis block, the first mined reward, and later transfers between holders. This guide checks the launch records and issuance rules, explains what they establish, and shows how to receive newly issued BNT through mining on bntpool.com.

## Did Blocknet have a premine?

<Answer>
No. Blocknet's genesis block, created on February 15, 2026, holds no transactions, so no coins existed before mining began. Since block 1, every block has paid its reward in a single coinbase output worth exactly the scheduled amount, so every BNT in circulation was mined by someone running the proof of work.
</Answer>

The genesis block assigns no reserve to founders, developers, investors or a foundation. Later blocks create the scheduled mining reward, with no additional protocol-mandated treasury output. These are issuance rules: they do not identify who mined a reward or who later received existing coins through a transfer.

## What is in Blocknet's genesis block?

The genesis block is hard-coded in the daemon, so every node starts from the same block 0. Its fields are fixed:

| Field | Value |
|---|---|
| Height | 0 |
| Timestamp | February 15, 2026, 20:58:38 EST (01:58:38 UTC on February 16) |
| Message | "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" |
| Previous hash | SHA3-256 of the message: `a94a7f2b…d909df` |
| Difficulty | 4, the network minimum |
| Nonce | 0 |
| Transactions | none |
| Merkle root | all zeros |

The message is the headline of a Wired story dated February 15, 2026, and the code cites the article as its source. Because the previous hash is derived from the message, the headline is fixed into the identity of block 0 and everything built on it.

The comment above the genesis function in the daemon says it plainly: "Genesis has no transactions - no coinbase, no burned coins. First real block at height 1 will have the first coinbase." Validation backs the comment up. A node checking block 0 rejects it unless it has exactly zero transactions and an all-zero merkle root, so even a modified node could not slip coins into the genesis block without being rejected by everyone else.

## How does consensus rule out a dev fund or treasury?

<Answer>
After genesis, each block starts with one coinbase transaction with no inputs, a zero fee and exactly one output equal to the scheduled reward. The released rules provide no separate founder or treasury output in that transaction. Holders can still make ordinary transfers from BNT they already own.
</Answer>

Several checks in the daemon work together here:

- **One coinbase per block.** The first transaction must be a coinbase, and no other transaction in the block may be one.
- **One output, no inputs.** A coinbase with zero or two outputs, or with an input, is invalid.
- **The exact reward.** Blocknet hides amounts with Pedersen commitments, but the coinbase is a special case. Its blinding factor is derived from public data (the transaction's public key and the block height), so every node can rebuild the commitment for the scheduled reward and compare. If the output commits to any other amount, the block is rejected.
- **An empty memo.** The coinbase memo must be empty, so block rewards can't carry extra data either.

Ordinary transactions can't create coins. Each one must balance: the inputs equal the outputs plus the fee, checked on the commitments themselves, and every output carries a Bulletproofs range proof showing its amount is in a valid range and can't be negative. That is how amounts stay private while issuance still follows the schedule exactly.

## How can you check Blocknet's fair launch yourself?

You don't have to take anyone's word for it. Both the chain and the code are public.

**On the explorer.** Open [block 0 on the Blocknet explorer](https://explorer.blocknetcrypto.com/block/0). It lists 0 transactions, a merkle root of 64 zeros, a nonce of 0 and a previous hash beginning `a94a7f2b`. The block reward line shows the schedule's amount for that height, but with no transactions in the block there is no output to receive it. Then open [block 1](https://explorer.blocknetcrypto.com/block/1): its only transaction is a coinbase with no inputs and one output, the first coins on the chain.

You can confirm that the previous hash really is the hash of the genesis message. With OpenSSL 1.1.1 or newer, run:

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

The output ends in `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`, the same value the explorer shows for block 0.

**In the code.** The daemon is open source at [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core). In [`block.go`](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go), look for these functions:

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock` builds block 0 with an empty transaction list, `validateGenesisBlock` rejects any block 0 that has transactions, and `validateCoinbaseConsensus` enforces the single output worth the block reward. In `transaction.go`, `validateCoinbase` and `verifyCommitmentBalance` cover the coinbase shape and the balance check for ordinary transactions.

## How did newly mined BNT reach other holders?

The explorer records the genesis timestamp as February 15, 2026 at 20:58:38 EST. Block 1 carries February 20, 2026 at 14:29:20 EST and contains the first coinbase transaction. These are separate records: genesis defines the common starting block, while block 1 begins paid issuance. The reward shown for the empty genesis block on an explorer displays the schedule amount; no output receives it.

After a mining reward becomes spendable, its owner can transfer BNT to someone else. A pool similarly sends later payments from rewards it has received. Those transfers redistribute existing coins; they do not repeat issuance. You can therefore hold and use BNT without personally mining the block that originally created it. The [send-and-receive guide](https://seineminer.com/blog/send-receive-blocknet/) covers ordinary payments, while the [supply guide](https://seineminer.com/blog/blocknet-supply/) explains the currency-wide schedule.

## Why does a fair launch matter to miners?

For miners, a fair launch means the block reward is the whole story of where coins come from.

- **All new coins go to miners.** With no premine and no treasury output, 100% of issuance is paid to the people who find blocks. Block 1 paid 723.25093035 BNT in its single coinbase output, and every block since has paid its full reward the same way.
- **Everyone plays by the same rules.** Since block 1, the only way to earn new BNT has been to run the proof of work. The [reward schedule](/blog/blocknet-block-reward-schedule/) is set in code and applies equally to the first miner and to someone starting today.
- **The proof of work suits ordinary computers.** Blocknet uses Argon2id with 2 GiB of memory per hash, which is designed to resist ASICs. That keeps mining open to desktops, laptops and servers people already own. The [Argon2id explainer on seineminer.com](https://seineminer.com/blog/argon2id-proof-of-work-explained/) covers why.
- **Issuance is easy to audit.** Every coin traces back to a coinbase output that every node checked, so the total ever issued at any height follows directly from the reward schedule.

If you are new to the coin as a whole, [What is Blocknet?](https://seineminer.com/blog/what-is-blocknet/) covers privacy, payment by name and the wallets.

## How do you mine Blocknet yourself?

To receive newly issued BNT through mining, use the seine miner with bntpool.com:

1. **Get an address.** Install the official Blocknet [command-line wallet](https://github.com/blocknetprivacy/blocknet/releases/latest) or [desktop wallet](https://github.com/blocknetprivacy/blocknet-gui/releases/latest) and copy your address, or use a Blocknet ID shortname such as `@name`.
2. **Download seine.** Get the archive for Windows, macOS or Linux from [seineminer.com](https://seineminer.com/#download) and unzip it. There is nothing to install.
3. **Run it and accept the defaults.** Start seine, paste your address or `@name`, and press Enter to accept the default pool, `stratum+tcp://bntpool.com:3333`. Seine starts hashing right away and tunes threads and kernels while it runs, on your CPU and on NVIDIA graphics cards on Windows and Linux.
4. **Watch your miner.** Look up your address on the [stats page](/stats) to see your hashrate, shares and balance, and follow the pool's finds on the [blocks page](/blocks).

The [guide to mining on bntpool.com](/blog/mine-blocknet-on-bntpool/) covers logging in with an address or name and checking your miner, the [Get Started page](/start) lists the pool's payout settings, and the [how to mine Blocknet guide](https://seineminer.com/blog/how-to-mine-blocknet/) walks through installing seine step by step on every system.

<CTA target="seine" title="Mine your share" text="Every BNT is mined. Download seine and start mining on bntpool.com in a few minutes." />
