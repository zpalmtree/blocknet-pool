---
title: Mine Blocknet straight to an exchange
description: 'Mine Blocknet straight to your SafeTrade or other exchange deposit address on bntpool.com. Add your deposit memo in seine and the pool pays to your account.'
date: 2026-11-04
tags: [Mining guide, bntpool, SafeTrade, Payouts]
faq:
  - q: 'Can I mine Blocknet directly to an exchange?'
    a: 'Yes. Start seine v0.3.1 or newer with your exchange deposit address and your deposit memo, as in ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. bntpool.com pays your rewards to that address with the memo attached, so the exchange credits them to your account.'
  - q: 'Why does seine ask for a payout memo?'
    a: 'Exchanges such as SafeTrade use one shared deposit address for all their customers and tell deposits apart by the memo. Seine knows these shared addresses, so when you enter one it asks for your memo and saves it with the address. Copy the memo exactly from the exchange deposit page.'
  - q: 'What is the minimum payout to an exchange on bntpool.com?'
    a: 'Payouts with a memo start at 10 BNT, as reported by the pool on November 4, 2026. Your confirmed balance builds up on the pool until it reaches that amount, then the pool sends it to the exchange address with your memo. Payouts to a personal wallet start at 1 BNT.'
  - q: 'How do I check my exchange mining balance?'
    a: 'On the bntpool.com stats page, enter the exchange deposit address, choose Add memo and enter your memo. Each address and memo pair has its own balance, so you see only your own hashrate, rigs and payouts, not those of other customers who use the same exchange address.'
related: [sell-blocknet-bnt-safetrade, mine-blocknet-on-bntpool, change-mining-wallet-address, how-pplns-payouts-work]
---

This guide shows you how to mine Blocknet straight to your exchange account on bntpool.com. You give seine the exchange's deposit address and your deposit memo, and the pool pays your rewards there with the memo attached, so the exchange credits them to you. It covers the command, the minimum payout and how to check your balance.

## Can you mine Blocknet directly to an exchange?

<Answer>
Yes. bntpool.com pays mining rewards to an exchange deposit address with your deposit memo attached. Start seine v0.3.1 or newer with the exchange address and your memo: ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. Payouts with a memo start at 10 BNT, and the exchange credits each one to your account.
</Answer>

Mining to an exchange saves a step when you plan to trade what you mine. Rewards arrive in your exchange account directly, ready to sell on the [BNT/USDT market](/blog/sell-blocknet-bnt-safetrade/), with no wallet transfer in between.

Exchanges such as SafeTrade give all their customers the same deposit address and tell deposits apart by a memo, a short code unique to your account. That is why the memo matters: it is what links a payout to you. The pool keeps a separate balance for each address and memo pair, so your rewards are never mixed with those of other miners who use the same exchange.

## How do you set up exchange mining in seine?

<Answer>
Copy the BNT deposit address and your deposit memo from the exchange's deposit page. Start seine with both: --address for the address and --payout-memo for the memo. When you enter a known exchange address at the first-run prompt, seine asks for the memo by itself and saves it. Check the log for the pool's confirmation, then mine as usual.
</Answer>

1. **Get your deposit details.** On the exchange, open the BNT deposit page and copy the deposit address and the memo. Copy the memo exactly: capital letters and leading zeros matter.
2. **Start seine with both.** On macOS or Linux:

   ```sh
   ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

   On Windows, in PowerShell:

   ```powershell
   .\seine.exe --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

3. **Let seine save it.** Seine saves the memo with the address, so later runs keep paying to your exchange account. If you type a known exchange address at the first-run prompt, seine asks for the memo and saves both.

The pool's [Get Started page](/start) has a "Mining to an exchange?" section that builds this command for you: paste the address and memo, and copy the result. Seine needs a memo for known shared exchange addresses such as SafeTrade's. Services, scripts and other unattended miners must pass `--payout-memo` on the command line, because there is no prompt to answer.

Seine checks the memo with the pool when it logs in. bntpool.com confirms it, and then mining starts. To name your rigs, pass `--pool-worker rig-01` as you would with a personal address.

## How are exchange payouts paid?

Rewards follow the same path as any bntpool.com payout. Your shares earn a portion of each block the pool finds under its six-hour PPLNS window, and each block's reward is confirmed after 60 blocks. Your confirmed balance then builds up on the pool.

The pool's `/api/info` endpoint reported these payout minimums on November 4, 2026:

| Destination | Minimum payout |
|---|---|
| Exchange address with a memo | 10 BNT |
| Personal wallet | 1 BNT |

Once your confirmed balance reaches 10 BNT, the pool sends it to the exchange address with your memo. The exchange then credits the deposit after its own confirmation count; its deposit page lists how many it needs. [How PPLNS payouts work](/blog/how-pplns-payouts-work/) explains the pool side in detail.

Solo mining works with a memo too. Put `solo:` in front of the exchange address and keep `--payout-memo`, and the pool pays the whole reward of each block you find to your exchange account. [Solo mining on bntpool.com](/blog/solo-mine-blocknet-on-bntpool/) explains solo rewards.

## How do you check your balance?

Open the [stats page](/stats), paste the exchange deposit address, choose **Add memo** and enter your memo. The page then shows your own hashrate, rigs, balance and payouts for that address and memo pair. Other customers of the same exchange have their own pairs and their own balances.

Each payout in **Recent Payouts** links to its transaction on the explorer. Compare it with the deposit history on the exchange to follow the coins into your account. [Track your miner on bntpool.com](/blog/track-your-miner-on-bntpool/) explains the rest of the stats page.

## How do you switch back to a personal wallet?

Start seine once with your new address and `--no-payout-memo`:

```sh
./seine --address YOUR_BLOCKNET_ADDRESS --no-payout-memo
```

Seine saves the new address without a memo, and new shares count toward your personal wallet. A balance already credited to the exchange pair stays with that pair and is paid to it once it reaches the minimum; you can follow it on the stats page. [Change your mining wallet address](/blog/change-mining-wallet-address/) explains how the pool credits work after an address change.

## Which setups support memos?

Memos need seine's direct connection to the pool, which is the default. Seine's daemon mode mines against your own node and pays the node's wallet, so it has no memo. The farm proxy doesn't carry memos, so connect exchange rigs to the pool directly. Seine mines only on a pool that confirms your memo, so your work is always paid to the account you chose.

<CTA target="pool" title="Mine to your exchange account" text="Start seine with your deposit address and memo, and bntpool.com pays your rewards straight to your exchange account from 10 BNT." />
