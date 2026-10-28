---
title: Track your miner on bntpool.com
description: Check your first accepted shares on bntpool.com, find your machine, compare hashrate, and follow estimated rewards through confirmation to your first payout.
date: 2026-10-25
updated: 2026-10-28
tags: [bntpool, Mining stats, Pool API, Blocknet]
faq:
  - q: Why is my hashrate on bntpool.com different from what seine shows?
    a: Seine counts every hash your machine computes, while the pool estimates your hashrate from the difficulty of the shares it accepts, averaged over the last hour. Shares arrive at random moments, so the pool figure moves around more and settles over time. Compare the 24-hour chart on My Stats with seine's average for a fair match.
  - q: Can I look up my bntpool.com stats with an @name?
    a: Yes. Type your Blocknet ID shortname, such as @name or $name, into the My Stats search box. The page resolves it through blocknet.id, shows your stats, and puts the full address it resolved to in the search box. Use that full address when you call the pool API from a script.
  - q: Does the bntpool.com API need an API key?
    a: No. The pool stats, blocks, payouts, luck, status and per-miner endpoints are public JSON over HTTPS, so a curl command or a home dashboard can read them without signing up. Amounts come in atomic units, so divide by 100,000,000 to get BNT, and poll every 30 to 60 seconds.
  - q: Why does My Stats show zero Estimated Rewards?
    a: Estimated Rewards appear once the pool finds a block while your shares are inside the payout window. For a new miner that can take a few rounds of mining. Your accepted shares and hashrate show up first, then estimated rewards, which mature into Confirmed Rewards and are paid out automatically.
related: [internet-speed-mining, pool-payout-missing-from-wallet, mine-multiple-computers-one-wallet, mine-blocknet-on-bntpool]
---

After your first accepted share, open My Stats to check that the pool is receiving your work. Follow your machine’s row, compare hashrate over time, then watch estimated rewards become confirmed rewards and a payment. Scripts and the public API come last.

If you haven't started mining yet, the [seine install guide](https://seineminer.com/blog/how-to-mine-blocknet/) gets you to your first shares in a few minutes.

## How do you check your miner's stats on bntpool.com?

<Answer>
Open My Stats at bntpool.com/stats, paste your Blocknet address or your @name, and press Lookup. The page shows your hashrate, accepted and rejected shares, estimated, confirmed and paid balances, a hashrate chart, one row per mining rig and your recent payouts. It keeps refreshing on its own while the tab is open.
</Answer>

Go to [My Stats](/stats) and enter the same address or [Blocknet ID shortname](https://seineminer.com/blog/blocknet-id-pay-by-name/) you gave seine. A name like `@name` or `$name` is resolved through blocknet.id, and the search box then shows the full address it points to. The page remembers your last lookup in that browser, so next time your numbers are already there.

1. Check **Shares Accepted** and **Recent Shares**. Accepted shares confirm that your work reached the pool, even when all reward balances are still zero.
2. Find your machine under **Workers**. Match its name to the one in Seine and check that its last-share time keeps updating.
3. A **Blocks Found** count of zero is normal: it counts blocks your own machines solved, not whether your shares earn a portion of the pool’s rewards.

<figure class="tracking-figure">
<picture><source media="(max-width: 640px)" srcset="/blog/tracking/tracking-first-mobile.webp" width="352" height="379"><img src="/blog/tracking/tracking-first-desktop.webp" width="1180" height="900" alt="Accepted shares are the first check. Real English interface with example data, not live earnings." loading="lazy" decoding="async"></picture>
<figcaption>Accepted shares are the first check. Real English interface with example data, not live earnings.</figcaption>
</figure>

## What does the My Stats page show?

The page is split into groups of cards and tables, each answering one question about your miner.

### Mining and shares

The **Mining** cards show your current **Hashrate**, how many **Blocks Found** your own rigs solved for the pool, and the date you started (**Mining Since**). Below them are **Shares Accepted**, **Shares Rejected**, **Reject Rate** and **Avg Difficulty**. A healthy rig shows a steady stream of accepted shares and a reject rate close to zero.

**Hashrate History** charts your hashrate over 1h, 24h, 7d or 30d. The longer ranges are the best way to see whether a rig has been running all night.

### One row per rig

If you run more than one machine, give each one its own name and the pool lists them separately, with hashrate, accepted and rejected shares and the time of the last share. Seine picks a name on its first run, `seine-` followed by four digits, and you can set your own on the command line:

```sh
./seine --pool-worker rig-01
```

The **Workers** table also shows each machine’s miner version. An update notice above your stats links to the [seine releases page](https://github.com/zpalmtree/seine/releases) when a version is old or unknown.

## Why does the pool's hashrate differ from seine's?

Seine counts every hash your machine computes. The pool can only see the shares you submit, so it estimates your hashrate from the total difficulty of your accepted shares over the last hour. Each share is a random event, and bntpool.com sets a per-connection difficulty that aims for about 10 shares every 5 minutes, so a short window carries some noise.

Two habits make the numbers line up:

- Compare averages: the 24h chart on My Stats against seine's average over the same stretch.
- Give a new rig time. For a rig that just started, the pool averages over the minutes it has been mining, so the first reading rests on a handful of shares. After an hour the full one-hour window applies.

The Recent Shares table tells you right away that shares are arriving, even before the hashrate figure settles.

## When should you expect your first payout?

There is no fixed countdown from your first share to your first payment. The pool must find a block while your shares are in its payout window. That block then needs 60 confirmations: roughly five hours on average **after the block is found**, with actual times varying. Once your confirmed balance reaches the current 0.1 BNT minimum, it is eligible for an automatic payout sweep. Check [Get Started](/start) for current settings.

Follow **Recent Payouts** for the payment’s status and transaction link. A queued payment has not been broadcast yet. If a sent payment is missing from your wallet, use the [payout troubleshooting guide](/blog/pool-payout-missing-from-wallet/).

### Balance

- **Estimated Rewards:** your share of blocks the pool has found that are still maturing.
- **Confirmed Rewards:** matured balance waiting for the next payout.
- **Paid Balance:** everything already sent to your address.

When you have rewards in flight, a table called **Estimated From Recent Blocks** lists each block with its height, your estimated credit, how many confirmations are left and when it was found. Each height links to the block on the [Blocknet explorer](https://explorer.blocknetcrypto.com), so you can check it yourself. [How PPLNS payouts work](/blog/how-pplns-payouts-work/) explains how each block's reward is split and when your balance is paid.

<figure class="tracking-figure">
<picture><source media="(max-width: 640px)" srcset="/blog/tracking/tracking-rewards-mobile.webp" width="352" height="524"><img src="/blog/tracking/tracking-rewards-desktop.webp" width="1081" height="217" alt="Estimated, confirmed and paid balances are separate stages. Real English interface with example amounts, not expected earnings." loading="lazy" decoding="async"></picture>
<figcaption>Estimated, confirmed and paid balances are separate stages. Real English interface with example amounts, not expected earnings.</figcaption>
</figure>

### Payouts and recent shares

**Recent Payouts** lists each payment to your address with its amount, a link to the transaction on the explorer, and a status of queued, unconfirmed or confirmed. **Recent Shares** shows your latest 20 shares with their job, rig, difficulty and status, which is the quickest way to confirm that a machine you just started is reaching the pool.

<span id="zero-hashrate"></span>

## Why is seine mining while My Stats shows zero hashrate?

Start with the payout address, pool connection and accepted shares. A local hashrate shows that the machine is hashing; the pool estimate needs accepted shares for the address you looked up.

1. **Match the address.** Look up the same receiving address or registered `@name` that seine uses. If you changed a shortname's address, restart seine so it resolves the name again, then check the resolved address on My Stats.
2. **Match the pool.** Check that seine uses `stratum+tcp://bntpool.com:3333`. A rig mining elsewhere or in solo mode will not add shares here. The [pool setup guide](/blog/mine-blocknet-on-bntpool/) walks through the connection.
3. **Check Recent Shares.** Wait for accepted shares, then let the page refresh. Shares arrive at variable intervals; a running local hashrate does not promise an immediate pool estimate. If accepted shares are arriving, compare averages over time.
4. **Read the connection message.** Use the table below when the miner has not reached accepted shares. Check [Status](/status) for any current incident.

| Message in seine | What to check next |
|---|---|
| `failed to connect to pool at` | Verify the hostname and port, your connection, and whether outbound TCP to port 3333 is allowed. |
| `login rejected:` | Read the reason that follows; check the receiving address or shortname and use the current seine release. |
| `stale job` | The share refers to an old assignment. Watch for new jobs and accepted shares; if it continues, check the connection and miner version. |
| `submit interrupted during reconnect` | Seine is reconnecting. Watch for login acceptance and fresh accepted shares after the connection returns. |

Zero **Estimated Rewards** is a separate question: accepted shares can arrive before a pool block credits rewards. Follow the [PPLNS guide](/blog/how-pplns-payouts-work/) for that sequence.

## Which public pages show the whole pool?

These pages need no lookup and show everything the pool does in the open. The [dashboard](/) gathers the headline numbers: connected miners, pool and network hashrate, blocks found and average luck.

### Blocks

[Blocks](/blocks) lists every block bntpool.com has found, newest first: height (linked to the explorer), reward, effort, round time, status and time found. Filter by Confirmed, Pending or Orphaned to see where each block stands.

### Payouts

[Payouts](/payouts) lists each payout batch with its total amount, the number of miners paid, the transaction links and a status. A batch appears as unconfirmed as soon as it is broadcast and turns confirmed shortly after its first block confirmation, so you can follow your own payment from the pool's side and on the explorer.

### Luck

[Luck](/luck) shows the effort of each round. Effort is the share work the pool accepted during the round divided by the block's difficulty: 100% means the block took exactly the expected amount of work, and lower means the pool found it sooner. Single rounds swing widely in both directions, so the dashboard's **Average Luck** is the figure to watch. The dashboard's **Pool Luck History** table links here through **View All**.

### Status

[Status](/status) is the pool's own health page. It shows whether the pool, public HTTP, API, Stratum, database and daemon are online, how fresh the current job template is, the chain height, uptime over the last 10 minutes, 6 hours, 24 hours and 7 days, and a history of incidents. Uptime is measured twice: by a monitor on the pool server and by an outside probe running on Cloudflare.

## What about seine's own dashboard?

The pool shows what it has credited to you. For a live view of the machine itself, seine's terminal dashboard shows hashrate and shares as they happen, and seine can also expose a local control API with REST, a server-sent event stream and Prometheus metrics at `/metrics`, bound to `127.0.0.1:9977` by default. The [control API guide](https://github.com/zpalmtree/seine/blob/master/docs/API.md) covers the flags and endpoints. Many miners use both: seine's metrics for each machine and the pool API for balances and payouts.

## How do you use the bntpool.com API?

<Answer>
The bntpool.com API is public JSON over HTTPS and needs no key. Call https://bntpool.com/api/miner/ followed by your address for your hashrate, rigs, shares and payouts, add /balance for your balances, and use /api/stats for pool and network hashrate. Amounts are atomic units: divide by 100,000,000 for BNT.
</Answer>

Every page above reads from these endpoints, so anything you see on the site you can fetch yourself:

| Endpoint | What it returns |
|---|---|
| `/api/stats` | Connected miners, pool hashrate, blocks found, network hashrate, current height |
| `/api/stats/history?range=24h` | Pool hashrate over time (`1h`, `24h`, `7d`, `30d`) |
| `/api/stats/insights` | Current round, average effort, recent rounds |
| `/api/info` | Pool settings: Stratum port, payout scheme and window, minimum payout |
| `/api/blocks?status=confirmed` | Found blocks, filterable by status |
| `/api/payouts/recent` | Payout batches with transaction hashes |
| `/api/luck` | Effort and duration of each round |
| `/api/status` | Service health, uptime windows, incidents |
| `/api/miner/{address}` | Your hashrate, totals, rigs, latest shares, payouts |
| `/api/miner/{address}/balance` | Estimated, confirmed and paid balances |
| `/api/miner/{address}/hashrate?range=24h` | Your hashrate history |

The list endpoints take `limit` (25 by default, up to 200) and `offset`, and answer with `items` and a `total`. The miner endpoints take a full address, so after an @name lookup on My Stats, copy the full address from the search box.

To read your miner from a terminal:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

With `jq` you can pull out single values, for example your hashrate and accepted shares, or your paid total in BNT:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

<CTA target="pool" title="Start mining and track it live" text="Join bntpool.com in a few minutes, then follow your hashrate, shares and balance on My Stats." />
