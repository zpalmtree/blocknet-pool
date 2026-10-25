---
title: Track your miner on bntpool.com
description: Look up your address or @name on bntpool.com to see hashrate, shares, balance and payouts, then follow blocks, luck and uptime or script the public API.
date: 2026-10-25
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
related: [mine-blocknet-on-bntpool, how-pplns-payouts-work, blocknet-block-reward-schedule]
---

Once seine is mining on bntpool.com, the pool shows you everything it sees: your hashrate, every accepted share, what you have earned and every payout sent to your address. This guide walks through the My Stats page, the public pages for the whole pool, and the public API you can use from scripts and home dashboards.

If you haven't started mining yet, the [seine install guide](https://seineminer.com/blog/how-to-mine-blocknet/) gets you to your first shares in a few minutes.

## How do you check your miner's stats on bntpool.com?

<Answer>
Open My Stats at bntpool.com/stats, paste your Blocknet address or your @name, and press Lookup. The page shows your hashrate, accepted and rejected shares, estimated, confirmed and paid balances, a hashrate chart, one row per mining rig and your recent payouts. It keeps refreshing on its own while the tab is open.
</Answer>

Go to [My Stats](/stats) and enter the same address or [Blocknet ID shortname](https://seineminer.com/blog/blocknet-id-pay-by-name/) you gave seine. A name like `@name` or `$name` is resolved through blocknet.id, and the search box then shows the full address it points to. The page remembers your last lookup in that browser, so next time your numbers are already there.

## What does the My Stats page show?

The page is split into groups of cards and tables, each answering one question about your miner.

### Balance

- **Estimated Rewards:** your share of blocks the pool has found that are still maturing.
- **Confirmed Rewards:** matured balance waiting for the next payout.
- **Paid Balance:** everything already sent to your address.

When you have rewards in flight, a table called **Estimated From Recent Blocks** lists each block with its height, your estimated credit, how many confirmations are left and when it was found. Each height links to the block on the [Blocknet explorer](https://explorer.blocknetcrypto.com), so you can check it yourself. [How PPLNS payouts work](/blog/how-pplns-payouts-work/) explains how each block's reward is split and when your balance is paid.

### Mining and shares

The **Mining** cards show your current **Hashrate**, how many **Blocks Found** your own rigs solved for the pool, and the date you started (**Mining Since**). Below them are **Shares Accepted**, **Shares Rejected**, **Reject Rate** and **Avg Difficulty**. A healthy rig shows a steady stream of accepted shares and a reject rate close to zero.

**Hashrate History** charts your hashrate over 1h, 24h, 7d or 30d. The longer ranges are the best way to see whether a rig has been running all night.

### One row per rig

If you run more than one machine, give each one its own name and the pool lists them separately, with hashrate, accepted and rejected shares and the time of the last share. Seine picks a name on its first run, `seine-` followed by four digits, and you can set your own on the command line:

```sh
./seine --pool-worker rig-01
```

A rig running an older seine gets an **update available** badge that links to the [seine releases page](https://github.com/zpalmtree/seine/releases), which makes it easy to spot a machine you forgot to upgrade.

### Payouts and recent shares

**Recent Payouts** lists each payment to your address with its amount, a link to the transaction on the explorer, and a status of queued, unconfirmed or confirmed. **Recent Shares** shows your latest 20 shares with their job, rig, difficulty and status, which is the quickest way to confirm that a machine you just started is reaching the pool.

## Why does the pool's hashrate differ from seine's?

Seine counts every hash your machine computes. The pool can only see the shares you submit, so it estimates your hashrate from the total difficulty of your accepted shares over the last hour. Each share is a random event, and bntpool.com sets a per-connection difficulty that aims for about 10 shares every 5 minutes, so a short window carries some noise.

Two habits make the numbers line up:

- Compare averages: the 24h chart on My Stats against seine's average over the same stretch.
- Give a new rig time. For a rig that just started, the pool averages over the minutes it has been mining, so the first reading rests on a handful of shares. After an hour the full one-hour window applies.

The Recent Shares table tells you right away that shares are arriving, even before the hashrate figure settles.

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

### Show your miner on a home dashboard

The same endpoint works as a sensor in a home dashboard. In Home Assistant, the RESTful integration reads it with a few lines of `configuration.yaml`:

```yaml
rest:
  - resource: https://bntpool.com/api/miner/YOUR_ADDRESS
    scan_interval: 60
    sensor:
      - name: "bntpool hashrate"
        value_template: "{{ value_json.hashrate | round(2) }}"
        unit_of_measurement: "H/s"
      - name: "bntpool accepted shares"
        value_template: "{{ value_json.total_accepted }}"
```

Polling every 30 to 60 seconds is plenty: the hashrate figure is an hourly average, and My Stats itself refreshes your miner details every 30 seconds. The public API is rate-limited per client. If a script gets an HTTP 429 response, it should back off and wait for the time in the `Retry-After` header before it tries again.

## What about seine's own dashboard?

The pool shows what it has credited to you. For a live view of the machine itself, seine's terminal dashboard shows hashrate and shares as they happen, and seine can also expose a local control API with REST, a server-sent event stream and Prometheus metrics at `/metrics`, bound to `127.0.0.1:9977` by default. The [control API guide](https://github.com/zpalmtree/seine/blob/master/docs/API.md) covers the flags and endpoints. Many miners use both: seine's metrics for each machine and the pool API for balances and payouts.

New to seine? [Download it](https://seineminer.com/#download), point it at the default pool, and your address will show up on My Stats within minutes of your first accepted share.

<CTA target="pool" title="Start mining and track it live" text="Join bntpool.com in a few minutes, then follow your hashrate, shares and balance on My Stats." />
