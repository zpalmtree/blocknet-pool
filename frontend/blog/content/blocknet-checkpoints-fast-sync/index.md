---
title: Sync a Blocknet node fast with bntpool.com checkpoints
description: Blocknet nodes fast-sync from bntpool.com's checkpoints.dat. See how checkpoints pin the chain, then download, verify and load the file in a few steps.
date: 2026-10-25
tags: [Blocknet node, Checkpoints, Fast sync, bntpool]
faq:
  - q: Where does a Blocknet node get its checkpoints?
    a: From bntpool.com. On first start, if the data directory has no checkpoints.dat, the Blocknet daemon downloads it from https://bntpool.com/checkpoints.dat and uses it for fast sync. You can also download the file yourself from the Checkpoints page, check its SHA-256 and place it in the data directory before you start the node.
  - q: How often does bntpool.com update checkpoints.dat?
    a: The pool's daemon rewrites the file as the chain grows, with one entry for every 100th block. Each entry is at least 100 blocks below the chain tip, past Blocknet's maximum reorganization depth, so a checkpoint only ever covers settled history. The Checkpoints page shows the latest height and when the file last changed.
  - q: Do checkpoints change my wallet balance or history?
    a: No. A node syncing with checkpoints still stores every block and records every output and key image in its indexes, so wallet scanning and balances work exactly as they do after a full sync. Checkpoints only let the node skip re-checking proof-of-work and signatures for history the checkpoint hashes already vouch for.
  - q: Can I sync a Blocknet node without checkpoints?
    a: Yes. Start the daemon with the --full-sync flag, or set full_sync to true in the blocknet config, and the node skips the checkpoint download and re-verifies proof-of-work and signatures for every block from genesis. It takes longer than a checkpoint sync, which is why checkpoints are the default for new nodes.
related: [pool-vs-solo-mining-blocknet, mine-blocknet-on-bntpool, track-your-miner-on-bntpool]
---

A new Blocknet node has to catch up with every block since genesis before it can serve a wallet or a solo miner. Checkpoints make that catch-up fast. This guide explains what bntpool.com's checkpoint file contains, how the Blocknet daemon uses it to skip the slowest checks while staying pinned to the real chain, and how to download, verify and load the file yourself.

## What are Blocknet checkpoints?

<Answer>
Blocknet checkpoints are a list of known block hashes, one for every 100th block, served by bntpool.com as checkpoints.dat. A syncing node checks that the blocks it receives match those hashes and skips re-verifying proof-of-work and signatures up to the last checkpoint, so the first sync finishes much faster.
</Answer>

The file is plain text with one `height:hash` pair per line. The first lines of the file on 2026-10-25 looked like this:

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

Each hash is the block's ID, the SHA3-256 hash of its header. The pool's own daemon writes the file from its chain and only includes blocks that are at least 100 blocks deep. A hundred blocks is Blocknet's hard finality, the deepest reorganization the protocol allows, so every checkpoint sits in settled history.

On 2026-10-25 at 02:04 EDT the file held 689 checkpoints, from height 100 up to height 68,900, in 48,811 bytes. It grows by one line roughly every 100 blocks, about every eight hours at five-minute blocks, so check the [Checkpoints page](/checkpoints) for the current height.

## How does a Blocknet node use checkpoints?

A node that has loaded checkpoints treats the last checkpoint height as its fast-sync target. Until its chain reaches that height, it relies on the published checkpoints for the two most expensive checks and runs every other check as normal:

- **Proof-of-work rests on the checkpoints.** Every Blocknet block's proof-of-work is an Argon2id hash over 2 GiB of memory. Recomputing one for every historical block is expensive, and the checkpoint hashes already vouch for that history.
- **RingCT signatures and range proofs rest on them too.** The daemon's own comments call output range-proof verification the single largest cost of a sync, because every output carries one, including each block's coinbase.
- **Every other check still runs.** Block linkage, the LWMA difficulty rule, timestamp rules, block size and shape, double-spend checks on key images, and the binding checks between each input and its signature all run as normal. Outputs and key images go into the node's indexes as usual, so wallet scanning and balances are the same as after a full sync.

Skipping applies only to blocks that extend the node's current tip, and only below the last checkpoint. Once the node passes that height it verifies every new block in full, proof-of-work included.

### How checkpoints pin the trusted chain

While a node is syncing toward the last checkpoint, any block at a checkpoint height must have exactly the hash in the file. A block with a different hash is rejected with a checkpoint mismatch error. Because checkpoints come every 100 blocks, a peer can't lead a fast-syncing node down a different history: the next checkpoint height would expose it.

Checkpoints also speed up every restart. When the daemon starts with blocks already on disk, it finds the highest checkpoint that matches its stored chain and re-checks only the blocks after it, instead of walking the whole chain from genesis.

## Does a new node download checkpoints automatically?

<Answer>
Yes. When the Blocknet daemon starts and its data directory has no checkpoints.dat, it downloads the file from https://bntpool.com/checkpoints.dat, loads it and fast-syncs up to the last checkpoint. The download is best effort: if it can't complete, the node starts anyway and verifies the chain in full.
</Answer>

That address is the default written into the Blocknet core itself, so out of the box every new node fast-syncs from the file bntpool.com publishes. The daemon only fetches the file when none exists, gives the request 15 seconds, and caps the download at 32 MiB.

## How do you use bntpool.com checkpoints step by step?

If you'd rather fetch and check the file yourself, or you want to refresh an older copy before a long resync, these steps do it by hand.

### 1. Find your data directory

The checkpoint file lives in the node's data directory, next to the chain database:

- **The `blocknet` manager:** `~/.config/bnt/data/mainnet/` on macOS and Linux, `C:\Users\<you>\.config\bnt\data\mainnet\` on Windows, unless you set `data_dir` in its config.
- **The core run directly:** `./blocknet-data-mainnet` in the folder you start it from, or whatever you pass with `--data`.

### 2. Download checkpoints.dat

Open [Checkpoints](/checkpoints) and press **Download checkpoints.dat**, or fetch it from a terminal. This example writes into the core's default data directory:

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

Downloading over an existing file replaces it, which is how you refresh an old copy.

### 3. Check the SHA-256

The Checkpoints page publishes the SHA-256 of the current file. Hash your copy and compare:

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

On macOS, use `shasum -a 256` instead. The hash changes every time the pool adds a checkpoint, so compare against the page right after you download.

### 4. Spot-check a checkpoint on the explorer

You can confirm any line against the public [Blocknet explorer](https://explorer.blocknetcrypto.com). Open the block page for a height in the file, for example `explorer.blocknetcrypto.com/block/68900`, and compare its hash with the line for 68,900. We did this for the last checkpoint on 2026-10-25 and the hashes matched.

### 5. Start the node

Start or restart the daemon. It reads `checkpoints.dat` at startup and fast-syncs up to the last checkpoint. If you run the core with its interactive shell (`--cli`), the `load-checkpoints` command loads the file into a running node and reports the fast-sync height:

```text
> load-checkpoints
```

Checkpoints help while a node is behind. Once your chain is past the last checkpoint height, the node is already verifying every block in full and a newer file changes nothing.

## How can you check the checkpoint file from a script?

The pool publishes the file's details as JSON at `https://bntpool.com/api/checkpoints`, with no API key:

| Field | What it means |
|---|---|
| `available` | Whether the pool has a file to serve |
| `url` | Where to download it |
| `entries` | Number of checkpoints in the file |
| `latest_height`, `latest_hash` | The highest checkpoint and its block hash |
| `bytes`, `sha256` | File size and SHA-256 |
| `updated_at` | When the file last changed, as a Unix timestamp |

The download itself carries `X-Checkpoint-Height` and `X-Checkpoint-Sha256` headers and an `ETag` built from the hash, so a script can send `If-None-Match` and get a quick `304 Not Modified` when nothing has changed:

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

Responses may be cached for 60 seconds, which is plenty for a file that changes about every eight hours.

<Note>
blocknetcrypto.com says the planned Faraday upgrade will bring a lighter proof of work so that new nodes catch up with the chain faster. Until then, checkpoints are the quick way to bring a node up to date.
</Note>

## What can you do with a synced node?

A synced node backs the Blocknet wallet and the node API, and it is what seine talks to when you solo mine with `--mode daemon`. [Pool vs solo mining for Blocknet](/blog/pool-vs-solo-mining-blocknet/) shows how to tell whether solo mining fits your hardware. If you'd rather start mining without running a node at all, seine mines on bntpool.com by default: the [seine install guide](https://seineminer.com/blog/how-to-mine-blocknet/) gets you to your first shares, and [mining on bntpool.com](/blog/mine-blocknet-on-bntpool/) covers the pool side.

<CTA target="pool" title="Mine on the pool that serves Blocknet checkpoints" text="Join bntpool.com in a few minutes and follow your hashrate, shares and payouts live." />
