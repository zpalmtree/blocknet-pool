# Batch 2 research notes: blocknet-checkpoints-fast-sync

Writer notes for `/blog/blocknet-checkpoints-fast-sync/` (2026-10-25). Pool code read from this
worktree (branch `claude/pool-blog`; the checkpoint handlers and `CheckpointsPage.tsx` are the same
on `origin/master` 02232a5). blocknet-core read with `git show pool:<path>` (c646196) and, for
cross-checks, `upstream/master` (514a8cf). The `blocknet` manager docs read with
`git -C blocknet show master:<path>` (973e56e). Live values fetched 2026-10-25 at 02:04 EDT.

## Intent

Someone running or setting up a Blocknet node wants to know what bntpool.com's checkpoints are,
how the daemon uses them to sync fast without leaving the real chain, and how to download, verify
and load `checkpoints.dat` by hand. No other bntpool.com or seineminer.com post owns node sync or
checkpoints; this post adds the code-verified explanation of what fast sync skips and what it still
checks, the step-by-step procedure, an explorer spot-check we ran, and the metadata API. Solo mining
is mentioned in one sentence only and left to `pool-vs-solo-mining-blocknet`.

## Keyword-map row (proposed)

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| blocknet checkpoints, blocknet fast sync, checkpoints.dat | `/blog/blocknet-checkpoints-fast-sync/` | Understand bntpool.com's checkpoints and use them to sync a Blocknet node fast | sync Blocknet node, Blocknet full sync, bntpool checkpoints API, verify checkpoints.dat SHA-256, load-checkpoints, Blocknet 检查点快速同步 / Blocknet 檢查點快速同步 |

## Facts used (not in the fact sheet) with sources

### blocknet-core (`pool` branch unless noted)

- Default checkpoint URL is `https://bntpool.com/checkpoints.dat`, used "when not in --full-sync
  mode"; filename `checkpoints.dat`; download cap 32 MiB. `checkpoints.go:19-27`. Same default on
  `upstream/master:checkpoints.go:23`.
- The file is fetched only if none exists (`os.Stat` → return), 15 s timeout, written to `.tmp`
  then renamed. `checkpoints.go:40-97` (`:41-43`, `:54`, `:63`, `:80`).
- File format: one `height:hash` per line, blank and `#` lines ignored, 64 hex chars, optional
  `0x`, height must be non-zero. `checkpoints.go:99-151`.
- Startup: unless `FullSync`, the daemon calls `ensureCheckpointsFile` then `loadCheckpointsFile`;
  failures are non-fatal ("should never prevent startup"); then `SetTrustedCheckpoints`.
  `daemon.go:309-333`.
- Startup re-verification starts from the highest checkpoint matching the stored chain, falling
  back to full `VerifyChain` when none matches. `daemon.go:335-346`; `block.go:797-845`.
- Fast-sync target = max checkpoint height; PoW skip only for tip-extending blocks while the
  chain is below that height. `block.go:981-1013` (`SetTrustedCheckpoints` `:985`,
  `shouldSkipPoWLocked` `:1002`); comment at `block.go:1093-1098`.
- Pinning: while below the target, a block at a checkpoint height whose hash differs is rejected
  ("checkpoint mismatch at height ..."); comment "ensures skipping PoW cannot be exploited to feed an
  alternate history". `block.go:1910-1925`.
- The same gate skips RingCT signature and range-proof verification ("the dominant cost of a fresh
  sync"). `block.go:1926-1946`; `transaction.go:952-963`, `:1018-1027`, `:1046-1049` ("the single
  largest cost of a sync, as it is paid on every output including every block's coinbase").
- Still checked when skipping: height linkage and parent timestamp, LWMA difficulty, median and
  future timestamp rules, min difficulty (`block.go:469-534`); size, coinbase shape
  (`block.go:546-576`); RingCT binding of key image and pseudo-output, double-spend
  (`transaction.go:1029-1043`); in-block duplicate key images (`block.go:588-600`); outputs and key
  images still indexed, "wallet scanning and balance calculation are completely unaffected"
  (`transaction.go:959-962`). PoW check gated at `block.go:536-544`.
- Writer: checkpoint every 100 blocks (`checkpoints_writer.go:11`) up to
  `(height - MaxReorgDepth) / 100 * 100` (`:38-40`), format `%d:%X` of the block hash (`:52`),
  atomic replace (`:67-90`). `MaxReorgDepth = 100` "Hard finality" (`block.go:57`).
- Flags: `--save-checkpoints`, `--full-sync` "Bypass checkpoints (download + verification) and sync
  naturally from peers". `main.go:86-87`. (On `upstream/master:main.go:181-182` these flags are
  marked deprecated in favor of `/api/settings`; the manager's `full_sync` still maps to the flag.)
- Interactive shell command `load-checkpoints` downloads if missing, loads and prints the fast-sync
  height. `cli.go:806`; `cli_cmd_daemon.go:327-358`; `cli_help.go:325-331`. Authenticated API
  `POST /api/checkpoints/load` does the same (`api_checkpoints.go:23-65`, `api_routes.go:23-24`,
  `:53-54`); not mentioned in the post.
- Core default data dir `./blocknet-data-mainnet`. `defaults_mainnet.go:7`.

### blocknet manager (`blocknet@master`)

- `data_dir` default `~/.config/bnt/data/mainnet/`, Windows `C:\Users\<you>\.config\bnt\data\mainnet\`.
  `docs/reference-config.md:73`.
- `full_sync` "Bypass checkpoint sync ... independently verifies the entire chain from genesis";
  maps to `--full-sync`. `docs/reference-config.md:80`, `:247`.

### Pool (this worktree)

- Public routes `GET /checkpoints.dat` and `GET /api/checkpoints`. `apps/blocknet-pool-api/src/api.rs:948`,
  `:957`; `README.md:221-222`, `:234-236` (serves "the latest daemon-generated checkpoint file" from
  `<daemon_data_dir>/checkpoints.dat`).
- Metadata fields `available, url, entries, latest_height, latest_hash, bytes, sha256, updated_at`.
  `api.rs:802-812`, built at `:5825-5870`.
- Headers: `Cache-Control: public, max-age=60` (`api.rs:116`), `Content-Disposition` attachment,
  `ETag "sha256:<hash>"`, `X-Checkpoint-Height`, `X-Checkpoint-Sha256` (`api.rs:5922-5975`);
  `If-None-Match` → 304 (`api.rs:1058-1078`, `:5926-5936`). File validated before serving
  (`api.rs:5872-5920`), max 32 MiB (`:115`).
- Checkpoints page: title "Blocknet checkpoints", intro "Pool daemon checkpoints for fast initial sync
  and trusted chain pinning", button "Download checkpoints.dat", curl command
  `curl -L <url> -o blocknet-data-mainnet/checkpoints.dat`, published SHA-256, `sha256sum` command.
  `frontend/src/pages/CheckpointsPage.tsx:41-46`, `:57-61`, `:79-80`, `:101-104`, `:111-113`,
  `:133-136`, `:149-152`.

### Live (2026-10-25 02:04 EDT)

- `https://bntpool.com/api/checkpoints`: `entries` 689, `latest_height` 68,900, `bytes` 48,811,
  `sha256` 35ded092…43cf5b, `updated_at` 1792902277 = 2026-10-25 00:24:37 EDT.
- `HEAD /checkpoints.dat` returned the headers above. First lines 100/200/300 as quoted in the post.
- `/api/stats` `current_job_height` 69,020 → (69,020 − 100) rounded down to 100 = 68,900, matching
  the writer formula.
- Explorer check: `https://explorer.blocknetcrypto.com/block/68900` shows hash
  105de201…03cb9010, matching the file's line for 68,900.

### Derived

- "About every eight hours": 100 blocks × 300 s = 30,000 s ≈ 8.3 h (fact sheet block time).

## Choices and notes for the integrator

- `related` is set to two published posts instead of `[]`: with five published posts the strict
  check warns on an empty list. Swap in batch-2 slugs as you see fit.
- No speedup figure is claimed (none measured); the post cites the code's own description of what
  is expensive.
- The trust model is stated plainly and positively: hashes come from the pool daemon's finalized
  chain, can be spot-checked on the explorer, and `--full-sync` is offered for self-verification.
- Small UI inconsistency noticed (not edited): the Checkpoints page says "Run this in the data
  directory" but its command is `sha256sum blocknet-data-mainnet/checkpoints.dat`, which runs from
  the data directory's parent.
- Translations: structure verified by running the checker on a scratch copy as if published (clean).
