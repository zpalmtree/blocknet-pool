# Hot Path / Audit Rollout Plan

Date: 2026-03-11
Repo: `blocknet-pool`
Primary host: `bntpool`
Retired bridge host: `oldpool`

## Goal

Deploy the new validation architecture safely:

- keep the Stratum hot loop responsive for new jobs and regular share accepts
- move non-critical verification to bounded background audit lanes
- preserve synchronous full verification only for candidate shares and forced-review paths
- expose enough timing/counter data to tune verifier concurrency on the new Ryzen host

## Scope In This Change

Changed areas:

- `src/engine.rs`
- `src/validation.rs`
- `src/store.rs`
- `src/pgdb.rs`
- `src/db.rs`
- `src/config.rs`
- `config.example.json`
- `apps/blocknet-pool-api/src/config.rs`
- `src/service_state.rs`
- `src/api.rs`
- `frontend/src/types.ts`
- `frontend/src/pages/AdminPage.tsx`

Behavior changes:

- regular non-candidate shares are accepted after cheap checks and stored as provisional immediately
- candidate shares still block on full verification
- forced-review / risk paths still use synchronous full verification
- background audit is split from the hot path and bounded by explicit queue/worker limits
- payout coverage audit is targeted and incremental, not full replay of pending windows
- runtime snapshot and admin shares page now expose audit depth/age/wait/duration plus hot accept counters

## Current Local Verification Status

Already passed locally:

- `cargo test -q`
- `npm --prefix frontend run build`

Not done yet:

- no commit
- no deploy
- no live host verification

## Key Runtime Defaults To Start With

These are the intended starting defaults for the new host:

- `candidate_verifiers = 1`
- `regular_verifiers = 2`
- `audit_verifiers = 1`
- `candidate_validation_queue = 64`
- `regular_validation_queue = 512`
- `audit_validation_queue = 128`
- `audit_max_addresses_per_tick = 4`
- `audit_max_shares_per_address = 8`

Reasoning:

- this box is memory-bandwidth limited, not CPU-count limited
- do not scale Argon threads to core count
- treat the box as roughly a `3-4` concurrent Argon hash machine until live timings prove otherwise

## Pre-Deploy Checklist

1. Confirm the repo is clean except for this release.
2. Re-run:
   - `npm --prefix frontend run build`
   - `cargo test -q`
3. Review the final diff for:
   - no replay/backfill logic on async runtime snapshot paths
   - no synchronous database work on Tokio worker threads
   - no old-host deploy target usage
4. Commit the change set.
5. Verify `ssh bntpool` lands on the new primary host.

## Deploy Procedure

This is a full pool deploy because Stratum/runtime behavior changed.

1. Build the frontend:

   `npm --prefix frontend run build`

2. Deploy with the normal production script:

   `bash scripts/deploy_bntpool.sh --skip-ui-build`

3. Record service restart times for:
   - `blocknet-pool-api.service`
   - `blocknet-pool-stratum.service`
   - `blocknet-pool-monitor.service`
   - `blocknet-pool-recoveryd.service`

4. Do not deploy to `oldpool`.

## Immediate Post-Deploy Checks

Run these immediately after the rollout:

1. Service health:
   - all four pool services `active`
   - no fresh panic lines in API or Stratum logs

2. Hot path health:
   - miners reconnect after the single Stratum restart
   - accepted shares resume within minutes
   - no repeated miner-side `pending submit acknowledgement(s) exceeded 30s`
   - no repeated `pool offline` loops

3. Job flow:
   - fresh jobs continue arriving
   - no apparent stall in template updates or job broadcasts

4. Validation telemetry:
   - overload mode remains `normal`
   - `submit_queue_depth` stays low
   - `regular_queue_depth` stays low
   - `audit_queue_depth` can rise temporarily, but should not block hot accepts
   - `hot_accepts` increases while shares are being mined
   - `sync_full_verifies` stays limited mostly to candidate / forced-review traffic

5. Runtime sanity:
   - connected miners/workers and hashrate look normal
   - reject rate does not spike unexpectedly
   - no new `Cannot start a runtime from within a runtime` panic

## First Tuning Window

Watch for at least `30-60 minutes` after deploy.

Key admin/runtime fields to monitor:

- overload mode
- candidate submit queue depth
- regular submit queue depth
- regular validation queue depth
- audit queue depth
- oldest queued age
- submit wait p95
- validation duration p95
- audit wait p95
- audit duration p95
- hot accepts
- sync full verifies
- audit enqueued / verified / rejected / deferred

What good looks like:

- hot accepts continue even if audit work accumulates
- candidate work stays fast
- audit queue may trail, but only in the background
- no miner-visible ACK timeout loops

## Tuning Rules

Start conservative. Only change one concurrency variable at a time.

If the hot path is healthy and audit never catches up:

- first try raising `audit_max_shares_per_address`
- if still needed, consider raising `audit_verifiers` from `1` to `2`

If ACK latency rises or miners report delayed accepts:

- lower `audit_verifiers`
- lower `audit_max_addresses_per_tick`
- lower `audit_max_shares_per_address`

If candidate latency rises:

- do not raise total Argon concurrency further
- keep `candidate_verifiers = 1` reserved
- reduce regular/audit concurrency before touching candidate capacity

Do not tune toward logical CPU count. Tune toward stable queue age and miner-visible latency.

## Success Criteria

The rollout is successful if all of the following hold:

- new work keeps flowing
- regular shares are accepted promptly
- candidate shares remain fully verified
- audit activity is visible but does not stall the hot loop
- no runtime panic or blocking-store regressions appear
- capped pending-block participants can be improved gradually when spare capacity exists, without replay storms

## Rollback Plan

Trigger rollback if any of the following appear and persist:

- repeated miner ACK timeout loops
- job delivery stalls
- sustained share acceptance collapse
- Stratum/API panic related to runtime or blocking-store usage
- audit path causing clear CPU or memory-bandwidth starvation

Rollback steps:

1. Revert to the prior known-good commit before this architecture change.
2. Deploy the reverted release to `bntpool` with the normal deploy script.
3. Verify miners reconnect and accepts resume.
4. Leave the admin telemetry changes out of the rollback only if they can be separated cleanly; otherwise revert the full batch.

## Follow-Up After Stable Deploy

If this rollout is stable, next work should be:

- benchmark concurrency on the new host with live timings and `seine`
- decide whether `audit_verifiers = 2` is safe
- improve admin copy so capped payout coverage is easier to interpret
- consider explicit alerting on candidate-lane saturation and audit starvation
