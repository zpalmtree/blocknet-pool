# Exchange memos: v0.3.0 rollout

Preparation only. Production activation and the gateway restart are deferred to
the Seine v0.3.0 release and require an explicit deployment request.

## Before release

- Run `python3 scripts/audit_exchange_rollout.py`. It uses bounded read-only
  queries and prints aggregate activity and balances, without customer memos or
  worker names. Worker counts are distinct names, not customers or connections.
- Review address-only shared balances and any pending sends. Do not guess who
  owns a balance or assign it to a newly entered memo. Keep those credits for
  separately authorized reconciliation. Repeat the audit immediately before
  activation because miners may continue earning in the meantime.
- Verify the current exchange deposit address against the exchange deposit page.
  Retain previous shared addresses in the registry when adding rotated ones.
- Back up accounting tables, including `payout_destinations` when present, and
  record the running gateway, engine and API revisions plus current settings.
- Verify release artifacts in isolated staging: old ordinary-wallet config,
  saved exchange config requiring a prompt, unattended missing-memo failure,
  two memos at one address, reconnect/failover, missing minimum metadata, and
  rejected/mismatched memo acknowledgment. Use synthetic memos and mock endpoints.
  Run `python3 scripts/check_memo_rollout.py --binary /path/to/seine` from the
  Seine checkout for the Linux packaged-binary checks. Repeat on the actual
  v0.3.0 artifacts; a development candidate is not the published release.

## Staged configuration

Until activation, leave both switches false (omitting them has the same effect):

```json
{
  "payout_memos_enabled": false,
  "enforce_shared_exchange_memos": false,
  "memo_min_payout_amount": 10.0
}
```

Keep the existing `min_payout_amount`; bntpool's audited regular minimum is 1 BNT.
The staged 10 BNT setting does not change address-only payouts. Confirm per-send
and per-tick payout caps accommodate 10 BNT. Avoid advertising exchange support
before the complete stack is ready.

## Explicitly authorized release window

1. Deploy the updated API/UI and engine with both switches still off using
   `scripts/deploy_bntpool.sh`. The gateway handover path preserves connections.
   Verify ordinary login, shares, payout eligibility and stats afterward.
2. Update the gateway using the deploy script's explicit `--restart-gateway`
   option. This step disconnects miners; schedule it with v0.3.0 and verify they
   reconnect. Do not restart the daemon for this feature.
3. Re-run the audit and resolve the treatment of legacy untagged accounts with
   the operator. Enabling enforcement rejects their future no-memo logins and
   holds their old address-only payouts. Ordinary wallets keep their behavior.
4. Explicitly set `enforce_shared_exchange_memos: true`,
   `payout_memos_enabled: true`, and `memo_min_payout_amount: 10.0` in the managed
   configuration. Apply to engine and API through the normal deploy workflow;
   these are restart settings, not live reload settings. Verify both services
   loaded the same policy before publishing setup instructions.
5. Verify `/api/info` reports enabled admissions, both minima and the registry.
   Check missing-memo rejection, distinct memo account acknowledgments and
   separate stats, and confirm a regular-wallet login still works. Verify memo
   bytes through wallet preview/recovery; a real send needs its own authorization.
6. Record deployed revisions, service restart times in America/New_York, and
   checks. Publish the miner and setup instructions only after pool readiness.

## Recovery

- Before any memo credit exists, reverting to the prior release/configuration
  is possible through the normal deploy workflow, after auditing pending sends.
- After memo credit exists, keep the memo-aware payout/recovery implementation
  and destination table. Never downgrade to a payout process that interprets an
  opaque account key as a wallet address.
- To stop new memo admissions, set `payout_memos_enabled: false` while retaining
  `enforce_shared_exchange_memos: true` and `memo_min_payout_amount: 10.0`. Earned
  memo balances retain their destinations and payout floor. This stops new
  logins, not necessarily sessions already connected; a full emergency stop
  needs a separately planned service action.
- For uncertain sends, use the existing payout pause and recovery procedure.
  Preserve journals/idempotency keys. Do not relabel balances or retry uncertain
  payments manually.
