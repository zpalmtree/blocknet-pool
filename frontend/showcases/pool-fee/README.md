# Pool fee showcase

Run `npm --prefix frontend run showcase:languages -- --port 5187` from the repo root.
Open http://localhost:5187/showcases/pool-fee/index.html for the reviewed captures.
The preview renders the real application with synthetic API responses.

The homepage prominently displays the fee from `/api/info`. Zero is shown as
`0%`; unavailable fee data stays unknown (`-`), including on the setup page.
The server fee remains the source of truth; the UI does not hardcode a free pool.

States: success (0%), empty pool history, loading, API error, missing fee,
nonzero fee, Arabic RTL, dark theme. There is no disabled control for this
read-only fee display. Desktop and mobile captures are stored here.

## Production rollout

The default and example config already specify `pool_fee_pct: 0`. Production uses
`/etc/blocknet/pool/config.json` and now sets this to 0. Deploying
frontend code alone does not change the charged fee.

Completed with explicit production approval on October 25, 2026:

1. Back up the production config and set only `pool_fee_pct` to `0`.
2. Restart Stratum through `BNTPOOL_FORCE_RESTART=1 scripts/deploy_bntpool.sh
   --stratum-only --skip-build` to reload the fee (miners briefly reconnect).
3. Deploy the homepage with `scripts/deploy_bntpool.sh --api-only`.
4. Verify `/api/info` reports `pool_fee_pct: 0`, homepage shows `Pool fee / 0%`,
   and pool health is good. Record service restart times in America/New_York.

Do not restart the daemon. The new Seine rate needs a separate miner release;
existing installed binaries retain their previous rate until upgraded.

Verified live: `/api/info` reports `0.0`, homepage shows `Pool fee / 0%` at
390px and 1440px, and the calculator defaults to 2%. Stratum restarted at
11:58:35 PM EDT; API/UI at 11:59:03 PM EDT. Monitor and recovery PIDs stayed
unchanged; no daemon restart. Health was good with 265 connected miners and
no ongoing incidents. Config backup:
`/etc/blocknet/pool/config.json.before-zero-fee-20261025-235822-EDT`.

Deployed revision: `aee60c8`. Deployment metadata marked `dirty=1` solely because
of an untracked symlink to the existing local Cargo build cache (`target`);
tracked source was clean. That symlink was removed after deployment.
