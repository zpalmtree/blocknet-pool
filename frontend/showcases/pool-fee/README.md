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
`/etc/blocknet/pool/config.json` and currently overrides this with 2.5. Deploying
frontend code alone does not change the charged fee.

After explicit production approval:

1. Back up the production config and set only `pool_fee_pct` to `0`.
2. Restart Stratum through `BNTPOOL_FORCE_RESTART=1 scripts/deploy_bntpool.sh
   --stratum-only --skip-build` to reload the fee (miners briefly reconnect).
3. Deploy the homepage with `scripts/deploy_bntpool.sh --api-only`.
4. Verify `/api/info` reports `pool_fee_pct: 0`, homepage shows `Pool fee / 0%`,
   and pool health is good. Record service restart times in America/New_York.

Do not restart the daemon. The new Seine rate needs a separate miner release;
existing installed binaries retain their previous rate until upgraded.
