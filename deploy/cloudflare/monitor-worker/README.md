# Cloudflare Public Monitor Worker

This worker provides the outside-in probe that the on-host monitor cannot.

What it does:

- checks `PUBLIC_MONITOR_URL` every minute
- tracks state in Workers KV so it can detect transitions
- posts signed incident transitions to `INGEST_URL`
- sends Discord notifications for public HTTP down/recovery events
- runs only on its cron trigger: `workers_dev = false` leaves it without a
  public URL, and `fetch` (if a route is ever added) returns the last stored
  status without probing or posting anything
- signs `checked_at` into every ingest body; the pool rejects events more than
  5 minutes from its clock or not newer than the last accepted one

Required setup:

1. Create a Workers KV namespace and update `wrangler.toml`.
2. Set Wrangler secrets:
   - `INGEST_SECRET`
   - `DISCORD_WEBHOOK_URL`
3. Keep `PUBLIC_MONITOR_URL` and `INGEST_URL` pointed at the production pool.

Deploy:

```bash
cd deploy/cloudflare/monitor-worker
wrangler deploy
```
