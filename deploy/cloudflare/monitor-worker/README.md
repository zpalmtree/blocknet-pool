# Cloudflare Public Monitor Worker

This worker provides the outside-in probe that the on-host monitor cannot.

What it does:

- checks `PUBLIC_MONITOR_URL` every minute
- tracks state in Workers KV so it can detect transitions
- posts signed incident transitions to `INGEST_URL`
- sends Discord notifications for public HTTP down/recovery events

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
