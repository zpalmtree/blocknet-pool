## Blocknet Pool

- Use `./scripts/deploy_bntpool.sh` for normal production deploys to `bntpool`.
- Production now runs split services: `blocknet-pool-api.service` and `blocknet-pool-stratum.service`.
- Service responsibilities:
  - `blocknet-pool-api.service` serves the HTTP API and embedded UI bundle.
  - Restarting `blocknet-pool-api.service` for API/UI-only changes should not disconnect miners from Stratum.
  - `blocknet-pool-stratum.service` owns Stratum, payouts, recovery, retention, and live runtime snapshot persistence.
  - Restarting `blocknet-pool-stratum.service` disconnects miners; avoid it unless Stratum-side code, config, or runtime behavior changed.
- Do not manually `scp` individual files or restart the pool services for routine deploys unless the user explicitly asks for an emergency hotfix path.
- Use `./scripts/deploy_bntpool.sh --migrate-split` only when migrating from the legacy combined service or when intentionally reinstalling the split systemd unit files on `bntpool`.
- The frontend bundle is built locally into `src/ui/dist/` and then embedded into the API binary during the local release build, so frontend-only changes still require an API rebuild/restart.
- The deploy script builds `blocknet-pool-api` and `blocknet-pool-stratum` locally, uploads the binaries, and only restarts the service whose binary changed, unless forced or running `--migrate-split`.
- There is no standalone frontend service in production; "restart the web UI" means restarting `blocknet-pool-api.service`.
- API/UI-only deploys should restart only `blocknet-pool-api.service` and should not restart Stratum when the Stratum binary hash is unchanged.
- The API uses a DB/meta-backed live runtime snapshot written by the Stratum service so pool/validation counters remain available after the split.
- After deploys, verify the active service states and record the restart timestamp(s) in the handoff.
