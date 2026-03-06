## Blocknet Pool

- Use `./scripts/deploy_bntpool.sh` for normal production deploys to `bntpool`.
- Production now runs split services: `blocknet-pool-api.service` and `blocknet-pool-stratum.service`.
- Service responsibilities:
  - `blocknet-pool-api.service` serves the HTTP API and embedded UI bundle.
  - `blocknet-pool-stratum.service` owns Stratum, payouts, recovery, retention, and live runtime snapshot persistence.
- Do not manually `scp` individual files or restart the pool services for routine deploys unless the user explicitly asks for an emergency hotfix path.
- Use `./scripts/deploy_bntpool.sh --migrate-split` only when migrating from the legacy combined service or when intentionally reinstalling the split systemd unit files on `bntpool`.
- The frontend bundle is built locally into `src/ui/dist/` and then embedded into the API binary during the remote release build, so frontend-only changes still require an API rebuild/restart.
- The deploy script builds `blocknet-pool-api` and `blocknet-pool-stratum` remotely and only restarts the service whose binary changed, unless forced or running `--migrate-split`.
- API/UI-only deploys should not restart Stratum when the Stratum binary hash is unchanged.
- The API uses a DB/meta-backed live runtime snapshot written by the Stratum service so pool/validation counters remain available after the split.
- After deploys, verify the active service states and record the restart timestamp(s) in the handoff.
