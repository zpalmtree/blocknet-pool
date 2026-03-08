# blocknet-pool

## Project structure

- `frontend/` - React/TypeScript SPA built with Vite
- `src/` - Rust backend for both the API and Stratum binaries
- `frontend/dist/` - Generated frontend bundle, embedded into the API binary at build time
- `scripts/deploy_bntpool.sh` - Normal production deploy path for `bntpool`
- `scripts/provision_bntpool_monitoring.sh` - Idempotent provisioning/update path for Prometheus/Alertmanager/exporters and monitoring configs on `bntpool`
- `scripts/deploy_cloudflare_monitor_worker.sh` - Deploy the outside-in Cloudflare Worker probe using the local TRMNL Cloudflare credentials
- `scripts/build_blocknet_daemon.sh` - Local helper for building a server-compatible `blocknet` daemon binary from the sibling `../blocknet` repo

## Deployment

- Use `./scripts/deploy_bntpool.sh` for normal production deploys to `bntpool`.
- Use `./scripts/provision_bntpool_monitoring.sh` when monitoring stack assets or on-host monitoring package config changes need to be installed on `bntpool`.
- Use `./scripts/deploy_cloudflare_monitor_worker.sh` for the external Cloudflare public probe. Do not hand-run `wrangler` with copied secrets when the script can do it repeatably.
- Use `./scripts/build_blocknet_daemon.sh` when you need a fresh daemon binary for `bntpool` without compiling on the live server.
- Do not manually `scp` individual files or restart pool services for routine deploys unless the user explicitly asks for an emergency hotfix path.
- Production runs split services: `blocknet-pool-api.service`, `blocknet-pool-stratum.service`, and `blocknet-pool-monitor.service`.
- Service responsibilities:
  - `blocknet-pool-api.service` serves the HTTP API and embedded UI bundle.
  - Restarting `blocknet-pool-api.service` for API/UI-only changes should not disconnect miners from Stratum.
  - `blocknet-pool-stratum.service` owns Stratum, payouts, recovery, retention, and live runtime snapshot persistence.
  - Restarting `blocknet-pool-stratum.service` disconnects miners; avoid it unless Stratum-side code, config, or runtime behavior changed.
  - `blocknet-pool-monitor.service` samples API/Stratum/DB/daemon health, persists monitor heartbeats/incidents, and exposes Prometheus metrics.
- The frontend UI is embedded in the Rust API binary. UI-only changes still require rebuilding and redeploying `blocknet-pool-api`.
- The deploy script builds `blocknet-pool-api`, `blocknet-pool-stratum`, and `blocknet-pool-monitor` locally, uploads the binaries, and only restarts the service whose binary changed, unless forced or running `--migrate-split`.
- `./scripts/deploy_bntpool.sh --monitor-only` is the repeatable path for monitor-only fixes when you need to update `blocknet-pool-monitor.service` without forcing an unnecessary API or Stratum restart.
- `./scripts/deploy_bntpool.sh --provision-monitoring` is the repeatable path when a release includes monitoring stack/unit/config changes that must be applied on-host.
- `./scripts/deploy_bntpool.sh --deploy-cloudflare` is the repeatable path when the Cloudflare Worker assets need to be deployed after the pool-side ingest secret is in place.
- There is no standalone frontend service in production. "Restart the web UI" means restarting `blocknet-pool-api.service`.
- API/UI-only deploys should restart only `blocknet-pool-api.service` and should not restart Stratum when the Stratum binary hash is unchanged.
- Use `./scripts/deploy_bntpool.sh --migrate-split` only when migrating from the legacy combined service or when intentionally reinstalling the split unit files on `bntpool`.
- `--skip-ui-build` is safe only if `npm --prefix frontend run build` already ran locally.
- Do not use `--skip-build` for UI changes. It skips the binary rebuild and upload, so embedded frontend changes will not reach production.
- The API uses a DB/meta-backed live runtime snapshot written by the Stratum service so pool and validation counters remain available after the split.
- After deploys, verify the active service states and record the restart timestamp(s) in the handoff.

### Typical deploy flow

1. Build frontend: `npm --prefix frontend run build`
2. Build Rust binaries:
   `cargo build --release --bin blocknet-pool-api --no-default-features --features api`
   `cargo build --release --bin blocknet-pool-stratum --no-default-features --features stratum`
   `cargo build --release --bin blocknet-pool-monitor --no-default-features --features monitor`
3. Provision monitoring when needed: `bash scripts/provision_bntpool_monitoring.sh`
4. Deploy app services: `bash scripts/deploy_bntpool.sh --skip-ui-build`
5. Deploy the Cloudflare public probe when needed: `bash scripts/deploy_cloudflare_monitor_worker.sh`

## Git

- The git repo root is `blocknet-pool/`, not the parent `blocknet/` workspace.
- Run git commands from `blocknet-pool/` or use `git -C /media/Code/blocknet/blocknet-pool`.
- The canonical repo instruction file is `AGENTS.md`. Do not add `agent.md` or `agents.md`; those filenames are ignored.

## Frontend

- Built with Vite, React, and TypeScript
- Styles live in `frontend/src/styles.css`
- CSS uses custom properties like `--accent`, `--surface`, `--border`, and `--muted`
- Stat cards use `.stat-card` as the base, with variants like `.stat-card--accent` and `.stat-card--flow`
