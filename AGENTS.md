# blocknet-pool

## Project structure

- `apps/` - Cargo app packages for API, Stratum, monitor, and recoveryd
- `crates/` - Shared Cargo library packages split by service boundary
- `frontend/` - React/TypeScript SPA built with Vite
- `src/` - Shared implementation modules included by the app/library packages
- `frontend/dist/` - Generated frontend bundle, embedded into the API binary at build time
- `scripts/deploy_bntpool.sh` - Normal production deploy path for the current primary pool host (`bntpool` SSH alias)
- `scripts/provision_bntpool_monitoring.sh` - Idempotent provisioning/update path for Prometheus/Alertmanager/exporters and monitoring configs on the current primary pool host
- `scripts/deploy_cloudflare_monitor_worker.sh` - Deploy the outside-in Cloudflare Worker probe using the local TRMNL Cloudflare credentials
- `scripts/build_blocknet_daemon.sh` - Local helper for building a server-compatible `blocknet-core` daemon binary from the sibling `../blocknet-core` repo; resolves branch `pool` into a temporary worktree unless intentionally overridden
- `scripts/deploy_blocknet_daemon_bntpool.sh` - Repeatable host-built daemon deploy path for the current primary pool host that updates `blocknetd.service`, switches the active core release, and records branch/revision metadata in the deployed release

## Deployment

- Use `./scripts/deploy_bntpool.sh` for normal production deploys to the `bntpool` SSH alias. Keep that alias pointed at the current primary production host.
- Use `./scripts/provision_bntpool_monitoring.sh` when monitoring stack assets or on-host monitoring package config changes need to be installed on the current primary production host.
- Use `./scripts/deploy_cloudflare_monitor_worker.sh` for the external Cloudflare public probe. Do not hand-run `wrangler` with copied secrets when the script can do it repeatably.
- Use `./scripts/build_blocknet_daemon.sh` when you need a fresh daemon binary for the current primary production host without compiling on the live server.
- Use `./scripts/deploy_blocknet_daemon_bntpool.sh` when you need to install or roll forward the daemon itself on the `bntpool` SSH alias. Keep daemon deploys separate from pool API/Stratum deploys.
- The daemon build/deploy path resolves branch `pool` from the sibling `../blocknet-core` repo without changing your current checkout. If the needed branch/ref is missing or stale locally, fetch it before deploying. You can still override the branch/ref intentionally with `BLOCKNET_DAEMON_BRANCH` or `--daemon-branch`.
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
- `./scripts/deploy_bntpool.sh --api-only` is the repeatable path for API/UI-only fixes when you need to update `blocknet-pool-api.service` without rebuilding or restarting Stratum, monitor, or recovery.
- `./scripts/deploy_bntpool.sh --monitor-only` is the repeatable path for monitor-only fixes when you need to update `blocknet-pool-monitor.service` without forcing an unnecessary API or Stratum restart.
- `./scripts/deploy_bntpool.sh --provision-monitoring` is the repeatable path when a release includes monitoring stack/unit/config changes that must be applied on-host.
- `./scripts/deploy_bntpool.sh --deploy-cloudflare` is the repeatable path when the Cloudflare Worker assets need to be deployed after the pool-side ingest secret is in place.
- There is no standalone frontend service in production. "Restart the web UI" means restarting `blocknet-pool-api.service`.
- API/UI-only deploys should restart only `blocknet-pool-api.service` and should not restart Stratum when the Stratum binary hash is unchanged.
- Use `./scripts/deploy_bntpool.sh --migrate-split` only when migrating from the legacy combined service or when intentionally reinstalling the split unit files on the current primary host.
- `--skip-ui-build` is safe only if `npm --prefix frontend run build` already ran locally.
- Do not use `--skip-build` for UI changes. It skips the binary rebuild and upload, so embedded frontend changes will not reach production.
- The retired bridge host should be referenced as `oldpool`, not `bntpool`.
- The deploy/provision scripts refuse `oldpool` or `5.161.113.120` unless `BNTPOOL_ALLOW_RETIRED_HOST=1` is set explicitly.
- Keep the SSH deploy user able to write `/opt/blocknet/blocknet-pool` without sudo. Prefer shared group access on the primary host over world-writable directories.
- The API uses a DB/meta-backed live runtime snapshot written by the Stratum service so pool and validation counters remain available after the split.
- After deploys, verify the active service states and record the restart timestamp(s) in the handoff.

### Typical deploy flow

1. Build frontend: `npm --prefix frontend run build`
2. Build Rust binaries:
   `cargo build --release -p blocknet-pool-api-app --bin blocknet-pool-api`
   `cargo build --release -p blocknet-pool-stratum-app --bin blocknet-pool-stratum`
   `cargo build --release -p blocknet-pool-monitor-app --bin blocknet-pool-monitor`
3. Provision monitoring when needed: `bash scripts/provision_bntpool_monitoring.sh`
4. Deploy app services:
   API/UI-only: `bash scripts/deploy_bntpool.sh --api-only --skip-ui-build`
   Full pool release: `bash scripts/deploy_bntpool.sh --skip-ui-build`
5. Deploy the Cloudflare public probe when needed: `bash scripts/deploy_cloudflare_monitor_worker.sh`

## Git

- The git repo root is `blocknet-pool/`, not the parent `blocknet/` workspace.
- Run git commands from `blocknet-pool/` or use `git -C /media/Code/blocknet/blocknet-pool`.
- The canonical repo instruction file is `AGENTS.md`. Do not add `agent.md` or `agents.md`; those filenames are ignored.

## Async and Database Safety

- Treat `PoolStore`, `PostgresStore`, and any `PoolEngine` path that can persist state as blocking code.
- Do not call synchronous Postgres-backed store methods directly from Tokio worker threads, Axum handlers, or long-lived async Stratum tasks.
- When async code needs one of those paths, move it behind `tokio::task::spawn_blocking` or an existing helper that already does that.
- This includes engine methods that look in-memory but may flush vardiff hints, incidents, payouts, or runtime state through the store.
- The failure signature for violating this rule is the Postgres panic `Cannot start a runtime from within a runtime`.
- If you are unsure whether a path touches the store, inspect it first and default to `spawn_blocking` until proven otherwise.

## Frontend

- Built with Vite, React, and TypeScript
- Styles live in `frontend/src/styles.css`
- CSS uses custom properties like `--accent`, `--surface`, `--border`, and `--muted`
- Stat cards use `.stat-card` as the base, with variants like `.stat-card--accent` and `.stat-card--flow`
