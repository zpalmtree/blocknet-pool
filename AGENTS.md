# blocknet-pool

## Project structure

- `frontend/` - React/TypeScript SPA built with Vite
- `src/` - Rust backend for both the API and Stratum binaries
- `frontend/dist/` - Generated frontend bundle, embedded into the API binary at build time
- `scripts/deploy_bntpool.sh` - Normal production deploy path for `bntpool`
- `scripts/build_blocknet_daemon.sh` - Local helper for building a server-compatible `blocknet` daemon binary from the sibling `../blocknet` repo

## Deployment

- Use `./scripts/deploy_bntpool.sh` for normal production deploys to `bntpool`.
- Use `./scripts/build_blocknet_daemon.sh` when you need a fresh daemon binary for `bntpool` without compiling on the live server.
- Do not manually `scp` individual files or restart pool services for routine deploys unless the user explicitly asks for an emergency hotfix path.
- Production runs split services: `blocknet-pool-api.service` and `blocknet-pool-stratum.service`.
- Service responsibilities:
  - `blocknet-pool-api.service` serves the HTTP API and embedded UI bundle.
  - Restarting `blocknet-pool-api.service` for API/UI-only changes should not disconnect miners from Stratum.
  - `blocknet-pool-stratum.service` owns Stratum, payouts, recovery, retention, and live runtime snapshot persistence.
  - Restarting `blocknet-pool-stratum.service` disconnects miners; avoid it unless Stratum-side code, config, or runtime behavior changed.
- The frontend UI is embedded in the Rust API binary. UI-only changes still require rebuilding and redeploying `blocknet-pool-api`.
- The deploy script builds `blocknet-pool-api` and `blocknet-pool-stratum` locally, uploads the binaries, and only restarts the service whose binary changed, unless forced or running `--migrate-split`.
- There is no standalone frontend service in production. "Restart the web UI" means restarting `blocknet-pool-api.service`.
- API/UI-only deploys should restart only `blocknet-pool-api.service` and should not restart Stratum when the Stratum binary hash is unchanged.
- Use `./scripts/deploy_bntpool.sh --migrate-split` only when migrating from the legacy combined service or when intentionally reinstalling the split unit files on `bntpool`.
- `--skip-ui-build` is safe only if `npm --prefix frontend run build` already ran locally.
- Do not use `--skip-build` for UI changes. It skips the binary rebuild and upload, so embedded frontend changes will not reach production.
- The API uses a DB/meta-backed live runtime snapshot written by the Stratum service so pool and validation counters remain available after the split.
- After deploys, verify the active service states and record the restart timestamp(s) in the handoff.

### Typical deploy flow

1. Build frontend: `npm --prefix frontend run build`
2. Build Rust binaries: `cargo build --release --bin blocknet-pool-api --bin blocknet-pool-stratum`
3. Deploy: `bash scripts/deploy_bntpool.sh --skip-ui-build`

## Git

- The git repo root is `blocknet-pool/`, not the parent `blocknet/` workspace.
- Run git commands from `blocknet-pool/` or use `git -C /media/Code/blocknet/blocknet-pool`.
- The canonical repo instruction file is `AGENTS.md`. Do not add `agent.md` or `agents.md`; those filenames are ignored.

## Frontend

- Built with Vite, React, and TypeScript
- Styles live in `frontend/src/styles.css`
- CSS uses custom properties like `--accent`, `--surface`, `--border`, and `--muted`
- Stat cards use `.stat-card` as the base, with variants like `.stat-card--accent` and `.stat-card--flow`
