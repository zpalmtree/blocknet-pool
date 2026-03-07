# blocknet-pool

## Project structure

- `frontend/` — React/TypeScript SPA (Vite build)
- `src/` — Rust backend (API + Stratum binary crate)
- `src/ui/dist/` — Built frontend output (committed, embedded in binary)
- `scripts/deploy_bntpool.sh` — Production deploy script

## Deployment

**The frontend UI is embedded in the Rust binary via `include_str!()` in `src/api.rs`.**
This means UI-only changes still require a full binary rebuild + upload to take effect.

### Deploy command

```sh
bash scripts/deploy_bntpool.sh --skip-ui-build
```

- Always rebuild binaries when deploying (never use `--skip-build` unless you're certain only non-embedded files changed)
- Use `--skip-ui-build` if you already ran `npm run build` in `frontend/`
- The script builds locally, uploads binaries via rsync, and restarts systemd services
- `--skip-build` skips both the local build AND the binary upload — it only syncs source files

### Deploy checklist

1. Build frontend: `npm --prefix frontend run build` (outputs to `src/ui/dist/`)
2. Build Rust binaries: `cargo build --release --bin blocknet-pool-api --bin blocknet-pool-stratum`
3. Deploy: `bash scripts/deploy_bntpool.sh --skip-ui-build` (uploads binaries + restarts services)

### Common mistake

Do NOT use `--skip-build` for UI changes. The UI is baked into the binary at compile time.
Syncing `src/ui/dist/` to the server does nothing — the running binary serves its embedded copy.

## Git

The git repo root is `blocknet-pool/`, not the parent `blocknet/` directory.
Always use `git -C /media/Code/blocknet/blocknet-pool` or `cd` into it first.

## Frontend

- Built with Vite + React + TypeScript
- Styles in `frontend/src/styles.css` (plain CSS, no preprocessor)
- CSS uses custom properties (`--accent`, `--surface`, `--border`, `--muted`, etc.)
- Stat cards: `.stat-card` base, `.stat-card--accent` for green bar, `.stat-card--flow` for inter-card arrows
