## Blocknet Pool

- Use `./scripts/deploy_bntpool.sh` for normal production deploys to `bntpool`.
- Do not manually `scp` individual files or restart `blocknet-pool.service` for routine deploys unless the user explicitly asks for an emergency hotfix path.
- The frontend bundle is built locally into `src/ui/dist/` and then embedded into the Rust binary during the remote release build, so UI changes are not live until the scripted deploy completes.
- After deploys, verify the service is active and record the restart timestamp in the handoff.
