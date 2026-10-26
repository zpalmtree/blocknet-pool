# Share guide evidence — October 26, 2026 (EDT)

Intent: interpret share outcomes and vardiff, diagnose recurring rejected submissions.
Tracking/zero-hashrate and PPLNS/payment walkthroughs remain separate owners.
Source revision: b037ab816db11e4db379fc2e394802c061c953e9.

- `crates/pool-runtime/src/engine.rs:180-214`: stale assignment reasons.
- `engine.rs:1110-1200`: assignment resolution, bound difficulty, miner/nonce
  checks and template+nonce duplicate key (not just assignment ID).
- `engine.rs:1285`: share target from assigned difficulty.
- `engine.rs:1490-1576`: daemon block acceptance, verified record and acknowledgment.
- `engine.rs:1580-1728`: low difficulty check, provisional record/acknowledgment.
- `engine.rs:note_share_and_maybe_adjust_difficulty`: variable difficulty.
- `crates/pool-runtime/src/payout.rs:weight_shares`: verified/provisional eligibility
  and difficulty weighting; acceptance alone is not a payout guarantee.
- `frontend/src/pages/MinerPage.tsx`: account counters, recent shares, per-rig data.

Ten shares * 100 difficulty = one * 1,000 = 1,000 total difficulty; 1 rejected /
(99 accepted + 1 rejected) * 100 = 1% by count. Arithmetic examples, not live
statistics. No fixed universal rejection threshold or guaranteed arrival interval.
Do not conflate a stale assignment with an orphaned block, or accepted/provisional
with fully verified, eligible or paid. Support guidance asks for scoped logs,
never wallet secrets. Cover uses an original language-neutral share-flow diagram,
not fabricated product telemetry. Existing list layout retained.

All translations authored with the active model, no external APIs; draft review
status retained. Related metadata in tracking and hashrate guides adds the new
article; translated prose unchanged, source hashes refreshed. API-only deployment.

## Verification

Strict source and all 19 translation checks passed. All 21 existing frontend tests passed; TypeScript/Vite and 260 blog pages plus 160 public pages built.
All 20 new localized pages passed metadata, canonical/hreflang, FAQ/schema, image and internal-anchor checks. Desktop/mobile English, Arabic and Thai, tables, graphics and existing blog lists were inspected through Windows Playwright. Language menus close on outside click. No page-level horizontal overflow was observed; long commands scroll inside their code block. The browser extension ethereum injection error is unrelated to the app. The static pool preview uses the API favicon constant for its favicon route.

Showcase: http://localhost:8874/ram-and-shares/ (source revisions, captures and live verification). These are static article pages; loading, empty, error and disabled application states are not part of this content change.
