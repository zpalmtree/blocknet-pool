# Configuration and destination changes — October 26, 2026

Approved scope: two operational guides in 20 languages, illustrations, verification and deployment. No hardware rental or mining runs. No independent linguistic review; translations retain draft review status.

## Evidence

Public miner release: v0.2.36 (GitHub latest checked October 26); source commit f3c8175eac0e01cd31870372b2c4989a598044d3. Pending master tuning changes are outside these claims.
- `seine@v0.2.36:src/user_config.rs:9–44`: JSON schema, reader/writer, missing file versus parse errors. Connection fields only; retain schema and generated dev_fee_pool_worker.
- `seine@v0.2.36:src/config.rs`, Config::parse, apply_user_config_defaults, persist_bootstrap_user_config, persist_user_config_with_dev_fee_pool_worker: data-dir lookup, explicit CLI wins, first-run saving, existing fields preserved if generated label added. Existing CLI overrides do not rewrite connection fields.
- `seine@v0.2.36:docs/guides/configuration.md`: working-directory-relative data path, nonpersistent dashboard thread count, shortname resolution fixed for each run including reconnects.
- `seine@v0.2.36:src/shortname.rs` and startup resolution: destination validation and name resolution failures stop startup.
- `blocknet-pool@4b77da1:crates/pool-runtime/src/stratum.rs:1008–1055`: resolved address retained for connection/stats.
- `blocknet-pool@4b77da1:crates/pool-runtime/src/payout.rs:735–840`: historical window shares weighted and allocated by their address; changing a miner's address has no retroactive reassignment.
- Same file:1095 onward selects existing balances under normal minimum/timing rules; 1395–1445 builds payout recipients from balance.address. A new mining destination cannot combine balances or bypass thresholds.
- Existing wallet, PPLNS and confirmation guides remain the owners of detailed wallet setup and reward rules. New article uses no promised payout time or unsourced fixed threshold.

## Search intent and overlap

Configuration guide: edit and persist settings on an existing installation; differs from upgrading binaries or installing startup tasks.
Address guide: transition existing rigs between destinations; differs from first connection or adding another rig under the same address.
Searches for mining payout address changes show heterogeneous account-based and address-based pool behavior. Other pools' procedures are not used as evidence for bntpool. No search-volume or ranking claim. Search Console demand remains unmeasured.

## Verification scope

Commands and JSON are checked against the pinned release. Launch examples would start mining; no mining or wallet change is performed during editorial verification. Browser checks cover rendered guides, links and responsive layout, not an actual financial transfer.

Completed before publication: strict checks for all 18 pool articles, 21 frontend tests, 380 blog pages and 160 localized public pages built; all 20 new pages passed metadata/schema/image audit and a 320px browser-frame overflow check. Desktop/mobile English, Arabic and Thai, commands, FAQs and list layout visually reviewed. Eight-word phrase overlap report: 0.0%; intent boundaries also reviewed against setup, multi-computer and PPLNS owners.
