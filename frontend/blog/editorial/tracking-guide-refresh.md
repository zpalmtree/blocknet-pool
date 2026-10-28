# Tracking guide refresh — October 28, 2026

Existing URL and search intent retained: first accepted share through first
payout on bntpool.com. All 20 versions now begin with the same address lookup,
accepted-share check and machine row, then explain hashrate and payout stages.
Zero Blocks Found does not mean no pool rewards. Existing `zero-hashrate` deep
link remains. Advanced API material is last; the Home Assistant detour is removed.

Sources checked: current `StatsPage.tsx`, `MinerUpdateBanner.tsx`, API response
types, and live `https://bntpool.com/api/info` (0.1 BNT minimum, 60 confirmations,
6h PPLNS window, checked October 28). Sixty five-minute target blocks average
about five hours after a block is found; there is no fixed first-payment time.
Existing PPLNS and missing-payment articles provide the next steps. The current
version notice is above stats, not a badge on each Workers row.

The source screenshots use the real UI with explicit example data. All locales
retain localized internal links, source-hash parity, visible FAQs, reciprocal
alternates and self-canonicals. Current article split remains: miner setup on
Seine; pool tracking, rewards and payouts on bntpool. No duplicate article added.

Checks and screenshot provenance: `frontend/showcases/pool-tracking/README.md`.
No reviewer gate. `Workers` wording warnings are intentional UI labels.
