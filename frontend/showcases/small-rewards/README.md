# Small rewards and payout progress

Run `npm --prefix frontend run showcase:rewards` from the repository root.
Open http://localhost:5197/showcases/small-rewards/?state=small&lang=en.

The real StatsPage and API client use synthetic responses. The 30 desktop and
mobile captures cover small balances, missing data, holds, loading, empty,
lookup errors, payout thresholds, queued payouts and settings failures.
The extra accepted-work panel has been removed; regular mining statistics remain.
The balance component preserves small rewards and shows the configured minimum
and confirmed-only shortfall. See source-revision.json for exact source hashes
and which unaffected captures were retained.

Validation: production build including TypeScript and all 20 catalogs; browser
review of desktop and mobile success, balance-error and verification holds.
No horizontal overflow. Payout calculations and API contracts are unchanged.
