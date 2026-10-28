# Currency issuance and payment security — October 28, 2026

## Scope and intent

Owner approved the shared queue: expand fair launch and the reward schedule for currency readers,
then publish payment security if it adds a distinct reader job. No new hardware, competitor outreach,
or daemon changes. All prose changes ship in 20 languages. No independent linguistic review is claimed.

- Fair launch retains its existing URL and verification walkthrough. Adds chronology and the movement
  from mined issuance into later transfers. Tightens claims about protocol allocations: absence of a
  reserved genesis/coinbase allocation does not identify the people who later mined or received BNT.
- Reward schedule retains its exact table and arithmetic. Adds issuance → maturity → ordinary transfer,
  including why pool payouts do not issue coins again and why cumulative issuance differs from spendable
  wallet balances. Historical October 25 observations remain explicitly dated.
- New `/blog/how-mining-secures-blocknet-payments/` owns payment-history security: linked commitments,
  competing branches, accumulated work, depth, reorganization boundaries and conflicting spends.

Closest owners reviewed: Seine `who-controls-blocknet` (roles and authority; brief chain-choice summary),
`how-crypto-mining-works` (mining introduction; brief confirmations paragraph),
`proof-of-work-vs-proof-of-stake` (mechanism comparison), and pool
`block-confirmations-mining-rewards` (coinbase maturity and accounting). The new guide follows a payment
through competing histories and separates work, authorization and double-spend checks. It does not repeat
OS setup, mining performance, pool payout instructions or an entire privacy-mechanism explanation.
Search intent is inferred from the approved currency direction; no measured query-volume/ranking claim.

## Sources checked

GitHub public releases API reports latest core v0.22.0, released October 15, 2026 at 9:57:38 PM EDT.
Annotated tag object: `651bd1c68b0001f3236905e9e52090f9e6253ae2`.
Released commit: `5a0a5b510c1dce46f4eeb12c33a291052e44e7b3`.
All implementation statements below refer to that public release, not the production fork's pending work.

- [Release](https://github.com/blocknetprivacy/core/releases/tag/v0.22.0)
- [Block validation and selection](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go):
  `addCumulativeWork` lines 36–41 sums difficulty; constants 49–53 define 300-second target, ten-block
  safe setting, 60-block coinbase maturity and `MaxReorgDepth = 100`; header serialization includes
  parent and Merkle root; 507–575 validates empty genesis and the single scheduled coinbase output;
  `ProcessBlock` 1741–1805 validates before comparing strict greater cumulative work;
  `enforceReorgFinalityLocked` 2421–2446 rejects a common ancestor below tip minus 100;
  `GetGenesisBlock` 2960–2978 constructs an empty transaction list and zero root.
- [Transaction validation](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/transaction.go):
  978 onward validates ordinary inputs, ring member eligibility, signature/key-image binding and already
  spent images, then range and commitment-balance checks. Trusted historical checkpoint synchronization
  has a separate skip-crypto path; prose discusses proposed/current blocks without claiming all historical
  sync modes recheck every cryptographic proof. More work does not supply spending authorization.
- [Issuance](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/miner.go):
  constants 16–33; `GetBlockReward` 383–397; `TotalEmittedSupply` 399 onward sums from height 1.
  Initial reward 72,325,093,035 atomic units, 200,000,000 tail, 8,640-block months, tail at month 48.
  Exact pre-tail total independently recalculated from the formula and excluding genesis:
  9,856,190,508,383,445 atomic units = 98,561,905.08383445 BNT, displayed as 98,561,905.08 BNT.
- [Explorer block 0](https://explorer.blocknetcrypto.com/block/0), fetched HTTP 200:
  zero transactions, zero Merkle root, timestamp February 15, 2026 at 8:58:38 PM EST,
  previous hash `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`.
  Its displayed 723.25093035 BNT schedule reward is not an actual output.
- [Explorer block 1](https://explorer.blocknetcrypto.com/block/1), fetched HTTP 200:
  timestamp February 20, 2026 at 2:29:20 PM EST; one coinbase, zero inputs, one output;
  displayed reward 723.25093035 BNT. Distinguish this recorded timestamp from the genesis timestamp.
  Do not infer the identity or number of launch participants from these records.
- [Official site](https://blocknetcrypto.com/), checked October 28: current Argon2id/five-minute chain
  and future Faraday description. No new Faraday details or dates are added to the security article.

The explorer and GitHub API were unavailable through the web fetch tool, then fetched successfully
with unauthenticated HTTPS requests. Public release source was inspected via local Git tag reads.
No private VEIL source or authenticated/private project material was used.

## Image and technical boundaries

Existing fair-launch and reward chart covers stay in the shared cover style. The new cover combines
block, shield and wallet symbols using the same renderer. It is conceptual, not an attack-cost chart
or a claimed protocol diagram. No new translated interface labels or interactive UI are needed.

Publish through the API-only deployment path after content checks, production build, browser checks,
source/translation structure checks and arithmetic verification. Preserve the miner's active sessions.

## Verification before release

- Strict checks pass for all three articles in all 20 languages (60 pages). Source hashes reflect
  translated changes; updated translations are marked draft without an independent-review claim.
- Compared all 40 existing locale files against the baseline: code fences and complete tables
  unchanged. Every new locale has six sections, nine matching links, v0.22.0/date and the 100-block
  boundary. Pool overlap checker reports 0.0% shared eight-word phrases; semantic owner review above
  is the distinct-intent decision, not that numerical measure alone.
- Production frontend build passes: i18n, TypeScript, Vite, blog/public generation and cover checks.
  22 articles × 20 locales = 440 article pages; 66 cover variants. Largest thumbnail 6,358 bytes;
  largest cover 85,469 bytes. English and localized self-canonicals/social images are checked.
- Windows Playwright inspected desktop/mobile content, Arabic RTL, Greek and Thai headings,
  chronological dates, issuance copy, reward tables and the unchanged list layout. At 390px,
  all 60 affected rendered pages have no document overflow. An existing 64-character inline hash
  caused overflow in fair launch; inline code now wraps while preformatted commands still scroll.
- Static preview lacks the API-served `/favicon.svg` route (confirmed in api.rs/ui.rs); this is a
  preview-only 404, not an article asset regression. Verify that route on production after deploy.
- Showcase: `http://localhost:8874/currency-and-security/`. Static articles have no loading/empty/
  disabled UI; relevant exceptional states are image failure, long text, RTL, and narrow tables/code.

Follow-up observation outside this cycle: the existing Seine proof-of-work/proof-of-stake overview
uses broader finality wording than this guide. A later source-refresh should align it with the
precise local reorganization boundary; this cycle does not claim to re-audit all older articles.
