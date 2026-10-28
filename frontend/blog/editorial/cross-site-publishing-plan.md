# Shared publishing plan: Seine and bntpool

Updated October 28, 2026 (EDT). Keep this file identical in both repositories:
`seine/site/editorial/` and `blocknet-pool/frontend/blog/editorial/`.
This is the shared prioritization document; each site's keyword map records existing URL ownership.

## Outcome and baseline

Grow relevant organic visits and useful next actions on BOTH domains. Seine should help readers
choose, download and run the miner; bntpool should help them earn BNT, understand the currency's
mining economics, join the pool and follow their rewards. Both can publish substantial general
Blocknet education. Bntpool is not restricted to operational support articles.

At this audit: Seine has 61 English source articles and bntpool has 21, each published in 20
languages. Those counts measure the catalog, not rankings, demand or quality. Recent currency,
wallet and privacy publishing favored Seine. No Search Console performance export was collected
for this pass; proposed search phrases below are intent hypotheses, not measured query volumes.

## Ownership by reader job

| Intent | Primary destination | Supporting coverage and next step |
|---|---|---|
| Download a Blocknet miner; choose supported platforms | seineminer.com `/` and `/#download` | OS guides give the detailed installation procedure and return to the real download section |
| Select mining hardware; improve local performance | Seine hardware/tuning guides and `/#performance` | Pool calculator consumes a reader's measured rate; do not duplicate hardware rankings |
| Join a Blocknet mining pool | bntpool.com `/` and `/start` | Pool setup guide explains workers and account lookup; Seine owns installation |
| Understand rewards, issuance and mining economics | bntpool reward schedule, calculator, fair-launch and pool guides | Seine's existing supply article retains the holder's supply overview; link to the full schedule |
| Understand BNT as an earned, transferable currency | bntpool new coverage where the reader job is distinct | Link to Seine's existing send/receive, timing, fungibility and wallet procedures instead of recreating them |
| Understand privacy mechanisms, wallet use and Faraday | Existing Seine owners | Bntpool may explain implications for earning and receiving BNT, with short context and a link to the mechanism owner |
| Find live network/pool data | bntpool `/stats`, `/blocks`, `/payouts`, `/luck`, `/status` | Articles explain interpretation and link to live tools; dated examples must not pose as live values |
| Understand chain security and mining's role in currency | New bntpool coverage, subject to the overlap check below | Seine keeps its existing software-role, rule-enforcement and general mining introductions |

One reader job gets one primary article across the two sites. Related subjects may appear on both
when the procedure or question is materially different. Use specific, helpful cross-links in the
reader's language. Do not copy articles between domains, add reciprocal links merely to inflate
link counts, or point unique articles' canonicals at the other site. Preserve existing URLs and
self-canonicals; relocation requires a separate, evidence-backed migration decision.

## Next publishing cycle, in order

Give the next cycle more bntpool editorial effort; do not require a permanent article-count quota.

| Priority | Site / action | Reader question and concrete work | Boundary / evidence required |
|---|---|---|---|
| 1 | bntpool: improve `/blog/blocknet-fair-launch/` | How did BNT enter circulation? Strengthen the currency origin/allocation explanation, chronology and links to issuance and earning BNT | Verify public launch records and released chain rules. Keep supply math in the reward schedule; no new fair-launch URL |
| 2 | bntpool: improve `/blog/blocknet-block-reward-schedule/` | Where do newly issued BNT come from, and what does the reward schedule mean? Add a plain-language path from block reward to circulating currency before the detailed table | Retain exact arithmetic and dates; link to Seine's supply overview and distinguish scheduled issuance from pool payouts |
| 3 | bntpool: research `how-mining-secures-blocknet-payments` | How does proof of work protect BNT's payment history? Explain competing histories, cumulative work and why deeper payments require more work to replace | Compare Seine's `who-controls-blocknet`, `how-crypto-mining-works`, `proof-of-work-vs-proof-of-stake` and pool confirmation guide first. Publish only if a substantive payment-security explanation is missing. Verify released daemon fork-choice rules; no invented attack-cost numbers |
| 4 | Seine: improve existing high-intent landing journey | Can a newcomer choose the right archive, verify it, and reach the correct setup guide from `/#download`? Audit the current release and platform links | Measure this flow and fix observed gaps; no extra download/OS keyword landing pages |
| 5 | bntpool: improve the home → `/start` → `/stats` journey | Can a newcomer understand the pool, connect and identify their worker? Audit search landing copy, current connection instructions and links to the correct guides | Use real public API facts and existing UI. Keep account-specific results out of indexable URLs |
| 6 | bntpool: research `blocknet-bnt-atomic-units` | How do wallet decimals, atomic units and rounding represent the same BNT amount? | Check whether fees, supply and payout guides already answer this adequately. Verify precision and rounding in released code. If the answer fits a short section, update an existing owner instead of making a thin article |
| 7 | Both: use actual search results to choose the following cycle | Which useful pages are getting impressions but failing to satisfy the query or attract clicks? | Follow the measurement procedure below; do not infer performance from article count |

This file is a plan, not a claim that the queued articles or landing-page revisions are already
published. No new article is part of the cover rollout. Before drafting, check BOTH keyword maps
and record the distinct reader job, nearest owner, primary sources, evidence added, and next action.
All article changes ship in all 20 languages; separate reviewer sign-off is not a publishing gate.

## Measurement and main-site SEO

For each domain, export Search Console **Web** search data for the latest complete 28 days and
preceding 28 days; retain dates, query, landing page, country/device where useful, clicks,
impressions, CTR and average position. Label brand and non-brand queries and compare like-for-like
language groups. New pages may not have enough data for a meaningful comparison. Never promise a
ranking increase from the cover change or a fixed publishing cadence.

Record the export date and source alongside the data; keep account data out of public build assets.
Check indexing and canonical selection for representative home, blog, translated and article pages.
Verify both properties and sitemap submissions when accessing the account; do not assume a domain
is verified because the sibling is. Manual indexing requests are optional, not a release gate.

Measure useful onward actions with existing privacy-respecting metrics where available:
- Seine: visits to download/platform guidance and outbound release-download clicks.
- bntpool: visits to `/start`, connection-help use and account-lookup navigation.
- Both: relevant article-to-product and cross-site navigation.

Do not call a navigation click an installed miner or an active pool worker. Do not add wallet-address
tracking or cross-site identity stitching. If existing metrics cannot answer a question, record the
gap before proposing instrumentation. Review after sufficient data accumulates, typically 4–8 weeks;
there is no new scheduled job or external outreach in this plan.

Maintain the technical foundation on both main sites and blogs: crawlable initial content, descriptive
titles and snippets, correct self-canonicals, reciprocal locale alternates, useful internal links,
fast images, sitemap membership and visible-content-matching structured data. Prioritize observed
errors and user journeys over adding more schema types or pages.

## Cover standard and publishing checks

See `cover-system.md`. Covers are separate from instructional figures. Both sites use the same
paper/green palette, safe area and frame, with unique article-specific artwork or a framed real
capture. Images do not contain duplicate article titles. Keep the dated list layout and compact
mobile text list. Share covers across locales; preserve translated headings and article text.

Before landing: run the site's content checks and production build; verify every published cover,
responsive image candidate, social image and image metadata; inspect real desktop/mobile pages,
RTL and long translated titles. Check the list, screenshot covers, illustrated covers and affected
article headers. Publish Seine via master deployment and pool via `--api-only`; verify live output.

## Source guidance

- [Google: helpful, reliable content](https://developers.google.com/search/docs/fundamentals/creating-helpful-content): prioritize reader purpose, original value and clear site focus.
- [Google: image SEO](https://developers.google.com/search/docs/appearance/google-images): crawlable images, relevant surrounding content, responsive assets and useful alternative text.
- [Google: localized versions](https://developers.google.com/search/docs/specialty/international/localized-versions): preserve language-specific URLs and reciprocal alternates.

These sources support the publishing principles; the site split and queue above are our editorial
judgment, not Google's prescribed domain strategy or evidence of current rankings.
