# bntpool.com blog

Static blog pages for bntpool.com, served by the pool API under `/blog/` in English and under
`/blog/<locale>/` in 19 translations (the same set as seineminer.com: id, es, de, fr, pt-br, ru,
it, pl, tr, vi, ar, nl, uk, el, ja, ko, th, zh-hans, zh-hant). It promotes bntpool.com, Blocknet and the seine miner, and
is the pool-side sibling of the seineminer.com blog (seine repo, `site/`). Both blogs follow the
same editorial rules; read the seine repo's `site/editorial/writing-guide.md` (first rule: promote
Blocknet, bntpool.com and seine, nothing negative, nothing false) and use its
`site/editorial/fact-sheet.md` as the source of facts.

| Path | What it is |
|---|---|
| `content/<slug>/index.md` | English post; `<locale>.md` beside it (`es.md`, `pt-BR.md`, `zh-Hans.md`, ...) are the translations |
| `messages/<locale>.json` | Interface strings, one file per locale with the same keys as `en.json` |
| `editorial/keyword-map.md` | Which bntpool.com URL owns which search job, and the boundary with seineminer.com posts |
| `blog.css`, `theme.js`, `lang.js` | Page styles, the dark-theme switch, and the language menu memory plus automatic switching to the reader's language (separate files: the app's CSP forbids inline ones) |
| `../scripts/build-blog.mjs` | Renders `frontend/dist/blog`; part of `npm run build` |
| `../scripts/check-blog.mjs` | Editorial and translation checks; the build runs it first |

## Writing a post

1. Pick a pool-side search job (getting started on the pool, payouts, stats, the coin itself) and
   check both keyword maps: this one and seineminer.com's. Don't duplicate a seineminer.com post;
   link to it instead.
2. Write `content/<slug>/index.md` with `draft: true`, in the same front-matter format and with
   the same `<Answer>`, `<Note>` and `<Warning>` blocks as the seineminer.com blog. Posts are
   Markdown (`.md`), not MDX.
3. End the body with exactly one call to action written for the post (the check requires it):
   `<CTA target="pool" title="…" text="…" />` sends readers to `/start`; `target="seine"` to the
   seine download. Translations translate `title` and `text`. Avoid ASCII double quotes inside them.
4. Link to pool pages as `/start`, `/stats`, `/blocks`, `/payouts`, `/luck`, `/status`,
   `/checkpoints`, to other posts as `/blog/<slug>/`, and to seineminer.com with full URLs.
   Translations prefix blog links with their locale (`/blog/es/<slug>/`, `/blog/pt-br/<slug>/`)
   and seineminer.com links with theirs (`https://seineminer.com/es/...`). Keep pool app links
   unprefixed in the source; the builder adds `?lang=<locale>` so readers stay in their language.
5. `node scripts/check-blog.mjs <slug> --strict` and `node scripts/check-blog.mjs --overlap <slug>`
   from `frontend/`.
6. Translate into all 19 locales following the seine repo's `site/editorial/translation-guide.md`
   (`node scripts/check-blog.mjs --hash <slug>` gives the `sourceHash`; check one locale with
   `node scripts/check-blog.mjs <slug> --locale es`). Keep `translation: draft` unless an optional
   separate review has happened, then use `translation: reviewed`. Remove `draft: true` from
   the English source and add the keyword-map row. The build fails while any translation of a
   published post is missing, stale, structurally invalid, or has an invalid status. Separate
   review is not a publishing gate (owner decision, 2026-10-25).
7. `npm --prefix frontend run build`, then rebuild and deploy the API (`./scripts/deploy_bntpool.sh
   --api-only`, which doesn't restart Stratum). Blog pages are embedded in the API binary.

## Rewards calculator

The rewards guide embeds `<RewardsCalculator />` in every language. Its controls and
initial HTML are generated from the blog catalogs; `rewards-calculator.js` is bundled
with `rewards-math.js` into a content-hashed asset loaded only by this article.
The calculator fetches only `/api/stats` and `/api/info`, never a wallet or entered
hashrate. Inputs expire after five minutes and require an explicit refresh.

Calculation and release sources rechecked October 26, 2026:

- `blocknet-core/miner.go`, `GetBlockReward`: initial reward 72,325,093,035 atomic
  units, tail 200,000,000, annual exponential decay 0.75, 8,640-block months and
  tail starting at month 48. `blocknet-core/block.go`: 300-second target interval.
- `seine@v0.2.36:src/dev_fee.rs`: flat 2% dev time on all pools and solo mode.
  `miner-release.json` pins this reviewed public release; the calculator displays
  its fee and version in all languages. A pool-reported user hashrate should
  use 0% to avoid deducting the miner fee twice. Historical v0.2.26 examples
  retain their dated release labels and original fee values.
- Live `/api/info` reported a 0% pool fee, 0.1 BNT minimum payout, 60-block
  maturity and a six-hour PPLNS window. The calculator still reads the fee live.
- Pool fee comes from `pool_fee_pct` in `/api/info`, never a hardcoded fallback.
- Estimates integrate scheduled block rewards over the next 288 or 2,016 blocks,
  hold network hashrate fixed, and compound the miner and pool fees. They exclude
  transaction fees and do not forecast PPLNS payout timing or pool luck.
- `tests/reward-schedule.json` is an independent Python/libm evaluation of all
  49 reward periods from the daemon formula, in atomic units. Arithmetic tests
  cover each boundary, tail emission, invalid API data, fees and decimal input.

Recheck emission constants and the default miner fee when either upstream contract
changes, including the planned Faraday upgrade. A separate linguistic review was
not performed; updated translations retain draft review status.

## Article images

Every published post needs a cover assigned in `scripts/blog-covers.mjs`. Use a real product
screenshot in `blog/images/` or a language-neutral editorial diagram. The build generates
responsive WebP covers and a per-article JPEG social image, embeds them with the blog, and
fails if a published post has no cover. The index follows the seine blog: dated list rows with a thumbnail at the inline end.
Below 640px the rows show text only; article headers retain the full cover at every width. Document screenshot provenance in `images/README.md`.

## Topic navigation

`../scripts/blog-topics.mjs` assigns one topic to each published source slug.
Add new articles there; an unassigned article fails the build. All translations
reuse that assignment and translate the topic names in the interface catalogs.
Indexes keep every article in static HTML, newest first within its topic. Fragment
links work without JavaScript; article breadcrumbs link back to the topic.
Hardware links lead to the matching Seine language; Seine links back to rewards.
No extra category URLs or sitemap entries are created.

Release drift is checked by Seine's **Content facts** GitHub workflow after its
Build Release workflow finishes. It checks the pool's canonical
`frontend/blog/miner-release.json` as well as Seine's editorial manifest against
the latest public release and its source. Update both manifests together after
reviewing a new release; pending miner source is not a published fee change.
Calculator rendering and tests use the reviewed manifest. The live pool fee still
comes exclusively from `/api/info`. This pass updates all 20 localized fee labels;
translations retain draft status, without an independent linguistic review claim.

## Shared planning and covers

Read the [publishing plan](editorial/cross-site-publishing-plan.md) before selecting work.
Bntpool also owns substantive Blocknet currency/economics topics where its reader job is distinct.
Follow the [shared cover standard](editorial/cover-system.md); both sites use the same frame,
palette and 440px thumbnail contract.
