# bntpool.com blog

Static blog pages for bntpool.com, served by the pool API under `/blog/` in English,
`/blog/zh-hans/` and `/blog/zh-hant/`. It promotes bntpool.com, Blocknet and the seine miner, and
is the pool-side sibling of the seineminer.com blog (seine repo, `site/`). Both blogs follow the
same editorial rules; read the seine repo's `site/editorial/writing-guide.md` (first rule: promote
Blocknet, bntpool.com and seine, nothing negative, nothing false) and use its
`site/editorial/fact-sheet.md` as the source of facts.

| Path | What it is |
|---|---|
| `content/<slug>/index.md` | English post; `zh-Hans.md` and `zh-Hant.md` beside it are the translations |
| `editorial/keyword-map.md` | Which bntpool.com URL owns which search job, and the boundary with seineminer.com posts |
| `blog.css`, `theme.js` | Page styles and the dark-theme switch (separate files: the app's CSP forbids inline ones) |
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
   Translations prefix blog links with their locale (`/blog/zh-hans/<slug>/`) but keep app
   links unprefixed, because the pool app is English-only.
5. `node scripts/check-blog.mjs <slug> --strict` and `node scripts/check-blog.mjs --overlap <slug>`
   from `frontend/`.
6. Translate (`node scripts/check-blog.mjs --hash <slug>` gives the `sourceHash`), review in a
   separate pass, set `translation: reviewed`, remove `draft: true`, add the keyword-map row.
7. `npm --prefix frontend run build`, then rebuild and deploy the API (`./scripts/deploy_bntpool.sh
   --api-only`, which doesn't restart Stratum). Blog pages are embedded in the API binary.
