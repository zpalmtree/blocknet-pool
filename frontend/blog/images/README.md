# Blog cover sources

- `seine-dashboard.webp`: the existing, public seine v0.2.22 dashboard capture from
  `seine/site/src/assets/dashboard.webp`. Contains only the already-redacted address.
- `pool-dashboard.png`: real public bntpool.com dashboard, captured through the shared
  Windows Chrome Playwright MCP on October 25, 2026, at 3:17 PM EDT. These are historical
  screen contents, not a live statistics widget. No individual miner data is included.
- Nine language-neutral editorial diagrams are defined in `scripts/blog-covers.mjs`.
  The reward chart uses the same emission formula as the article; the other diagrams
  explain concepts rather than displaying live statistics. They contain no translated
  prose, so the same cover is suitable for every locale.

The blog build generates versioned 1200×675 covers, 600×338 thumbnails, and JPEG social
images. To add a post, assign its screenshot or illustration in `blog-covers.mjs`.

- `pool-luck-history.png`: real public Luck page captured with the shared Windows
  Playwright MCP on October 26, 2026, around 2:00 AM EDT. Historical pool-wide
  values only. The article build produces a 1200×833 JPEG for inline use.
- `poolLuck()` illustrates 50%, 100% and 200% effort with proportional bars and
  a reference at 100%. These are explanatory examples, not observed rounds.

`accepted-rejected-stale-shares` uses the `shareOutcomes()` code-native diagram in
`blog-covers.mjs`: a miner, submitted work, and check/cross/clock outcomes. It has
no language-dependent text or invented dashboard values. Added October 26, 2026.

- `mining-difficulty-explained`: original language-neutral SVG diagram in
  `blog-covers.mjs`, showing a smaller qualifying region in the same field of
  possible outcomes. Schematic, not measured probabilities. October 26, 2026.

- `stratum-mining-protocol`: original sequence illustration in `blog-covers.mjs`,
  miner/pool/node columns with literal protocol method labels. Arrows show login,
  node template delivery, job, share submission, block candidate and reply.
  Conceptual flow, not a packet capture. October 26, 2026.
