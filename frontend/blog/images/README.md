# Blog cover sources

- `seine-dashboard.webp`: the existing, public seine v0.2.22 dashboard capture from
  `seine/site/src/assets/dashboard.webp`. Contains only the already-redacted address.
- `pool-dashboard.png`: real public bntpool.com dashboard, captured through the shared
  Windows Chrome Playwright MCP on October 25, 2026, at 3:17 PM EDT. These are historical
  screen contents, not a live statistics widget. No individual miner data is included.
- Eight language-neutral editorial diagrams are defined in `scripts/blog-covers.mjs`.
  The reward chart uses the same emission formula as the article; the other diagrams
  explain concepts rather than displaying live statistics. They contain no translated
  prose, so the same cover is suitable for every locale.

The blog build generates versioned 1200×675 covers, 600×338 thumbnails, and JPEG social
images. To add a post, assign its screenshot or illustration in `blog-covers.mjs`.
