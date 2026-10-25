---
name: bntpool-blog
description: Write, translate (Simplified and Traditional Chinese), check and publish posts for the bntpool.com blog, which promotes the pool, Blocknet and the seine miner. Use when asked to write or update a bntpool.com blog post, pick pool blog topics, or change the blog build.
---

# bntpool.com blog

**First rule:** every post promotes bntpool.com, Blocknet and seine, with nothing negative about
any of them (no downsides, risk sections, weak results, unflattering caveats or comparisons where
we look worse), and nothing false: omit rather than misstate. Don't volunteer fees.

Read `frontend/blog/README.md` first. It shares its editorial rules, fact sheet and post format
with the seineminer.com blog in the seine repo (`site/editorial/`). Check both keyword maps so a
pool post never targets a search job a seineminer.com post owns; link to that post instead.

- **New post:** draft `frontend/blog/content/<slug>/index.md` (`draft: true`), then run
  `node scripts/check-blog.mjs <slug> --strict` and `--overlap <slug>` from `frontend/`.
  Translate to `zh-Hans.md` and `zh-Hant.md` (mainland vs Taiwan terms, blog links localized to
  `/blog/zh-hans/...`, pool app links unprefixed), get a separate review pass, set
  `translation: reviewed` and the `sourceHash`, add the keyword-map row, and publish by removing
  `draft: true`. Publishing needs the user's go-ahead.
- **Build:** `npm --prefix frontend run build` renders `frontend/dist/blog` (the check runs first);
  the API embeds it through build.rs and serves it from `apps/blocknet-pool-api/src/blog.rs`.
  Pages can't use inline scripts or styles because of the app's CSP.
- **Deploy:** `./scripts/deploy_bntpool.sh --api-only` (blog pages live in the API binary;
  Stratum isn't restarted). Deploys need the user's request and must not collide with other
  sessions' pool deploys.
