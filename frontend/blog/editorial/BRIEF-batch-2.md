# bntpool.com blog batch 2 brief (2026-10-25)

Three posts written in parallel, one writer each. Shared rules:

- Work only in `/home/zp/Code/blocknet/blocknet-pool-blog/frontend/blog/` of this worktree. Don't
  commit, push, deploy, switch branches, or edit code, the keyword map or other posts.
- Read first: `frontend/blog/README.md` (format and link rules for this blog); the seineminer.com
  rules it points to, in the seine worktree at
  `/home/zp/Code/blocknet/seine-blog-batch2/site/editorial/writing-guide.md` (first rule: promote
  Blocknet, bntpool.com and seine, nothing negative, nothing false; don't volunteer fees) and
  `.../editorial/README.md` (front matter, `<Answer>`/`<Note>` blocks, translation rules); the fact
  sheet at `.../editorial/fact-sheet.md`; and one finished example post with its translations,
  `/home/zp/Code/blocknet/seine-blog-batch2/site/src/content/blog/how-to-mine-blocknet/`
  (`index.mdx`, `zh-Hans.mdx`, `zh-Hant.mdx`). Our posts are `.md` files with the same front matter.
- Create only: `content/<slug>/index.md`, `zh-Hans.md`, `zh-Hant.md`, and
  `editorial/batch-2-<slug>.md` (research notes).
- English: `draft: true`, `date: 2026-10-25`, `related: []`, 900-1,600 words, 2+ question
  headings, 1-2 `<Answer>` blocks (40-60 words), 3-5 FAQ entries (40-60 words), 2-6 tags,
  description 140-160 characters. Plain American English, no "X, not Y" lines, no taglines, no
  "this is not the BLOCK project" notes (readers know Blocknet).
- Links: pool pages `/start`, `/stats`, `/blocks`, `/payouts`, `/luck`, `/status`,
  `/checkpoints`; seineminer.com with full URLs (`https://seineminer.com/#download`,
  `https://seineminer.com/blog/how-to-mine-blocknet/`, and other seineminer.com posts where they
  fit: `/blog/what-is-blocknet/`, `/blog/blocknet-id-pay-by-name/`,
  `/blog/blocknet-privacy-features/`, `/blog/argon2id-proof-of-work-explained/`,
  `/blog/blocknet-mining-hardware-hashrate/`, `/blog/mine-blocknet-on-mac/`). Don't link other
  bntpool.com posts; the integrator adds those.
- Facts: only from the fact sheet, or verified in code with a citation in your notes: the pool
  code is this worktree (`/home/zp/Code/blocknet/blocknet-pool-blog`, master), the frontend pages
  are in `frontend/src/pages/`, and live values come from `https://bntpool.com/api/info` and
  `/api/stats` (date them; don't present volatile numbers as fixed). blocknet-core: read with
  `git -C /home/zp/Code/blocknet/blocknet-core show pool:<path>`. Never invent numbers.
- Translations: `zh-Hans.md` and `zh-Hant.md` with `translation: draft` and `sourceHash` from
  `node scripts/check-blog.mjs --hash <slug>` (run in `frontend/`) after the English is final.
  Blog links are localized (`/blog/zh-hans/...`); pool app links and external links stay as they
  are; seineminer.com links in translations should point at the same locale
  (`https://seineminer.com/zh-hans/...`, `https://seineminer.com/zh-hant/...`).
- Check: `node scripts/check-blog.mjs <slug> --strict` in `frontend/` until clean.
- Report in at most 20 lines: intent sentence, keyword-map row (incl. one Chinese phrase),
  suggested related bntpool.com slugs from this batch, and facts you added with sources.

## Batch 2 additions

- End the body with exactly one `<CTA target="pool|seine" title="…" text="…" />` written for the post
  (see `frontend/blog/README.md`; no ASCII double quotes inside title/text). Translate title/text.
- You may link the five published bntpool.com posts: `/blog/mine-blocknet-on-bntpool/`,
  `/blog/how-pplns-payouts-work/`, `/blog/track-your-miner-on-bntpool/`,
  `/blog/blocknet-block-reward-schedule/`, `/blog/blocknet-fair-launch/` (localized in translations).
- Use `draft: true`; drafts may link to other drafts.

## Assignments

| Slug | Owns | Leaves to |
|---|---|---|
| `blocknet-checkpoints-fast-sync` | the pool's checkpoints: what `/checkpoints` and `checkpoints.dat` provide, how a Blocknet node uses checkpoints to sync fast and pin the trusted chain (verify in blocknet-core: `api_checkpoints.go` and wherever checkpoints are loaded; and the pool's checkpoint handlers in apps/blocknet-pool-api), step-by-step use | solo mining to the pool-vs-solo post |
| `pool-vs-solo-mining-blocknet` | choosing pool or solo for Blocknet: expected blocks per day = 288 × (your H/s ÷ network H/s), with a clearly hypothetical worked example; why a pool turns that into steady payouts (PPLNS, link the PPLNS post); solo is supported by seine (`--mode daemon`, link the seineminer.com guide) and suits large operations; framed positively for both, with bntpool.com as the easy default | payout mechanics to the PPLNS post |
| `estimate-blocknet-mining-rewards` | estimating what your hardware earns in BNT: the formula, where to read network hashrate and the current reward on the pool dashboard, a dated worked example using live values from `/api/stats` and the reward schedule (label it as a snapshot), hashrate examples from the fact sheet's best-per-generation table, how PPLNS smooths the result. You may say once, plainly, that the pool and miner fees come out of the estimate, without figures. No price, fiat or profit claims | the reward table to the reward-schedule post |
