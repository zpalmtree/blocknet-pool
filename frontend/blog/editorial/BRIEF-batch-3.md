# bntpool.com blog batch 3 brief (2026-10-25)

Two general explainers written in parallel, one writer each. Shared rules:

- Work only in `/home/zp/Code/blocknet/blocknet-pool-blog/frontend/blog/` of this worktree. Don't
  commit, push, deploy, switch branches, or edit code, the keyword map or other posts.
- Read first: `frontend/blog/README.md` (format and link rules for this blog); the seineminer.com
  rules it points to, in the seine worktree at
  `/home/zp/Code/blocknet/seine-blog-batch3/site/editorial/writing-guide.md` (first rule: promote
  Blocknet, bntpool.com and seine, nothing negative, nothing false; don't volunteer fees) and
  `.../editorial/README.md` (front matter, `<Answer>`/`<Note>` blocks, translation rules); the fact
  sheet at `.../editorial/fact-sheet.md`; and one finished example post with its translations,
  `/home/zp/Code/blocknet/seine-blog-batch3/site/src/content/blog/how-to-mine-blocknet/`
  (`index.mdx`, `zh-Hans.mdx`, `zh-Hant.mdx`). Our posts are `.md` files with the same front matter.
- Create only: `content/<slug>/index.md`, `zh-Hans.md`, `zh-Hant.md`, and
  `editorial/batch-3-<slug>.md` (research notes).
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

- These are GENERAL explainers targeting generic searches anyone makes ("what is a mining pool",
  "what is hashrate"). Explain the concept properly, then use bntpool.com and Blocknet as the worked
  example. Other projects may be mentioned factually, never disparagingly; no rankings.
- All eight published bntpool.com posts may be linked (see `frontend/blog/content/`).

## Assignments

| Slug | Generic search job | bntpool.com / Blocknet angle |
|---|---|---|
| `what-is-a-mining-pool` | what a mining pool is, why miners pool, payout schemes in brief (PPLNS, PPS, solo), what to look for | bntpool.com as the example (PPLNS 6h window, 60 confirmations, 0.1 BNT minimum, live stats); link PPLNS and pool-vs-solo posts |
| `what-is-hashrate` | what hashrate is, units (H/s, kH/s, MH/s...), your hashrate vs pool vs network, why it fluctuates | Blocknet's H/s scale (memory-hard, so numbers are small), network hashrate ≈ difficulty ÷ 300, pool dashboard; link estimate and stats posts |
