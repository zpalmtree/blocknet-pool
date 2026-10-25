# bntpool.com blog batch 1 brief (2026-10-25)

Five posts written in parallel, one writer each. Shared rules:

- Work only in `/home/zp/Code/blocknet/blocknet-pool-blog/frontend/blog/` of this worktree. Don't
  commit, push, deploy, switch branches, or edit code, the keyword map or other posts.
- Read first: `frontend/blog/README.md` (format and link rules for this blog); the seineminer.com
  rules it points to, in the seine worktree at
  `/home/zp/Code/blocknet/seine-blog-batch1/site/editorial/writing-guide.md` (first rule: promote
  Blocknet, bntpool.com and seine, nothing negative, nothing false; don't volunteer fees) and
  `.../editorial/README.md` (front matter, `<Answer>`/`<Note>` blocks, translation rules); the fact
  sheet at `.../editorial/fact-sheet.md`; and one finished example post with its translations,
  `/home/zp/Code/blocknet/seine-blog-batch1/site/src/content/blog/how-to-mine-blocknet/`
  (`index.mdx`, `zh-Hans.mdx`, `zh-Hant.mdx`). Our posts are `.md` files with the same front matter.
- Create only: `content/<slug>/index.md`, `zh-Hans.md`, `zh-Hant.md`, and
  `editorial/batch-1-<slug>.md` (research notes).
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

## Assignments

| Slug | Owns | Leaves to |
|---|---|---|
| `mine-blocknet-on-bntpool` | joining and using bntpool.com: the pool address `stratum+tcp://bntpool.com:3333` as seine's default, logging in with an address or `@name`, confirming your miner on the stats page, what the Get Started page covers | installing seine (link the seineminer.com guide) |
| `how-pplns-payouts-work` | how bntpool.com pays: PPLNS over a 6-hour window, why it rewards steady miners, 60 confirmations before payout, the 0.1 BNT minimum, tentative rewards from unconfirmed blocks, luck and effort pages | stats-page tour |
| `track-your-miner-on-bntpool` | the stats page and public pages: hashrate, shares, balance, payouts history, blocks, luck, status; the public API (`/api/miner/{address}`, `/api/stats`, etc.) for scripts and dashboards | payout mechanics |
| `blocknet-block-reward-schedule` | the emission schedule as a table (monthly reward steps, 2 BNT tail from month 48), 5-minute blocks, 288 blocks a day, ~100M before the tail, what it means for miners (earlier is larger rewards, stated factually) | the fair-launch story |
| `blocknet-fair-launch` | no premine, no dev fund, no treasury: the genesis block with no coinbase, every coin mined, one coinbase output per block equal to the reward; why that matters to miners; mining it yourself with seine on bntpool.com | the emission table |
