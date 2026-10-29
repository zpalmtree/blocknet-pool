# SafeTrade buying and selling guides

Prepared October 29, 2026 (EDT). English sources and all 19 translations.

## Sources and scope

- https://safetrade.com/announcements/blocknet-listing — public announcement,
  displayed publication date October 28, 2026. Verified using the configured
  Playwright MCP. Links the listed Blocknet project to blocknetcrypto.com.
- https://safetrade.com/exchange/BNT-USDT — verified live through the same MCP.
  Public BNT/USDT book, trade history, price in USDT, amount in BNT, Limit,
  Market and Stop Limit tabs, open-order and trade-history sections. Logged-out
  inspection only; no account created, order placed or funds transferred.
- https://safetrade.com/fees and https://safetrade.com/terms — official current
  policy destinations. Articles link there instead of fixing numbers or access
  claims that can change.
- Existing published Seine desktop-wallet and transfer-time guides supply the
  wallet workflow boundary. Existing pool PPLNS and missing-wallet-receipt
  guides supply the payout boundary. Read both canonical keyword maps before
  choosing these new, distinct jobs.

Account-specific deposit addresses, supported USDT networks, deposit/withdrawal
availability, confirmation counts, limits and verification status were not
authenticated or tested. The guides tell readers to use the current account
screens; they do not claim specific values, universal access or no-KYC trading.
The illustration of 100 BNT at 0.10 USDT = 10 USDT is arithmetic, explicitly
not a live quote. No price forecast, profitability claim or ranking promise.

## Editorial and visual review

Both English sources pass strict editorial checks. The eight-word overlap
check finds no shared phrases with existing articles. Semantic boundaries are
recorded in the keyword map; the buy and sell workflows are distinct.

Covers use existing shared editorial symbols and palette. They are conceptual
illustrations, not fabricated exchange screens. The showcase captures the
actual blog renderer with the complete articles at desktop and mobile widths.

Stable preview: http://localhost:8875/
Showcase startup: `python3 -m http.server 8875 --bind 0.0.0.0 --directory /home/zp/Code/blocknet/showcases/safetrade-blog`

Article preview startup: `python3 -m http.server 8876 --bind 0.0.0.0 --directory /home/zp/Code/blocknet/showcases/safetrade-blog/site`

Full frontend build and cover validation passed. All 40 article pages have one H1,
self-canonical URLs, BlogPosting/BreadcrumbList/FAQPage data, three visible FAQs,
and loaded covers. Desktop and mobile images were reviewed; mobile has no
horizontal overflow. Existing worker-terminology warnings in the tracking
article are unrelated. Browser console errors after fixing the preview favicon
were from the installed wallet extension, not the article.

The user approved translation and publication, and removed the separate approval
checkpoint for future requested blog work on October 29, 2026 (EDT). The repo
AGENTS.md, blog skill and README now record this narrow API-only standing
permission. Unrelated production changes retain their own authorization rules.

Both guides are complete in all 20 languages. Translations were written by the
active model with no external translation service or delegated agents. Separate
linguistic review was not performed; `translation: draft` records review status
and does not prevent publication. Source hashes, protected links, section/block
counts and FAQs pass strict checks for every locale.

Validation: 22 frontend tests and 127 API tests passed (one optional fixture
export test ignored). All 40 rendered articles pass canonical, reciprocal
hreflang, structured-data, sitemap and internal-blog-link checks. Visual review
caught missing paragraph separators after translated Answer blocks; corrected
before publication so announcement and cross-guide links render correctly.

Deployment uses `scripts/deploy_bntpool.sh --api-only`; no daemon, Stratum
or gateway restart is needed. Deployment evidence and final captures are kept
in the stable local showcase above.
