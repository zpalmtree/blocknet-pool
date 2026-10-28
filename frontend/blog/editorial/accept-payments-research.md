# Accepting Blocknet payments — October 28, 2026

Approved: publish a seller workflow in all 20 languages with screenshots and the
shared cover style. Manual order reconciliation is the distinct reader job. Both
keyword maps and the existing payment-link, payment-proof and transfer-time owners
were read before drafting. Do not duplicate their detailed procedures or imply a
payment processor, automatic order integration or measured merchant demand.

## Evidence

- Official GUI latest-release endpoint still reports v0.22.0, release URL
  https://github.com/blocknetprivacy/blocknet-gui/releases/tag/v0.22.0.
  Released commit `661fff98e047f395489a0d2eba88f572c2a89935`.
- Released GUI `main.js:1418–1444`: Generate Payment Link reads amount and memo,
  URL-encodes query fields, creates blocknet and bntpay strings and updates the QR.
  This code does not broadcast a transfer or enforce an invoice deadline.
- `index.html` Receive Payment Request controls are request-amount, request-memo,
  request-generate-btn; History has its own received records. `main.js:1224,1721,3123`
  decodes incoming memo_hex. Dashboard and History display the same wallet records.
- Public core tag v0.22.0 `wallet/wallet.go:713–738`: ordinary-output maturity uses
  current height minus containing height >=10; coinbase uses60. No guaranteed timing.
- Core v0.22.0 `api_handlers.go:350–440`: verify-proof checks tx_key against public
  key, then reports recipient matches and total_matched; valid key alone is insufficient.
  API reference: https://blocknetcrypto.com/api.
- Public inputs use rings/key images, not a conventional sender address. Ask the
  customer for an agreed refund destination; review a refund as a new wallet send.
- Order policy, reconciliation, quotes and recordkeeping are editorial procedural
  recommendations, not claims of built-in invoice enforcement. Example arithmetic:
  1.25 BNT requested −1.00 BNT received =0.25 BNT remaining; count verified receipts once.

## Images

Real released GUI frontend with deterministic local API fixtures; no daemon, keys,
funds, signing or broadcast. Sound/notifications disabled, connect-src none. Capture
through shared Windows Chrome Playwright MCP. Reused the release-verified transfer-time
fixture with memo_hex for `order-1042` and a dated demo receipt; frontend main.js,
styles.css, loader.js and qr.js unchanged. The address visibly says DEMO_ONLY_DO_NOT_SEND.

Captured October 28, 2026, around 4:53 PM EDT: request form element at 1200px viewport,
receipt History at1200×440. The request capture is a real element crop, no redrawn UI.
WebP conversion with Sharp; captions/alt text translated, images enlarge on click.
Cover uses shared `scene` primitives: document → wallet → check on paper/green palette.

Stable showcase: http://localhost:8874/accept-payments/.
Start: `python3 -m http.server 8874 --directory /home/zp/Code/blocknet/showcases/blog-i18n`.
Fixture source and release provenance are retained under that directory; no private
wallet implementation or user wallet data enters the public repo.
