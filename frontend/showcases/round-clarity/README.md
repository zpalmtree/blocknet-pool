# Current round clarity

The dashboard reads `/api/stats/insights` and renders accepted-work effort and
round elapsed time as the primary values. The existing average-interval estimate
is secondary; the elapsed/ETA comparison and overdue badge are removed. Backend
calculations, polling and historical luck badges are unchanged.

The scale runs from 0% to 200%+ with a visible 100% average-work marker. It uses
the same color across the range, retains the exact effort above 200%, and is
hidden from assistive technology because the value and explanation are already
available as text. Unknown values use dashes and an unfilled scale.

## Preview

From the repo root:

```sh
npm --prefix frontend ci
npm --prefix frontend run showcase:round
```

Interactive app: http://localhost:5194/?lang=en
Gallery: http://localhost:5194/showcases/round-clarity/index.html
Use `round=low`, `zero`, `high`, `unknown`, or `no-rate`; `demo=loading` or
`demo=error`; and any supported `lang`. The fixture fetch wrapper is injected
only by the showcase configuration and never calls the live pool.

A stable copy can be served from the workspace root:

```sh
python3 -m http.server 18975 --directory showcases/current-round
```

Open http://localhost:18975/ for the reviewed captures.

## Verification

- All 22 frontend tests pass; production build, type check, 20-language catalog
  validation, content checks and asset budgets pass.
- Real dashboard captures at 1366px and 390px cover normal, low, zero, above-scale,
  unknown values, absent hashrate estimate, loading, API error, Arabic and dark.
  Every capture was visually reviewed. No separate disabled control exists.
- Every supported locale renders the new labels and populated values at 390px
  with no page overflow and all scale labels inside the card.
- The 114.3% fixture occupies 57.15% of the 0–200 scale. The 100% label and marker
  align at its midpoint; 312.4% remains exact in text with a capped fill.
- The API request/response contract and database calculations are unchanged;
  fixture responses exercise the real client and dashboard rendering path.
- No application console errors in the successful preview. API-error fixtures
  deliberately return 503; browser extensions may log unrelated injection errors.

Source hashes identify the exact captured code; translation provenance remains
machine-assisted without independent linguistic review.
