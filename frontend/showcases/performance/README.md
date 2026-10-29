# Pool frontend performance

This showcase runs the compiled production SPA with synthetic API fixtures.
It covers the dashboard chart, deferred miner/admin pages, bounded history reuse,
and page-download recovery without contacting the pool or requiring credentials.

From the repository root:

```sh
npm --prefix frontend ci
npm --prefix frontend run build
npm --prefix frontend run showcase:performance
```

- Interactive app: http://localhost:5195/?demo=success&lang=en
- Reviewed gallery: http://localhost:5195/showcases/performance/index.html
- Dashboard states: `demo=success`, `empty`, `loading`, `error`, `race`.
- Miner stats: `/stats?demo=farm` (47 workers and 26 payouts), or `demo=empty`.
- Deferred page states: `/stats?demo=chunk-loading` or `demo=chunk-error`.
  The download fixture is scoped to this single-user preview server. Let a slow
  download finish before switching fault modes. The recovery link reloads the
  normal route, removing the fixture fault.
- Add `lang=fr` or another supported locale. Themes use the normal app control.

A stable workspace copy of the reviewed gallery is available at
`showcases/pool-performance` in the parent workspace:

```sh
python3 -m http.server 18976 --directory /home/zp/Code/blocknet/showcases/pool-performance
```

Open http://localhost:18976/. Source hashes are in `verification.json`.

## Results and limits

On the same public-source build (without optional private integrations), entry
JavaScript fell from 319,868 to 234,335 bytes; gzip fell from 94,632 to 73,846 bytes.
Stats, admin and optional page code now load separately, with focus/hover preloads.
The Rust API embeds the emitted chunks and gives their hashed URLs immutable
caching. CSS remains one shared file. A failed chunk has a localized recovery
link that reloads the current entry point.

For an 8,640-point chart at 1,080 CSS pixels, drawing makes 4,322 curve calls per
pass, versus 17,276 before reduction. Updates draw once instead of twice;
normalization and smoothing are reused across resize/theme changes. Sampling
preserves time order, endpoints and each screen bucket's extrema. These are
work-count measurements, not claims about frame rate on every device.

Only chart history is cached: 30 seconds for pool history and 10 seconds for
miner history, matching the existing refresh cadence. The cache is per client,
in memory, capped at 12 entries, and never stores failures. Balances, payouts,
stats, operator reads and writes retain their previous behavior. The initial
history response is unchanged; this pass does not reduce its wire size.

The browser race check selected 30d and then 1h while 30d was delayed. The late
response caused no extra draw and 1h remained selected. Returning to 30d reused
its fresh response, made no extra fetch and drew once. A production asset failure
showed the recovery link; reloading successfully rendered the page again.

## Verification

- 27 frontend tests pass, including cache expiry/isolation/eviction/failure cases
  and chart sampling with dense, empty and coincident timestamps.
- Full production build passes type checking, 20-language catalog validation,
  static page/blog generation and existing cover asset budgets.
- All 15 Rust UI tests pass, including exact bytes, MIME/cache headers and unknown
  paths for generated chunks. The existing ignored HTML-export helper is not a
  runtime test.
- Sixteen reviewed real-app captures cover desktop (1366px) and mobile (390px):
  dense success, empty, loading, API error, paged miner results, disabled controls,
  page loading and failed page download. No document overflow in these states.
- Chrome checks also cover paging, dark theme, French lazy loading, and admin
  entry. Success flows have no application console errors. Fault fixtures produce
  their expected errors; browser extensions may log unrelated injection messages.

Deploy through the normal API-only build/deploy path when authorized. No Stratum
or daemon changes are needed, and this performance pass does not deploy itself.
