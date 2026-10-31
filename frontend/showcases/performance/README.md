# Pool frontend performance

This showcase runs the compiled production SPA with synthetic API fixtures.
It never contacts the pool or requires credentials.

From the repository root:

```sh
npm --prefix frontend ci
npm --prefix frontend run build
npm --prefix frontend run showcase:performance
```

- Interactive app: http://localhost:5195/?demo=success&lang=en
- Reviewed gallery: http://localhost:5195/showcases/performance/index.html
- Dashboard: `demo=success`, `empty`, `loading`, `error`, `race`.
- Miner stats: `/stats?demo=farm` (47 workers and 26 payouts), or `demo=empty`.
- Deferred pages: `/stats?demo=chunk-loading` or `demo=chunk-error`.
  These faults are scoped to this single-user preview server. Let a slow download
  finish before switching fault modes. The recovery link removes the fault.
- Add `lang=fr` or another supported locale. Themes use the normal app control.

The stable workspace gallery is http://localhost:18976/:

```sh
python3 -m http.server 18976 --directory /home/zp/Code/blocknet/showcases/pool-performance
```

## October 31 rendering and formatting pass

The supplied Firefox profile covers about 19 seconds of navigation and scrolling.
The pool main thread is 96% idle, with only 0.6% of samples categorized as
JavaScript and a maximum sampled event delay of 7.52 ms. The shared renderer uses
software WebRender (SWGL); compositor scroll-presentation markers reach 81.8 ms.
Renderer samples are browser-wide and cannot all be attributed to this page.
The raw profile, browsing data and addresses are intentionally excluded here.

The sticky navigation previously applied a 16px backdrop blur and saturation
filter to scrolling content. It now uses the same theme colors opaquely, with no
backdrop filter. Sticky positioning, borders, shadows and controls are retained.
This removes a recurring rendering operation. A new Firefox recording would be
needed to quantify the scroll-latency improvement on the original setup; Chrome
visual checks are not a substitute for that measurement.

Table timestamps, chart date/time labels and percentages now reuse locale-specific
Intl formatters. The formatting benchmark performs 250 timestamp/percentage pairs
(ten 25-row refreshes) with seven alternating before/after pairs after warm-up.
In shared Windows Chrome, median formatting time was 11.3 ms before and 0.3 ms
after. This is a synthetic formatting measurement, not an overall page speedup.
The allocation regression test verifies four date formatters and one percentage
formatter across those refreshes, rather than one formatter per cell.

To repeat the benchmark in the showcase browser console:

```js
(await import('/showcases/performance/benchmark.mjs')).benchmarkFormatting()
```

Earlier deferred-page loading, bounded history caching and chart point reduction
remain in place. This pass changes no API contracts, polling rates, balances,
payout accounting, daemon behavior or Stratum behavior.

## Verification

- All 33 frontend tests pass. New tests compare formatting with the previous
  implementation across seven locales, several dates and percentage precisions,
  and check formatter reuse.
- The full production build passes TypeScript, 20-language validation, static
  page/blog generation and existing cover asset budgets.
- Eighteen reviewed Playwright MCP captures show the compiled app at 1366px and
  390px: dashboard success/empty/loading/error, disabled controls, populated/empty
  miner stats, deferred page loading/failure and dark navigation while scrolled.
- Browser checks confirm opaque, unfiltered headers in both themes, sticky
  positioning, mobile menu open/navigation/close, chart range selection, localized
  timestamp tooltips and percentages (English, French and Arabic), no document
  overflow in inspected states and successful page-download recovery.
- Success flows have no application console errors. Download-failure fixtures
  deliberately return 404; API-failure fixtures return 503.
- Source hashes and benchmark samples are recorded in `verification.json`.

No backend code changed; Rust tests were not rerun for this CSS/formatting change.
Deploy with the normal API-only path when authorized. No Stratum or daemon restart
is needed. This pass does not deploy itself.
