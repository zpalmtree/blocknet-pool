import { createDisplayFormatters } from '../../src/lib/display-format.ts';

// Run from this showcase's browser console:
// (await import('/showcases/performance/benchmark.mjs')).benchmarkFormatting()
// A synthetic formatting workload, not a page-load or frame-rate benchmark.
export async function benchmarkFormatting() {
  const format = createDisplayFormatters('en');
  const timestamp = Date.UTC(2026, 9, 31, 18, 5, 39);
  const previous = () => {
    for (let row = 0; row < 250; row++) {
      new Date(timestamp + row * 1000).toLocaleString('en', { calendar: 'gregory' });
      new Intl.NumberFormat('en', {
        numberingSystem: 'latn', minimumFractionDigits: 1, maximumFractionDigits: 1,
      }).format(row / 3);
    }
  };
  const current = () => {
    for (let row = 0; row < 250; row++) {
      format.timestamp(timestamp + row * 1000);
      format.percent(row / 3);
    }
  };
  const measure = (run) => {
    const start = performance.now();
    run();
    return performance.now() - start;
  };
  previous();
  current();
  const before = [];
  const after = [];
  for (let pair = 0; pair < 7; pair++) {
    if (pair % 2) {
      after.push(measure(current));
      before.push(measure(previous));
    } else {
      before.push(measure(previous));
      after.push(measure(current));
    }
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  const median = (values) => [...values].sort((a, b) => a - b)[3];
  return { rows: 250, pairs: 7, before_ms: before, after_ms: after,
    before_median_ms: median(before), after_median_ms: median(after) };
}
