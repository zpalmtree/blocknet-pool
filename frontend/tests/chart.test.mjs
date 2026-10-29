import assert from 'node:assert/strict';
import { test } from 'node:test';
import { reduceChartPoints } from '../src/lib/chart.ts';

test('dense charts preserve endpoints and narrow peaks in timestamp order', () => {
  const points = Array.from({length: 9000}, (_, i) => ({ t: i * 300000, v: 100 + Math.sin(i) }));
  points[2123].v = 10000;
  points[2124].v = -500;
  for (const width of [1, 390, 1366]) {
    const result = reduceChartPoints(points, width);
    assert(result.length <= width * 2 + 2);
    assert.strictEqual(result[0], points[0]);
    assert.strictEqual(result.at(-1), points.at(-1));
    assert(result.includes(points[2123]));
    assert(result.includes(points[2124]));
    assert(result.every((p, i) => i === 0 || p.t > result[i - 1].t));
  }
  assert.equal(points.length, 9000);
});

test('sparse and empty charts keep their data unchanged', () => {
  for (const points of [[], [{t: 1, v: 0}], [{t: 1, v: 2}, {t: 2, v: 0}]]) {
    assert.strictEqual(reduceChartPoints(points, 390), points);
  }
});

test('coincident timestamps still bound drawing work and retain extrema', () => {
  const points = Array.from({length: 100}, (_, i) => ({t: 1, v: i}));
  const reduced = reduceChartPoints(points, 1);
  assert(reduced.length <= 4);
  assert.strictEqual(reduced[0], points[0]);
  assert.strictEqual(reduced.at(-1), points.at(-1));
});
