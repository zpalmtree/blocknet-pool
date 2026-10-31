import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createDisplayFormatters } from '../src/lib/display-format.ts';

test('reused formatters preserve localized table timestamps, chart ticks and percentages', () => {
  for (const locale of ['en', 'fr', 'de', 'ar', 'fa', 'ja', 'hi']) {
    const format = createDisplayFormatters(locale);
    for (const ms of [0, Date.UTC(2026, 0, 1), Date.UTC(2026, 9, 31, 18, 5, 39)]) {
      const date = new Date(ms);
      assert.equal(format.timestamp(ms), date.toLocaleString(locale, { calendar: 'gregory' }));
      assert.equal(format.time(ms), date.toLocaleTimeString(locale, { hour: '2-digit', minute: '2-digit' }));
      for (const compact of [true, false]) {
        assert.equal(format.chartDate(ms, compact), date.toLocaleDateString(locale, {
          calendar: 'gregory', month: compact ? 'numeric' : 'short', day: 'numeric',
        }));
      }
    }
    for (const digits of [0, 1, 2, 4]) {
      for (const value of [0, 1.2345, -12.3456, 123456.789]) {
        assert.equal(format.percent(value, digits), `${new Intl.NumberFormat(locale, {
          numberingSystem: 'latn', maximumFractionDigits: digits, minimumFractionDigits: digits,
        }).format(value)}%`);
      }
    }
    assert.equal(format.timestamp(NaN), 'Invalid Date');
    assert.throws(() => format.percent(1, -1), RangeError);
  }
});

test('repeated table refreshes do not construct per-cell formatters', () => {
  const OriginalDate = Intl.DateTimeFormat;
  const OriginalNumber = Intl.NumberFormat;
  let dates = 0;
  let numbers = 0;
  Intl.DateTimeFormat = class extends OriginalDate { constructor(...args) { super(...args); dates++; } };
  Intl.NumberFormat = class extends OriginalNumber { constructor(...args) { super(...args); numbers++; } };
  try {
    const format = createDisplayFormatters('en');
    for (let refresh = 0; refresh < 10; refresh++) {
      for (let row = 0; row < 25; row++) {
        format.timestamp(Date.UTC(2026, 9, 31) + row * 1000);
        format.percent(row / 3);
        format.time(row * 1000);
        format.chartDate(row * 1000, false);
      }
    }
    assert.equal(dates, 4);
    assert.equal(numbers, 1);
  } finally {
    Intl.DateTimeFormat = OriginalDate;
    Intl.NumberFormat = OriginalNumber;
  }
});
