import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createCoinFormatters } from '../src/lib/coin-format.ts';

test('small rewards remain visible down to one atomic unit in balances and tables', () => {
  const { formatCoins, formatCompactCoins, formatCoinAmount } = createCoinFormatters('en');
  for (const [atoms, amount] of [
    [0, '0.00'], [1, '0.00000001'], [1234, '0.00001234'], [100000, '0.001'],
    [499999, '0.00499999'], [500000, '0.005'], [1000000, '0.01'],
    [99999999, '0.99999999'], [100000000, '1.00'], [123456789, '1.23'],
  ]) {
    assert.equal(formatCoinAmount(atoms), amount);
    assert.equal(formatCoins(atoms), `${amount} BNT`);
    assert.equal(formatCompactCoins(atoms), `${amount} BNT`);
  }
  assert.equal(formatCompactCoins(100000000000), '1K BNT');
  assert.equal(formatCoins(-1), '-0.00000001 BNT');
});

test('small rewards use the chosen locale and never expose invalid numbers', () => {
  const { formatCoins, formatCoinAmount } = createCoinFormatters('de');
  assert.equal(formatCoins(1234), '0,00001234 BNT');
  assert.equal(formatCoins(100000000000), '1.000,00 BNT');
  for (const value of [null, undefined, NaN, Infinity]) {
    assert.equal(formatCoinAmount(value), '0,00');
  }
});
