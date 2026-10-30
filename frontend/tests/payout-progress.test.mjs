import assert from 'node:assert/strict';
import { test } from 'node:test';
import { minimumPayoutAtoms, payoutProgress } from '../src/lib/payout-progress.ts';
import { createCoinFormatters } from '../src/lib/coin-format.ts';

test('payout progress uses confirmed atomic units and preserves the final atomic unit', () => {
  const minimum = minimumPayoutAtoms(1);
  assert.deepEqual(payoutProgress(minimum, 379117, 0), { state: 'below', amount: 99620883 });
  assert.deepEqual(payoutProgress(minimum, 99999999, 0), { state: 'below', amount: 1 });
  assert.equal(payoutProgress(minimum, 100000000, 0).state, 'met');
  assert.equal(payoutProgress(minimum, 200000000, 0).state, 'met');
  assert.equal(payoutProgress(minimum, 0, 0).amount, minimum);
  assert.equal(payoutProgress(0, 0, 0).state, 'empty');
  assert.equal(payoutProgress(0, 1, 0).state, 'met');
  const { formatExactCoins } = createCoinFormatters('en');
  assert.equal(formatExactCoins(minimumPayoutAtoms(5) - 1), '4.99999999 BNT');
  assert.equal(formatExactCoins(1), '0.00000001 BNT');
  assert.equal(formatExactCoins(minimumPayoutAtoms(1.23456789)), '1.23456789 BNT');
});

test('queued payouts are not mistaken for a new shortfall; unavailable data is not zero', () => {
  assert.deepEqual(payoutProgress(100000000, 100000000, 100000000), { state: 'queued', amount: 100000000 });
  assert.equal(payoutProgress(100000000, 0, 100000000).state, 'queued');
  for (const balance of [undefined, NaN, -1]) {
    assert.equal(payoutProgress(100000000, balance, 0).state, 'unavailable');
  }
  for (const minimum of [undefined, null, NaN, Infinity, -1, 1e20]) {
    assert.equal(minimumPayoutAtoms(minimum), null);
  }
  assert.equal(minimumPayoutAtoms(0), 0);
  assert.equal(minimumPayoutAtoms(0.00000001), 1);
});
