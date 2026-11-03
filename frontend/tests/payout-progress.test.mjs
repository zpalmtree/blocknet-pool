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

test('the front page payout block fills toward the account minimum and marks it once passed', async () => {
  const { heroPayout } = await import('../src/lib/payout-progress.ts');
  const minimum = minimumPayoutAtoms(1);
  assert.deepEqual(heroPayout(minimum, 42_000_000, 0), { state: 'below', confirmed: 42_000_000, minimum, remaining: 58_000_000, fillPct: 42 });
  assert.deepEqual(heroPayout(minimum, 0, 0), { state: 'below', confirmed: 0, minimum, remaining: minimum, fillPct: 0 });
  const ready = heroPayout(minimum, 200_000_000, 0);
  assert.equal(ready.state, 'ready');
  assert.equal(ready.minimumPct, 50);
  assert.deepEqual(heroPayout(minimum, 184_000_000, 184_000_000), { state: 'queued', amount: 184_000_000 });
  assert.deepEqual(heroPayout(null, 1, 0), { state: 'unavailable' });
  assert.deepEqual(heroPayout(minimum, undefined, 0), { state: 'unavailable' });
  // Exchange accounts carry their own, higher minimum.
  const exchange = minimumPayoutAtoms(10);
  assert.equal(heroPayout(exchange, 420_000_000, 0).state, 'below');
  assert.equal(heroPayout(exchange, 1_240_000_000, 0).minimumPct.toFixed(1), '80.6');
});
