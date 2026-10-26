import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { blockReward, scheduledRewards, liveInputs, decimalInput, estimateRewards } from '../blog/rewards-math.js';
import { calculatorHtml } from '../scripts/rewards-calculator.mjs';
import { LOCALES } from '../src/i18n/locales.mjs';

// Independent Python/libm evaluation of blocknet-core/miner.go GetBlockReward,
// in atomic units. Includes the abrupt month-48 switch to the 2 BNT tail.
const schedule = JSON.parse(readFileSync(new URL('./reward-schedule.json', import.meta.url), 'utf8'));
test('reward schedule matches every consensus month and both sides of its boundaries', () => {
  for (let month = 0; month <= 48; month++) {
    assert.equal(blockReward(Math.max(1, month * 8640)), schedule[month] / 1e8);
    if (month > 0) assert.equal(blockReward(month * 8640 - 1), schedule[month - 1] / 1e8);
  }
  assert.equal(blockReward(1000000), 2);
});

test('day and week estimates integrate reward changes rather than multiplying today by seven', () => {
  assert.equal(scheduledRewards(69119, 1), (schedule[7] + 287 * schedule[8]) / 1e8);
  const beforeTail = 48 * 8640 - 100;
  assert.equal(scheduledRewards(beforeTail, 7), 100 * (schedule[47] / 1e8) + 1916 * 2);
  assert.equal(scheduledRewards(48 * 8640, 7), 2016 * 2);
  assert.notEqual(scheduledRewards(beforeTail, 7), scheduledRewards(beforeTail, 1) * 7);
});

test('fees are compounded, zero is meaningful, and pool-reported hashrates can skip the miner fee', () => {
  const inputs = { height: 48 * 8640, network: 1000, poolFee: 2.5 };
  const values = estimateRewards(inputs, 100, 1);
  assert.equal(values.day, 57.6);
  assert.equal(values.week, 403.20000000000005);
  assert.equal(values.netDay, 57.6 * .99 * .975);
  assert.equal(estimateRewards(inputs, 100, 0).netDay, 57.6 * .975);
  assert.deepEqual(estimateRewards(inputs, 0, 1), { day: 0, week: 0, netDay: 0, netWeek: 0 });
  assert.equal(estimateRewards(inputs, 100, 100).netDay, 0);
  assert.equal(estimateRewards({ ...inputs, poolFee: 0 }, 100, 0).netDay, 57.6);
  assert.equal(estimateRewards({ ...inputs, poolFee: 100 }, 100, 0).netDay, 0);
});

test('missing or malformed network data never becomes a zero-cost estimate', () => {
  const stats = { chain: { current_job_height: 69276, network_hashrate: 100000 } };
  assert.deepEqual(liveInputs(stats, { pool_fee_pct: 0 }), { height: 69276, network: 100000, poolFee: 0 });
  for (const fee of [undefined, null, -1, 101, '2.5', Infinity, NaN]) assert.throws(() => liveInputs(stats, { pool_fee_pct: fee }));
  for (const network of [0, -1, null, '100', Infinity, NaN]) assert.throws(() => liveInputs({ chain: { ...stats.chain, network_hashrate: network } }, { pool_fee_pct: 2.5 }));
  for (const height of [0, -1, 1.5, null, '69276', Infinity]) assert.throws(() => blockReward(height));
  for (const days of [0, -1, 1.5, 8, Infinity]) assert.throws(() => scheduledRewards(69276, days));
  for (const rate of [-1, Infinity, NaN, 100001]) assert.throws(() => estimateRewards({ height: 69276, network: 100000, poolFee: 2.5 }, rate, 1));
});

test('decimal input handles locale conventions without silently accepting malformed text', () => {
  for (const input of ['49.4', '49,4', '٤٩٫٤', '۴۹٫۴', ' 49.4 ']) assert.equal(decimalInput(input), 49.4);
  assert.equal(decimalInput('0'), 0);
  for (const input of ['', ' ', '-1', '1e3', 'Infinity', '1,234.5', '49 H/s', '1 234', '0x10']) assert.ok(Number.isNaN(decimalInput(input)), input);
});

test('every calculator has localized controls, a non-JavaScript explanation and accessible empty state', () => {
  const english = JSON.parse(readFileSync(new URL('../blog/messages/en.json', import.meta.url), 'utf8'));
  const keys = Object.keys(english).filter(key => key.startsWith('calc'));
  for (const { code } of LOCALES) {
    const messages = JSON.parse(readFileSync(new URL(`../blog/messages/${code}.json`, import.meta.url), 'utf8'));
    for (const key of keys) {
      assert.ok(messages[key]?.trim(), `${code}:${key}`);
      assert.deepEqual(messages[key].match(/\{\w+\}/g) ?? [], english[key].match(/\{\w+\}/g) ?? []);
      // Short labels can legitimately match English (Dutch: "Per week").
      if (code !== 'en' && english[key].length > 20) assert.notEqual(messages[key], english[key]);
    }
    const html = calculatorHtml(code);
    assert.ok(html.includes('id="mining-calculator"'));
    assert.ok(html.includes('<noscript>'));
    assert.ok(html.includes('<fieldset disabled>'));
    assert.ok(html.includes('aria-live="polite" hidden'));
    assert.ok(!html.includes('undefined'));
  }
});

test('calculator identifies its reviewed release and uses its published bntpool fee in every locale', () => {
  const release = JSON.parse(readFileSync(new URL('../blog/miner-release.json', import.meta.url)));
  // Evidence: v0.2.26 src/dev_fee.rs, BNTPOOL_DEV_FEE_PERCENT = 1.0.
  assert.equal(release.tag, 'v0.2.26');
  assert.equal(release.bntpoolMinerFeePercent, 1);
  for (const { code } of LOCALES) {
    const html = calculatorHtml(code);
    assert.match(html, /id="calc-fee"[^>]*value="1"/);
    assert.ok(html.includes('v0.2.26'));
    assert.ok(!/\{(?:version|fee)\}/.test(html));
  }
});
