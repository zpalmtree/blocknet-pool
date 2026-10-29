import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createApiClient } from '../src/api/client.ts';

test('overlapping reads share a request; a later refresh fetches again', async () => {
  let calls = 0;
  let finish;
  globalThis.fetch = () => {
    calls++;
    return new Promise((resolve) => { finish = resolve; });
  };
  const api = createApiClient(() => '', () => {});
  const first = api.getStats();
  const second = api.getStats();
  assert.equal(calls, 1);
  finish(Response.json({ workers: 10 }));
  assert.deepEqual(await first, await second);
  const third = api.getStats();
  assert.equal(calls, 2);
  finish(Response.json({ workers: 11 }));
  assert.equal((await third).workers, 11);
});

test('429 and 503 respect Retry-After across telemetry routes without repeated toasts', async () => {
  const originalNow = Date.now;
  try {
    for (const status of [429, 503]) {
      let now = 10000;
      Date.now = () => now;
      let calls = 0;
      const errors = [];
      globalThis.fetch = async () => {
        calls++;
        return Response.json({ error: 'busy' }, { status, headers: { 'retry-after': '5' } });
      };
      const api = createApiClient(() => '', (error) => errors.push(error));
      await assert.rejects(api.getStats());
      await assert.rejects(api.getMinerBalance('miner'));
      assert.equal(calls, 1);
      assert.equal(errors.length, 1);
      now += 5000;
      await assert.rejects(api.getStats());
      assert.equal(calls, 2);
    }
  } finally {
    Date.now = originalNow;
  }
});

test('reward job polls 202 and only returns the completed breakdown', async () => {
  const originalTimeout = globalThis.setTimeout;
  try {
    const delays = [];
    globalThis.setTimeout = (callback, delay) => {
      delays.push(delay);
      queueMicrotask(callback);
      return 0;
    };
    let calls = 0;
    globalThis.fetch = async (_path, options) => {
      assert.equal(options.headers['x-api-key'], 'test-key');
      calls++;
      return calls === 1
        ? Response.json({ status: 'pending' }, { status: 202, headers: { 'retry-after': '1' } })
        : Response.json({ block: { height: 123 }, participants: [] });
    };
    const api = createApiClient(() => 'test-key', () => {});
    const [a, b] = await Promise.all([
      api.getAdminBlockRewardBreakdown(123), api.getAdminBlockRewardBreakdown(123),
    ]);
    assert.deepEqual(a, b);
    assert.equal(a.block.height, 123);
    assert.equal(calls, 2);
    assert.deepEqual(delays, [1000]);
  } finally {
    globalThis.setTimeout = originalTimeout;
  }
});

test('history cache reuses fresh ranges and expires at the polling interval', async () => {
  const originalNow = Date.now;
  const originalFetch = globalThis.fetch;
  try {
    let now = 1000;
    Date.now = () => now;
    const calls = [];
    globalThis.fetch = async (path) => {
      calls.push(path);
      return Response.json([{ timestamp: now, hashrate: calls.length }]);
    };
    const api = createApiClient(() => '', () => {});
    const week = await api.getStatsHistory('7d');
    await api.getStatsHistory('30d');
    assert.strictEqual(await api.getStatsHistory('7d'), week);
    assert.equal(calls.length, 2);
    now += 30000;
    assert.notDeepEqual(await api.getStatsHistory('7d'), week);
    assert.equal(calls.length, 3);
    const miner = await api.getMinerHashrate('alice', '1h');
    assert.strictEqual(await api.getMinerHashrate('alice', '1h'), miner);
    await api.getMinerHashrate('bob', '1h');
    now += 10000;
    assert.notDeepEqual(await api.getMinerHashrate('alice', '1h'), miner);
    assert.equal(calls.length, 6);
  } finally {
    Date.now = originalNow;
    globalThis.fetch = originalFetch;
  }
});

test('history cache bounds retained ranges and does not cache failures', async () => {
  const originalFetch = globalThis.fetch;
  try {
    let calls = 0;
    globalThis.fetch = async () => { calls++; return Response.json([]); };
    const api = createApiClient(() => '', () => {});
    for (let i = 0; i < 13; i++) await api.getMinerHashrate(String(i), '1h');
    await api.getMinerHashrate('0', '1h');
    assert.equal(calls, 14);
    globalThis.fetch = async () => { calls++; return Response.json({}, {status: 500}); };
    await assert.rejects(api.getStatsHistory('7d'));
    globalThis.fetch = async () => { calls++; return Response.json([]); };
    assert.deepEqual(await api.getStatsHistory('7d'), []);
    assert.equal(calls, 16);
  } finally { globalThis.fetch = originalFetch; }
});
