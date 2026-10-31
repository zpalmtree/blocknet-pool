import assert from 'node:assert/strict';
import { test } from 'node:test';
import { knownExchange, validMemo, shellQuote } from '../src/lib/payout-destination.ts';
import { readDestination, rememberDestination, LAST_MINER_LOOKUP_KEY } from '../src/lib/storage.ts';

test('memo validation preserves identifiers and measures UTF-8 bytes', () => {
  assert.equal(validMemo('ID000123'), true);
  assert.equal(validMemo('é'.repeat(62)), true);
  assert.equal(validMemo('é'.repeat(63)), false);
  assert.equal(validMemo('a\nb'), false);
  assert.equal(knownExchange({shared_exchange_addresses: [{address:'new',name:'Exchange'}]}, 'new').name, 'Exchange');
  assert.equal(knownExchange({shared_exchange_addresses: [{address:'new',name:'Exchange'}]}, 'old'), undefined);
  assert.equal(shellQuote("x'$(do-not-run)", false), "'x'\"'\"'$(do-not-run)'");
  assert.equal(shellQuote("x'$(do-not-run)", true), "'x''$(do-not-run)'");
});

test('saved destinations retain legacy users and separate exchange customers', () => {
  const values = new Map();
  globalThis.localStorage = { getItem: key => values.get(key) ?? null, setItem: (key,value) => values.set(key,value), removeItem: key => values.delete(key) };
  localStorage.setItem(LAST_MINER_LOOKUP_KEY, 'ordinary');
  assert.deepEqual(readDestination(), { address: 'ordinary', memo: '' });
  rememberDestination({address:'shared',memo:'ID000123'});
  assert.deepEqual(readDestination(), {address:'shared',memo:'ID000123'});
  assert.equal(localStorage.getItem(LAST_MINER_LOOKUP_KEY), null);
  rememberDestination({address:'shared',memo:'ID000124'});
  assert.equal(readDestination().memo, 'ID000124');
  rememberDestination(null);
  assert.deepEqual(readDestination(), {address:'',memo:''});
});
