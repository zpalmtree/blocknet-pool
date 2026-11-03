import test from 'node:test';
import assert from 'node:assert/strict';
import { seineSupportsSolo } from '../src/lib/solo.ts';

test('the solo guide waits for a Seine release after v0.3.1', () => {
  for (const version of ['0.3.1', 'v0.3.1', '0.3.0', '0.2.44', '', 'next', undefined, null]) {
    assert.equal(seineSupportsSolo(version), false, String(version));
  }
  for (const version of ['0.3.2', 'v0.3.2', '0.4.0', '1.0.0', 'v0.3.10-rc1']) {
    assert.equal(seineSupportsSolo(version), true, version);
  }
});
