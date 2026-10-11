import test from 'node:test';
import assert from 'node:assert/strict';
import { runSilentRecognition } from '../src/assets/rs-recognition/native-client.js';

const key = 'rs_native_recognition_confirmation_v1';
function setup() {
  const values = new Map([[key, 'stale'], ['rs_native_recognition_session_v1', 'tab-session']]);
  const storage = { getItem: k => values.get(k), setItem: (k, v) => values.set(k, v), removeItem: k => values.delete(k) };
  return { values, storage, args: { endpoint: 'https://example.invalid/test/rs-inputs/native-recognition',
    persistentStorage: storage, sessionStorageImpl: storage, now: () => 1000,
    uuid: () => 'new-device', redirectOnRecognized: false, navigate: () => assert.fail('must stay public') } };
}

test('confirmation contains only existing server success, tab ID and observation time; every revisit revalidates', async () => {
  const f = setup(); let calls = 0;
  const fetchImpl = async () => {
    calls++;
    assert.equal(f.values.has(key), false, 'stale confirmation cleared before request');
    return Response.json({ ok: true, recognized: true, first_name: 'Private', person_name: 'Private Member' });
  };
  for (let i = 0; i < 2; i++) {
    assert.equal(await runSilentRecognition({ ...f.args, fetchImpl }), true);
    assert.deepEqual(JSON.parse(f.values.get(key)), { session_uid: 'tab-session', confirmed: true, confirmed_at: 1000 });
  }
  assert.equal(calls, 2);
});

for (const outcome of ['rejected', 'http-error', 'network-error', 'bad-json']) {
  test(`${outcome} removes prior confirmation and never navigates`, async () => {
    const f = setup();
    assert.equal(await runSilentRecognition({ ...f.args, fetchImpl: async () => {
      if (outcome === 'network-error') throw new Error('offline');
      if (outcome === 'bad-json') return new Response('not json');
      return Response.json({ ok: outcome === 'rejected', recognized: false }, { status: outcome === 'http-error' ? 503 : 200 });
    } }), false);
    assert.equal(f.values.has(key), false);
  });
}

test('local expiry clears confirmation before revalidation and replaces the expired fallback token', async () => {
  const f = setup();
  f.values.set('rs_recognition_device_token_v1', 'expired-device');
  f.values.set('rs_recognition_device_expires_v1', '1');
  await runSilentRecognition({ ...f.args, fetchImpl: async (_url, options) => {
    assert.equal(f.values.has(key), false);
    assert.equal(JSON.parse(options.body).device_token, 'new-device');
    return Response.json({ ok: true, recognized: false });
  } });
  assert.equal(f.values.has(key), false);
});

test('an older success cannot restore confirmation after a newer rejection', async () => {
  const f = setup(); let finish;
  const earlier = runSilentRecognition({ ...f.args, fetchImpl: () => new Promise(resolve => { finish = resolve; }) });
  assert.equal(await runSilentRecognition({ ...f.args, fetchImpl: async () => Response.json({ ok: true, recognized: false }) }), false);
  finish(Response.json({ ok: true, recognized: true }));
  assert.equal(await earlier, false);
  assert.equal(f.values.has(key), false);
});

test('an older transport failure cannot erase a newer server confirmation', async () => {
  const f = setup(); let fail;
  const earlier = runSilentRecognition({ ...f.args, fetchImpl: () => new Promise((_resolve, reject) => { fail = reject; }) });
  assert.equal(await runSilentRecognition({ ...f.args, fetchImpl: async () => Response.json({ ok: true, recognized: true }) }), true);
  fail(new Error('old request failed'));
  assert.equal(await earlier, false);
  assert.equal(JSON.parse(f.values.get(key)).confirmed, true);
});

for (const error of ['session_identity_conflict', 'session_closed']) {
  test(`${error} clears confirmation and rotates only for the next normal check`, async () => {
    const f = setup(); let calls = 0;
    assert.equal(await runSilentRecognition({ ...f.args, fetchImpl: async () => {
      calls++; return Response.json({ ok: false, recognized: false, error }, { status: 409 });
    } }), false);
    assert.equal(calls, 1); assert.equal(f.values.has(key), false);
    assert.equal(f.values.get('rs_native_recognition_session_v1'), 'new-device');
  });
}
