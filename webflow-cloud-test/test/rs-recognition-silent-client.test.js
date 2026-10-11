import test from 'node:test';
import assert from 'node:assert/strict';
import * as client from '../src/assets/rs-recognition/native-client.js';

test('silent visit sends a remembered device, refreshes expiry and redirects home without UI', async () => {
  const values = new Map([['rs_recognition_device_token_v1', '81a81b40-f954-4ad5-8911-8fc3bf91c4a1']]);
  const storage = { getItem: k => values.get(k), setItem: (k,v) => values.set(k,v), removeItem: k => values.delete(k) };
  const navigations = [];
  const result = await client.runSilentRecognition({ endpoint: 'https://example.invalid/test/rs-inputs/native-recognition',
    persistentStorage: storage, sessionStorageImpl: storage, now: () => 1000, uuid: () => 'session-001', pagePath: '/rs-recognize?private=excluded',
    navigate: path => navigations.push(path), fetchImpl: async (url, options) => {
      assert.equal(new URL(url).searchParams.get('operation'), 'recognize');
      assert.equal(options.credentials, 'same-origin');
      assert.equal(JSON.parse(options.body).device_token, values.get('rs_recognition_device_token_v1'));
      assert.equal(JSON.parse(options.body).page_path, '/rs-recognize');
      return Response.json({ ok: true, recognized: true });
    } });
  assert.equal(result, true); assert.deepEqual(navigations, ['/']);
  assert.equal(Number(values.get('rs_recognition_device_expires_v1')), 1000 + 365*86400000);
});

test('expired local recognition cannot resurrect a token, and errors never redirect', async () => {
  const values = new Map([['rs_recognition_device_token_v1', 'expired'], ['rs_recognition_device_expires_v1','1']]);
  const storage = { getItem:k=>values.get(k), setItem:(k,v)=>values.set(k,v), removeItem:k=>values.delete(k) };
  let redirected = false;
  const result = await client.runSilentRecognition({ endpoint:'https://example.invalid/test/rs-inputs/native-recognition',
    persistentStorage:storage, sessionStorageImpl:storage, now:()=>1000, uuid:()=> 'new-device', navigate:()=>{redirected=true;},
    fetchImpl:async (_url,options)=>{assert.notEqual(JSON.parse(options.body).device_token,'expired'); return Response.json({ok:false,error:'unavailable'},{status:503});} });
  assert.equal(result,false); assert.equal(redirected,false);
});

for (const pagePath of ['/', '/ring-status', '/about-ringstatus']) {
  for (const outcome of ['known', 'unknown', 'failure', 'network-error']) {
    test(`public silent ${outcome} visit stays on ${pagePath}`, async () => {
      const values = new Map([
        ['rs_recognition_device_token_v1', 'remembered-device'],
        ['rs_native_recognition_session_v1', 'remembered-session'],
      ]);
      const storage = { getItem: k => values.get(k), setItem: (k,v) => values.set(k,v), removeItem: k => values.delete(k) };
      const navigations = [];
      const result = await client.runSilentRecognition({
        endpoint: 'https://example.invalid/test/rs-inputs/native-recognition',
        persistentStorage: storage, sessionStorageImpl: storage,
        pagePath, redirectOnRecognized: false, now: () => 1000,
        navigate: path => navigations.push(path),
        fetchImpl: async (url, options) => {
          assert.equal(new URL(url).searchParams.get('operation'), 'recognize');
          assert.equal(options.method, 'POST');
          assert.equal(options.credentials, 'same-origin');
          assert.deepEqual(JSON.parse(options.body), {
            device_token: 'remembered-device', session_uid: 'remembered-session', page_path: pagePath,
          });
          if (outcome === 'network-error') throw new Error('network unavailable');
          if (outcome === 'failure') return Response.json({ ok: false }, { status: 503 });
          return Response.json({ ok: true, recognized: outcome === 'known' });
        },
      });
      assert.equal(result, outcome === 'known');
      assert.deepEqual(navigations, []);
      assert.equal(values.get('rs_recognition_device_token_v1'), 'remembered-device');
      assert.equal(values.get('rs_native_recognition_session_v1'), 'remembered-session');
      assert.equal(values.get('rs_recognition_device_expires_v1'),
        outcome === 'known' ? String(1000 + 365 * 86400000) : undefined);
    });
  }
}

test('gated silent success keeps its default redirect to the member launcher', async () => {
  const values = new Map();
  const storage = { getItem: k => values.get(k), setItem: (k,v) => values.set(k,v), removeItem: k => values.delete(k) };
  const navigations = [];
  const result = await client.runSilentRecognition({
    endpoint: 'https://example.invalid/test/rs-inputs/native-recognition',
    persistentStorage: storage, sessionStorageImpl: storage,
    pagePath: '/rs-recognize', launcherPath: '/rs-member', uuid: () => 'test-uid',
    navigate: path => navigations.push(path),
    fetchImpl: async () => Response.json({ ok: true, recognized: true }),
  });
  assert.equal(result, true);
  assert.deepEqual(navigations, ['/rs-member']);
});
