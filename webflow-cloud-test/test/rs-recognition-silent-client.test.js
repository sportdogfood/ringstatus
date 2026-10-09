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
