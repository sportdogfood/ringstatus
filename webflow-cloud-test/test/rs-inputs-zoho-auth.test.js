import test from 'node:test';
import assert from 'node:assert/strict';
import { createZohoTokenProvider, runtimeZohoTokenProvider } from '../src/lib/rs-inputs-zoho-auth.js';
const env = { ZOHO_CLIENT_ID: 'fixture-client', ZOHO_CLIENT_SECRET: 'fixture-secret', ZOHO_REFRESH_TOKEN: 'fixture-refresh' };
const success = { access_token: 'fixture-access', expires_in: 3600, api_domain: 'https://www.zohoapis.com' };

test('OAuth requires app credentials before any network request', () => {
  assert.throws(() => createZohoTokenProvider({ env: {} }), { code: 'crm_credentials_missing' });
});
test('OAuth refresh keeps secrets out of URLs, caches, coalesces and refreshes before expiry', async () => {
  let now = 0, calls = 0;
  const getToken = createZohoTokenProvider({ env, now: () => now, fetchImpl: async (url, options) => {
    calls++; assert.equal(url, 'https://accounts.zoho.com/oauth/v2/token');
    assert.equal(options.redirect, 'error'); assert.equal(options.method, 'POST');
    assert.equal(new URLSearchParams(options.body).get('refresh_token'), env.ZOHO_REFRESH_TOKEN);
    return Response.json({ ...success, access_token: `fixture-${calls}` });
  } });
  assert.deepEqual(await Promise.all([getToken(), getToken()]), ['fixture-1', 'fixture-1']);
  assert.equal(calls, 1); now = 3500000; assert.equal(await getToken(), 'fixture-1');
  now = 3540000; assert.equal(await getToken(), 'fixture-2'); assert.equal(calls, 2);
});
test('OAuth rejects wrong region and redacts provider errors', async () => {
  for (const body of [{ error: 'invalid_client', secret: env.ZOHO_CLIENT_SECRET }, { ...success, api_domain: 'https://www.zohoapis.eu' }, { ...success, expires_in: 0 }]) {
    const getToken = createZohoTokenProvider({ env, fetchImpl: async () => Response.json(body) });
    await assert.rejects(getToken(), e => e.status === 503 && !JSON.stringify(e).includes(env.ZOHO_CLIENT_SECRET));
  }
});
test('runtime provider isolates bindings and discards cached credential on rotation', () => {
  const bindings = { ...env }, f = () => {};
  const a = runtimeZohoTokenProvider(bindings, f);
  assert.equal(runtimeZohoTokenProvider(bindings, f), a);
  assert.notEqual(runtimeZohoTokenProvider({ ...bindings }, f), a);
  bindings.ZOHO_REFRESH_TOKEN = 'rotated'; assert.notEqual(runtimeZohoTokenProvider(bindings, f), a);
});
