import { InputError } from './rs-inputs.js';

// US production only. Organization is independently checked by the CRM adapter.
// Credentials remain in runtime bindings; neither connector credentials nor browser
// values are accepted here. Never automatically replay a data write on auth failure.
export function createZohoTokenProvider({ env, fetchImpl = fetch, now = Date.now }) {
  const keys = ['ZOHO_CLIENT_ID', 'ZOHO_CLIENT_SECRET', 'ZOHO_REFRESH_TOKEN'];
  if (keys.some(key => typeof env[key] !== 'string' || !env[key].trim())) {
    throw new InputError('crm_credentials_missing', 503);
  }
  let token = '', expiresAt = 0, pending;
  return async function getToken() {
    if (token && now() < expiresAt) return token;
    if (!pending) {
      pending = (async () => {
        let response;
        try {
          response = await fetchImpl('https://accounts.zoho.com/oauth/v2/token', {
            method: 'POST', redirect: 'error', signal: AbortSignal.timeout(15000),
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({ grant_type: 'refresh_token', client_id: env.ZOHO_CLIENT_ID,
              client_secret: env.ZOHO_CLIENT_SECRET, refresh_token: env.ZOHO_REFRESH_TOKEN }).toString()
          });
        } catch { throw new InputError('crm_authorization_unavailable', 503); }
        const data = await response.json().catch(() => null);
        if (!response.ok || data?.error || typeof data?.access_token !== 'string' || !data.access_token ||
            !Number.isFinite(data.expires_in) || data.expires_in <= 60) {
          throw new InputError('crm_authorization_failed', 503);
        }
        if (data.api_domain !== 'https://www.zohoapis.com') throw new InputError('wrong_crm_api_domain', 503);
        token = data.access_token;
        expiresAt = now() + (Math.min(data.expires_in, 3600) - 60) * 1000;
        return token;
      })();
    }
    try { return await pending; } finally { pending = undefined; }
  };
}

// Reuse within a Worker without retaining arbitrary callers/credentials in a map.
const providers = new WeakMap();
export function runtimeZohoTokenProvider(env, fetchImpl = fetch) {
  let cached = providers.get(env);
  if (!cached || cached.fetchImpl !== fetchImpl || cached.clientId !== env.ZOHO_CLIENT_ID ||
      cached.clientSecret !== env.ZOHO_CLIENT_SECRET || cached.refreshToken !== env.ZOHO_REFRESH_TOKEN) {
    cached = { fetchImpl, clientId: env.ZOHO_CLIENT_ID, clientSecret: env.ZOHO_CLIENT_SECRET,
      refreshToken: env.ZOHO_REFRESH_TOKEN, getToken: createZohoTokenProvider({ env, fetchImpl }) };
    providers.set(env, cached);
  }
  return cached.getToken;
}
