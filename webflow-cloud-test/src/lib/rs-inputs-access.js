// Invitation access for the owned isolated trial. Recognition is not authentication.
import { InputError } from './rs-inputs.js';
const BASE = 'app9kOZdIaGyKk5uG';
const COOKIE = '__Secure-rs_input_access';
const TTL = 8 * 60 * 60;
const encoder = new TextEncoder();
const b64 = bytes => btoa(String.fromCharCode(...bytes)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
const unb64 = value => Uint8Array.from(atob(value.replace(/-/g, '+').replace(/_/g, '/')), c => c.charCodeAt(0));
export const randomAccessToken = () => b64(crypto.getRandomValues(new Uint8Array(32)));
export const accessHash = async value => [...new Uint8Array(await crypto.subtle.digest('SHA-256', encoder.encode(value)))].map(b => b.toString(16).padStart(2, '0')).join('');
const fail = (code, status = 401) => { throw new InputError(code, status); };
const permitted = row => ['invited', 'approved'].includes(row?.fields?.input_access) && ['active', 'test'].includes(String(row?.fields?.status).toLowerCase());
const validUid = value => typeof value === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(value);
function actorOf(row) {
  if (!permitted(row) || !validUid(row.fields.person_uid)) fail('authentication_required');
  return { id: row.fields.person_uid, barnIds: [], permissions: ['inputs:read', 'inputs:write', 'barns:create'], profile: { personUid: row.fields.person_uid, name: row.fields.person_name || '', email: row.fields.email || '' } };
}
export function createAccessStore({ env, fetchImpl = fetch }) {
  if (env.RS_INPUTS_BASE_ID !== BASE || (env.RS_INPUTS_RECOGNITION_BASE_ID && env.RS_INPUTS_RECOGNITION_BASE_ID !== BASE) || env.RS_INPUTS_WRITE_MODE !== 'isolated-trial') fail('access_trial_configuration_required', 503);
  if (!env.AIRTABLE_TOKEN) fail('storage_credentials_missing', 503);
  const endpoint = `https://api.airtable.com/v0/${BASE}/rs_people_test`;
  async function call(method, { formula, fields, id } = {}) {
    const url = new URL(endpoint);
    if (formula) { url.searchParams.set('filterByFormula', formula); url.searchParams.set('maxRecords', '2'); }
    let response;
    try { response = await fetchImpl(url, { method, redirect: 'error', headers: { Authorization: `Bearer ${env.AIRTABLE_TOKEN}`, 'Content-Type': 'application/json' }, signal: AbortSignal.timeout(15000), ...(fields ? { body: JSON.stringify({ records: [{ ...(id ? { id } : {}), fields }] }) } : {}) }); }
    catch { fail(method === 'GET' ? 'storage_unavailable' : 'write_outcome_unknown', 503); }
    const body = await response.json().catch(() => null);
    if (!response.ok || !Array.isArray(body?.records)) fail(method === 'GET' ? 'storage_unavailable' : 'write_outcome_unknown', 503);
    if (body.offset || body.records.length > 1) fail('ambiguous_access_identity', 409);
    return body.records[0] || null;
  }
  return {
    async byUid(uid) { if (!validUid(uid)) fail('authentication_required'); return call('GET', { formula: `{person_uid} = '${uid}'` }); },
    async byHash(hash) { if (!/^[a-f0-9]{64}$/.test(hash)) fail('invalid_invitation'); return call('GET', { formula: `{input_invite_hash} = '${hash}'` }); },
    async update(id, fields) { return call('PATCH', { id, fields }); }
  };
}
function scope(request) {
  const url = new URL(request.url);
  const path = url.pathname.slice(0, url.pathname.lastIndexOf('/'));
  if (url.protocol !== 'https:' || !/^\/[A-Za-z0-9_/-]*rs-inputs$/.test(path)) fail('invalid_access_origin', 400);
  return { audience: `${url.origin}${path}`, path };
}
async function signingKey(env) {
  if (!/^[a-fA-F0-9]{64}$/.test(env.RS_INPUTS_SESSION_SECRET || '')) fail('access_secret_missing', 503);
  const bytes = Uint8Array.from(env.RS_INPUTS_SESSION_SECRET.match(/../g), b => parseInt(b, 16));
  return crypto.subtle.importKey('raw', bytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign', 'verify']);
}
const cookie = (value, path, age = TTL) => `${COOKIE}=${value}; Path=${path}; Max-Age=${age}; HttpOnly; Secure; SameSite=Strict`;
export function createInputAccess({ env, fetchImpl = fetch, now = () => Date.now(), store = createAccessStore({ env, fetchImpl }) }) {
  async function session(request) {
    const entries = (request.headers.get('Cookie') || '').split(';').map(s => s.trim()).filter(s => s.startsWith(`${COOKIE}=`));
    if (entries.length !== 1) fail('authentication_required');
    const value = entries[0].slice(COOKIE.length + 1);
    if (!/^[A-Za-z0-9_-]{1,1800}\.[A-Za-z0-9_-]{43}$/.test(value)) fail('authentication_required');
    const [payload, signature] = value.split('.');
    if (b64(unb64(signature)) !== signature || b64(unb64(payload)) !== payload) fail('authentication_required');
    const key = await signingKey(env);
    if (!await crypto.subtle.verify('HMAC', key, unb64(signature), encoder.encode(payload))) fail('authentication_required');
    let claims;
    try { claims = JSON.parse(new TextDecoder().decode(unb64(payload))); } catch { fail('authentication_required'); }
    const time = Math.floor(now() / 1000);
    if (!claims || !validUid(claims.sub) || !/^[a-f0-9]{32}$/.test(claims.version) || claims.aud !== scope(request).audience || !Number.isInteger(claims.exp) || !Number.isInteger(claims.iat) || claims.iat > time || claims.exp <= time || claims.exp - claims.iat !== TTL) fail('authentication_required');
    const row = await store.byUid(claims.sub);
    if (!row || row.fields.input_session_version !== claims.version) fail('authentication_required');
    return actorOf(row);
  }
  async function redeem(request, token) {
    if (typeof token !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(token)) fail('invalid_invitation');
    const key = await signingKey(env); // Validate configuration before consuming a link.
    const { audience, path } = scope(request);
    const row = await store.byHash(await accessHash(token));
    if (!row || !permitted(row) || !Number.isFinite(Date.parse(row.fields.input_invite_expires_at)) || Date.parse(row.fields.input_invite_expires_at) <= now() || !/^[a-f0-9]{32}$/.test(row.fields.input_session_version || '')) fail('invalid_invitation');
    const unique = await store.byUid(row.fields.person_uid);
    if (!unique || unique.id !== row.id || unique.fields.input_invite_hash !== row.fields.input_invite_hash || unique.fields.input_session_version !== row.fields.input_session_version || !permitted(unique)) fail('invalid_invitation');
    const actor = actorOf(unique);
    const time = Math.floor(now() / 1000);
    const payload = b64(encoder.encode(JSON.stringify({ sub: actor.id, version: unique.fields.input_session_version, aud: audience, iat: time, exp: time + TTL })));
    const signature = b64(new Uint8Array(await crypto.subtle.sign('HMAC', key, encoder.encode(payload))));
    // Sequential isolated-trial consumption only. Airtable cannot enforce atomic CAS.
    // Lost response consumes the link without delivering a session: operator reissues.
    const saved = await store.update(row.id, { input_invite_hash: '', input_invite_expires_at: null });
    if (!saved || saved.id !== row.id || saved.fields.input_invite_hash || saved.fields.input_session_version !== unique.fields.input_session_version || !permitted(saved)) fail('write_outcome_unknown', 503);
    return { actor, cookie: cookie(`${payload}.${signature}`, path) };
  }
  return { session, redeem, logout: request => cookie('', scope(request).path, 0) };
}
async function jsonBody(request) {
  if (!request.headers.get('Content-Type')?.toLowerCase().startsWith('application/json')) fail('invalid_content_type', 415);
  const reader = request.body?.getReader();
  if (!reader) fail('invalid_request', 400);
  const chunks = []; let size = 0;
  while (true) { const { done, value } = await reader.read(); if (done) break; size += value.length; if (size > 1024) { await reader.cancel(); fail('request_too_large', 413); } chunks.push(value); }
  const bytes = new Uint8Array(size); let offset = 0; for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
  try { return JSON.parse(new TextDecoder().decode(bytes)); } catch { fail('invalid_request', 400); }
}
export async function handleAccessRoute(request, env, fetchImpl, inputHandler, log = event => console.info(JSON.stringify(event))) {
  const traceId = crypto.randomUUID();
  const operation = new URL(request.url).pathname.split('/').at(-1);
  const respond = (body, status = 200, extra = {}) => Response.json(body, { status, headers: { 'Cache-Control': 'no-store', 'X-Request-Id': traceId, ...extra } });
  try {
    if (!['GET', 'POST'].includes(request.method)) fail('method_not_allowed', 405);
    if (request.method === 'POST' && request.headers.get('Origin') !== new URL(request.url).origin) fail('origin_denied', 403);
    const access = createInputAccess({ env, fetchImpl });
    if (operation === 'logout') {
      if (request.method !== 'POST') fail('method_not_allowed', 405);
      return respond({ ok: true }, 200, { 'Set-Cookie': access.logout(request) });
    }
    if (operation === 'access' && request.method === 'POST') {
      const body = await jsonBody(request);
      if (!body || Object.keys(body).length !== 1 || !('token' in body)) fail('invalid_request', 400);
      const result = await access.redeem(request, body.token);
      log({ event: 'rs_input_access', traceId, operation: 'redeem', actorId: result.actor.id, status: 200 });
      return respond({ ok: true, actor: result.actor }, 200, { 'Set-Cookie': result.cookie });
    }
    const actor = await access.session(request);
    if (operation === 'access') return respond({ ok: true, actor });
    return await inputHandler(actor);
  } catch (error) {
    const status = error.status || 503, code = error.code || 'access_unavailable';
    log({ event: 'rs_input_access', traceId, operation, status, code });
    return respond({ ok: false, error: code }, status);
  }
}
