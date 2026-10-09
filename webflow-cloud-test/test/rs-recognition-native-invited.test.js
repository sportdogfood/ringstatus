import { installControlDatabase } from '../test-support/recognize-control-db.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { handleNativeRecognition } from '../src/lib/rs-recognition-native.js';
import { createInputAccess, accessHash, randomAccessToken } from '../src/lib/rs-inputs-access.js';
import { env as routeEnv } from 'cloudflare:workers';
import { ALL as nativeEntry } from '../src/pages/rs-inputs/native-recognition.js';
import { createRecoverySms } from '../src/lib/rs-recognition-sms.js';

const origin = 'https://example.invalid';
const env = { AIRTABLE_TOKEN: 'fixture', RS_INPUTS_BASE_ID: 'app9kOZdIaGyKk5uG', RS_INPUTS_RECOGNITION_BASE_ID: 'app9kOZdIaGyKk5uG', RS_INPUTS_WRITE_MODE: 'isolated-trial', RS_INPUTS_SESSION_SECRET: 'ab'.repeat(32) };
async function fixture({ expired = false } = {}) {
  let nextId = 2;
  const tables = new Map(), calls = [];
  const names = { tbly1PM5iFYqVzKSm: 'rs_people_test', tblfkRSJAEMzuzApR: 'rs_devices_test', tblgDWKi0Bb6OcoqS: 'rs_phone_aliases_test', tblWjbASVMIjFLyW8: 'rs_recognition_sessions_test', tblxC4SYtzYhJNcJS: 'rs_sms_requests', tblF1Hdqi3yoKTZPV: 'rs_sms_events' };
  const rows = table => { if (!tables.has(table)) tables.set(table, []); return tables.get(table); };
  const token = randomAccessToken();
  const person = { id: 'recSynthetic00001', fields: { person_uid: 'synthetic-invited', person_name: 'Synthetic', first_name: 'Synthetic', last_name: 'Fixture', primary_phone_e164: '+12025550199', member_pin: '0199', status: 'Active', access_level: 'Guest', input_access: 'invited', input_invite_hash: await accessHash(token), input_invite_expires_at: new Date(Date.now() + 3600000).toISOString(), input_session_version: 'a'.repeat(32) } };
  rows('rs_people_test').push(person);
  const fetchImpl = async (input, options = {}) => {
    const url = new URL(input), [, base, rawTable, id] = url.pathname.split('/').filter(Boolean);
    assert.equal(url.origin, 'https://api.airtable.com'); assert.equal(base, env.RS_INPUTS_BASE_ID);
    const table = names[rawTable] || rawTable;
    assert.ok(Object.values(names).includes(table));
    calls.push({ table, method: options.method || 'GET' });
    if (!options.method || options.method === 'GET') {
      if (id) {
        const row = rows(table).find(r => r.id === id);
        if (row && table === 'rs_sms_requests') row.fields['primary_phone_e164 (from person_uid)'] = [person.fields.primary_phone_e164];
        return Response.json(structuredClone(row || {}));
      }
      const formula = url.searchParams.get('filterByFormula');
      const clauses = formula ? [...formula.matchAll(/\{([^}]+)\}\)?\s*=\s*'([^']*)'/g)] : [];
      const found = clauses.length ? rows(table).filter(r => {
        const match = ([, key, value]) => formula.includes('LOWER({' + key + '})') ? String(r.fields[key] || '').toLowerCase() === value : r.fields[key] === value;
        return formula.startsWith('AND(') ? clauses.every(match) : clauses.some(match);
      }) : rows(table);
      if (table === 'rs_sms_requests') for (const row of found) row.fields['primary_phone_e164 (from person_uid)'] = [person.fields.primary_phone_e164];
      return Response.json({ records: structuredClone(found) });
    }
    const output = JSON.parse(options.body).records.map(update => {
      let row = update.id && rows(table).find(r => r.id === update.id);
      if (!row) { row = { id: 'rec' + String(++nextId).padStart(14, '0'), fields: {} }; rows(table).push(row); }
      Object.assign(row.fields, update.fields); return structuredClone(row);
    });
    return Response.json({ records: output });
  };
  const access = createInputAccess({ env, fetchImpl, ...(expired ? { now: () => Date.now() - 9 * 3600000 } : {}) });
  const redeemed = await access.redeem(new Request(origin + '/test/rs-inputs/access'), token);
  assert.match(redeemed.cookie, /Path=\/test\/rs-inputs;/);
  const cookie = redeemed.cookie.split(';')[0];
  calls.length = 0;
  const request = (operation, body, headers = {}, authenticated = true) => new Request(origin + '/test/rs-inputs/native-recognition?operation=' + operation + (operation === 'device' ? '&device_token=synthetic-device' : ''), {
    method: body ? 'POST' : 'GET', headers: { ...(authenticated ? { Cookie: cookie } : {}), ...(body ? { Origin: origin, 'Content-Type': 'application/json' } : {}), ...headers }, ...(body ? { body: JSON.stringify(body) } : {})
  });
  return { person, calls, rows, fetchImpl, request };
}
const action = (name, extra = {}) => ({ action: name, device_token: 'synthetic-device', session_uid: 'synthetic-session', session_event_uid: 'synthetic-' + name, ...extra });

const smsEnv = { ...env, RS_RECOGNITION_SMS_QUEUE_MODE: 'ready-gated', RS_RECOGNITION_SMS_RECORD_MODE: 'test', RS_RECOGNITION_SMS_TEST_RECIPIENT: '+12025550199', RS_INPUTS_ONBOARDING_URL: origin + '/test/onboarding', RS_RECOGNITION_AUTOMATION_SECRET: 'fixture-automation-secret-32-characters' };
const recovery = { ...action('recovery'), session_event_uid: 'synthetic-recovery-request-0001', first: 'Synthetic', last: 'Fixture', to_e164: '+12025550000', person_uid: 'spoofed' };

test('silent recognition uses a known active device without Inputs login and records one canonical visit', async () => {
  const f = await fixture();
  const deviceToken = '81a81b40-f954-4ad5-8911-8fc3bf91c4a1';
  f.rows('rs_devices_test').push({ id: 'recDevice00000001', fields: { device_token: deviceToken, status: 'Active', person: [f.person.id] } });
  const payload = { device_token: deviceToken, session_uid: 'silent-session-001', person_record_id: 'recSpoofed0000001', page_path: '/rs-recognize?private=excluded' };
  const call = () => handleNativeRecognition(f.request('recognize', payload, {}, false), env, f.fetchImpl);
  const response = await call();
  assert.equal(response.status, 200);
  const body = await response.json();
  assert.equal(body.recognized, true);
  assert.equal(body.first_name, 'Synthetic');
  assert.equal('primary_phone_e164' in body, false);
  assert.equal('email' in body, false);
  assert.match(response.headers.get('Set-Cookie'), /Max-Age=31536000; HttpOnly; Secure; SameSite=Lax/);
  assert.doesNotMatch(response.headers.get('Set-Cookie'), /rs_input_access/);
  assert.equal((await call()).status, 200);
  assert.equal(f.rows('rs_recognition_sessions_test').length, 1);
  assert.deepEqual(f.rows('rs_recognition_sessions_test')[0].fields.person, [f.person.id]);
  assert.equal(f.rows('rs_recognition_sessions_test')[0].fields.page_path, '/rs-recognize');
  assert.equal((await handleNativeRecognition(f.request('action', action('update_profile'), {}, false), env, f.fetchImpl)).status, 401);
});

test('silent recognition rejects ambiguous, retired and revoked devices and cross-origin requests', async () => {
  const f = await fixture();
  const token = '81a81b40-f954-4ad5-8911-8fc3bf91c4a1';
  const device = { id: 'recDevice00000001', fields: { device_token: token, status: 'Retired', person: [f.person.id] } };
  f.rows('rs_devices_test').push(device);
  const payload = { device_token: token, session_uid: 'silent-denied-001' };
  const call = () => handleNativeRecognition(f.request('recognize', payload, {}, false), env, f.fetchImpl);
  assert.equal((await (await call()).json()).recognized, false);
  device.fields.status = 'Active'; f.person.fields.input_access = 'revoked';
  assert.equal((await (await call()).json()).recognized, false);
  f.person.fields.input_access = 'invited';
  f.rows('rs_devices_test').push(structuredClone(device));
  assert.equal((await call()).status, 409);
  assert.equal((await handleNativeRecognition(f.request('recognize', payload, { Origin: 'https://other.invalid' }, false), env, f.fetchImpl)).status, 403);
});

test('phone fallback without an Inputs session reuses SMS recovery and never grants access', async () => {
  const f = await fixture();
  for (const sms of ['2025550199', '12025550199']) {
    const response = await handleNativeRecognition(f.request('action', { ...action('phone_login'), session_event_uid: 'phone-recovery-same-001', sms }, {}, false), smsEnv, f.fetchImpl);
    assert.equal(response.status, 200); assert.equal((await response.json()).accepted, true);
    assert.equal(response.headers.get('Set-Cookie'), null);
  }
  assert.equal(f.rows('rs_sms_requests').length, 1);
  assert.deepEqual(f.rows('rs_sms_requests')[0].fields.person_uid, [f.person.id]);
  assert.equal(f.rows('rs_devices_test').length, 0);
});

test('lost-session recovery queues only canonical invited person, reuses request identity and stores no private message', async () => {
  const f = await fixture();
  const request = () => f.request('action', recovery, {}, false);
  const results = await Promise.all([1, 2].map(() => handleNativeRecognition(request(), smsEnv, f.fetchImpl)));
  assert.ok(results.every(r => r.status === 200));
  const body = await results[0].json(); assert.equal(body.delivery_status, 'unconfirmed');
  assert.equal(f.rows('rs_sms_requests').length, 1); assert.equal(f.rows('rs_sms_events').length, 1);
  const row = f.rows('rs_sms_requests')[0]; assert.deepEqual(row.fields.person_uid, [f.person.id]); assert.equal(row.fields.status, 'ready');
  assert.equal('to_e164' in row.fields, false); assert.equal('body' in row.fields, false);
  assert.equal(f.person.fields.access_level, 'Guest');
});

test('private automation preparation uses existing invitation primitives and logs submission without claiming delivery', async () => {
  const f = await fixture(); const sms = createRecoverySms({ env: smsEnv, fetchImpl: f.fetchImpl });
  await sms.request(recovery);
  const row = f.rows('rs_sms_requests')[0];
  const post = (operation, body, authorized = true) => new Request(origin + '/test/rs-inputs/native-recognition?operation=' + operation, { method: 'POST', headers: { 'Content-Type': 'application/json', ...(authorized ? { Authorization: 'Bearer ' + smsEnv.RS_RECOGNITION_AUTOMATION_SECRET } : {}) }, body: JSON.stringify(body) });
  assert.equal((await handleNativeRecognition(post('sms_prepare', { request_record_id: row.id }, false), smsEnv, f.fetchImpl)).status, 403);
  const prepared = await handleNativeRecognition(post('sms_prepare', { request_record_id: row.id }), smsEnv, f.fetchImpl);
  assert.equal(prepared.status, 200); const handoff = await prepared.json(); assert.equal(handoff.to, f.person.fields.primary_phone_e164);
  assert.equal(f.person.fields.input_access, 'invited'); assert.equal(f.person.fields.access_level, 'Guest');
  assert.equal((await handleNativeRecognition(f.request('device'), smsEnv, f.fetchImpl)).status, 200, 'recovery must not invalidate the prior signed session');
  assert.equal(row.fields.status, 'processing');
  assert.equal((await handleNativeRecognition(post('sms_prepare', { request_record_id: row.id }), smsEnv, f.fetchImpl)).status, 409);
  const sid = 'SM' + '1'.repeat(32);
  const outcome = await handleNativeRecognition(post('sms_outcome', { request_record_id: row.id, outcome: 'submitted', provider_message_sid: sid }), smsEnv, f.fetchImpl);
  assert.equal((await outcome.json()).delivered, false); assert.equal(row.fields.status, 'submitted');
  assert.equal((await handleNativeRecognition(post('sms_outcome', { request_record_id: row.id, outcome: 'delivered', provider_message_sid: sid }), smsEnv, f.fetchImpl)).status, 400);
  const link = new URL(handoff.body.slice(handoff.body.indexOf('https://')));
  assert.equal(link.pathname, '/rs-recognize', 'SMS must land on the native Webflow page, not the Astro prototype');
  const token = new URLSearchParams(link.hash.slice(1)).get('invite');
  const access = createInputAccess({ env: smsEnv, fetchImpl: f.fetchImpl });
  const restored = await access.redeem(new Request(origin + '/test/rs-inputs/access'), token);
  assert.equal(restored.actor.id, f.person.fields.person_uid);
  await assert.rejects(access.redeem(new Request(origin + '/test/rs-inputs/access'), token), { code: 'invalid_invitation' });
  assert.ok(f.rows('rs_sms_events').every(r => !JSON.stringify(r).includes(token)));
  assert.ok(!JSON.stringify(row).includes(token));
});

test('uninvited and wrong test recipient recovery is neutral and produces no sendable request', async () => {
  const f = await fixture(); f.person.fields.input_access = 'revoked';
  const sms = createRecoverySms({ env: smsEnv, fetchImpl: f.fetchImpl });
  assert.equal((await sms.request(recovery)).accepted, true); assert.equal(f.rows('rs_sms_requests').length, 0);
  f.person.fields.input_access = 'approved';
  assert.equal((await createRecoverySms({ env: { ...smsEnv, RS_RECOGNITION_SMS_TEST_RECIPIENT: '+12025550000' }, fetchImpl: f.fetchImpl }).request(recovery)).accepted, true);
  assert.equal(f.rows('rs_sms_requests').length, 0);
});

test('native invitation cookie supplies canonical profile, confirms device, edits and logs without exposing PIN', async () => {
  const f = await fixture();
  const lookup = await handleNativeRecognition(f.request('device'), env, f.fetchImpl);
  const initial = await lookup.json();
  assert.equal(initial.invited_profile, true); assert.equal(initial.recognized, false); assert.equal(initial.requires_device_confirmation, true);
  assert.equal('pin' in initial, false); assert.equal('member_pin' in initial, false);
  const confirmed = await handleNativeRecognition(f.request('action', action('confirm_device', { person_uid: 'spoofed' })), env, f.fetchImpl);
  assert.equal((await confirmed.json()).confirmed, true);
  const edited = await handleNativeRecognition(f.request('action', action('update_profile', { user: 'Synthetic Updated', sms: '2025550199', pin: '', person_record_id: 'recAnother0000001' })), env, f.fetchImpl);
  const updated = await edited.json(); assert.equal(updated.person_name, 'Synthetic Updated'); assert.equal('pin' in updated, false);
  assert.equal(f.person.fields.member_pin, '0199'); assert.equal(f.person.fields.access_level, 'Guest');
  const logged = await handleNativeRecognition(f.request('session', { ...action('ignored'), person_record_id: 'recAnother0000001', event_result: 'matched' }), env, f.fetchImpl);
  assert.equal(logged.status, 200);
  const event = f.rows('rs_recognition_sessions_test').find(r => r.fields.idempotency_key === 'native_lookup:synthetic-ignored');
  assert.deepEqual(event.fields.person, [f.person.id]);
});

test('native missing/forged session, wrong origin and revoked invitation never grant access or write', async () => {
  const f = await fixture();
  for (const request of [f.request('device', null, {}, false), f.request('device', null, { Cookie: '__Secure-rs_input_access=forged' }), f.request('action', action('confirm_device'), { Origin: 'https://attacker.invalid' })]) {
    const response = await handleNativeRecognition(request, env, f.fetchImpl); assert.ok([401, 403].includes(response.status));
  }
  f.person.fields.input_access = 'revoked';
  assert.equal((await handleNativeRecognition(f.request('device'), env, f.fetchImpl)).status, 401);
  assert.ok(f.calls.every(c => c.method === 'GET'));
});

test('native registration is invitation-only and missing SMS recovery returns failure, never delivered/queued success', async () => {
  const f = await fixture();
  const create = await handleNativeRecognition(f.request('action', action('create_profile')), env, f.fetchImpl);
  assert.equal(create.status, 403); assert.equal((await create.json()).error, 'invitation_only');
  const recovery = await handleNativeRecognition(f.request('action', action('recovery', { email: 'unused@example.invalid' })), env, f.fetchImpl);
  assert.equal(recovery.status, 503); assert.deepEqual(await recovery.json(), { ok: false, error: 'sms_recovery_unconfigured' });
  assert.ok(f.calls.every(c => c.method === 'GET'));
});

test('authorized native entry reuses real scoped cookie and rejects expired access without trusting locals', async () => {
  Object.assign(routeEnv, env);
  const savedFetch = globalThis.fetch;
  try {
    const f = await fixture(); globalThis.fetch = f.fetchImpl;
    const response = await nativeEntry({ request: f.request('device'), locals: { rsInputActor: { id: 'forged' } } });
    assert.equal(response.status, 200); assert.equal((await response.json()).person_uid, f.person.fields.person_uid);
    const expired = await fixture({ expired: true }); globalThis.fetch = expired.fetchImpl;
    const denied = await nativeEntry({ request: expired.request('action', action('confirm_device')), locals: { rsInputActor: { id: expired.person.fields.person_uid } } });
    assert.equal(denied.status, 401);
    assert.ok(expired.calls.every(c => c.method === 'GET'));
  } finally { globalThis.fetch = savedFetch; }
});

installControlDatabase(env, smsEnv);

test('independent recovery callers create one queue row and prepare only one SMS', async () => {
  const f = await fixture();
  const make = () => createRecoverySms({ env: smsEnv, fetchImpl: (...args) => f.fetchImpl(...args) });
  const requests = await Promise.allSettled([make().request(recovery), make().request(recovery)]);
  assert.ok(requests.some(r => r.status === 'fulfilled'));
  assert.equal(f.rows('rs_sms_requests').length, 1);
  assert.equal(f.rows('rs_sms_events').filter(r => r.fields.event_type === 'requested').length, 1);
  const id = f.rows('rs_sms_requests')[0].id;
  const prepared = await Promise.allSettled([make().prepare(id), make().prepare(id)]);
  assert.equal(prepared.filter(r => r.status === 'fulfilled').length, 1);
  assert.equal(f.rows('rs_sms_events').filter(r => r.fields.event_type === 'attempt_started').length, 1);
});
