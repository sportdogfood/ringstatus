import { installControlDatabase } from '../test-support/recognize-control-db.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { createInputRecognition } from '../src/lib/rs-inputs-recognition.js';
import { recordRecognitionSession } from '../src/lib/rs-recognition-session.js';
import { createInputAccess, accessHash, randomAccessToken } from '../src/lib/rs-inputs-access.js';
const env = { AIRTABLE_TOKEN: 'fixture', RS_INPUTS_BASE_ID: 'app9kOZdIaGyKk5uG', RS_INPUTS_RECOGNITION_BASE_ID: 'app9kOZdIaGyKk5uG', RS_INPUTS_WRITE_MODE: 'isolated-trial', RS_INPUTS_SESSION_SECRET: 'ab'.repeat(32) };
// Former counterexamples now assert the required repaired behavior. Runtime
// durability is tested separately with independent Workers and persistent D1.
test('local repair: persisted retired state prevents a repeated mutation after modeled runtime restart', async () => {
  let deviceWrites = 0, auditWrites = 0;
  const person = { id: 'recSynthetic00001', fields: { person_uid: 'synthetic-invited', status: 'Active', access_level: 'Guest', input_access: 'invited' } };
  const device = { id: 'recSynthetic00002', fields: { person: [person.id], status: 'Active' } };
  const provider = async (input, options = {}) => {
    const url = new URL(input); assert.equal(url.pathname.split('/')[2], env.RS_INPUTS_BASE_ID);
    if (options.method === 'PATCH') { deviceWrites++; device.fields.status = 'Retired'; return Response.json({ records: [device] }); }
    if (options.method === 'POST') { auditWrites++; return auditWrites === 1 ? Response.json({}, { status: 502 }) : Response.json({ records: [{ id: 'recSynthetic00003' }] }); }
    if (url.pathname.endsWith(person.id)) return Response.json(person);
    return Response.json({ records: /rs_devices_test|tblfkRSJAEMzuzApR/.test(url.pathname) ? [device] : /rs_people_test|tbly1PM5iFYqVzKSm/.test(url.pathname) ? [person] : auditWrites ? [{ id: 'recSynthetic00003' }] : [] });
  };
  const input = { action: 'retire_device', values: {}, device_token: 'synthetic-device', requestId: 'synthetic-restart' };
  const make = () => createInputRecognition({ env, principalPersonUid: person.fields.person_uid, verifiedInputAccess: true, fetchImpl: (...args) => provider(...args) });
  await assert.rejects(make().action(input, new Request('https://example.invalid/test/rs-inputs/recognition')), { code: 'session_event_create_failed' });
  assert.equal((await make().action(input, new Request('https://example.invalid/test/rs-inputs/recognition'))).device, 'retired');
  assert.equal(deviceWrites, 1); assert.equal(auditWrites, 1);
});

test('concurrent Airtable event prechecks create at most one row for one event identity', async () => {
  let arrivals = 0, creates = 0, release;
  const barrier = new Promise(resolve => { release = resolve; });
  const provider = async (input, options = {}) => {
    assert.equal(new URL(input).pathname.split('/')[2], env.RS_INPUTS_BASE_ID);
    if (!options.method || options.method === 'GET') { if (++arrivals === 2) release(); await barrier; return Response.json({ records: [] }); }
    creates++; return Response.json({ records: [{ id: 'rec' + String(creates).padStart(14, '0') }] });
  };
  const payload = { session_uid: 'synthetic-session', session_event_uid: 'synthetic-event', event_type: 'recognition', event_result: 'matched', idempotency_key: 'synthetic-same-event' };
  const results = await Promise.allSettled([1, 2].map(() => recordRecognitionSession({ env, fetchImpl: provider, request: new Request('https://example.invalid/'), payload })));
  assert.equal(creates, 1);
  assert.equal(results.filter(r => r.status === 'fulfilled').length, 1);
});

test('one durable invitation claim permits only one cookie', async () => {
  const token = randomAccessToken();
  const row = { id: 'recSynthetic00001', fields: { person_uid: 'synthetic-invited', status: 'Active', input_access: 'invited', input_invite_hash: await accessHash(token), input_invite_expires_at: new Date(Date.now() + 3600000).toISOString(), input_session_version: 'a'.repeat(32) } };
  let clears = 0;
  const store = { byHash: async () => structuredClone(row), byUid: async () => structuredClone(row), update: async () => { clears++; return { ...row, fields: { ...row.fields, input_invite_hash: '', input_invite_expires_at: null } }; } };
  const access = createInputAccess({ env, store });
  const results = await Promise.allSettled([1, 2].map(() => access.redeem(new Request('https://example.invalid/test/rs-inputs/access'), token)));
  assert.equal(results.filter(r => r.status === 'fulfilled' && r.value.cookie).length, 1);
  assert.equal(clears, 1);
});

installControlDatabase(env);
