import test from 'node:test';
import assert from 'node:assert/strict';
import { run, main } from '../scripts/rs-inputs-e2e.mjs';
import { handleInputRequest, InputError } from '../src/lib/rs-inputs.js';

const SESSION = 'rs_fixture_session=explicit-test-only';
const TOKEN = 'device_token_existing_fixture';
const RETIRED = 'device_token_retired_fixture';
const PERSON = 'person_fixture_known';
const config = { execute: true, baseUrl: 'https://fixture.example.test/test', expectedOrigin: 'https://fixture.example.test', environment: 'isolated-trial', cookie: SESSION, deviceToken: TOKEN, retiredDeviceToken: RETIRED, expectedPersonId: PERSON, runId: 'test_run_001' };
function fixture() {
  const rows = { barn: [], users: [], riders: [], horses: [], locations: [] }; const events = new Map(); const calls = [];
  const store = {
    async list(kind) { return structuredClone(rows[kind]); },
    async put(kind, record, { expectedRevision } = {}) { const i = rows[kind].findIndex(row => row.id === record.id); if (i >= 0 && rows[kind][i].revision !== expectedRevision) throw new InputError('record_changed', 409); if (i < 0) rows[kind].push(structuredClone(record)); else rows[kind][i] = structuredClone(record); },
    async event(id) { return events.get(id); },
    async appendEvent(event) { events.set(event.eventId, structuredClone(event)); }
  };
  const profile = { person_uid: PERSON, person_name: 'Private Fixture Name', first_name: 'Private', last_name: 'Fixture', sms: '2025550123', pin: '0123', email: 'private@example.test' };
  const actor = { id: 'fixture-actor', permissions: ['inputs:read', 'inputs:write', 'barns:create'], barnIds: [], profile: { personUid: PERSON, name: profile.person_name, email: profile.email } };
  const recognition = { async lookup(token) { return token === TOKEN ? { ok: true, recognized: true, profile: { ...profile }, device: 'active' } : { ok: true, recognized: false, profile: null, device: token === RETIRED ? 'retired' : 'unknown' }; }, async action() { throw new Error('Existing identity must never be mutated by this runner'); } };
  async function fetchImpl(url, init) {
    assert.equal(new URL(url).origin, config.expectedOrigin); assert.ok(new URL(url).pathname.startsWith('/test/rs-inputs/')); assert.equal(init.redirect, 'error');
    calls.push({ url, init });
    return handleInputRequest({ request: new Request(url, init), actor: new Headers(init.headers).get('Cookie') === SESSION ? actor : undefined, store, recognition });
  }
  return { rows, events, calls, fetchImpl };
}

test('API journey exercises actual domain service with isolated provider and leaves review records', async () => {
  const f = fixture(); const progress = [];
  const result = await run({ ...config, fetchImpl: f.fetchImpl, onProgress: row => progress.push(row) });
  assert.equal(result.status, 'PASS'); assert.equal(result.scope, 'API_ONLY');
  assert.ok(result.phases.every(row => row.status === 'PASS'));
  assert.deepEqual(Object.fromEntries(Object.entries(f.rows).map(([kind, rows]) => [kind, rows.length])), { barn: 1, users: 2, riders: 1, horses: 1, locations: 1 });
  assert.equal(f.rows.horses[0].revision, 2); assert.equal(f.rows.horses[0].name, 'QA test_run_001 Horse edited');
  assert.equal(f.rows.riders[0].userId, f.rows.users.find(row => !row.recognitionPersonId).id);
  assert.equal(f.rows.users.filter(row => row.recognitionPersonId === PERSON).length, 1);
  assert.ok(f.calls.filter(call => new URL(call.url).pathname.endsWith('/recognition')).every(call => call.init.method === 'GET'));
  const edits = f.calls.filter(call => call.init.body && JSON.parse(call.init.body).draft?.name === 'QA test_run_001 Horse edited');
  assert.equal(edits.length, 2); assert.equal(edits[0].init.body, edits[1].init.body);
  const links = f.calls.filter(call => new URL(call.url).pathname.endsWith('/profile-link')).map(call => JSON.parse(call.init.body));
  assert.notEqual(links[0].requestId, links[1].requestId); assert.deepEqual(Object.keys(links[0]).sort(), ['barnId', 'requestId']);
  const output = JSON.stringify({ result, progress });
  for (const secret of [SESSION, TOKEN, RETIRED, 'Private Fixture Name', 'private@example.test', '2025550123']) assert.equal(output.includes(secret), false);
});

test('default dry run requires no credentials and performs no fetch', async () => {
  const result = await run({ fetchImpl() { throw new Error('No call allowed'); } });
  assert.equal(result.status, 'DRY_RUN'); assert.ok(result.prerequisites.length > 0); assert.ok(result.phases.every(row => row.status === 'NOT_RUN'));
  assert.equal((await main([], {}, { fetchImpl() { throw new Error('No call allowed'); } })).status, 'DRY_RUN');
});

test('explicit trial, observed origin, credentials, expected identity, and run ID gate every request', async () => {
  const cases = [ { environment: 'production' }, { expectedOrigin: 'https://other.test' }, { cookie: '' }, { deviceToken: '' }, { expectedPersonId: '' }, { runId: '' }, { baseUrl: 'http://fixture.example.test/test' }, { baseUrl: 'https://user:secret@fixture.example.test/test' }, { baseUrl: 'https://fixture.example.test/test?token=secret' }, { cookie: 'x=y\r\nOther: bad' } ];
  for (const change of cases) { const result = await run({ ...config, ...change, fetchImpl() { assert.fail('No request allowed'); } }); assert.equal(result.status, 'BLOCKED'); assert.ok(result.phases.every(row => row.status === 'NOT_RUN')); }
});

test('wrong known identity fails before writes and does not reveal returned personal data', async () => {
  const f = fixture(); const result = await run({ ...config, expectedPersonId: 'wrong-person', fetchImpl: f.fetchImpl });
  assert.equal(result.status, 'FAIL'); assert.equal(result.failedPhase, 'known-recognition'); assert.equal(result.errorCode, 'known_person_mismatch');
  assert.equal(f.rows.barn.length, 0); assert.equal(result.phases.find(row => row.phase === 'create-barn').status, 'NOT_RUN');
});

test('failure stops immediately and retains IDs without upstream details or automatic retry', async () => {
  const f = fixture(); let failures = 0;
  const fetchImpl = (url, init) => { if (init.body && JSON.parse(init.body).kind === 'locations') { failures++; return Response.json({ ok: false, error: 'storage_unavailable', detail: `secret ${SESSION} ${TOKEN}` }, { status: 502 }); } return f.fetchImpl(url, init); };
  const result = await run({ ...config, fetchImpl });
  assert.equal(result.status, 'FAIL'); assert.equal(result.failedPhase, 'create-location'); assert.equal(failures, 1); assert.equal(f.rows.horses.length, 0);
  assert.ok(result.ids['create-barn']); assert.ok(result.ids['create-rider']); assert.equal(result.phases.find(row => row.phase === 'create-horse').status, 'NOT_RUN');
  assert.equal(JSON.stringify(result).includes('secret'), false);
});

test('unexpected authorization write is reported by ID then stops', async () => {
  let count = 0; const result = await run({ ...config, fetchImpl: async () => { count++; return Response.json({ ok: true, record: { id: 'rs_unexpected_record' } }); } });
  assert.equal(count, 1); assert.equal(result.failedPhase, 'session-required'); assert.equal(result.ids['session-required'], 'rs_unexpected_record');
});

test('transport uncertainty stops without retry or leaking its exception', async () => {
  let count = 0; const result = await run({ ...config, fetchImpl: async () => { count++; throw new Error(`credential ${SESSION}`); } });
  assert.equal(count, 1); assert.equal(result.errorCode, 'request_outcome_unknown'); assert.equal(JSON.stringify(result).includes(SESSION), false);
});

test('missing retired fixture completes the main journey but reports PARTIAL', async () => {
  const f = fixture(); const result = await run({ ...config, retiredDeviceToken: undefined, fetchImpl: f.fetchImpl });
  assert.equal(result.status, 'PARTIAL'); assert.equal(result.phases.find(row => row.phase === 'retired-recognition').status, 'SKIPPED_NO_FIXTURE');
  assert.equal(result.phases.find(row => row.phase === 'reload').status, 'PASS');
  assert.equal(f.rows.horses[0].revision, 2);
  assert.equal(f.calls.some(call => call.url.includes(RETIRED)), false);
});

test('CLI accepts secrets through named environment only and rejects ambiguous mode', async () => {
  const f = fixture(); const args = ['--run', '--base-url', config.baseUrl, '--expected-origin', config.expectedOrigin, '--environment', config.environment, '--run-id', config.runId, '--expected-person-id', PERSON, '--cookie-env', 'TEST_COOKIE', '--device-token-env', 'TEST_DEVICE'];
  const result = await main(args, { TEST_COOKIE: SESSION, TEST_DEVICE: TOKEN }, { fetchImpl: f.fetchImpl }); assert.equal(result.status, 'PARTIAL');
  for (const invalid of [['--cookie', SESSION], ['--device-token', TOKEN], ['--run', '--dry-run'], ['--cookie-env', 'literal=secret']]) await assert.rejects(main(invalid, {}, { fetchImpl() { assert.fail('No request allowed'); } }));
});
