// Isolated local-handler integration driver. Requires an authorized MCP controller
// on stdin/stdout. Never imported by application routes and never opens a server.
// Authentication is a fixture: this does NOT verify sign-in, deployed HTTP, or UI.
import { createInterface } from 'node:readline';
import { writeFile } from 'node:fs/promises';
import { handleInputRoute } from '../src/pages/rs-inputs/[operation].js';
import { run } from '../scripts/rs-inputs-e2e.mjs';

const runId = process.argv[2];
const reportPath = process.argv[3];
if (!/^rs_live_[A-Za-z0-9_]{8,48}$/.test(runId || '') || !reportPath) throw new Error('Explicit unique fixture run ID and report path required');
const baseId = 'app9kOZdIaGyKk5uG';
const origin = 'https://local-handler.invalid'; // Request object only; never navigated or fetched.
const allowed = new Set(['rs_people_test', 'rs_devices_test', 'rs_phone_aliases_test', 'rs_recognition_sessions_test', 'rs_input_barns', 'rs_input_users', 'rs_input_riders', 'rs_input_horses', 'rs_input_locations', 'rs_input_events']);
const lines = createInterface({ input: process.stdin, terminal: false });
let nextId = 0;
const pending = new Map();
const expired = new Set();
const emit = value => process.stdout.write(JSON.stringify(value) + '\n');
lines.on('line', line => {
  const response = JSON.parse(line);
  const request = pending.get(response.id);
  if (!request && expired.has(response.id)) return; // A timeout leaves write outcome unknown.
  if (!request) throw new Error('Unexpected bridge response');
  pending.delete(response.id);
  request.resolve(Response.json(response.body, { status: response.status }));
});
lines.on('close', () => { for (const request of pending.values()) request.reject(new Error('Connector controller disconnected')); });
async function connectorFetch(value, options = {}) {
  const url = new URL(value);
  const path = url.pathname.split('/').filter(Boolean).map(decodeURIComponent);
  if (url.origin !== 'https://api.airtable.com' || path[0] !== 'v0' || path[1] !== baseId || !allowed.has(path[2]) || path.length > 4) throw new Error('Connector target outside isolated test');
  const id = ++nextId;
  const result = new Promise((resolve, reject) => {
    let timer;
    const clear = () => { clearTimeout(timer); options.signal?.removeEventListener('abort', abort); };
    const abort = () => { clear(); pending.delete(id); expired.add(id); reject(new Error('Connector outcome unknown after timeout/abort')); };
    if (options.signal?.aborted) { reject(new Error('Request already aborted')); return; }
    timer = setTimeout(abort, 45000);
    options.signal?.addEventListener('abort', abort, { once: true });
    pending.set(id, { resolve: value => { clear(); resolve(value); }, reject: error => { clear(); reject(error); } });
  });
  if (!pending.has(id)) return result;
  // Intentionally omit Authorization and all other headers: MCP owns credentials.
  emit({ type: 'connector-request', id, url: url.href, method: options.method || 'GET', body: options.body ? JSON.parse(options.body) : undefined });
  return result;
}
const env = { RS_INPUTS_BASE_ID: baseId, RS_INPUTS_RECOGNITION_BASE_ID: baseId, AIRTABLE_TOKEN: 'CONNECTOR_TEST_ONLY_NOT_A_PAT', RS_INPUTS_WRITE_MODE: 'isolated-trial' };
const uid = part => `${runId}_${part}`;
const actor = { id: uid('actor'), barnIds: [], permissions: ['inputs:read', 'inputs:write', 'barns:create'], profile: { personUid: uid('person'), name: `QA ${runId} Identity`, email: '' } };
const fixtureCookie = 'fixture_actor=present'; // Test selector, not a signed or authenticated session.
const deviceToken = uid('device_token'), retiredToken = uid('retired_token'), ambiguousToken = uid('ambiguous_token');
const fixtures = { devices: [] };
let unknownFixtureCreation = false;
async function tableRequest(table, method, body) {
  const response = await connectorFetch(`https://api.airtable.com/v0/${baseId}/${table}`, { method, body: JSON.stringify(body) });
  if (!response.ok) throw new Error(`Fixture ${method} failed: ${response.status}`);
  return response.json();
}
async function create(table, fields) {
  try {
    const record = (await tableRequest(table, 'POST', { records: [{ fields }] })).records[0];
    if (!record?.id) throw new Error('Fixture creation ID missing');
    return record;
  } catch (error) { unknownFixtureCreation = true; throw error; }
}
async function route(url, init = {}, selectedActor) {
  return handleInputRoute({ request: new Request(url, init), locals: selectedActor ? { rsInputActor: selectedActor } : {} }, env, connectorFetch);
}
const checks = [];
async function expect(label, response, status, code) {
  const body = await response.json();
  if (response.status !== status || (code && body.error !== code)) throw new Error(`${label}: expected ${status}/${code}, received ${response.status}/${body.error}`);
  checks.push({ phase: label, status: 'PASS' });
  emit({ type: 'progress', phase: label, status: 'PASS' });
  return body;
}
let report;
try {
  fixtures.person = await create('rs_people_test', { person_uid: uid('person'), person_name: actor.profile.name, first_name: 'QA', last_name: runId, primary_phone_e164: '+12025550199', status: 'Active', access_level: 'member' });
  fixtures.alias = await create('rs_phone_aliases_test', { alias_uid: uid('alias'), alias_phone_e164: '+12025550199', alias_type: 'Mobile', status: 'Active', person: [fixtures.person.id] });
  for (const [suffix, token, status] of [['active', deviceToken, 'Active'], ['retired', retiredToken, 'Retired'], ['duplicate_a', ambiguousToken, 'Active'], ['duplicate_b', ambiguousToken, 'Active']]) fixtures.devices.push(await create('rs_devices_test', { device_uid: uid(suffix), device_token: token, status, person: [fixtures.person.id], recognition_source: 'Web', first_seen_at: new Date().toISOString() }));
  const journey = await run({ baseUrl: origin, expectedOrigin: origin, environment: 'isolated-trial', cookie: fixtureCookie, deviceToken, retiredDeviceToken: retiredToken, expectedPersonId: uid('person'), runId, execute: true, fetchImpl: (url, init) => route(url, init, init.headers.Cookie === fixtureCookie ? actor : undefined), onProgress: value => emit({ type: 'progress', ...value }) });
  report = { ...journey, scope: 'LOCAL_HANDLER_LIVE_AIRTABLE_VIA_MCP', prerequisites: ['Explicit fixture actor, not a real authenticated session', 'Authorized Airtable MCP connection to the owned isolated base'], authenticatedSessionVerified: false, browserVerified: false, deployedHttpVerified: false, nativeRestCredentialVerified: false, limitations: ['Missing-actor checks exercise the handler guard only, not session issuance.', 'Airtable operations use MCP translation; native HTTP credential and transport behavior remain unverified.', 'Provider concurrency and atomicity are not established.', 'Actual application registration and identity/membership integration remain incomplete.'], extraChecks: checks, fixtures: { person: fixtures.person.id, alias: fixtures.alias.id, devices: fixtures.devices.map(d => d.id) } };
  if (journey.status !== 'PASS') throw new Error(`Journey failed: ${journey.failedPhase || journey.errorCode}`);
  await expect('ambiguous-device-rejected', await route(`${origin}/rs-inputs/recognition?device_token=${ambiguousToken}`, {}, actor), 409, 'ambiguous_device');
  await expect('wrong-principal-rejected', await route(`${origin}/rs-inputs/recognition?device_token=${deviceToken}`, {}, { ...actor, profile: { ...actor.profile, personUid: uid('wrong_person') } }), 403, 'verified_profile_required');
  const post = body => ({ method: 'POST', headers: { Origin: origin, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  await expect('read-only-actor-write-rejected', await route(`${origin}/rs-inputs/record`, post({ kind: 'barn', draft: { name: 'Denied' }, requestId: uid('denied') }), { ...actor, permissions: ['inputs:read'] }), 403, 'permission_denied');
  await expect('empty-name-rejected', await route(`${origin}/rs-inputs/record`, post({ kind: 'barn', draft: { name: '' }, requestId: uid('empty_name') }), actor), 400, 'name_required');
  await expect('other-owner-barn-rejected', await route(`${origin}/rs-inputs/record`, post({ kind: 'users', barnId: journey.ids['create-barn'], draft: { name: 'Denied' }, requestId: uid('other_owner') }), { ...actor, id: uid('other_actor'), barnIds: [] }), 404, 'barn_not_found');
  // Exercise the original recognition action/session logger through the wrapper.
  await expect('confirm-device-and-log', await route(`${origin}/rs-inputs/recognition`, post({ action: 'confirm_device', device_token: deviceToken, values: {}, requestId: uid('confirm') }), actor), 200);
  const sessionResponse = await connectorFetch(`https://api.airtable.com/v0/${baseId}/rs_recognition_sessions_test?filterByFormula=${encodeURIComponent(`{session_event_uid} = 'inputs_${uid('confirm')}'`)}`);
  const sessions = await sessionResponse.json();
  if (sessions.records?.length !== 1 || sessions.records[0].fields.person?.[0] !== fixtures.person.id || sessions.records[0].fields.device?.[0] !== fixtures.devices[0].id || sessions.records[0].fields.page_path !== '/rs-inputs/recognition') throw new Error('Recognition audit readback mismatch');
  report.recognitionAuditId = sessions.records[0].id;
  await tableRequest('rs_recognition_sessions_test', 'PATCH', { records: [{ id: report.recognitionAuditId, fields: { automation_status: 'processed', automation_processed_at: new Date().toISOString() } }] });
  checks.push({ phase: 'recognition-audit-links-and-path', status: 'PASS' });
} catch (error) {
  report ||= { scope: 'LOCAL_HANDLER_LIVE_AIRTABLE_VIA_MCP', phases: [], extraChecks: checks, authenticatedSessionVerified: false, browserVerified: false, deployedHttpVerified: false };
  report.status = 'FAIL'; report.driverError = error.message;
} finally {
  try {
    if (fixtures.devices.length) await tableRequest('rs_devices_test', 'PATCH', { records: fixtures.devices.map(d => ({ id: d.id, fields: { status: 'Retired' } })) });
    if (fixtures.alias) await tableRequest('rs_phone_aliases_test', 'PATCH', { records: [{ id: fixtures.alias.id, fields: { status: 'Inactive' } }] });
    if (fixtures.person) await tableRequest('rs_people_test', 'PATCH', { records: [{ id: fixtures.person.id, fields: { status: 'Inactive', access_level: 'Guest' } }] });
    report.fixtureRetirementWritesAcknowledged = !unknownFixtureCreation;
  } catch { report.status = 'FAIL'; report.fixtureRetirementWritesAcknowledged = false; }
  report.fixtures = { person: fixtures.person?.id, alias: fixtures.alias?.id, devices: fixtures.devices.map(d => d.id) };
  report.unknownFixtureCreation = unknownFixtureCreation;
  await writeFile(reportPath, JSON.stringify(report, null, 2) + '\n');
  emit({ type: 'complete', report });
  lines.close();
  process.exitCode = report.status === 'PASS' ? 0 : 1;
}
