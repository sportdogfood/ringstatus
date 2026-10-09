import test from 'node:test';
import assert from 'node:assert/strict';
import { handleInputRequest, saveInputRecord, readInputState, InputError } from '../src/lib/rs-inputs.js';
import { createAirtableInputStore } from '../src/lib/rs-inputs-airtable.js';

function fixtureStore() {
  const rows = { barn: [], users: [], riders: [], horses: [], locations: [] };
  const events = new Map();
  return {
    rows, events, failAudit: false,
    async list(kind) { return structuredClone(rows[kind]); },
    async put(kind, record, { expectedRevision } = {}) {
      const index = rows[kind].findIndex(row => row.id === record.id);
      if (index >= 0 && rows[kind][index].revision !== expectedRevision) throw new InputError('record_changed', 409);
      if (index < 0) rows[kind].push(structuredClone(record));
      else rows[kind][index] = structuredClone(record);
    },
    async event(id) { return events.get(id); },
    async appendEvent(event) { if (this.failAudit) throw new Error('audit unreachable'); events.set(event.eventId, structuredClone(event)); }
  };
}
const actor = { id: 'actor-a', permissions: ['inputs:read', 'inputs:write', 'barns:create'], barnIds: [], profile: { personUid: 'person-a', name: 'Avery Rider', email: 'avery@example.test' } };
const payload = (kind, name, barnId = '', extra = {}) => ({ kind, barnId, requestId: crypto.randomUUID(), draft: { name, ...extra } });
async function save(store, input, principal = actor) { return saveInputRecord({ store, actor: principal, payload: input }); }
async function barn(store) { return (await save(store, payload('barn', 'Trial Barn'))).record.id; }
test('complete persistent input journey: barn, profile link, rider, location, horse, inline edit, reload', async () => {
  const store = fixtureStore();
  const barnId = await barn(store);
  const link = { store, actor, profileLink: true, payload: { barnId, requestId: crypto.randomUUID() } };
  const user = (await saveInputRecord(link)).record;
  assert.equal(user.recognitionPersonId, 'person-a');
  await saveInputRecord({ ...link, payload: { ...link.payload, requestId: crypto.randomUUID() } });
  assert.equal(store.rows.users.length, 1);
  const rider = (await save(store, payload('riders', 'Avery', barnId, { userId: user.id }))).record;
  const location = (await save(store, payload('locations', 'Home Barn', barnId, { address: 'Synthetic trial address' }))).record;
  const horse = (await save(store, payload('horses', 'Juniper', barnId, { riderId: rider.id, locationId: location.id }))).record;
  const edited = await save(store, { ...payload('horses', 'Juniper II', barnId, { ...horse, name: 'Juniper II' }), expectedRevision: horse.revision });
  assert.equal(edited.record.revision, 2);
  const state = await readInputState(store, actor, barnId);
  assert.equal(state.horses[0].name, 'Juniper II');
  assert.equal(state.horses[0].riderId, rider.id);
  assert.equal(state.horses[0].locationId, location.id);
  assert.equal(state.riders[0].userId, user.id);
  assert.equal(state.selectedBarn, barnId);
  assert.equal('requestHash' in state.horses[0], false);
});
test('required fields, same-barn duplicate names and relationship boundaries', async () => {
  const store = fixtureStore(); const barnId = await barn(store);
  await assert.rejects(save(store, payload('users', '   ', barnId)), { code: 'name_required' });
  await assert.rejects(save(store, payload('users', 'Avery', barnId, { email: 'invalid' })), { code: 'invalid_email' });
  await save(store, payload('users', 'Avery', barnId));
  await assert.rejects(save(store, payload('users', 'avery', barnId)), { code: 'duplicate_name' });
  await assert.rejects(save(store, payload('horses', 'Juniper', barnId, { riderId: 'foreign-rider' })), { code: 'invalid_relationship' });
  const otherBarn = (await save(store, payload('barn', 'Second Barn'))).record.id;
  const otherRider = (await save(store, payload('riders', 'Other Rider', otherBarn))).record;
  await assert.rejects(save(store, payload('horses', 'Juniper', barnId, { riderId: otherRider.id })), { code: 'invalid_relationship' });
});
test('actor permissions and tenant visibility fail closed', async () => {
  const store = fixtureStore(); const barnId = await barn(store);
  const stranger = { ...actor, id: 'stranger' };
  assert.deepEqual((await readInputState(store, stranger)).barns, []);
  await assert.rejects(save(store, payload('users', 'Injected', barnId), stranger), { status: 404 });
  await assert.rejects(save(store, payload('users', 'Denied', barnId), { ...actor, permissions: ['inputs:read'] }), { status: 403 });
  await assert.rejects(readInputState(store, undefined), { status: 401 });
  await assert.rejects(saveInputRecord({ store, actor: { ...actor, profile: undefined }, profileLink: true, payload: { barnId, requestId: crypto.randomUUID() } }), { code: 'verified_profile_required' });
});
test('barn names remain unique within visible barns; replay cannot bypass revoked ownership', async () => {
  const store = fixtureStore();
  const input = payload('barn', 'Original Barn');
  await save(store, input);
  await assert.rejects(save(store, payload('barn', 'original barn')), { code: 'duplicate_name' });
  store.rows.barn[0].ownerUid = 'new-owner';
  await assert.rejects(save(store, input), { code: 'barn_not_found' });
});
test('stable retry, changed-payload rejection and stale edit preserve committed record', async () => {
  const store = fixtureStore(); const barnId = await barn(store);
  const input = payload('horses', 'Juniper', barnId);
  const first = await save(store, input);
  const second = await save(store, input);
  assert.equal(second.record.id, first.record.id);
  assert.equal(store.rows.horses.length, 1);
  await assert.rejects(save(store, { ...input, draft: { name: 'Other Horse' } }), { code: 'request_id_reused' });
  await save(store, { ...payload('horses', 'Changed', barnId, { id: first.record.id }), expectedRevision: 1 });
  await assert.rejects(save(store, { ...payload('horses', 'Stale overwrite', barnId, { id: first.record.id }), expectedRevision: 1 }), { code: 'record_changed' });
  assert.equal(store.rows.horses[0].name, 'Changed');
});
test('uncertain audit result is repaired on identical retry without duplicating the record', async () => {
  const store = fixtureStore(); const barnId = await barn(store);
  const input = payload('locations', 'Paddock', barnId);
  store.failAudit = true;
  await assert.rejects(save(store, input), { code: 'write_outcome_unknown' });
  assert.equal(store.rows.locations.length, 1);
  store.failAudit = false;
  const response = await save(store, input);
  assert.equal(response.record.revision, 1);
  assert.equal(store.rows.locations.length, 1);
  assert.equal(store.events.size, 2);
});
test('request boundary rejects spoofed actors, cross-origin writes and malformed requests', async () => {
  const store = fixtureStore();
  const request = (body, origin = 'https://ringstatus.test') => new Request('https://ringstatus.test/test/rs-inputs/record', { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: origin, 'X-Actor-Id': actor.id }, body });
  assert.equal((await handleInputRequest({ request: request('{}'), store })).status, 401);
  assert.equal((await handleInputRequest({ request: request('{}', 'https://other.test'), store, actor })).status, 403);
  assert.equal((await handleInputRequest({ request: request('{'), store, actor })).status, 400);
  assert.equal((await handleInputRequest({ request: request('[]'), store, actor })).status, 400);
  const response = await handleInputRequest({ request: request(JSON.stringify(payload('barn', 'HTTP Barn'))), store, actor });
  assert.equal(response.status, 200); assert.equal(response.headers.get('Cache-Control'), 'no-store');
  assert.equal((await response.json()).state.barns[0].name, 'HTTP Barn');
});
test('Airtable adapter never targets infrastructure/legacy bases and refuses unqualified writes', async () => {
  for (const base of ['', 'appZahVgD156cMAe3', 'apptdhhNzduxm5gjn']) assert.throws(() => createAirtableInputStore({ env: { RS_INPUTS_BASE_ID: base, AIRTABLE_TOKEN: 'fixture' } }), { code: 'clean_input_base_required' });
  const store = createAirtableInputStore({ env: { RS_INPUTS_BASE_ID: 'app9kOZdIaGyKk5uG', AIRTABLE_TOKEN: 'fixture' }, fetchImpl: async () => { throw new Error('must not call'); } });
  await assert.rejects(store.put('users', {}), { code: 'storage_concurrency_not_qualified' });
  assert.equal(store.capabilities.atomicCompareAndSwap, false);
});
test('Airtable pagination and stale-write preflight use canonical IDs', async () => {
  const calls = [];
  const store = createAirtableInputStore({ env: { RS_INPUTS_BASE_ID: 'app9kOZdIaGyKk5uG', AIRTABLE_TOKEN: 'fixture', RS_INPUTS_WRITE_MODE: 'isolated-trial' }, fetchImpl: async (url, init) => {
    calls.push({ url: String(url), ...init });
    if (new URL(url).searchParams.has('filterByFormula')) return Response.json({ records: [{ id: 'recInternal', fields: { entity_uid: 'canonical-a', revision: 3 } }] });
    const next = new URL(url).searchParams.has('offset');
    return Response.json({ records: [{ fields: { entity_uid: next ? 'canonical-b' : 'canonical-a', barn_uid: 'barn-a', name: next ? 'B' : 'A', revision: 1 } }], ...(next ? {} : { offset: 'page-2' }) });
  } });
  assert.deepEqual((await store.list('users')).map(row => row.id), ['canonical-a', 'canonical-b']);
  await assert.rejects(store.put('users', { id: 'canonical-a' }, { expectedRevision: 1 }), { code: 'record_changed' });
  assert.equal(calls.some(call => call.method !== 'GET'), false);
});
