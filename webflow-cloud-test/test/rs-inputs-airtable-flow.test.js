import { withInputLifecycle } from '../test-support/rs-inputs-lifecycle-schema.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createAirtableInputStore } from '../src/lib/rs-inputs-airtable.js';
import { handleInputRequest, digest } from '../src/lib/rs-inputs.js';

// Contract integration only: the provider below is an in-memory REST double.
// It does not establish Airtable atomicity, deployed access controls or live writes.
const schema = withInputLifecycle(JSON.parse(await readFile(new URL('../config/rs-inputs-schema.json', import.meta.url), 'utf8')));
const baseId = 'app9kOZdIaGyKk5uG';
const origin = 'https://ringstatus.test';
const actor = { id: 'flow-actor', permissions: ['inputs:read', 'inputs:write', 'barns:create'], barnIds: [] };

function fakeAirtable({ pageSize = 1 } = {}) {
  const fields = new Map(schema.tables.map(table => [table.name, new Map(table.fields.map(field => [field.name, field]))]));
  const rows = new Map(schema.tables.map(table => [table.name, []]));
  const calls = [];
  const schemaFailures = [];
  const faults = [];
  let nextId = 0;
  function checkFields(table, values) {
    for (const [name, value] of Object.entries(values)) {
      const field = fields.get(table).get(name);
      assert.ok(field, `Unexpected field ${table}.${name}`);
      if (field.type === 'number') {
        assert.equal(typeof value, 'number', `${name} must be numeric`);
        assert.ok(Number.isFinite(value));
        if (field.options?.precision === 0) assert.ok(Number.isInteger(value), `${name} must be integer`);
      } else if (['singleLineText', 'multilineText', 'email'].includes(field.type)) {
        assert.equal(typeof value, 'string', `${name} must be text`);
      } else if (field.type === 'singleSelect') {
        assert.ok(field.options.choices.some(choice => choice.name === value), `Invalid choice for ${name}`);
      } else if (field.type === 'dateTime') {
        assert.equal(typeof value, 'string');
        assert.ok(Number.isFinite(Date.parse(value)), `${name} must contain an ISO timestamp`);
      } else {
        assert.fail(`Fixture needs explicit type handling for ${field.type}`);
      }
    }
  }
  async function fetchImpl(value, init) {
    const url = new URL(value);
    const path = url.pathname.split('/').filter(Boolean).map(decodeURIComponent);
    assert.equal(url.origin, 'https://api.airtable.com');
    assert.deepEqual(path.slice(0, 2), ['v0', baseId]);
    assert.equal(path.length, 3);
    const table = path[2];
    assert.ok(rows.has(table), `Unexpected table ${table}`);
    assert.equal(init.headers.Authorization, 'Bearer fixture-token');
    assert.equal(init.headers['Content-Type'], 'application/json');
    assert.equal(init.redirect, 'manual');
    assert.ok(init.signal instanceof AbortSignal);
    assert.ok(['GET', 'PATCH'].includes(init.method));
    const body = init.body ? JSON.parse(init.body) : null;
    calls.push({ url: String(url), table, method: init.method, body });
    const faultIndex = faults.findIndex(f => f.table === table && f.method === init.method);
    const fault = faultIndex < 0 ? null : faults.splice(faultIndex, 1)[0];
    if (fault?.when === 'before') return Response.json({ error: { type: 'FIXTURE_FAILURE' } }, { status: 503 });
    if (init.method === 'GET') {
      let matching = rows.get(table);
      const formula = url.searchParams.get('filterByFormula');
      if (formula) {
        const match = /^\{(entity_uid|event_uid)\} = '((?:\\.|[^'])*)'$/.exec(formula);
        assert.ok(match, `Unexpected filterByFormula: ${formula}`);
        const lookup = match[2].replace(/\\(['\\])/g, '$1');
        matching = matching.filter(row => row.fields[match[1]] === lookup);
      }
      const offset = Number(url.searchParams.get('offset') || 0);
      assert.ok(Number.isInteger(offset) && offset >= 0);
      const page = structuredClone(matching.slice(offset, offset + pageSize));
      return Response.json({ records: page, ...(offset + pageSize < matching.length ? { offset: String(offset + pageSize) } : {}) });
    }
    const key = table === 'rs_input_events' ? 'event_uid' : 'entity_uid';
    assert.deepEqual(body.performUpsert, { fieldsToMergeOn: [key] });
    assert.equal(body.records.length, 1);
    const results = [];
    for (const incoming of body.records) {
      try { checkFields(table, incoming.fields); }
      catch (error) { schemaFailures.push(error.message); throw error; }
      assert.equal(typeof incoming.fields[key], 'string');
      assert.ok(incoming.fields[key]);
      const matching = rows.get(table).filter(row => row.fields[key] === incoming.fields[key]);
      assert.ok(matching.length <= 1, 'Ambiguous fixture upsert');
      const row = matching[0] || { id: `rec${String(++nextId).padStart(14, '0')}`, fields: {} };
      row.fields = { ...row.fields, ...structuredClone(incoming.fields) };
      if (!matching.length) rows.get(table).push(row);
      results.push(structuredClone(row));
    }
    if (fault?.when === 'after-commit') throw new Error('Fixture connection lost after Airtable committed');
    return Response.json({ records: results });
  }
  return { rows, calls, faults, schemaFailures, fetchImpl };
}

function newStore(provider) {
  return createAirtableInputStore({
    env: { RS_INPUTS_BASE_ID: baseId, AIRTABLE_TOKEN: 'fixture-token', RS_INPUTS_WRITE_MODE: 'isolated-trial' },
    fetchImpl: provider.fetchImpl,
    minimumIntervalMs: 0
  });
}
function draft(kind, name, barnId = '', extra = {}) {
  return { kind, barnId, requestId: crypto.randomUUID(), draft: { name, ...extra } };
}
async function post(provider, payload, principal = actor) {
  const response = await handleInputRequest({
    request: new Request(`${origin}/test/rs-inputs/record`, { method: 'POST', headers: { Origin: origin, 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }),
    actor: principal,
    store: newStore(provider)
  });
  assert.equal(response.headers.get('Cache-Control'), 'no-store');
  assert.equal(response.headers.get('Vary'), 'Cookie');
  assert.ok(response.headers.get('X-Request-Id'));
  return { status: response.status, ...(await response.json()) };
}
async function saved(provider, input) {
  const result = await post(provider, input);
  assert.equal(result.status, 200, JSON.stringify(result));
  assert.equal(result.ok, true);
  return result.record;
}
const patches = (provider, table) => provider.calls.filter(call => call.table === table && call.method === 'PATCH');

test('HTTP → actual Airtable adapter → schema-aware REST: complete onboarding, edit, replay and fresh read', async () => {
  const provider = fakeAirtable();
  const barn = await saved(provider, draft('barn', 'Contract Trial Barn'));
  const user = await saved(provider, draft('users', 'Trial User', barn.id, { email: 'user@example.test' }));
  const rider = await saved(provider, draft('riders', 'Trial Rider', barn.id, { userId: user.id }));
  const location = await saved(provider, draft('locations', 'Home', barn.id, { address: 'Synthetic address' }));
  const horse = await saved(provider, draft('horses', 'Juniper', barn.id, { riderId: rider.id, locationId: location.id }));
  const edit = { ...draft('horses', 'Juniper II', barn.id, { id: horse.id, riderId: rider.id, locationId: location.id }), expectedRevision: horse.revision };
  const changed = await saved(provider, edit);
  assert.equal(changed.revision, 2);
  const replayed = await saved(provider, edit);
  assert.deepEqual(replayed, changed);
  assert.equal(patches(provider, 'rs_input_horses').length, 2, 'Replay must not write the entity again');
  const auditRows = provider.rows.get('rs_input_events');
  assert.equal(auditRows.length, 6);
  assert.equal(patches(provider, 'rs_input_events').length, 6);
  const editEventId = await digest(`${actor.id}|${edit.requestId}`);
  const editAudit = auditRows.find(row => row.fields.event_uid === editEventId).fields;
  assert.equal(editAudit.actor_uid, actor.id);
  assert.equal(editAudit.action, 'update');
  assert.equal(editAudit.kind, 'horses');
  assert.equal(editAudit.outcome, 'committed');
  assert.deepEqual(JSON.parse(editAudit.result_json), changed);

  // Ensure pagination is exercised with actual persisted records, not a canned response.
  await saved(provider, draft('users', 'Second Trial User', barn.id));
  const response = await handleInputRequest({ request: new Request(`${origin}/test/rs-inputs/state?barn_id=${barn.id}`), actor, store: newStore(provider) });
  assert.equal(response.status, 200);
  const { state } = await response.json();
  assert.equal(state.users.length, 2);
  assert.equal(state.selectedBarn, barn.id);
  assert.equal(state.riders[0].userId, user.id);
  assert.equal(state.horses[0].riderId, rider.id);
  assert.equal(state.horses[0].locationId, location.id);
  assert.equal(state.horses[0].name, 'Juniper II');
  assert.equal('ownerUid' in state.barns[0], false);
  assert.equal('requestHash' in state.horses[0], false);
  assert.ok(provider.calls.some(call => new URL(call.url).searchParams.has('offset')));
  const storedHorse = provider.rows.get('rs_input_horses')[0];
  assert.equal(storedHorse.fields.entity_uid, horse.id);
  assert.notEqual(storedHorse.id, horse.id, 'Canonical identity must not become Airtable record identity');
  assert.equal(storedHorse.fields.rider_uid, rider.id);
  assert.equal(storedHorse.fields.location_uid, location.id);
  assert.deepEqual(provider.schemaFailures, []);
});

test('actual adapter preserves relationship boundaries and refuses stale edits after reload', async () => {
  const provider = fakeAirtable();
  const first = await saved(provider, draft('barn', 'First'));
  const second = await saved(provider, draft('barn', 'Second'));
  const otherRider = await saved(provider, draft('riders', 'Other Rider', second.id));
  const forbidden = await post(provider, draft('horses', 'Invalid Association', first.id, { riderId: otherRider.id }));
  assert.equal(forbidden.status, 400);
  assert.equal(forbidden.error, 'invalid_relationship');
  assert.equal(provider.rows.get('rs_input_horses').length, 0);
  const horse = await saved(provider, draft('horses', 'Original', first.id));
  await saved(provider, { ...draft('horses', 'Updated', first.id, { id: horse.id }), expectedRevision: 1 });
  const stale = await post(provider, { ...draft('horses', 'Stale', first.id, { id: horse.id }), expectedRevision: 1 });
  assert.equal(stale.status, 409);
  assert.equal(stale.error, 'record_changed');
  assert.equal(provider.rows.get('rs_input_horses')[0].fields.name, 'Updated');
  const denied = await post(provider, draft('users', 'Unauthorized', first.id), { ...actor, id: 'other-actor' });
  assert.equal(denied.status, 404);
  assert.equal(provider.rows.get('rs_input_users').length, 0);
  assert.deepEqual(provider.schemaFailures, []);
});

test('committed entity plus failed audit repairs on identical retry through a fresh adapter', async () => {
  const provider = fakeAirtable();
  const barn = await saved(provider, draft('barn', 'Audit Failure Barn'));
  const input = draft('locations', 'Paddock', barn.id, { address: 'Trial address' });
  provider.faults.push({ table: 'rs_input_events', method: 'PATCH', when: 'before' });
  const failed = await post(provider, input);
  assert.equal(failed.status, 503);
  assert.equal(failed.error, 'write_outcome_unknown');
  assert.equal(provider.rows.get('rs_input_locations').length, 1);
  assert.equal(provider.rows.get('rs_input_events').length, 1, 'Only barn audit should exist');
  const committed = structuredClone(provider.rows.get('rs_input_locations')[0]);
  const retry = await saved(provider, input);
  assert.equal(retry.id, committed.fields.entity_uid);
  assert.equal(retry.revision, 1);
  assert.deepEqual(provider.rows.get('rs_input_locations')[0], committed);
  assert.equal(patches(provider, 'rs_input_locations').length, 1, 'Recovery must not reapply the entity write');
  assert.equal(provider.rows.get('rs_input_events').length, 2);
  assert.deepEqual(provider.schemaFailures, []);
});

test('PATCH timeout after commit returns unknown outcome; fresh retry repairs audit without duplicate entity', async () => {
  const provider = fakeAirtable();
  const barn = await saved(provider, draft('barn', 'Timeout Barn'));
  const input = draft('horses', 'Timeout Horse', barn.id);
  provider.faults.push({ table: 'rs_input_horses', method: 'PATCH', when: 'after-commit' });
  const first = await post(provider, input);
  assert.equal(first.status, 503);
  assert.equal(first.error, 'write_outcome_unknown');
  assert.equal(provider.rows.get('rs_input_horses').length, 1);
  assert.equal(provider.rows.get('rs_input_events').length, 1);
  const retry = await saved(provider, input);
  assert.equal(retry.revision, 1);
  assert.equal(provider.rows.get('rs_input_horses').length, 1);
  assert.equal(patches(provider, 'rs_input_horses').length, 1);
  assert.equal(provider.rows.get('rs_input_events').length, 2);
  const reused = await post(provider, { ...input, draft: { name: 'Different Payload' } });
  assert.equal(reused.status, 409);
  assert.equal(reused.error, 'request_id_reused');
  assert.equal(provider.rows.get('rs_input_horses')[0].fields.name, 'Timeout Horse');
  assert.deepEqual(provider.schemaFailures, []);
});
