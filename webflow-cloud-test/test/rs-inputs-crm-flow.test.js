import { controlDatabase } from '../test-support/recognize-control-db.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { handleAuthenticatedInputRoute, handleInputRoute } from '../src/pages/rs-inputs/[operation].js';
import { issueAccess } from '../scripts/rs-inputs-access.mjs';
import { CRM_BARN_MAPPING, CRM_BARN_PREFIX, createInputStore } from '../src/lib/rs-inputs-store.js';

const bindings = { RS_INPUTS_BASE_ID: 'app9kOZdIaGyKk5uG', RS_INPUTS_RECOGNITION_BASE_ID: 'app9kOZdIaGyKk5uG',
  RS_INPUTS_WRITE_MODE: 'isolated-trial', RS_INPUTS_BARN_STORAGE: 'zoho-crm', AIRTABLE_TOKEN: 'fixture',
  RS_INPUTS_SESSION_SECRET: 'ab'.repeat(32), ZOHO_CLIENT_ID: 'fixture', ZOHO_CLIENT_SECRET: 'fixture', ZOHO_REFRESH_TOKEN: 'fixture' };
const origin = 'https://crm-flow.test';
function fixture() {
  const env = { ...bindings, RS_RECOGNITION_CONTROL_DB: controlDatabase() }, tables = new Map(), calls = [], crm = [], audits = [];
  let n = 0, tick = 0, failAudit = false, loseCrmResponse = false, cookie;
  const rows = table => { if (!tables.has(table)) tables.set(table, []); return tables.get(table); };
  const person = { id: 'recPerson00000001', fields: { person_uid: 'rs_auth_crm_test_person', person_name: 'TEST Person', status: 'Active', access_level: 'Guest' } };
  rows('rs_people_test').push(person);
  async function fetchImpl(input, init = {}) {
    const url = new URL(input), method = init.method || 'GET';
    calls.push({ host: url.host, path: url.pathname, method, headers: init.headers });
    if (url.host === 'accounts.zoho.com') return Response.json({ access_token: 'fixture-access', expires_in: 3600, api_domain: 'https://www.zohoapis.com' });
    if (url.host === 'www.zohoapis.com') {
      assert.equal(init.headers.Authorization, 'Zoho-oauthtoken fixture-access');
      if (url.pathname.endsWith('/org')) return Response.json({ org: [{ zgid: '941333935' }] });
      if (url.pathname.endsWith('/settings/modules/Accounts')) return Response.json({ modules: [{ api_name: 'Accounts', api_supported: true }] });
      if (url.pathname.endsWith('/settings/fields')) return Response.json({ fields: Object.entries(CRM_BARN_MAPPING.barns.fields).map(([key, api_name]) => ({ api_name,
        data_type: key === 'revision' ? 'integer' : 'text', operation_type: { api_create: true, api_update: true }, unique: key === 'entity_uid' ? { case_sensitive: false } : {} })) });
      const id = url.pathname.match(/\/Accounts\/(\d+)$/)?.[1];
      if (method === 'GET') return Response.json({ data: structuredClone(id ? crm.filter(row => row.id === id) : crm), info: { more_records: false } });
      const body = JSON.parse(init.body), values = body.data[0];
      assert.deepEqual(body.trigger, []);
      let row = id ? crm.find(row => row.id === id) : null;
      if (method === 'POST') {
        if (crm.some(row => row.RS_Entity_UID === values.RS_Entity_UID)) return Response.json({ data: [{ status: 'error', code: 'DUPLICATE_DATA' }] }, { status: 207 });
        row = { id: String(1000 + ++n) }; crm.push(row);
      } else {
        assert.equal(method, 'PUT'); assert.ok(row);
        if (init.headers['If-Unmodified-Since'] !== row.Modified_Time) return Response.json({ code: 'ALREADY_MODIFIED' }, { status: 412 });
      }
      Object.assign(row, values, { Modified_Time: new Date(Date.UTC(2026, 9, 6, 15, 0, ++tick)).toISOString() });
      if (loseCrmResponse) { loseCrmResponse = false; throw new Error('simulated lost CRM response after commit'); }
      return Response.json({ data: [{ status: 'success', code: 'SUCCESS', details: { id: row.id, Modified_Time: row.Modified_Time } }] });
    }
    assert.equal(url.host, 'api.airtable.com');
    const [, base, table, id] = url.pathname.split('/').filter(Boolean).map(decodeURIComponent);
    assert.equal(base, env.RS_INPUTS_BASE_ID);
    if (method === 'GET') {
      if (id) return Response.json(structuredClone(rows(table).find(row => row.id === id) || {}));
      let found = rows(table); const formula = url.searchParams.get('filterByFormula');
      if (formula) {
        const clauses = [...formula.matchAll(/\{([^}]+)\}\s*=\s*'((?:\\.|[^'])*)'/g)]; assert.ok(clauses.length);
        found = found.filter(row => clauses.some(([, key, value]) => row.fields[key] === value.replace(/\\(['\\])/g, '$1')));
      }
      return Response.json({ records: structuredClone(found) });
    }
    if (table === 'tblwts3huk3w1ACjh' && failAudit) { failAudit = false; throw new Error('simulated uncertain audit'); }
    const body = JSON.parse(init.body), out = [];
    for (const entry of body.records) {
      let row = entry.id ? rows(table).find(row => row.id === entry.id) : body.performUpsert ? rows(table).find(row => body.performUpsert.fieldsToMergeOn.every(key => row.fields[key] === entry.fields[key])) : null;
      if (!row) { row = { id: `rec${String(++n).padStart(14, '0')}`, fields: {} }; rows(table).push(row); }
      Object.assign(row.fields, entry.fields); out.push(structuredClone(row));
      if (table === 'tblwts3huk3w1ACjh') audits.push(structuredClone(row));
    }
    return Response.json({ records: out });
  }
  const request = (op, body, useCookie = true) => new Request(`${origin}/test/rs-inputs/${op}`, {
    method: body ? 'POST' : 'GET', headers: { ...(body ? { Origin: origin, 'Content-Type': 'application/json' } : {}),
      ...(cookie && useCookie ? { Cookie: cookie } : {}) }, ...(body ? { body: JSON.stringify(body) } : {}) });
  const route = (op, body, useCookie) => handleAuthenticatedInputRoute({ request: request(op, body, useCookie) }, env, fetchImpl);
  async function login() {
    const grant = await issueAccess({ env, personUid: person.fields.person_uid, decision: 'invited', fetchImpl });
    const response = await route('access', { token: grant.token }); assert.equal(response.status, 200);
    cookie = response.headers.get('Set-Cookie').split(';')[0];
  }
  return { env, fetchImpl, route, request, login, crm, rows, calls, audits, person,
    failNextAudit: () => { failAudit = true; }, loseNextCrmResponse: () => { loseCrmResponse = true; } };
}
const body = async (response, status = 200) => { const result = await response.json(); assert.equal(response.status, status, JSON.stringify(result)); return result; };

test('signed-in existing Barn API saves, edits and reloads one CRM Account; roster/audit remain Airtable', async () => {
  const f = fixture(); await body(await f.route('state'), 401); assert.equal(f.calls.length, 0);
  await f.login();
  const create = { kind: 'barn', draft: { name: 'TEST RingStatus CRM Input' }, requestId: 'crm_barn_create_01' };
  const saved = await body(await f.route('record', create));
  const barnId = saved.record.id, providerId = f.crm[0].id;
  assert.equal(f.crm[0].RS_Entity_UID, CRM_BARN_PREFIX + barnId);
  assert.equal(f.crm[0].RS_Owner_UID, f.person.fields.person_uid);
  assert.equal(f.rows('tblRvTwo3HYPUkZou').length, 0);
  await body(await f.route('record', create)); assert.equal(f.crm.length, 1);
  const edit = { kind: 'barn', barnId, draft: { id: barnId, name: 'TEST RingStatus CRM Input saved' }, expectedRevision: 1, requestId: 'crm_barn_edit_01' };
  await body(await f.route('record', edit)); await body(await f.route('record', edit));
  await body(await f.route('record', { ...edit, requestId: 'crm_barn_stale_01' }), 409);
  const state = (await body(await f.route('state'))).state;
  assert.equal(state.barns.length, 1); assert.equal(state.barns[0].name, edit.draft.name);
  assert.equal(state.barns[0].revision, 2); assert.equal(f.crm[0].id, providerId); assert.equal(f.crm.length, 1);
  await body(await f.route('record', { kind: 'horses', barnId, draft: { name: 'TEST Horse' }, requestId: 'crm_horse_create_01' }));
  assert.equal(f.rows('tblpyyaOMgjLzLvkP')[0].fields.barn_uid, barnId);
  assert.equal(f.rows('tblwts3huk3w1ACjh').length, 3);
  assert.ok(f.rows('tblwts3huk3w1ACjh').every(row => row.fields.event_uid.startsWith('crm-barns:')));
  await issueAccess({ env: f.env, personUid: f.person.fields.person_uid, decision: 'revoked', fetchImpl: f.fetchImpl });
  await body(await f.route('state'), 401);
});
test('a failed audit after committed CRM creation recovers without a second Account', async () => {
  const f = fixture(); await f.login(); f.failNextAudit();
  const payload = { kind: 'barn', draft: { name: 'TEST Recovery Barn' }, requestId: 'crm_uncertain_01' };
  assert.equal((await body(await f.route('record', payload), 503)).error, 'write_outcome_unknown');
  assert.equal(f.crm.length, 1);
  await body(await f.route('record', payload)); assert.equal(f.crm.length, 1); assert.equal(f.rows('tblwts3huk3w1ACjh').length, 1);
});
test('a lost CRM response after commit retries the same create or edit without applying twice', async () => {
  const f = fixture(); await f.login(); f.loseNextCrmResponse();
  const payload = { kind: 'barn', draft: { name: 'TEST CRM Lost Response' }, requestId: 'crm_lost_response_create' };
  assert.equal((await body(await f.route('record', payload), 503)).error, 'write_outcome_unknown');
  assert.equal(f.crm.length, 1);
  const saved = await body(await f.route('record', payload)); assert.equal(f.crm.length, 1);
  f.loseNextCrmResponse();
  const edit = { kind: 'barn', barnId: saved.record.id, draft: { id: saved.record.id, name: 'TEST CRM Recovered' }, expectedRevision: 1, requestId: 'crm_lost_response_edit' };
  await body(await f.route('record', edit), 503); await body(await f.route('record', edit));
  assert.equal(f.crm.length, 1); assert.equal(f.crm[0].RS_Revision, 2);
  assert.equal(f.crm[0].Account_Name, edit.draft.name);
  assert.equal(f.calls.filter(call => call.host === 'www.zohoapis.com' && call.method === 'POST').length, 1);
  assert.equal(f.calls.filter(call => call.host === 'www.zohoapis.com' && call.method === 'PUT').length, 1);
});
test('different owners and read-only principals cannot update the CRM barn', async () => {
  const f = fixture(); await f.login();
  const saved = await body(await f.route('record', { kind: 'barn', draft: { name: 'TEST Private' }, requestId: 'crm_private_01' }));
  const request = f.request('record', { kind: 'barn', barnId: saved.record.id, draft: { id: saved.record.id, name: 'Wrong' }, expectedRevision: 1, requestId: 'crm_denied_01' });
  const actor = { id: 'another-person', permissions: ['inputs:read', 'inputs:write', 'barns:create'] };
  await body(await handleInputRoute({ request: request.clone(), locals: { rsInputActor: actor } }, f.env, f.fetchImpl), 404);
  actor.id = f.person.fields.person_uid; actor.permissions = ['inputs:read'];
  await body(await handleInputRoute({ request: request.clone(), locals: { rsInputActor: actor } }, f.env, f.fetchImpl), 403);
  assert.equal(f.crm[0].Account_Name, 'TEST Private'); assert.equal(f.crm[0].RS_Revision, 1);
});
test('default stays Airtable and invalid storage/absent CRM credentials fail closed', () => {
  const { RS_INPUTS_BARN_STORAGE, ...env } = bindings;
  const f = () => assert.fail('configuration must not fetch');
  assert.ok(createInputStore({ env, fetchImpl: f }));
  assert.throws(() => createInputStore({ env: { ...env, RS_INPUTS_BARN_STORAGE: 'wrong' }, fetchImpl: f }), { code: 'invalid_barn_storage' });
  assert.throws(() => createInputStore({ env: { ...bindings, ZOHO_REFRESH_TOKEN: '' }, fetchImpl: f }), { code: 'crm_credentials_missing' });
});

test('switching trial storage cannot reuse an Airtable barn ID or attach its roster', async () => {
  const f = fixture(); f.env.RS_INPUTS_BARN_STORAGE = 'airtable'; await f.login();
  const payload = { kind: 'barn', draft: { name: 'Airtable barn' }, requestId: 'shared_request_id' };
  const old = await body(await f.route('record', payload));
  await body(await f.route('record', { kind: 'horses', barnId: old.record.id, draft: { name: 'Airtable-only horse' }, requestId: 'original_horse_id' }));
  f.env.RS_INPUTS_BARN_STORAGE = 'zoho-crm';
  const fresh = await body(await f.route('record', { ...payload, draft: { name: 'Separate CRM barn' } }));
  assert.notEqual(fresh.record.id, old.record.id);
  assert.equal(fresh.state.horses.length, 0);
  assert.equal(f.rows('tblpyyaOMgjLzLvkP').length, 1);
});
