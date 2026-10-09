import { withInputLifecycle } from '../test-support/rs-inputs-lifecycle-schema.mjs';
import { installControlDatabase } from '../test-support/recognize-control-db.mjs';
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
const actor = { id: 'flow-actor', permissions: ['inputs:read', 'inputs:write', 'barns:create'], barnIds: [], profile: {personUid:'flow-actor',name:'Synthetic Person',email:'synthetic@example.test'} };

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


import vm from 'node:vm';
import { stripTypeScriptTypes } from 'node:module';
import { createInputAccess } from '../src/lib/rs-inputs-access.js';
import { handleAuthenticatedInputRoute } from '../src/pages/rs-inputs/[operation].js';

const apiSource = stripTypeScriptTypes(await readFile(new URL('../src/components/rs-onboarding/api.ts', import.meta.url), 'utf8'), {mode:'transform'}).replace(/^export /gm, '');
const env = { RS_INPUTS_BASE_ID: baseId, RS_INPUTS_RECOGNITION_BASE_ID: baseId, RS_INPUTS_BARN_STORAGE:'airtable', RS_INPUTS_WRITE_MODE:'isolated-trial', AIRTABLE_TOKEN:'fixture-token', RS_INPUTS_SESSION_SECRET:'a'.repeat(64) };
installControlDatabase(env);

async function journey() {
  const provider = fakeAirtable({pageSize:100});
  const person = {id:'recSyntheticOnly1', fields:{person_uid:actor.id,person_name:actor.profile.name,email:actor.profile.email,status:'test',input_access:'approved',input_session_version:'b'.repeat(32),input_invite_hash:'fixture',input_invite_expires_at:new Date(Date.now()+60000).toISOString()}};
  const invitationStore = {async byHash(){return person;},async byUid(){return person;},async update(id,fields){Object.assign(person.fields,fields);return person;}};
  const access = createInputAccess({env,store:invitationStore});
  let cookie = (await access.redeem(new Request(`${origin}/test/rs-inputs/access`), 'a'.repeat(43))).cookie.split(';')[0];
  const requests = [], accessCalls = [];
  let failNextTransport = false, malformedNext = false;
  const providerFetch = async (value, init) => {
    const url = new URL(value);
    assert.equal(url.origin, 'https://api.airtable.com');
    assert.equal(url.pathname.split('/')[2], baseId);
    if(url.pathname.endsWith('/rs_people_test')) {
      accessCalls.push({url:String(url),method:init.method});
      assert.equal(init.method,'GET','Normal input actions cannot mutate access/recognition');
      return Response.json({records:[structuredClone(person)]});
    }
    return provider.fetchImpl(value,init);
  };
  const browserFetch = async (url, init={}) => {
    assert.ok(url.startsWith('/test/rs-inputs/'));
    assert.equal(init.credentials,'same-origin');
    assert.equal(init.cache,'no-store');
    const body = init.body ? JSON.parse(init.body) : null;
    requests.push({url,body});
    if(failNextTransport){failNextTransport=false;throw Error('Synthetic connection fault');}
    const request = new Request(`${origin}${url}`, {...init,headers:{...init.headers,Origin:origin,...(cookie?{Cookie:cookie}:{})}});
    // Run the existing public authentication and dispatch. The adapter pacing is
    // real here; HTTP and provider fetch are in-process doubles, never the network.
    const response = await handleAuthenticatedInputRoute({request},env,providerFetch);
    if(malformedNext){malformedNext=false;return Response.json({ok:true,record:{},state:{}});}
    return response;
  };
  function newClient() {
    const context = vm.createContext({fetch:browserFetch,crypto,URLSearchParams});
    vm.runInContext(apiSource+'\nglobalThis.api = createInputsApi("/test/rs-inputs");',context);
    return context.api;
  }
  return {provider,person,requests,accessCalls,newClient,api:newClient(),providerFetch,
    clearCookie(){cookie='';},failTransport(){failNextTransport=true;},malformed(){malformedNext=true;}};
}
const inputDraft = (name,extra={}) => ({name,email:'',userId:'',riderId:'',locationId:'',address:'',...extra});
const assertCode = (code) => err => {assert.equal(err.code,code);return true;};

test('existing client → authenticated route → Inputs tables/audit → fresh client state', async () => {
  const f=await journey();
  const identity=await f.api.access();assert.equal(identity.id,actor.id);
  const barn=(await f.api.record('barn',inputDraft('Synthetic Barn'),'')).record;
  const user=(await f.api.profileLink(barn.id)).record;
  assert.equal(user.recognitionPersonId,actor.id);
  const rider=(await f.api.record('riders',inputDraft('Synthetic Rider',{userId:user.id}),barn.id)).record;
  const place=(await f.api.record('locations',inputDraft('Synthetic Location',{address:'Synthetic address'}),barn.id)).record;
  const horse=(await f.api.record('horses',inputDraft('Synthetic Horse',{riderId:rider.id,locationId:place.id}),barn.id)).record;
  await f.api.record('horses',inputDraft('Edited Horse',{...horse,name:'Edited Horse'}),barn.id);
  const state=await f.newClient().state(barn.id);
  assert.equal(state.horses[0].name,'Edited Horse');assert.equal(state.horses[0].revision,2);
  assert.equal(state.horses[0].riderId,rider.id);assert.equal(state.horses[0].locationId,place.id);
  assert.equal(state.users[0].email,actor.profile.email);
  assert.equal(f.provider.rows.get('rs_input_events').length,6);
  assert.equal(f.provider.rows.get('rs_input_events').at(-1).fields.outcome,'committed');
  assert.ok(f.provider.calls.every(c=>new URL(c.url).pathname.startsWith(`/v0/${baseId}/rs_input_`)));
  assert.equal('ownerUid' in state.barns[0],false);assert.equal('requestHash' in state.horses[0],false);
  for(const table of ['rs_input_barns','rs_input_users','rs_input_riders','rs_input_horses','rs_input_locations']) for(const row of f.provider.rows.get(table)){assert.equal(row.fields.status,'Active');assert.equal(row.fields.record_mode,'Test');}
  assert.ok(f.provider.rows.get('rs_input_events').every(row=>row.fields.record_mode==='Test'));
  assert.deepEqual(f.provider.schemaFailures,[]);
});

test('unknown save outcome keeps same request ID and retry does not repeat committed entity',async()=>{
  const f=await journey();const barn=(await f.api.record('barn',inputDraft('Retry Barn'),'')).record;
  const draft=inputDraft('Retry Horse');
  f.provider.faults.push({table:'rs_input_events',method:'PATCH',when:'before'});
  await assert.rejects(f.api.record('horses',draft,barn.id),assertCode('write_outcome_unknown'));
  const first=f.requests.at(-1).body;
  const result=await f.api.record('horses',draft,barn.id);
  assert.equal(f.requests.at(-1).body.requestId,first.requestId);
  assert.equal(result.record.revision,1);assert.equal(patches(f.provider,'rs_input_horses').length,1);
  assert.equal(f.provider.rows.get('rs_input_events').length,2);
});

test('client transport failure and malformed committed response retain retry identity',async()=>{
  const f=await journey();const draft=inputDraft('Transport Barn');f.failTransport();
  await assert.rejects(f.api.record('barn',draft,''),/Could not connect/);
  const requestId=f.requests.at(-1).body.requestId;f.malformed();
  await assert.rejects(f.api.record('barn',draft,''),/could not be verified/);
  assert.equal(f.requests.at(-1).body.requestId,requestId);
  const result=await f.api.record('barn',draft,'');
  assert.equal(f.requests.at(-1).body.requestId,requestId);assert.equal(result.record.revision,1);
  assert.equal(patches(f.provider,'rs_input_barns').length,1);
});

test('stale revision, invalid relationship and duplicate name leave records unchanged',async()=>{
  const f=await journey();const barn=(await f.api.record('barn',inputDraft('Guard Barn'),'')).record;
  const horse=(await f.api.record('horses',inputDraft('Original'),barn.id)).record;
  await f.api.record('horses',inputDraft('Current',{...horse,name:'Current'}),barn.id);
  await assert.rejects(f.api.record('horses',inputDraft('Stale',{...horse,name:'Stale'}),barn.id),assertCode('record_changed'));
  await assert.rejects(f.api.record('horses',inputDraft('Current'),barn.id),assertCode('duplicate_name'));
  await assert.rejects(f.api.record('horses',inputDraft('Bad Link',{riderId:'foreign'}),barn.id),assertCode('invalid_relationship'));
  const state=await f.api.state(barn.id);assert.equal(state.horses.length,1);assert.equal(state.horses[0].name,'Current');
  assert.equal(f.provider.rows.get('rs_input_events').length,3);
});

test('revoked identity and missing cookie cannot read or mutate Input tables',async()=>{
  const f=await journey();f.person.fields.input_access='revoked';
  await assert.rejects(f.api.state(),assertCode('authentication_required'));
  assert.equal(f.provider.calls.length,0);
  f.person.fields.input_access='approved';f.clearCookie();
  await assert.rejects(f.api.record('barn',inputDraft('Denied'),''),assertCode('authentication_required'));
  assert.equal(f.provider.calls.length,0);
});

test('public entry rejects missing/wrong base and wrong-origin request before any provider call',async()=>{
  for(const patch of [{RS_INPUTS_BASE_ID:undefined},{RS_INPUTS_BASE_ID:'appOtherSynthetic'},{RS_INPUTS_RECOGNITION_BASE_ID:'appOtherSynthetic'}]){
    const response=await handleAuthenticatedInputRoute({request:new Request(`${origin}/test/rs-inputs/state`)},{...env,...patch},()=>assert.fail('No provider call permitted'));
    assert.equal(response.status,503);assert.equal((await response.json()).error,'access_trial_configuration_required');
  }
  const response=await handleAuthenticatedInputRoute({request:new Request(`${origin}/test/rs-inputs/record`,{method:'POST',headers:{Origin:'https://other.test','Content-Type':'application/json'},body:'{}'})},env,()=>assert.fail('No provider call permitted'));
  assert.equal(response.status,403);assert.equal((await response.json()).error,'origin_denied');
});
