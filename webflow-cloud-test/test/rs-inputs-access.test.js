import test from 'node:test';
import assert from 'node:assert/strict';
import { createInputAccess, accessHash, randomAccessToken } from '../src/lib/rs-inputs-access.js';
import { issueAccess } from '../scripts/rs-inputs-access.mjs';
import { handleAuthenticatedInputRoute } from '../src/pages/rs-inputs/[operation].js';
const env = { RS_INPUTS_BASE_ID: 'app9kOZdIaGyKk5uG', RS_INPUTS_RECOGNITION_BASE_ID: 'app9kOZdIaGyKk5uG', RS_INPUTS_WRITE_MODE: 'isolated-trial', AIRTABLE_TOKEN: 'fixture', RS_INPUTS_SESSION_SECRET: 'ab'.repeat(32) };
const origin = 'https://access.test';
const request = (op='access', body, cookie, options={}) => new Request(`${origin}/test/rs-inputs/${op}`, { method: body ? 'POST' : 'GET', headers: { ...(body ? { Origin: origin, 'Content-Type': 'application/json' } : {}), ...(cookie ? { Cookie: cookie } : {}), ...options.headers }, ...(body ? { body: JSON.stringify(body) } : {}), ...Object.fromEntries(Object.entries(options).filter(([k])=>k!=='headers')) });
function provider() {
  let n=0; const tables=new Map(), calls=[];
  const rows = table => { if (!tables.has(table)) tables.set(table, []); return tables.get(table); };
  const person = { id: 'recPerson00000001', fields: { person_uid: 'person_invited', person_name: 'Invited Person', status: 'Active', access_level: 'Guest' } };
  rows('rs_people_test').push(person);
  async function fetchImpl(input, init={}) {
    const url=new URL(input), [, base, table, id]=url.pathname.split('/').filter(Boolean).map(decodeURIComponent); assert.equal(base, env.RS_INPUTS_BASE_ID);
    calls.push({table,method:init.method||'GET'});
    if (!init.method || init.method==='GET') {
      if(id) return Response.json(rows(table).find(r=>r.id===id)||{}, {status:rows(table).some(r=>r.id===id)?200:404});
      const formula=url.searchParams.get('filterByFormula'); let found=rows(table);
      if(formula) {
        const clauses=[...formula.matchAll(/\{([^}]+)\}\s*=\s*'((?:\\.|[^'])*)'/g)]; assert.ok(clauses.length,formula);
        found=found.filter(r=>clauses.some(([,k,v])=>r.fields[k]===v.replace(/\\(['\\])/g,'$1')));
      }
      return Response.json({records:structuredClone(found)});
    }
    const body=JSON.parse(init.body), out=[];
    for(const update of body.records) {
      let row=update.id?rows(table).find(r=>r.id===update.id):body.performUpsert?rows(table).find(r=>body.performUpsert.fieldsToMergeOn.every(k=>r.fields[k]===update.fields[k])):null;
      if (!row) { row={id:'rec'+String(++n).padStart(14,'0'),fields:{}}; rows(table).push(row); }
      Object.assign(row.fields,update.fields); out.push(structuredClone(row));
    }
    return Response.json({records:out});
  }
  const route=(req,bindings=env)=>handleAuthenticatedInputRoute({request:req,locals:{rsInputActor:{id:'forged',permissions:['barns:create']}}},bindings,fetchImpl);
  return {person,rows,fetchImpl,route,calls};
}
const json = async (response,status) => { const body=await response.json(); assert.equal(response.status,status,JSON.stringify(body)); return body; };
async function invited(f,decision='invited') { return issueAccess({env,personUid:f.person.fields.person_uid,decision,fetchImpl:f.fetchImpl}); }
async function login(f) { const invite=await invited(f);const response=await f.route(request('access',{token:invite.token}));await json(response,200);return response.headers.get('Set-Cookie').split(';')[0]; }

test('public entry ignores forged locals, device cookies and access headers',async()=>{
 const f=provider();await json(await f.route(request('state',undefined,'rs_device_token=known',{headers:{'X-Actor-Id':'person_invited'}})),401);assert.equal(f.calls.length,0);
});
test('invited and approved both establish a scoped signed session; Guest membership unchanged',async()=>{
 for(const decision of ['invited','approved']) {const f=provider(), grant=await invited(f,decision);const response=await f.route(request('access',{token:grant.token}));const body=await json(response,200);const set=response.headers.get('Set-Cookie');assert.match(set,/Path=\/test\/rs-inputs; Max-Age=28800; HttpOnly; Secure; SameSite=Strict/);assert.equal(body.actor.id,f.person.fields.person_uid);assert.equal(f.person.fields.access_level,'Guest');assert.equal(f.person.fields.input_invite_hash,'');await json(await f.route(request('access',undefined,set.split(';')[0])),200);await json(await f.route(request('access',{token:grant.token})),401);}
});
test('expired/revoked/malformed invitations fail; missing secret never consumes invitation',async()=>{
 for(const condition of ['expired','revoked','malformed','missing-secret']){const f=provider(),grant=await invited(f),hash=f.person.fields.input_invite_hash;if(condition==='expired')f.person.fields.input_invite_expires_at='2000-01-01T00:00:00Z';if(condition==='revoked')f.person.fields.input_access='revoked';await json(await f.route(request('access',{token:condition==='malformed'?'phone1234':grant.token}),condition==='missing-secret'?{...env,RS_INPUTS_SESSION_SECRET:''}:env),condition==='missing-secret'?503:401);assert.equal(f.person.fields.input_invite_hash,hash);}
});
test('cross-origin and oversized redemption cannot read or consume a grant',async()=>{
 const f=provider();await json(await f.route(request('access',{token:randomAccessToken()},undefined,{headers:{Origin:'https://other.test'}})),403);await json(await f.route(request('access',{token:'x'.repeat(2048)})),413);assert.equal(f.calls.length,0);
});
test('duplicate invitation or canonical person identities fail closed',async()=>{
 for(const duplicateHash of [true,false]){const f=provider(),grant=await invited(f);f.rows('rs_people_test').push({id:'recOther00000001',fields:{...f.person.fields,...(duplicateHash?{person_uid:'other'}:{input_invite_hash:'different'})}});await json(await f.route(request('access',{token:grant.token})),409);assert.ok(f.person.fields.input_invite_hash);}
});
test('forged/expired/wrong-origin sessions and provider failure cannot access records',async()=>{
 const f=provider(),cookie=await login(f);const last=cookie.at(-1);await json(await f.route(request('state',undefined,cookie.slice(0,-1)+(last==='A'?'B':'A'))),401);
 const later=createInputAccess({env,fetchImpl:f.fetchImpl,now:()=>Date.now()+9*60*60*1000});await assert.rejects(later.session(request('state',undefined,cookie)),{code:'authentication_required'});
 const access=createInputAccess({env,fetchImpl:f.fetchImpl});await assert.rejects(access.session(new Request('https://other.test/test/rs-inputs/state',{headers:{Cookie:cookie}})),{code:'authentication_required'});
 await assert.rejects(createInputAccess({env,fetchImpl:async()=>{throw Error('offline');}}).session(request('state',undefined,cookie)),{code:'storage_unavailable'});
});
test('reissue recovers consumed/lost response; invalidates old session and previous link',async()=>{
 const f=provider(),cookie=await login(f),grant=await invited(f);await json(await f.route(request('state',undefined,cookie)),401);const newer=await invited(f);await json(await f.route(request('access',{token:grant.token})),401);await json(await f.route(request('access',{token:newer.token})),200);
 await issueAccess({env,personUid:f.person.fields.person_uid,decision:'revoked',fetchImpl:f.fetchImpl});await json(await f.route(request('state',undefined,cookie)),401);
});
test('unknown consumption response issues no session and remains an explicit unknown outcome',async()=>{
 const f=provider(),grant=await invited(f);const access=createInputAccess({env,fetchImpl:async(u,o)=>{const r=await f.fetchImpl(u,o);if(o.method==='PATCH')throw Error('response lost');return r;}});
 await assert.rejects(access.redeem(request('access',{token:grant.token}),grant.token),{code:'write_outcome_unknown'});assert.equal(f.person.fields.input_invite_hash,'');
});
test('production mode cannot enable trial invite redemption or session access',async()=>{
 const f=provider();await json(await f.route(request('access',{token:randomAccessToken()}),{...env,RS_INPUTS_WRITE_MODE:'production'}),503);assert.equal(f.calls.length,0);
});
test('cookie-backed invitation -> new device -> profile edit -> barn save/reload; no legacy member grant',async()=>{
 const f=provider(),cookie=await login(f);const get=op=>f.route(request(op,undefined,cookie));const post=(op,body)=>f.route(request(op,body,cookie));
 const first=await json(await get('recognition'),200);assert.equal(first.recognized,false);assert.equal(first.profile.person_uid,'person_invited');assert.equal(f.rows('rs_devices_test').length,0);
 const device='device_invited_fixture';await json(await post('recognition',{action:'confirm_device',device_token:device,values:{},requestId:'invite_confirm_001'}),200);
 assert.equal(f.rows('rs_devices_test').length,1);assert.equal(f.person.fields.access_level,'Guest');
 await json(await post('recognition',{action:'update_profile',device_token:device,values:{person_name:'Invited Complete',first_name:'Invited',last_name:'Complete',sms:'2025550188',pin:'0188',email:'invite@example.test'},requestId:'invite_profile_001'}),200);
 assert.equal(f.person.fields.person_name,'Invited Complete');assert.equal(f.rows('rs_people_test').length,1);
 const saved=await json(await post('record',{kind:'barn',draft:{name:'Invitation Trial Barn'},requestId:'invite_barn_001'}),200);assert.equal(f.rows('rs_input_barns')[0].fields.owner_uid,'person_invited');
 const state=await json(await get('state'),200);assert.equal(state.state.barns[0].id,saved.record.id);assert.equal(state.state.users.length,0);
 const linked=await json(await post('profile-link',{barnId:saved.record.id,requestId:'invite_link_001'}),200);assert.equal(linked.record.recognitionPersonId,'person_invited');
 const logout=await f.route(request('logout',{},cookie));await json(logout,200);assert.match(logout.headers.get('Set-Cookie'),/Max-Age=0/);await json(await f.route(request('state')),401);
});

test('operator can revoke an inactive person and rotate the old session version',async()=>{
 const f=provider();await invited(f);const previous=f.person.fields.input_session_version;f.person.fields.status='Inactive';await issueAccess({env,personUid:f.person.fields.person_uid,decision:'revoked',fetchImpl:f.fetchImpl});assert.equal(f.person.fields.input_access,'revoked');assert.equal(f.person.fields.input_invite_hash,'');assert.notEqual(f.person.fields.input_session_version,previous);
});
test('invited Guest phone lookup uses verified principal without granting legacy membership',async()=>{
 const f=provider(),cookie=await login(f);f.person.fields.primary_phone_e164='+12025550188';
 const result=await json(await f.route(request('recognition',{action:'phone_login',device_token:'guest_phone_device',values:{identifier:'2025550188'},requestId:'guest_phone_001'},cookie)),200);assert.equal(result.recognized,true);assert.equal(f.person.fields.access_level,'Guest');
});
test('invited bootstrap cannot take over a foreign or retired device',async()=>{
 for(const retired of [true,false]){const f=provider(),cookie=await login(f);f.rows('rs_devices_test').push({id:'recDevice00000001',fields:{device_token:'occupied',status:retired?'Retired':'Active',person:['recForeign0000001']}});f.rows('rs_people_test').push({id:'recForeign0000001',fields:{person_uid:'foreign_person',person_name:'Other',status:'Active',input_access:'approved'}});
 await json(await f.route(request('recognition',{action:'confirm_device',device_token:'occupied',values:{},requestId:'foreign_device_001'},cookie)),403);assert.equal(f.rows('rs_devices_test')[0].fields.person[0],'recForeign0000001');}
});
