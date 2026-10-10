import test, {afterEach} from 'node:test';
import {subscriptionControlDatabase} from '../test-support/subscription-control-db.mjs';
const testDatabases=[];
afterEach(()=>{for(const db of testDatabases.splice(0))db.close();});
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createInputAccess, accessHash } from '../src/lib/rs-inputs-access.js';


const module = await import('../src/lib/rs-input-subscriptions.js').catch(()=>null);
const storeModule = await import('../src/lib/rs-input-subscription-store.js').catch(()=>null);
const BASE='app9kOZdIaGyKk5uG', SUB='tblbpALv7flu3NNEE', EVENTS='tblwts3huk3w1ACjh';
const schema=JSON.parse(readFileSync(new URL('../test-support/rs-subscription-schema.json',import.meta.url)));
schema.find(t=>t.id===SUB).fields.push({id:'fldXUxFnjBwpUpBYA',name:'preferences_json',type:'multilineText'});
const {subscriptionAlertDefinitions:defs}=await import('../src/lib/rs-input-subscription-alerts.js');
const env={RS_INPUTS_BASE_ID:BASE,RS_INPUTS_RECOGNITION_BASE_ID:BASE,RS_INPUTS_WRITE_MODE:'isolated-trial',AIRTABLE_TOKEN:'synthetic',RS_INPUTS_SESSION_SECRET:'ab'.repeat(32)};
const origin='https://synthetic.invalid';
function fixture(){
 const db=subscriptionControlDatabase();testDatabases.push(db);const bindings={...env,RS_RECOGNITION_CONTROL_DB:db};
 const tables={rs_people_test:[{id:'recPerson',fields:{person_uid:'person_1',person_name:'Synthetic',input_access:'approved',status:'test',input_session_version:'cd'.repeat(16)}}],
 tblRvTwo3HYPUkZou:[{id:'recBarn',fields:{entity_uid:'barn_1',barn_uid:'barn_1',name:'Synthetic barn',owner_uid:'person_1',revision:1}}],
 tblYgoeLEey05xgw9:[{id:'recUser',fields:{entity_uid:'user_1',barn_uid:'barn_1',name:'Synthetic user',owner_uid:'person_1',recognition_person_uid:'person_1',revision:1}}],
 [SUB]:[{id:'recSub',fields:{entity_uid:'sub_1',barn_uid:'barn_1',user_uid:'user_1',engine_scope:'WEC',target_type:'Barn',target_uid:'barn_1',alert_types:'approved_started',phone_e164:'+12025550148',status:'Draft',consent_state:'Granted',consent_at:'2026-10-08T00:00:00.000Z',consent_source:'synthetic-grant',notice_version:'synthetic-v1',owner_uid:'person_1',revision:1,request_uid:'seed',request_hash:'a'.repeat(64),record_mode:'Test'}}],[EVENTS]:[]};
 const calls=[];let loseSub=false,failAudit=false,badResponse=false,auditReadDelay=0;let seq=0;
 const fetchImpl=async(input,init={})=>{
  const url=new URL(input);assert.equal(url.hostname,'api.airtable.com');const parts=url.pathname.split('/').filter(Boolean);assert.equal(parts[1],BASE);
  const table=decodeURIComponent(parts[2]),method=init.method||'GET';calls.push({table,method});
  assert.ok(tables[table],table);let rows=tables[table];
  if(method==='GET'){
   if(parts[3])return Response.json(rows.find(r=>r.id===parts[3])||{error:'NOT_FOUND'});
   const formula=url.searchParams.get('filterByFormula');
   if(formula){const terms=[...formula.matchAll(/\{([^}]+)\}\s*=\s*'([^']*)'/g)];assert.ok(terms.length,formula);rows=rows.filter(r=>terms.every(([,key,value])=>r.fields[key]===value));}
   const snapshot=structuredClone(rows);if(table===EVENTS&&auditReadDelay)await new Promise(resolve=>setTimeout(resolve,auditReadDelay));
   return Response.json({records:snapshot});
  }
  assert.equal(method,'PATCH');
  if(failAudit && table===EVENTS) throw Error('synthetic audit unavailable');
  const body=JSON.parse(init.body);const named=schema.find(t=>t.id===table);assert.ok(named);
  const fields=Object.fromEntries(Object.entries(body.records[0].fields).map(([key,value])=>{const field=named.fields.find(f=>f.id===key||f.name===key);assert.ok(field,'unverified field '+key);return [field.name,value]}));
  const keys=body.performUpsert.fieldsToMergeOn.map(k=>named.fields.find(f=>f.id===k||f.name===k)?.name||k);
  let row=rows.find(r=>keys.every(k=>r.fields[k]===fields[k]));if(!row){row={id:'recNew'+(++seq),fields:{}};rows.push(row);}
  Object.assign(row.fields,fields);
  // Airtable omits empty fields from records returned by its API.
  for(const key of Object.keys(row.fields))if(row.fields[key]===''||row.fields[key]===null||row.fields[key]===undefined)delete row.fields[key];
  if(loseSub && table===SUB){loseSub=false;throw Error('synthetic lost response');}
  if(badResponse)return Response.json({records:[]});
  return Response.json({records:[structuredClone(row)]});
 };
 const build=(options={})=>{assert.equal(typeof module?.createSubscriptionHandler,'function','subscription action implementation missing');return module.createSubscriptionHandler({env:bindings,fetchImpl,definitions:defs,approvedAlertIds:{WEC:{class_started:'approved_started',class_completed:'approved_completed'}},minimumIntervalMs:0,clock:()=> '2026-10-09T01:00:00.000Z',...options});};
 async function cookie(){
  const token='A'.repeat(43);const row=tables.rs_people_test[0];row.fields.input_invite_hash=await accessHash(token);row.fields.input_invite_expires_at=new Date(Date.now()+600000).toISOString();
  const access=createInputAccess({env:bindings,store:{async byHash(){return structuredClone(row)},async byUid(){return structuredClone(row)},async update(id,fields){Object.assign(row.fields,fields);return structuredClone(row)}}});
  return (await access.redeem(new Request(origin+'/test/rs-inputs/access'),token)).cookie.split(';')[0];
 }
 return {tables,calls,fetchImpl,build,cookie,lose(){loseSub=true},auditFail(value){failAudit=value},bad(){badResponse=true},delayAuditReads(ms){auditReadDelay=ms}};
}
const req=(cookie,body,barn='barn_1',extra={})=>new Request(origin+'/test/rs-inputs/subscriptions?barn_id='+barn,{method:body?'POST':'GET',headers:{...(cookie?{Cookie:cookie}:{}),...(body?{Origin:origin,'Content-Type':'application/json'}:{}),...extra},...(body?{body:JSON.stringify(body)}:{})});
const revoke={action:'revoke',barnId:'barn_1',subscriptionId:'sub_1',expectedRevision:1,requestId:'revoke_request_001'};
const preferences=()=>({version:1,enabled:true,phone:'2025550148',timeZone:'America/New_York',variables:Object.fromEntries(defs.map(d=>[d.key,{enabled:!!d.input,value:d.input==='time'?'10:15':d.presets?String(d.presets[0]):''}]))});
async function result(response,status){const b=await response.json();assert.equal(response.status,status,JSON.stringify(b));return b;}

test('person preferences retain every timing, reload, update and opt out without inventing barn or engine',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();
 f.tables.tblRvTwo3HYPUkZou=[];f.tables.tblYgoeLEey05xgw9=[];
 const body={action:'preferences',requestId:'preferences_save_001',preferences:preferences()};
 const saved=await result(await handler(req(cookie,body)),200);
 assert.equal(saved.preferences.phone,'+12025550148');
 assert.deepEqual(saved.preferences.variables,body.preferences.variables);
 const record=f.tables[SUB].find(r=>r.fields.preferences_json).fields;
 assert.equal(record.target_type,'Person');assert.equal(record.target_uid,'person_1');
 assert.equal(record.barn_uid,undefined);assert.equal(record.engine_scope,undefined);
 assert.equal(record.status,'Draft');assert.equal(record.consent_state,'Granted');
 const read=()=>new Request(origin+'/test/rs-inputs/subscriptions?view=preferences',{headers:{Cookie:cookie}});
 assert.deepEqual((await result(await handler(read()),200)).preferences,saved.preferences);
 assert.equal((await result(await handler(req(cookie,body)),200)).revision,1);
 const off={...body,requestId:'preferences_off_001',expectedRevision:1,preferences:{...saved.preferences,enabled:false}};
 assert.equal((await result(await handler(req(cookie,off)),200)).preferences.enabled,false);
 assert.deepEqual((await result(await handler(read()),200)).preferences.variables,body.preferences.variables);
 await result(await handler(req(cookie,{...off,requestId:'preferences_stale_001'})),409);
 assert.equal(f.tables[EVENTS].length,2);
 const again=await result(await handler(req(cookie,{...body,requestId:'preferences_again_001',expectedRevision:2})),200);
 assert.equal(again.preferences.enabled,true);assert.equal(again.revision,3);
 assert.equal(f.tables[SUB].find(r=>r.fields.preferences_json).fields.revoked_at,undefined);
});

test('preference save reconciles a lost storage reply once without duplicate record or audit',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();
 const body={action:'preferences',requestId:'preferences_lost_001',preferences:preferences()};
 f.lose();await result(await handler(req(cookie,body)),503);
 const saved=await result(await handler(req(cookie,body)),200);
 assert.equal(saved.revision,1);assert.equal(f.tables[SUB].filter(r=>r.fields.preferences_json).length,1);assert.equal(f.tables[EVENTS].length,1);
 await result(await handler(req(cookie,{...body,preferences:{...body.preferences,enabled:false}})),409);
});

test('recognized browser reads preferences through the same existing cookie and rejects impersonation',async()=>{
 const f=fixture(),handler=f.build(),token='81a81b40-f954-4ad5-8911-8fc3bf91c4a1';
 f.tables.rs_people_test[0].id='recPerson00000001';
 f.tables.rs_devices_test=[{id:'recDevice00000001',fields:{device_token:token,status:'Active',person:['recPerson00000001']}}];
 const cookie='__Host-rs_recognition_device='+token;
 const read=new Request(origin+'/test/rs-inputs/subscriptions?view=preferences',{headers:{Cookie:cookie}});
 assert.equal((await result(await handler(read),200)).preferences,null);
 await result(await handler(req(cookie,{action:'preferences',requestId:'preferences_actor_001',ownerUid:'other',preferences:preferences()})),400);
 f.tables.rs_people_test[0].fields.input_access='revoked';
 await result(await handler(new Request(read)),401);
 assert.equal(f.calls.filter(c=>c.method==='PATCH').length,0);
});

test('preferences reject unknown or invalid parameters before storage and preserve explicit off',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();
 for(const change of [p=>p.variables.class_starts_in1.value='9999',p=>p.variables.groom_tasks_at1.value='25:15',p=>p.timeZone='not-a-zone',p=>p.variables.injected={enabled:true,value:''}]){
  const p=preferences();change(p);await result(await handler(req(cookie,{action:'preferences',requestId:'invalid_preference_001',preferences:p})),400);
 }
 assert.equal(f.calls.filter(c=>c.method==='PATCH').length,0);
});

test('all eight parameterized native alerts fail before any persistence',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();
 const parameterized=defs.filter(def=>def.input);assert.equal(parameterized.length,8);
 for(const def of parameterized){
  const body=await result(await handler(req(cookie,{action:'update',barnId:'barn_1',subscriptionId:'sub_1',expectedRevision:1,requestId:'parameter_'+def.key,alertKeys:[def.key]})),409);
  assert.equal(body.error,'parameter_storage_unmapped');
 }
 assert.equal(f.calls.filter(c=>c.method==='PATCH').length,0);
});

test('Live records are neither returned nor changed by this trial route',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();f.tables[SUB][0].fields.record_mode='Live';
 assert.deepEqual((await result(await handler(req(cookie)),200)).subscriptions,[]);
 assert.equal((await result(await handler(req(cookie,revoke)),409)).error,'test_record_required');
 assert.equal(f.calls.filter(c=>c.method==='PATCH').length,0);
});

test('grant audit remains intact and old grant replay returns current revocation',async()=>{
 const f=fixture(),cookie=await f.cookie();f.tables[SUB]=[];
 const handler=f.build({consentFlow:{approved:true,source:'synthetic-explicit',noticeVersion:'synthetic-v2'}});
 const grant={action:'grant',barnId:'barn_1',engineScope:'WEC',requestId:'grant_audit_001',phone:'+12025550149',alertKeys:['class_started']};
 const initial=await result(await handler(req(cookie,grant)),200);const grantAudit=structuredClone(f.tables[EVENTS][0]);
 await result(await handler(req(cookie,{...revoke,subscriptionId:initial.subscription.entity_uid})),200);
 assert.deepEqual(f.tables[EVENTS][0],grantAudit);assert.equal(f.tables[EVENTS].length,2);
 const replay=await result(await handler(req(cookie,grant)),200);assert.equal(replay.subscription.consent_state,'Revoked');
 assert.equal(JSON.parse(grantAudit.fields.result_json).consentState,'Granted');
 assert.equal(JSON.parse(f.tables[EVENTS][1].fields.result_json).consentState,'Revoked');
});

test('safe update may only remove exact existing alert identifiers without any engine catalog',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build({approvedAlertIds:undefined});
 const update={action:'update',barnId:'barn_1',subscriptionId:'sub_1',expectedRevision:1,requestId:'remove_alerts_001',alertTypes:[]};
 const body=await result(await handler(req(cookie,update)),200);
 assert.equal(body.subscription.alert_types,'');assert.equal(body.subscription.consent_state,'Granted');assert.equal(body.subscription.status,'Draft');
 await result(await handler(req(cookie,{...update,expectedRevision:2,requestId:'add_unknown_001',alertTypes:['unapproved_new']})),409);
});

test('prototype-property action names reject before mutation',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();
 await result(await handler(req(cookie,{...revoke,action:'constructor'})),400);
 assert.equal(f.calls.filter(c=>c.method==='PATCH').length,0);
});
test('inconsistent audit lineage cannot authorize a later overwrite',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();
 await result(await handler(req(cookie,revoke)),200);
 f.tables[EVENTS][0].fields.entity_uid='forged_other_subscription';
 const response=await handler(req(cookie,{action:'update',barnId:'barn_1',subscriptionId:'sub_1',expectedRevision:2,requestId:'lineage_update_001',alertKeys:['class_completed']}));
 await result(response,502);assert.equal(f.tables[SUB][0].fields.revision,2);
});
test('storage pins exact base and validates required fields before writes',async()=>{
 assert.equal(typeof storeModule?.createSubscriptionStore,'function','subscription store implementation missing');
 let calls=0;assert.throws(()=>storeModule.createSubscriptionStore({env:{...env,RS_INPUTS_BASE_ID:'appOther'},fetchImpl:()=>{calls++}}),{code:'subscription_destination_required'});assert.equal(calls,0);
 const f=fixture(),store=storeModule.createSubscriptionStore({env,fetchImpl:f.fetchImpl,minimumIntervalMs:0});
 await assert.rejects(store.put({...f.tables[SUB][0].fields,consent_state:'Granted',consent_source:''},1),{code:'consent_evidence_required'});
 assert.equal(f.calls.filter(c=>c.method!=='GET').length,0);
});
test('authenticated read only exposes own canonical user/barn subscriptions',async()=>{
 const f=fixture(),cookie=await f.cookie();f.tables[SUB].push({id:'recOther',fields:{...f.tables[SUB][0].fields,entity_uid:'other_sub',user_uid:'other_user'}});
 const body=await result(await f.build()(req(cookie)),200);assert.equal(body.subscriptions.length,1);assert.equal(body.subscriptions[0].entity_uid,'sub_1');assert.equal(body.subscriptions[0].owner_uid,undefined);assert.equal(f.calls.filter(c=>c.method!=='GET').length,0);
});
test('missing, recognized-only, cross-origin and revoked access never writes',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();
 await result(await handler(req(null,revoke)),401);await result(await handler(req('rs_device_token=known',revoke)),401);
 await result(await handler(req(cookie,revoke,'barn_1',{Origin:'https://foreign.invalid'})),403);
 f.tables.rs_people_test[0].fields.input_access='revoked';await result(await handler(req(cookie,revoke)),401);
 assert.equal(f.calls.filter(c=>c.method!=='GET').length,0);
});
test('foreign barn/user ambiguity and forged body identity fail closed',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();await result(await handler(req(cookie,{...revoke,userId:'forged'})),400);
 f.tables.tblYgoeLEey05xgw9.push({id:'recDup',fields:{...f.tables.tblYgoeLEey05xgw9[0].fields,entity_uid:'dup'}});
 await result(await handler(req(cookie,revoke)),409);f.tables.tblYgoeLEey05xgw9.pop();
 f.tables.tblRvTwo3HYPUkZou[0].fields.owner_uid='foreign';await result(await handler(req(cookie,revoke)),404);
 assert.equal(f.calls.filter(c=>c.method!=='GET').length,0);
});
test('missing control database prevents all mutations',async()=>{
 const f=fixture(),cookie=await f.cookie();const body=await result(await f.build({env})(req(cookie,revoke)),503);assert.equal(body.error,'subscription_control_database_required');assert.equal(f.calls.filter(c=>c.method!=='GET').length,0);
});
test('explicit revoke works without grant notice/catalog/phone and persists matching safe audit',async()=>{
 const f=fixture(),cookie=await f.cookie();const body=await result(await f.build({approvedAlertIds:undefined,consentFlow:undefined})(req(cookie,revoke)),200);
 assert.equal(body.subscription.consent_state,'Revoked');const row=f.tables[SUB][0].fields;assert.equal(row.status,'Revoked');assert.equal(row.revoked_at,'2026-10-09T01:00:00.000Z');assert.equal(row.phone_e164,'+12025550148');assert.equal(row.alert_types,'approved_started');assert.equal(row.notice_version,'synthetic-v1');
 const event=f.tables[EVENTS][0].fields;assert.equal(event.action,'consent_revoked');assert.equal(event.record_mode,'Test');assert.equal(event.outcome,'committed');assert.ok(!event.result_json.includes('+12025550148'));
});
test('unchanged replay writes nothing; changed same request is rejected',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();await result(await handler(req(cookie,revoke)),200);const count=f.calls.filter(c=>c.method==='PATCH').length;
 await result(await handler(req(cookie,revoke)),200);assert.equal(f.calls.filter(c=>c.method==='PATCH').length,count);
 await result(await handler(req(cookie,{...revoke,action:'update',alertKeys:['class_started']})),409);
});
test('lost subscription response retries without repeating mutation and repairs missing audit',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();f.lose();await result(await handler(req(cookie,revoke)),503);assert.equal(f.tables[SUB][0].fields.revision,2);
 await result(await handler(req(cookie,revoke)),200);assert.equal(f.tables[SUB][0].fields.revision,2);assert.equal(f.calls.filter(c=>c.table===SUB&&c.method==='PATCH').length,1);assert.equal(f.tables[EVENTS].length,1);
});
test('audit failure reports uncertain outcome; retry repairs audit without duplicate revocation',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();f.auditFail(true);await result(await handler(req(cookie,revoke)),503);f.auditFail(false);
 await result(await handler(req(cookie,revoke)),200);assert.equal(f.tables[SUB][0].fields.revision,2);assert.equal(f.tables[EVENTS].length,1);
});
test('preference update preserves consent evidence and withdrawal, never activates',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();await result(await handler(req(cookie,revoke)),200);
 const update={action:'update',barnId:'barn_1',subscriptionId:'sub_1',expectedRevision:2,requestId:'update_request_001',alertKeys:['class_completed']};
 await result(await handler(req(cookie,update)),200);const row=f.tables[SUB][0].fields;assert.equal(row.alert_types,'approved_completed');assert.equal(row.status,'Revoked');assert.equal(row.consent_state,'Revoked');assert.equal(row.revoked_at,'2026-10-09T01:00:00.000Z');
 await result(await handler(req(cookie,{...update,requestId:'update_request_002',expectedRevision:3,alertKeys:['groom_tasks_at1']})),409);
});
test('concurrent stale edits with real SQLite yield one mutation and one conflict',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();const responses=await Promise.all([handler(req(cookie,revoke)),handler(req(cookie,{...revoke,requestId:'revoke_request_002'}))]);
 assert.deepEqual(responses.map(r=>r.status).sort(),[200,409]);assert.equal(f.tables[SUB][0].fields.revision,2);
});
test('grant missing approved flow fails; explicit synthetic grant remains Draft/Test',async()=>{
 const f=fixture(),cookie=await f.cookie();f.tables[SUB]=[];const grant={action:'grant',barnId:'barn_1',engineScope:'WEC',requestId:'grant_request_001',phone:'+12025550149',alertKeys:['class_started']};
 await result(await f.build()(req(cookie,grant)),409);
 const b=await result(await f.build({consentFlow:{approved:true,source:'synthetic-explicit',noticeVersion:'synthetic-v2'}})(req(cookie,grant)),200);
 assert.equal(b.subscription.status,'Draft');assert.equal(b.subscription.record_mode,'Test');assert.equal(b.subscription.consent_state,'Granted');
});

test('duplicate new scope is rejected without touching the existing subscription',async()=>{
 const f=fixture(),cookie=await f.cookie();const handler=f.build({consentFlow:{approved:true,source:'synthetic-explicit',noticeVersion:'synthetic-v2'}});
 const body=await result(await handler(req(cookie,{action:'grant',barnId:'barn_1',engineScope:'WEC',requestId:'duplicate_request_001',phone:'+12025550149',alertKeys:['class_started']})),409);
 assert.equal(body.error,'subscription_scope_exists');assert.equal(f.calls.filter(c=>c.method==='PATCH').length,0);
});

test('pending audit prevents a later mutation from overwriting consent evidence',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();f.auditFail(true);await result(await handler(req(cookie,revoke)),503);f.auditFail(false);
 const body=await result(await handler(req(cookie,{action:'update',barnId:'barn_1',subscriptionId:'sub_1',expectedRevision:2,requestId:'later_update_001',alertKeys:['class_completed']})),503);
 assert.equal(body.error,'subscription_reconciliation_required');assert.equal(f.tables[SUB][0].fields.revision,2);
});

test('oversized input and malformed provider responses cannot produce success',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();await result(await handler(req(cookie,{...revoke,junk:'x'.repeat(17000)})),413);
 f.bad();await result(await handler(req(cookie,revoke)),503);assert.equal(f.tables[EVENTS].length,0);
});

test('one actor reusing a request across two barns cannot race the shared audit key',async()=>{
 const f=fixture(),cookie=await f.cookie(),handler=f.build();
 f.delayAuditReads(100);
 f.tables.tblRvTwo3HYPUkZou.push({id:'recBarn2',fields:{...f.tables.tblRvTwo3HYPUkZou[0].fields,entity_uid:'barn_2',barn_uid:'barn_2'}});
 f.tables.tblYgoeLEey05xgw9.push({id:'recUser2',fields:{...f.tables.tblYgoeLEey05xgw9[0].fields,entity_uid:'user_2',barn_uid:'barn_2'}});
 f.tables[SUB].push({id:'recSub2',fields:{...f.tables[SUB][0].fields,entity_uid:'sub_2',barn_uid:'barn_2',user_uid:'user_2',target_uid:'barn_2'}});
 const responses=await Promise.all([handler(req(cookie,revoke)),handler(req(cookie,{...revoke,barnId:'barn_2',subscriptionId:'sub_2'}))]);
 assert.deepEqual(responses.map(r=>r.status).sort(),[200,409]);
 assert.equal(f.calls.filter(c=>c.table===SUB&&c.method==='PATCH').length,1);
});
