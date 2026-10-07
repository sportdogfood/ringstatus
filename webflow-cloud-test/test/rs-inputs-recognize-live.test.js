import test from 'node:test';
import assert from 'node:assert/strict';
import { runRecognitionLive, TARGET, BASE } from '../scripts/rs-recognize-live.mjs';
import { handleAuthenticatedInputRoute } from '../src/pages/rs-inputs/[operation].js';
const env={RS_CI_TARGET:TARGET,RS_CI_PERSON_UID:'rs_ci_recognize_fixture',RS_CI_AIRTABLE_TOKEN:'private-test-pat'};
function fixture({loseConfirmation=false,failRetirement=false,lateConfirmation=false,grantFailure=false,name='QA CI Recognize automation'}={}){
 const bindings={RS_INPUTS_BASE_ID:BASE,RS_INPUTS_RECOGNITION_BASE_ID:BASE,RS_INPUTS_WRITE_MODE:'isolated-trial',AIRTABLE_TOKEN:env.RS_CI_AIRTABLE_TOKEN,RS_INPUTS_SESSION_SECRET:'ab'.repeat(32)};
 let n=0,lateCommit;const tables=new Map(),calls=[];const rows=t=>{if(!tables.has(t))tables.set(t,[]);return tables.get(t);};
 const person={id:'recPerson00000001',fields:{person_uid:env.RS_CI_PERSON_UID,person_name:name,status:'Active',input_access:'revoked',access_level:'Guest'}};rows('rs_people_test').push(person);
 async function provider(input,init={}){
  const url=new URL(input);assert.equal(url.origin,'https://api.airtable.com');const [,base,table,id]=url.pathname.split('/').filter(Boolean);assert.equal(base,BASE);calls.push({table,method:init.method||'GET'});
  if(!init.method||init.method==='GET'){
   if(id)return Response.json(rows(table).find(r=>r.id===id)||{},{status:rows(table).some(r=>r.id===id)?200:404});
   const formula=url.searchParams.get('filterByFormula');let found=rows(table);
   if(formula){const clauses=[...formula.matchAll(/\{([^}]+)\}\s*=\s*'((?:\\.|[^'])*)'/g)];assert.ok(clauses.length,formula);found=found.filter(r=>clauses.some(([,k,v])=>r.fields[k]===v.replace(/\\(['\\])/g,'$1')));}
   return Response.json({records:structuredClone(found)});
  }
  const body=JSON.parse(init.body),out=[];
  if(grantFailure&&table==='rs_people_test'&&body.records.some(r=>r.fields.input_access==='invited'))return Response.json({error:'provider_failure'},{status:503});
  if(failRetirement&&table==='rs_devices_test'&&body.records.some(r=>r.fields.status==='Retired'))throw Error('private-test-pat');
  for(const update of body.records){let row=update.id?rows(table).find(r=>r.id===update.id):null;if(!row){row={id:'rec'+String(++n).padStart(14,'0'),fields:{}};rows(table).push(row);}Object.assign(row.fields,update.fields);out.push(structuredClone(row));}
  return Response.json({records:out});
 }
 async function fetchImpl(input,init={}){
  const url=new URL(input);if(url.origin==='https://api.airtable.com')return provider(input,init);
  assert.ok(url.href.startsWith(TARGET+'/rs-inputs/'));
  if(lateConfirmation && init.body && JSON.parse(init.body).action==='confirm_device'){const body=JSON.parse(init.body);lateCommit=()=>rows('rs_devices_test').push({id:'recLate000000001',fields:{device_token:body.device_token,person:[person.id],status:'Active'}});throw Error('timeout');}
  const result=await handleAuthenticatedInputRoute({request:new Request(url,init),locals:{}},bindings,provider);
  if(loseConfirmation&&init.body&&JSON.parse(init.body).action==='confirm_device')throw Error('private-test-pat');
  return result;
 }
 return {fetchImpl,rows,person,calls,commitLate:()=>lateCommit()};
}
test('live recognition runner executes the complete real-handler journey with isolated REST fixtures',async()=>{
 const f=fixture();const report=await runRecognitionLive({execute:true,env,fetchImpl:f.fetchImpl});
 assert.equal(report.status,'PASS',JSON.stringify(report));assert.equal(report.cleanup,'PASS');assert.ok(report.phases.length>=18);
 assert.equal(f.rows('rs_devices_test').length,1);assert.equal(f.rows('rs_devices_test')[0].fields.status,'Retired');
 assert.equal(f.rows('rs_recognition_sessions_test').filter(r=>r.fields.session_event_uid.endsWith('_confirm')).length,1);
 assert.equal(f.person.fields.input_access,'revoked');assert.equal(f.person.fields.input_invite_hash,'');assert.equal(f.person.fields.access_level,'Guest');
 assert.equal(JSON.stringify(report).includes(env.RS_CI_AIRTABLE_TOKEN),false);
});
test('runner defaults to dry run and blocks missing/wrong configuration before network access',async()=>{
 const noFetch=()=>assert.fail('network forbidden');assert.equal((await runRecognitionLive({fetchImpl:noFetch})).status,'DRY_RUN');
 for(const change of [{RS_CI_TARGET:'https://ringstatus.com'},{RS_CI_PERSON_UID:'real_person'},{RS_CI_AIRTABLE_TOKEN:''}])assert.equal((await runRecognitionLive({execute:true,env:{...env,...change},fetchImpl:noFetch})).status,'BLOCKED');
});
test('runner refuses a non-dedicated person without changing any data',async()=>{
 const f=fixture({name:'Existing real user'});const r=await runRecognitionLive({execute:true,env,fetchImpl:f.fetchImpl});assert.equal(r.status,'FAIL');assert.equal(r.failedPhase,'fixture-preflight');assert.ok(f.calls.every(c=>c.method==='GET'));
});
test('lost confirmation response still revokes access and retires the committed device',async()=>{
 const f=fixture({loseConfirmation:true});const r=await runRecognitionLive({execute:true,env,fetchImpl:f.fetchImpl});assert.equal(r.status,'FAIL');assert.equal(r.failedPhase,'confirm-browser-association');assert.equal(r.cleanup,'UNVERIFIED_WRITE_OUTCOME');assert.equal(r.unknownWriteOutcome,true);assert.equal(f.person.fields.input_access,'revoked');assert.equal(f.rows('rs_devices_test')[0].fields.status,'Retired');assert.equal(JSON.stringify(r).includes(env.RS_CI_AIRTABLE_TOKEN),false);
});
test('cleanup failure is reported as FAIL and never hidden by successful earlier checks',async()=>{
 const f=fixture({failRetirement:true});const r=await runRecognitionLive({execute:true,env,fetchImpl:f.fetchImpl});assert.equal(r.status,'FAIL');assert.equal(r.cleanup,'FAIL');assert.ok(r.cleanupErrors.includes('fixture_cleanup_unverified'));assert.equal(f.person.fields.input_access,'revoked');
});

test('late commit after empty cleanup readback cannot produce a false cleanup PASS',async()=>{
 const f=fixture({lateConfirmation:true});const r=await runRecognitionLive({execute:true,env,fetchImpl:f.fetchImpl});assert.equal(f.rows('rs_devices_test').length,0);assert.equal(r.status,'FAIL');assert.equal(r.cleanup,'UNVERIFIED_WRITE_OUTCOME');f.commitLate();assert.equal(f.rows('rs_devices_test')[0].fields.status,'Active');
});

test('invitation issuance provider failure remains an unresolved write after cleanup',async()=>{
 const f=fixture({grantFailure:true});const r=await runRecognitionLive({execute:true,env,fetchImpl:f.fetchImpl});assert.equal(r.status,'FAIL');assert.equal(r.failedPhase,'issue-dedicated-invitation');assert.equal(r.cleanup,'UNVERIFIED_WRITE_OUTCOME');assert.equal(f.person.fields.input_access,'revoked');
});
