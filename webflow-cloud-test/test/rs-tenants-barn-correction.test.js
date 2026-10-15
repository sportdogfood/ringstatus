import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
import {createTenantBarnBridge,TABLE} from '../src/lib/rs-tenant-barn.js';
import {controlDatabase} from '../test-support/recognize-control-db.mjs';
import {bindMainForms,FORMS,PAGE_ID,ROOT} from '../src/assets/rs-barn-onboarding/barn-native-adapter.mjs';
const processor=await readFile(new URL('../scripts/tenant-barn-processor.js',import.meta.url),'utf8');
const actor={id:'test-owner',permissions:['inputs:read','inputs:write','barns:create'],barnIds:[],profile:{personUid:'test-owner',name:'TEST Owner',email:''}};
const barn={id:'rs_'+'a'.repeat(32),barnId:'rs_'+'a'.repeat(32),name:'TEST Existing barn',revision:2,ownerUid:actor.id,requestUid:'old',requestHash:'old'};
const draft=name=>({id:barn.id,name,email:'',userId:'',riderId:'',locationId:'',address:'',revision:2});
function fixture(){
 const db=controlDatabase(),env={AIRTABLE_TOKEN:'fixture',RS_INPUTS_BASE_ID:'app9kOZdIaGyKk5uG',RS_INPUTS_WRITE_MODE:'isolated-trial',RS_INPUTS_SESSION_SECRET:'ab'.repeat(32),RS_RECOGNITION_CONTROL_DB:db};
 const data=Object.fromEntries(Object.values(TABLE).map(t=>[t,[]]));let seq=0;
 const insert=(table,fields)=>{const row={id:'rec'+String(++seq).padStart(14,'0'),fields:structuredClone(fields)};data[table].push(row);return row;};
 insert(TABLE.roles,{rec_name:'owner'});
 const call=async(table,{id,formula,fields,method='GET'}={})=>{
  if(method!=='GET'){assert.equal(table,TABLE.submissions);return [structuredClone(insert(table,fields))];}
  if(id){const row=data[table].find(r=>r.id===id);assert.ok(row);return structuredClone(row);}
  let result=data[table];if(formula){const pairs=[...formula.matchAll(/\{([^}]+)\} = '([^']*)'/g)];result=result.filter(r=>pairs.every(([,k,v])=>r.fields[k]===v));}return structuredClone(result);
 };
 const legacy={async list(kind){return kind==='barn'?[structuredClone(barn)]:[]},async event(){return null},async put(){assert.fail('legacy write')},async appendEvent(){assert.fail('legacy audit write')}};
 const fetchImpl=async()=>Response.json({records:[{id:'recRecognizeTest',fields:{person_uid:actor.id,input_access:'approved',status:'Test'}}]});
 const bridge=()=>createTenantBarnBridge({env,legacy,actor,call,fetchImpl});
 const selects=new Set(['Status','record_mode','entity_type','status','Processing status']);
 const links=new Set(['Person','Tenant','Roles','Rider','Horse','people','Membership']);
 const normalize=fields=>Object.fromEntries(Object.entries(fields).map(([k,v])=>[k,links.has(k)?v.map(x=>x.id):selects.has(k)?v.name:v]));
 const wrap=r=>r&&({id:r.id,getCellValue(k){const v=r.fields[k];return v===undefined?null:links.has(k)?v.map(id=>({id})):selects.has(k)?{name:v}:v;}});
 const base={getTable(table){assert.ok(data[table]);return {
  async selectRecordsAsync(){return {records:data[table].map(wrap)}},async selectRecordAsync(id){return wrap(data[table].find(r=>r.id===id))},
  async createRecordAsync(fields){return insert(table,normalize(fields)).id},async updateRecordAsync(id,fields){const r=data[table].find(x=>x.id===id);assert.ok(r);Object.assign(r.fields,normalize(fields));}
 };}};
 async function process(row,{loseComplete=false}={}){return vm.runInNewContext(`(async()=>{${processor}})()`,{base,input:{config:()=>({recordId:row.id})},output:{set(){}},fetch:async(url,init)=>{
  assert.equal(url,'https://ringstatus.com/test/rs-inputs/tenant-barn-process');const body=JSON.parse(init.body);
  if(loseComplete&&body.action==='complete')throw Error('connection_lost');
  try{return Response.json(await bridge()[body.action](body.submissionId,body.signature));}catch(e){return Response.json({ok:false,error:e.code||e.message,processingClaimed:e.processingClaimed===true},{status:e.status||500});}
 }});}
 return {db,data,insert,bridge,process};
}
test('native editor -> intake -> actual automation script -> canonical Barn and Rider -> readback/reload',async()=>{
 const f=fixture();try{
  const payload={kind:'barn',draft:draft('TEST Changed barn'),barnId:barn.id,expectedRevision:2,requestId:'barn-save-1'};
  await assert.rejects(f.bridge().intake(payload),e=>e.code==='tenant_save_pending');
  assert.equal(f.data[TABLE.barn].length,0,'intake must not write the business record');
  await f.process(f.data[TABLE.submissions][0]);
  const saved=await f.bridge().intake(payload);assert.equal(saved.record.name,'TEST Changed barn');assert.equal(saved.record.revision,3);
  assert.equal(f.data[TABLE.barn][0].fields.tenant_official_name,'TEST Changed barn');
  assert.equal((await f.bridge().store.list('barn'))[0].name,'TEST Changed barn');
  assert.equal(barn.name,'TEST Existing barn','legacy source is preserved');
  assert.equal(f.data[TABLE.memberships].length,1);assert.equal(f.data[TABLE.memberships][0].fields.Status,'Enabled');
  const rider={kind:'riders',draft:{name:'TEST New rider',userId:''},barnId:barn.id,requestId:'rider-add-1'};
  await assert.rejects(f.bridge().intake(rider),e=>e.code==='tenant_save_pending');await f.process(f.data[TABLE.submissions][1]);
  const result=await f.bridge().intake(rider);assert.equal(result.record.name,'TEST New rider');assert.equal(result.state.riders.length,1);
  assert.equal(f.data[TABLE.roster][0].fields.Rider[0],f.data[TABLE.riders][0].id);
  await f.bridge().intake(rider);assert.equal(f.data[TABLE.riders].length,1,'same operation is not duplicated');
  f.data[TABLE.memberships][0].fields.Status='Disabled';assert.equal((await f.bridge().store.list('barn')).length,0,'revoked canonical ownership cannot fall back to legacy');
 }finally{f.db.close()}
});
test('a forged envelope cannot claim or write; imported same-name record is not duplicated',async()=>{
 const f=fixture();try{
  const p={kind:'barn',draft:draft('Existing imported'),barnId:barn.id,expectedRevision:2,requestId:'collision-1'};
  await assert.rejects(f.bridge().intake(p),e=>e.code==='tenant_save_pending');const row=f.data[TABLE.submissions][0];
  await assert.rejects(f.bridge().claim(row.id,'0'.repeat(64)),e=>e.code==='invalid_submission');
  f.insert(TABLE.barn,{tenant_official_name:'Existing imported'});
  await assert.rejects(f.process(row),/existing_record_mapping_required/);
  assert.equal(f.data[TABLE.barn].length,1);assert.equal(row.fields['Processing status'],'Needs review');
 }finally{f.db.close()}
});
test('two different operations cannot win the same entity revision',async()=>{
 const f=fixture();try{
  for(const requestId of ['race-first','race-second'])await assert.rejects(f.bridge().intake({kind:'barn',draft:draft(requestId),barnId:barn.id,expectedRevision:2,requestId}),e=>e.code==='tenant_save_pending');
  const [a,b]=f.data[TABLE.submissions];
  await f.bridge().claim(a.id,JSON.parse(a.fields['Submitted changes']).signature);
  await assert.rejects(f.bridge().claim(b.id,JSON.parse(b.fields['Submitted changes']).signature),e=>e.code==='tenant_save_needs_review');
  assert.equal(f.data[TABLE.barn].length,0);
 }finally{f.db.close()}
});
function nativeFixture(code){
 const node=()=>({tagName:'BUTTON',value:'',addEventListener(){},removeEventListener(){}}),forms={};
 for(const spec of Object.values(FORMS)){const elements=Object.fromEntries(Object.values(spec.fields).map(s=>[s,node()]));elements['.rs-barn-v24-primary']=node();elements['.rs-barn-v24-secondary']=node();forms[spec.form]={...node(),querySelectorAll:s=>elements[s]?[elements[s]]:[]};}
 const root={querySelectorAll:s=>forms[s]?[forms[s]]:[]},document={documentElement:{getAttribute:()=>PAGE_ID},querySelectorAll:s=>s===ROOT?[root]:[]};
 const navigation=[],errors=[];let reads=0;
 const app=bindMainForms({document,navigate:p=>navigation.push(p),view:{state(){},feedback:e=>errors.push(e),busy(){},editor(){}},api:{async access(){const e=Error(code);e.code=code;throw e},async state(){reads++}}});
 return {app,navigation,errors,reads:()=>reads};
}
test('signed-out entry redirects once; storage failure stays on form and does not fetch data',async()=>{
 const guest=nativeFixture('authentication_required');await guest.app.initialize();assert.deepEqual(guest.navigation,['/rs-recognize']);assert.equal(guest.reads(),0);
 const outage=nativeFixture('storage_unavailable');await outage.app.initialize();assert.deepEqual(outage.navigation,[]);assert.equal(outage.errors[0].value,'storage_unavailable');assert.equal(outage.reads(),0);
});

test('lost completion and repeat runners never downgrade Applied; historical replay survives later edit',async()=>{
 const f=fixture();try{
  const payload={kind:'barn',draft:draft('TEST Saved'),barnId:barn.id,expectedRevision:2,requestId:'ack-save-1'};
  await assert.rejects(f.bridge().intake(payload),e=>e.code==='tenant_save_pending');const row=f.data[TABLE.submissions][0];
  await assert.rejects(f.process(row,{loseComplete:true}),/connection_lost/);assert.equal(row.fields['Processing status'],'Applied');
  await f.process(row);assert.equal(row.fields['Processing status'],'Applied');assert.equal(f.data[TABLE.barn].length,1);
  const p2={...payload,draft:{...payload.draft,name:'TEST Later',revision:3},expectedRevision:3,requestId:'ack-save-2'};
  await assert.rejects(f.bridge().intake(p2),e=>e.code==='tenant_save_pending');await f.process(f.data[TABLE.submissions][1]);
  const older=await f.bridge().intake(payload);assert.equal(older.record.name,'TEST Saved');assert.equal(older.state.barns[0].name,'TEST Later');
 }finally{f.db.close()}
});
test('renaming an unmapped imported Barn cannot evade the source-name collision',async()=>{
 const f=fixture();try{
  f.insert(TABLE.barn,{tenant_official_name:barn.name});
  await assert.rejects(f.bridge().intake({kind:'barn',draft:draft('Renamed import'),barnId:barn.id,expectedRevision:2,requestId:'old-name-1'}),e=>e.code==='tenant_save_pending');
  await assert.rejects(f.process(f.data[TABLE.submissions][0]),/existing_record_mapping_required/);assert.equal(f.data[TABLE.barn].length,1);assert.equal(f.data[TABLE.barn][0].fields.tenant_official_name,barn.name);
 }finally{f.db.close()}
});
test('a losing invocation does not alter a Pending submission owned by another runner',async()=>{
 const f=fixture();try{
  await assert.rejects(f.bridge().intake({kind:'barn',draft:draft('TEST Concurrent'),barnId:barn.id,expectedRevision:2,requestId:'concurrent-1'}),e=>e.code==='tenant_save_pending');const row=f.data[TABLE.submissions][0];
  await f.bridge().claim(row.id,JSON.parse(row.fields['Submitted changes']).signature);
  await assert.rejects(f.process(row),/tenant_save_needs_review/);assert.equal(row.fields['Processing status'],'Pending');assert.equal(f.data[TABLE.barn].length,0);
 }finally{f.db.close()}
});

test('a stale competing submission becomes Needs review without downgrading the applied winner',async()=>{
 const f=fixture();try{
  const payload={kind:'barn',draft:draft('TEST Winner'),barnId:barn.id,expectedRevision:2,requestId:'winner-one'};
  await assert.rejects(f.bridge().intake(payload),e=>e.code==='tenant_save_pending');
  const other={...payload,draft:draft('TEST Loser'),requestId:'loser-two'};
  await assert.rejects(f.bridge().intake(other),e=>e.code==='tenant_save_pending');
  await f.process(f.data[TABLE.submissions][0]);
  await assert.rejects(f.process(f.data[TABLE.submissions][1]),/record_changed/);
  assert.equal(f.data[TABLE.submissions][0].fields['Processing status'],'Applied');assert.equal(f.data[TABLE.submissions][1].fields['Processing status'],'Needs review');
  await assert.rejects(f.bridge().intake(other),e=>e.code==='tenant_save_needs_review');assert.equal(f.data[TABLE.barn][0].fields.tenant_official_name,'TEST Winner');
 }finally{f.db.close()}
});

test('canonical permissions follow the unique owner role and current tenant membership only',async()=>{
 const f=fixture();try{
  const payload={kind:'barn',draft:draft('TEST Role binding'),barnId:barn.id,expectedRevision:2,requestId:'role-binding-1'};
  await assert.rejects(f.bridge().intake(payload),e=>e.code==='tenant_save_pending');await f.process(f.data[TABLE.submissions][0]);
  const membership=f.data[TABLE.memberships][0],owner=f.data[TABLE.roles][0];
  assert.deepEqual(membership.fields.Roles,[owner.id]);
  assert.equal((await f.bridge().store.list('barn')).length,1);
  const staff=f.insert(TABLE.roles,{rec_name:'staff'});membership.fields.Roles=[staff.id];
  assert.equal((await f.bridge().store.list('barn')).length,0,'staff label must not acquire owner rights');
  membership.fields.Roles=[owner.id];membership.fields.Tenant=['recDifferentTenant'];
  assert.equal((await f.bridge().store.list('barn')).length,0,'membership of another tenant must not authorize this barn');
  membership.fields.Tenant=[f.data[TABLE.barn][0].id];f.insert(TABLE.roles,{rec_name:'owner'});
  await assert.rejects(f.bridge().store.list('barn'),e=>e.code==='tenant_owner_role_required');
 }finally{f.db.close()}
});
