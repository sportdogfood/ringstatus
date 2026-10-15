import test from 'node:test';
import assert from 'node:assert/strict';
import {createTenantBarnBridge,createTenantRest,TABLE} from '../src/lib/rs-tenant-barn.js';
import {controlDatabase} from '../test-support/recognize-control-db.mjs';
import {bindMainForms,FORMS,PAGE_ID,ROOT} from '../src/assets/rs-barn-onboarding/barn-native-adapter.mjs';
const actor={id:'test-owner',permissions:['inputs:read','inputs:write','barns:create'],barnIds:[],profile:{personUid:'test-owner',name:'TEST Owner',email:''}};
const barn={id:'rs_'+'a'.repeat(32),barnId:'rs_'+'a'.repeat(32),name:'TEST Existing barn',revision:2,ownerUid:actor.id,requestUid:'old',requestHash:'old'};
const draft=name=>({id:barn.id,name,email:'',userId:'',riderId:'',locationId:'',address:'',revision:2});
function fixture(){
 const db=controlDatabase(),env={AIRTABLE_TOKEN:'fixture',RS_INPUTS_BASE_ID:'app9kOZdIaGyKk5uG',RS_INPUTS_WRITE_MODE:'isolated-trial',RS_INPUTS_SESSION_SECRET:'ab'.repeat(32),RS_RECOGNITION_CONTROL_DB:db};
 const data=Object.fromEntries(Object.values(TABLE).map(t=>[t,[]]));let seq=0;
 const insert=(table,fields)=>{const row={id:'rec'+String(++seq).padStart(14,'0'),fields:structuredClone(fields)};data[table].push(row);return row;};
 insert(TABLE.roles,{rec_name:'owner'});
 const call=async(table,{id,formula,fields,method='GET'}={})=>{
  if(method!=='GET'){assert.notEqual(table,TABLE.submissions,'direct save must not create intake');if(method==='PATCH'){const row=data[table].find(r=>r.id===id);assert.ok(row);Object.assign(row.fields,structuredClone(fields));return [structuredClone(row)];}return [structuredClone(insert(table,fields))];}
  if(id){const row=data[table].find(r=>r.id===id);assert.ok(row);return structuredClone(row);}
  let result=data[table];if(formula){const pairs=[...formula.matchAll(/\{([^}]+)\} = '([^']*)'/g)];result=result.filter(r=>pairs.every(([,k,v])=>r.fields[k]===v));}return structuredClone(result);
 };
 const legacy={async list(kind){return kind==='barn'?[structuredClone(barn)]:[]},async event(){return null},async put(){assert.fail('legacy write')},async appendEvent(){assert.fail('legacy audit write')}};
 const fetchImpl=async()=>Response.json({records:[{id:'recRecognizeTest',fields:{person_uid:actor.id,input_access:'approved',status:'Test'}}]});
 const bridge=()=>createTenantBarnBridge({env,legacy,actor,call,fetchImpl});
 return {db,data,insert,bridge,call,env,legacy,fetchImpl};
}

const payload=(name='TEST Saved',requestId='direct-save-1')=>({kind:'barn',draft:draft(name),barnId:barn.id,expectedRevision:2,requestId});
test('direct canonical Barn edit, Rider add/edit, reload and same-operation replay never create intake',async()=>{
 const f=fixture();try{
  const p=payload(),saved=await f.bridge().intake(p);assert.equal(saved.record.name,'TEST Saved');assert.equal(saved.record.revision,3);assert.equal(saved.record.ownerUid,undefined);
  assert.equal(f.data[TABLE.barn][0].fields.tenant_official_name,'TEST Saved');assert.equal(f.data[TABLE.memberships].length,1);assert.equal((await f.bridge().store.list('barn'))[0].name,'TEST Saved');assert.equal(barn.name,'TEST Existing barn');
  const r={kind:'riders',draft:{name:'TEST New rider',userId:''},barnId:barn.id,requestId:'direct-rider-1'};
  const added=await f.bridge().intake(r);assert.equal(added.state.riders.length,1);assert.equal(f.data[TABLE.roster][0].fields.Rider[0],f.data[TABLE.riders][0].id);
  const edit={...r,draft:{id:added.record.id,name:'TEST Rider edited',userId:''},expectedRevision:1,requestId:'direct-rider-2'};assert.equal((await f.bridge().intake(edit)).record.revision,2);
  const replay=await f.bridge().intake(r);assert.equal(replay.record.name,'TEST New rider');assert.equal(replay.state.riders[0].name,'TEST Rider edited');assert.equal(f.data[TABLE.riders].length,1);assert.equal(f.data[TABLE.events].length,3);assert.equal(f.data[TABLE.submissions].length,0);
 }finally{f.db.close()}
});
test('explicit new Barn creates only mapped owner membership and canonical record',async()=>{
 const f=fixture();try{const result=await f.bridge().intake({kind:'barn',draft:{name:'TEST New barn'},requestId:'create-barn-1'});assert.equal(result.record.revision,1);assert.equal(f.data[TABLE.barn].length,1);assert.equal(f.data[TABLE.memberships][0].fields.Status,'Enabled');assert.equal(f.data[TABLE.people][0].fields.recognition_person_uid,actor.id);}finally{f.db.close()}
});
test('same-operation concurrency and lost browser reply produce one canonical application',async()=>{
 const f=fixture();try{const p=payload();const results=await Promise.allSettled([f.bridge().intake(p),f.bridge().intake(p)]);assert.equal(results.filter(r=>r.status==='fulfilled').length,1);assert.equal(f.data[TABLE.barn].length,1);assert.equal((await f.bridge().intake(p)).record.name,'TEST Saved');assert.equal(f.data[TABLE.events].length,1);await assert.rejects(f.bridge().intake({...p,draft:draft('Other')}),{code:'request_id_reused'});}finally{f.db.close()}
});
test('unknown create outcome is retained without a second POST or fabricated Saved',async()=>{
 const f=fixture();try{let creates=0;const call=async(t,o)=>{if(t===TABLE.barn&&o?.method==='POST'){creates++;await f.call(t,o);throw Error('lost create reply');}return f.call(t,o)};const bridge=()=>createTenantBarnBridge({env:f.env,legacy:f.legacy,actor,call,fetchImpl:f.fetchImpl});await assert.rejects(bridge().intake(payload()),/lost create reply/);await assert.rejects(bridge().intake(payload()),{code:'write_outcome_unknown'});assert.equal(creates,1);assert.equal(f.data[TABLE.events].length,0);}finally{f.db.close()}
});
test('imported same-name collision and stale revision cause no canonical effects',async()=>{
 const f=fixture();try{f.insert(TABLE.barn,{tenant_official_name:barn.name});await assert.rejects(f.bridge().intake(payload('Renamed import')),{code:'existing_record_mapping_required'});assert.equal(f.data[TABLE.barn].length,1);assert.equal(f.data[TABLE.events].length,0);await assert.rejects(f.bridge().intake({...payload(),expectedRevision:1,requestId:'stale-operation'}),{code:'record_changed'});}finally{f.db.close()}
});
test('revoked membership, wrong role and wrong tenant do not fall back to legacy ownership',async()=>{
 const f=fixture();try{await f.bridge().intake(payload());const m=f.data[TABLE.memberships][0];m.fields.Status='Disabled';assert.equal((await f.bridge().store.list('barn')).length,0);await assert.rejects(f.bridge().intake({...payload(),expectedRevision:3,requestId:'revoked-op-1'}));m.fields.Status='Enabled';m.fields.Roles=[f.insert(TABLE.roles,{rec_name:'staff'}).id];assert.equal((await f.bridge().store.list('barn')).length,0);m.fields.Roles=[f.data[TABLE.roles][0].id];m.fields.Tenant=['recDifferentTenant'];assert.equal((await f.bridge().store.list('barn')).length,0);assert.equal(f.data[TABLE.events].length,1);}finally{f.db.close()}
});
test('expired actor and missing person relationship cannot write canonical records',async()=>{
 const f=fixture();try{const bridge=createTenantBarnBridge({env:f.env,legacy:f.legacy,actor,call:f.call,fetchImpl:async()=>Response.json({records:[]})});await assert.rejects(bridge.intake(payload()),{code:'permission_denied'});assert.equal(f.data[TABLE.barn].length,0);await f.bridge().intake(payload());const r={kind:'riders',draft:{name:'TEST Linked rider',userId:'missing'},barnId:barn.id,requestId:'missing-person-1'};await assert.rejects(f.bridge().intake(r),{code:'invalid_relationship'});assert.equal(f.data[TABLE.riders].length,0);}finally{f.db.close()}
});
test('historical Failed and Pending submissions stay stopped and are not migrated',async()=>{
 for(const status of ['Failed','Pending']){const f=fixture();try{
 const p=payload(),{digest}=await import('../src/lib/rs-inputs.js'),eventId=await digest(`tenants-barn-v1|${actor.id}|${p.requestId}`),e={version:1,kind:'barn-rider',entityKind:'barn',actor,payload:p,event:{eventId},targetId:''};const key=await crypto.subtle.importKey('raw',Uint8Array.from('ab'.repeat(32).match(/../g),x=>parseInt(x,16)),{name:'HMAC',hash:'SHA-256'},false,['sign']);const signature=Buffer.from(await crypto.subtle.sign('HMAC',key,new TextEncoder().encode('tenant-barn-v1|'+JSON.stringify(e)))).toString('hex');const row=f.insert(TABLE.submissions,{'Submission ID':eventId,'Actor ID':actor.id,'Target table ID':TABLE.barn,'Target record ID':'','Processing status':status,'Submitted changes':JSON.stringify({...e,signature})});await assert.rejects(f.bridge().intake(p),{code:status==='Pending'?'tenant_save_pending':'tenant_save_needs_review'});assert.equal(row.fields['Processing status'],status);assert.equal(f.data[TABLE.barn].length,0);
 }finally{f.db.close()}}
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



test('REST writes use fixed base/allowlisted fields and fresh target readback after mutation',async()=>{
 let value='Before';const requests=[];
 const call=createTenantRest({AIRTABLE_TOKEN:'fixture',RS_INPUTS_WRITE_MODE:'isolated-trial'},async(url,init)=>{
  requests.push({url:String(url),method:init.method,body:init.body&&JSON.parse(init.body)});
  if(init.method==='PATCH'){value=JSON.parse(init.body).records[0].fields.tenant_official_name;return Response.json({records:[{id:'recTarget',fields:{tenant_official_name:value}}]});}
  return Response.json({id:'recTarget',fields:{tenant_official_name:value}});
 });
 assert.equal((await call(TABLE.barn,{id:'recTarget'})).fields.tenant_official_name,'Before');
 await call(TABLE.barn,{id:'recTarget',method:'PATCH',fields:{tenant_official_name:'After'}});
 assert.equal((await call(TABLE.barn,{id:'recTarget'})).fields.tenant_official_name,'After');
 assert.equal(requests.length,3);assert.equal(requests[1].url,`https://api.airtable.com/v0/app7BXcw801n19QKF/${TABLE.barn}`);assert.equal(requests[1].body.records[0].id,'recTarget');
 await assert.rejects(call(TABLE.submissions,{method:'POST',fields:{}}),{code:'tenant_write_boundary'});
 await assert.rejects(call(TABLE.barn,{method:'PATCH',id:'recTarget',fields:{consent_state:'Granted'}}),{code:'tenant_write_boundary'});
});

test('two different operations cannot commit one revision; duplicate name and disabled roster stay unchanged',async()=>{
 const f=fixture();try{
 await f.bridge().intake(payload());const a={...payload('First','next-first-1'),expectedRevision:3},b={...payload('Second','next-second-1'),expectedRevision:3};const results=await Promise.allSettled([f.bridge().intake(a),f.bridge().intake(b)]);assert.equal(results.filter(x=>x.status==='fulfilled').length,1);assert.equal(f.data[TABLE.events].length,2);
 const r={kind:'riders',draft:{name:'TEST Rider',userId:''},barnId:barn.id,requestId:'roster-add-1'};const rider=await f.bridge().intake(r);await assert.rejects(f.bridge().intake({...r,requestId:'duplicate-add-1'}),{code:'duplicate_name'});f.data[TABLE.roster][0].fields.Status='Disabled';await assert.rejects(f.bridge().intake({...r,draft:{...r.draft,id:rider.record.id,name:'Forbidden'},expectedRevision:1,requestId:'roster-edit-1'}));assert.equal(f.data[TABLE.riders][0].fields.name,'TEST Rider');assert.equal(f.data[TABLE.submissions].length,0);
 }finally{f.db.close()}
});


test('rejected requested-name collision leaves operation and revision available for valid correction',async()=>{
 const f=fixture();try{
 f.insert(TABLE.barn,{tenant_official_name:'Taken name'});
 await assert.rejects(f.bridge().intake(payload('Taken name')),{code:'existing_record_mapping_required'});
 const counts=await f.env.RS_RECOGNITION_CONTROL_DB.prepare('SELECT COUNT(*) AS n FROM recognize_claims').bind().first();assert.equal(counts.n,0);
 const saved=await f.bridge().intake(payload('Available name'));assert.equal(saved.record.name,'Available name');assert.equal(saved.record.revision,3);assert.equal(f.data[TABLE.barn].length,2);assert.equal(f.data[TABLE.memberships][0].fields.Name,`${actor.profile.name} \u00b7 owner`);
 }finally{f.db.close()}
});

test('missing canonical person for valid legacy user does not reserve Rider operation',async()=>{
 const f=fixture();try{
 await f.bridge().intake(payload());const list=f.legacy.list;f.legacy.list=async k=>k==='users'?[{id:'legacy_user',barnId:barn.id,recognitionPersonId:'linked_person'}]:list(k);
 const p={kind:'riders',draft:{name:'Linked rider',userId:'legacy_user'},barnId:barn.id,requestId:'linked-person-1'};
 await assert.rejects(f.bridge().intake(p),{code:'person_mapping_required'});assert.equal(f.data[TABLE.riders].length,0);
 const person=f.insert(TABLE.people,{entity_uid:'legacy_user',recognition_person_uid:'linked_person'});const saved=await f.bridge().intake(p);assert.equal(saved.record.userId,'legacy_user');assert.deepEqual(f.data[TABLE.riders][0].fields.people,[person.id]);
 }finally{f.db.close()}
});

test('actual REST create/link/omitted-empty readback confirms saved state with no intake or preference effects',async()=>{
 const f=fixture();try{
 const requests=[];const strip=row=>row&&({...row,fields:Object.fromEntries(Object.entries(row.fields).filter(([,v])=>v!==''&&(!Array.isArray(v)||v.length)))});
 const fetchImpl=async(url,init={})=>{
  const u=new URL(url);if(!u.pathname.includes('/app7BXcw801n19QKF/'))return f.fetchImpl(url,init);
  const parts=u.pathname.split('/'),table=parts[3],id=parts[4],method=init.method||'GET';requests.push({table,method});
  const fields=init.body?JSON.parse(init.body).records[0].fields:undefined;
  const result=await f.call(table,{id:method==='GET'?id:JSON.parse(init.body).records[0].id,method,fields,formula:u.searchParams.get('filterByFormula')});
  return Response.json(Array.isArray(result)?{records:result.map(strip)}:strip(result));
 };
 const bridge=()=>createTenantBarnBridge({env:f.env,legacy:f.legacy,actor,fetchImpl});
 const saved=await bridge().intake(payload());assert.equal(saved.state.barns[0].name,'TEST Saved');assert.equal(f.data[TABLE.memberships][0].fields.Name,`${actor.profile.name} \u00b7 owner`);
 const list=f.legacy.list;f.legacy.list=async k=>k==='users'?[{id:'legacy_user',barnId:barn.id,recognitionPersonId:'linked_person'}]:list(k);const person=f.insert(TABLE.people,{entity_uid:'legacy_user',recognition_person_uid:'linked_person'});
 const p={kind:'riders',draft:{name:'REST linked rider',userId:'legacy_user'},barnId:barn.id,requestId:'rest-rider-1'};
 const rider=await bridge().intake(p);assert.equal(rider.state.riders[0].userId,'legacy_user');assert.deepEqual(f.data[TABLE.riders][0].fields.people,[person.id]);
 const clear=await bridge().intake({...p,draft:{id:rider.record.id,name:'REST linked rider',userId:''},expectedRevision:1,requestId:'rest-rider-2'});assert.equal(clear.state.riders[0].userId,'');assert.deepEqual(f.data[TABLE.riders][0].fields.people,[]);
 assert.equal(requests.some(x=>x.method==='POST'&&x.table===TABLE.submissions),false);assert.ok(requests.filter(x=>x.method!=='GET').every(x=>Object.values(TABLE).includes(x.table)));assert.equal(f.data[TABLE.submissions].length,0);
 }finally{f.db.close()}
});

test('disabled roster before completion cannot produce Saved or a completion receipt',async()=>{
 const f=fixture();try{await f.bridge().intake(payload());const call=async(t,o)=>{const result=await f.call(t,o);if(t===TABLE.events&&o?.method==='POST'&&o.fields.kind==='riders')f.data[TABLE.roster][0].fields.Status='Disabled';return result};const bridge=()=>createTenantBarnBridge({env:f.env,legacy:f.legacy,actor,call,fetchImpl:f.fetchImpl});const p={kind:'riders',draft:{name:'Completion race',userId:''},barnId:barn.id,requestId:'final-state-1'};await assert.rejects(bridge().intake(p),{code:'tenant_confirmation_missing'});await assert.rejects(bridge().intake(p));assert.equal(f.data[TABLE.riders].length,1);}finally{f.db.close()}
});

test('direct-save source is valid UTF-8 and retains established owner label',async()=>{
 const {readFile}=await import('node:fs/promises');const text=new TextDecoder('utf-8',{fatal:true}).decode(await readFile(new URL('../src/lib/rs-tenant-barn.js',import.meta.url)));assert.ok(text.includes(' \u00b7 owner'));assert.equal(text.includes('\ufffd'),false);
});


test('WEC-style record response verifies Rider target and roster without loading unrelated collections',async()=>{
 const f=fixture();try{
  await f.bridge().intake(payload());
  const lists=[];const legacy={...f.legacy,async list(kind){lists.push(kind);return f.legacy.list(kind)}};
  const bridge=()=>createTenantBarnBridge({env:f.env,legacy,actor,call:f.call,fetchImpl:f.fetchImpl,includeState:false});
  const p={kind:'riders',draft:{name:'TEST Selective rider',userId:''},barnId:barn.id,requestId:'selective-rider-1'};
  const result=await bridge().intake(p);assert.equal(result.state,undefined);assert.equal(result.record.name,p.draft.name);assert.equal(result.record.ownerUid,undefined);
  assert.equal(lists.includes('horses'),false);assert.equal(lists.includes('locations'),false);
  f.data[TABLE.roster][0].fields.Status='Disabled';
  await assert.rejects(bridge().intake({...p,draft:{id:result.record.id,name:'TEST rejected',userId:''},expectedRevision:1,requestId:'selective-revoked'}));
  assert.equal(f.data[TABLE.events].length,2);
 }finally{f.db.close()}
});
