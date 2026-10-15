import test from 'node:test';
import assert from 'node:assert/strict';
import {build} from 'esbuild';
import {controlDatabase} from '../test-support/recognize-control-db.mjs';
import {claimOnce} from '../src/lib/rs-recognition-claims.js';
import {runTenantSave,readTenantSave} from '../src/lib/rs-tenant-save-operation.js';
import {bindMainForms,FORMS,PAGE_ID,ROOT} from '../src/assets/rs-barn-onboarding/barn-native-adapter.mjs';
const compiled=await build({entryPoints:['src/components/rs-onboarding/api.ts'],bundle:true,write:false,format:'esm',platform:'browser'});
const {createInputsApi}=await import('data:text/javascript;base64,'+Buffer.from(compiled.outputFiles[0].text).toString('base64'));
const actor={id:'owner',profile:{personUid:'owner',name:'Owner',email:''}};
const record={id:'user1',barnId:'barn1',name:'Owner',recognitionPersonId:'owner',revision:1};
const state={version:1,barns:[{id:'barn1',barnId:'barn1',name:'Barn',revision:1}],selectedBarn:'barn1',users:[record],riders:[],horses:[],locations:[]};
function memory(){const rows=new Map();return {async read(k){return structuredClone(rows.get(k)||null)},async mutate(k,fn){const r=fn(structuredClone(rows.get(k)||null));rows.set(k,r.state);return r.result}}}
test('confirmed server save survives local confirm failure and retains original operation',async()=>{
 const old=globalThis.fetch,storage=memory(),mutate=storage.mutate;let cleanup=false;
 storage.mutate=async(k,fn)=>{if(cleanup&&k.includes('native-users'))throw Error('disk unavailable');return mutate(k,fn)};
 globalThis.fetch=async url=>{if(url.endsWith('/access'))return Response.json({ok:true,actor});if(url.endsWith('/state'))return Response.json({ok:true,state});cleanup=true;return Response.json({ok:true,record:{...record,revision:2}})};
 try{const api=createInputsApi('/api',{durableBarn:true,storage});await api.access();await api.state();const saved=await api.record('users',{id:'user1',name:'Owner',revision:1},'barn1');assert.equal(saved.record.revision,2);assert.ok((await api.pendingDraft('users')).requestId);}finally{globalThis.fetch=old}
});
test('HTTP evidence survives non-JSON errors and failing local error persistence',async()=>{
 const old=globalThis.fetch,storage=memory(),mutate=storage.mutate;let failStorage=false;
 storage.mutate=async(k,fn)=>{if(failStorage)throw Error('secondary storage error');return mutate(k,fn)};
 globalThis.fetch=async url=>{if(url.endsWith('/access'))return Response.json({ok:true,actor});if(url.endsWith('/state'))return Response.json({ok:true,state});failStorage=true;return new Response('upstream unavailable',{status:502,headers:{'X-Request-Id':'trace-test','Content-Type':'text/html'}})};
 try{const api=createInputsApi('/api',{durableBarn:true,storage});await api.access();await api.state();await assert.rejects(api.record('users',{name:'Owner'},'barn1'),e=>e.code==='save_502'&&e.status===502&&e.requestId==='trace-test'&&e.contentType==='text/html');}finally{globalThis.fetch=old}
});
test('opening existing own profile uses editor without any save/link request',async()=>{
 const node=()=>({value:'',closest(){return null},addEventListener(){},removeEventListener(){}}),forms={};
 for(const spec of Object.values(FORMS)){const elements=Object.fromEntries(Object.values(spec.fields).map(s=>[s,node()]));elements['.rs-barn-v24-primary']=node();elements['.rs-barn-v24-secondary']=node();forms[spec.form]={...node(),querySelectorAll:s=>elements[s]?[elements[s]]:[]};}
 const root={querySelectorAll:s=>forms[s]?[forms[s]]:[]},document={documentElement:{getAttribute:()=>PAGE_ID},querySelectorAll:s=>s===ROOT?[root]:[]};let opened;
 const app=bindMainForms({document,view:{state(){},feedback(){},busy(){},editor:e=>opened=e},api:{async access(){return actor},async state(){return structuredClone(state)},async profileLink(){assert.fail('opening must not write')},resetDraftRequest(){}}});
 await app.initialize();assert.equal(await app.profileLink(),true);assert.equal(opened.record.id,'user1');assert.equal(forms['#email-form-2'].querySelectorAll('#User-name')[0].value,'Owner');
});
async function operationFixture(busyAt,steps={}){const db=controlDatabase(),env={RS_RECOGNITION_CONTROL_DB:db},context={actorId:'owner',payloadHash:'hash',envelope:{event:{eventId:crypto.randomUUID(),inputHash:'hash'}},steps,busy:'old',busyAt};const c=await claimOnce(env,'tenant-barn-direct-operation',context.envelope.event.eventId);await db.prepare("UPDATE recognize_claims SET result_json = ? WHERE claim_key = ?").bind(JSON.stringify(context),c.key).run();return {db,env,context,key:c.key,prior:(await readTenantSave(env,context.envelope.event.eventId)).row};}
test('active executor stays protected; expired executor resumes using readback without duplicate apply',async()=>{
 for(const active of [true,false]){const f=await operationFixture(active?100000:0,{target:'pending'});let applies=0;try{const run=()=>runTenantSave({...f,now:()=>100001,work:async op=>({record:await op.step('target',async()=>record,async()=>{applies++;return record})})});if(active)await assert.rejects(run(),{code:'tenant_save_in_progress'});else assert.equal((await run()).record.id,record.id);assert.equal(applies,0);}finally{f.db.close()}}
});
test('expired uncertain write is never blindly replayed',async()=>{const f=await operationFixture(0,{target:'pending'});try{await assert.rejects(runTenantSave({...f,now:()=>100001,work:async op=>({record:await op.step('target',async()=>null,async()=>assert.fail('duplicate write'))})}),{code:'write_outcome_unknown'});}finally{f.db.close()}});
test('competing expired recoveries have only one executor',async()=>{const f=await operationFixture(0);let writes=0;try{const work=async op=>({record:await op.step('target',async()=>writes?record:null,async()=>{writes++;return record})});const results=await Promise.allSettled([runTenantSave({...f,now:()=>100001,work}),runTenantSave({...f,now:()=>100001,work})]);assert.equal(results.filter(r=>r.status==='fulfilled').length,1);assert.equal(writes,1);}finally{f.db.close()}});
test('replaced executor cannot release the successor revision reservation',async()=>{
 const db=controlDatabase(),env={RS_RECOGNITION_CONTROL_DB:db},eventId=crypto.randomUUID(),key=(await readTenantSave(env,eventId)).key,context={actorId:'owner',payloadHash:'hash',envelope:{event:{eventId,inputHash:'hash'}}};let time=0,oldReady,oldResume,newReady,newResume;
 const ready=new Promise(r=>oldReady=r),resume=new Promise(r=>oldResume=r),successorReady=new Promise(r=>newReady=r),successorResume=new Promise(r=>newResume=r);
 try{
 const old=runTenantSave({env,key,prior:null,context,now:()=>time,work:async op=>{await op.reserveRevision('same-revision');oldReady();await resume;return {record:await op.step('target',async()=>null,async()=>record)}}});
 await ready;time=61000;const prior=(await readTenantSave(env,eventId)).row;
 const successor=runTenantSave({env,key,prior,context,now:()=>time,work:async op=>{await op.reserveRevision('same-revision');await op.step('target',async()=>record,async()=>assert.fail());newReady();await successorResume;return {record}}});
 await successorReady;oldResume();await assert.rejects(old,{code:'tenant_save_in_progress'});
 assert.equal((await claimOnce(env,'tenant-barn-entity-revision','same-revision')).won,false);newResume();assert.equal((await successor).record.id,record.id);
 }finally{db.close()}
});
test('zero-effect owned failure atomically releases operation and revision',async()=>{
 const db=controlDatabase(),env={RS_RECOGNITION_CONTROL_DB:db},eventId=crypto.randomUUID(),key=(await readTenantSave(env,eventId)).key,context={envelope:{event:{eventId,inputHash:'hash'}}};
 try{await assert.rejects(runTenantSave({env,key,prior:null,context,work:async op=>{await op.reserveRevision('cleanup-revision');throw Error('validation')}}),/validation/);assert.equal((await readTenantSave(env,eventId)).row,null);assert.equal((await claimOnce(env,'tenant-barn-entity-revision','cleanup-revision')).won,true);}finally{db.close()}
});
test('retry diagnostics never retain the previous failure request ID',async()=>{const {createPersistence}=await import('../src/assets/rs-inputs/persistence.js');const p=createPersistence({scope:{actorId:'owner',resource:'users',tenantId:'barn1'},storage:memory()});const op=await p.prepareIndependent({kind:'users',barnId:'barn1',draft:{name:'Test'},requestId:'same-operation'});await p.fail(op,'save_502',{status:502,requestId:'old-trace'});assert.equal((await p.load()).pending.error,'save_502');await p.fail(op,'connection_failed');assert.equal((await p.load()).pending.errorDetails,undefined);});
