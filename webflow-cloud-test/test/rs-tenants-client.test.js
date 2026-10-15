import test from 'node:test';
import assert from 'node:assert/strict';
import {build} from 'esbuild';
const compiled=await build({entryPoints:['src/components/rs-onboarding/api.ts'],bundle:true,write:false,format:'esm',platform:'browser'});
const {createInputsApi}=await import('data:text/javascript;base64,'+Buffer.from(compiled.outputFiles[0].text).toString('base64'));
function memory(){const data=new Map();return {data,async read(k){return structuredClone(data.get(k)||null)},async mutate(k,fn){const out=fn(structuredClone(data.get(k)||null));data.set(k,structuredClone(out.state));return structuredClone(out.result)}};}
const a={id:'owner-a',profile:{personUid:'owner-a',name:'A',email:''}},b={id:'owner-b',profile:{personUid:'owner-b',name:'B',email:''}};
const state={version:1,barns:[],users:[],riders:[],horses:[],locations:[],selectedBarn:''};
const draft={name:'TEST Draft',email:'',userId:'',riderId:'',locationId:'',address:''};
test('offline Save persists first, reload retains original operation and empty new-Barn context',async()=>{
 const original=globalThis.fetch,storage=memory();let offline=false,posted=[];
 globalThis.fetch=async(url,init)=>{if(offline)throw Error('offline');if(url.endsWith('/access'))return Response.json({ok:true,actor:a});posted.push(JSON.parse(init.body));assert.equal(init.headers['X-RS-Actor'],a.id);return Response.json({ok:true,record:{id:'rs_new',name:draft.name,revision:1},state});};
 try{
  const api=createInputsApi('/api',{durableBarn:true,storage});await api.access();offline=true;await assert.rejects(api.record('barn',draft,''),/Could not connect/);
  const pending=await api.pendingDraft('barn');assert.equal(pending.barnId,'');assert.equal(pending.draft.name,draft.name);const requestId=pending.requestId;
  offline=false;const reopened=createInputsApi('/api',{durableBarn:true,storage});await reopened.access();const restored=await reopened.pendingDraft('barn');await reopened.record(restored.kind,restored.draft,restored.barnId);
  assert.equal(posted.length,1);assert.equal(posted[0].requestId,requestId);assert.equal(await reopened.pendingDraft('barn'),null);
 }finally{globalThis.fetch=original}
});
test('cookie actor switch never reassigns or posts the previous actor draft',async()=>{
 const original=globalThis.fetch,storage=memory();let actor=a,posts=0;
 globalThis.fetch=async(url)=>{if(url.endsWith('/access'))return Response.json({ok:true,actor});posts++;throw Error('unexpected post');};
 try{
  const api=createInputsApi('/api',{durableBarn:true,storage});await api.access();actor=b;
  await assert.rejects(api.record('barn',draft,''),e=>e.code==='authentication_required');assert.equal(posts,0);
  assert.equal((await api.pendingDraft('barn')).draft.name,draft.name);
  const newActor=createInputsApi('/api',{durableBarn:true,storage});await newActor.access();assert.equal(await newActor.pendingDraft('barn'),null);
 }finally{globalThis.fetch=original}
});


test('only durable Barn/Rider writes await direct confirmation; other request deadlines remain',async()=>{
 const original=globalThis.fetch,seen=[];
 globalThis.fetch=async(url,init)=>{seen.push({url,signal:init.signal,body:init.body&&JSON.parse(init.body)});if(url.endsWith('/access'))return Response.json({ok:true,actor:a});if(url.includes('/state'))return Response.json({ok:true,state});return Response.json({ok:true,record:{id:'rs_new',name:draft.name,revision:1},state});};
 try{
 const api=createInputsApi('/api',{durableBarn:true,storage:memory()});await api.access();await api.state();await api.record('barn',draft,'');await api.record('riders',draft,'rs_barn');await api.record('horses',draft,'rs_barn');await api.profileLink('rs_barn');const legacy=createInputsApi('/api');await legacy.record('barn',draft,'');
 const records=seen.filter(x=>x.url.endsWith('/record'));assert.equal(records[0].signal,undefined);assert.equal(records[1].signal,undefined);assert.ok(records[2].signal instanceof AbortSignal);assert.ok(records[3].signal instanceof AbortSignal);assert.ok(seen.filter(x=>!x.url.endsWith('/record')).every(x=>x.signal instanceof AbortSignal));
 }finally{globalThis.fetch=original}
});

test('genuine direct-save transport failure retains exact operation without automatic resubmission',async()=>{
 const original=globalThis.fetch,storage=memory();let posts=0;
 globalThis.fetch=async(url,init)=>{if(url.endsWith('/access'))return Response.json({ok:true,actor:a});posts++;assert.equal(init.signal,undefined);throw Error('connection lost');};
 try{const api=createInputsApi('/api',{durableBarn:true,storage});await api.access();await assert.rejects(api.record('riders',draft,'rs_barn'),/Could not connect/);const pending=await api.pendingDraft('riders');assert.ok(pending.requestId);assert.equal(pending.draft.name,draft.name);await new Promise(r=>setTimeout(r,30));assert.equal(posts,1);assert.equal((await api.pendingDraft('riders')).requestId,pending.requestId);}finally{globalThis.fetch=original}
});

test('actual client + direct REST adapter stays pending past15s then confirms while preserving later edit', {timeout:120000},async()=>{
 const {recognizeTenantsFixture}=await import('../test-support/recognize-tenants-fixture.mjs');const {TABLE}=await import('../src/lib/rs-tenant-barn.js');const {createPersistence}=await import('../src/assets/rs-inputs/persistence.js');const f=await recognizeTenantsFixture(),original=globalThis.fetch,storage=memory(),origin='https://ringstatus.com';
 f.insert(TABLE.roles,{rec_name:'owner'});const identity=await (await f.dispatch(new Request(origin+'/test/rs-inputs/native-recognition?operation=action',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify({action:'credential',kind:'phone',value:'6318752160',request_id:crypto.randomUUID()})}))).json();assert.equal(identity.verified,true);
 let recordStarted,notifyStarted;const started=new Promise(r=>notifyStarted=r);const requestTimes=[];
 globalThis.fetch=async(url,init)=>{const begin=Date.now(),isRecord=url.endsWith('/record');if(isRecord){assert.equal(init.signal,undefined);recordStarted=begin;notifyStarted();}else assert.ok(init.signal instanceof AbortSignal);const headers={...init.headers,Origin:origin,Cookie:`__Host-rs_recognition_device=${identity.device_token}`};const response=await f.dispatch(new Request(url,{...init,headers}));if(isRecord){const remaining=16000-(Date.now()-begin);if(remaining>0)await new Promise(r=>setTimeout(r,remaining));}requestTimes.push({path:new URL(url).pathname,elapsedMs:Date.now()-begin});return response;};
 try{
 const api=createInputsApi(origin+'/test/rs-inputs',{durableBarn:true,storage});await api.access();const loaded=await api.state();const barn=loaded.barns[0];let settled=false;const operation=api.record('barn',{...draft,id:barn.id,name:'TEST Delayed REST confirmation',revision:barn.revision},barn.id).finally(()=>{settled=true});await started;await new Promise(r=>setTimeout(r,15100));assert.equal(settled,false,'15s must not produce Retry');const pending=await api.pendingDraft('barn');assert.ok(pending?.requestId);const device=createPersistence({scope:{actorId:f.actor,resource:'native-barn',tenantId:'editor'},storage});await device.edit({...pending,draft:{...pending.draft,name:'TEST Later unsaved edit'}});
 const result=await operation;assert.equal(result.record.name,'TEST Delayed REST confirmation');assert.equal(f.rows[TABLE.barn][0].fields.tenant_official_name,result.record.name);assert.equal(result.state.barns[0].name,result.record.name);assert.equal(await api.pendingDraft('barn'),null);const kept=await device.load();assert.equal(kept.dirty,true);assert.equal(kept.draft.draft.name,'TEST Later unsaved edit');assert.equal(f.rows[TABLE.events].filter(r=>r.fields.kind==='barn').length,1);assert.equal(f.rows[TABLE.submissions].length,0);assert.equal(f.writes.filter(x=>x.table===TABLE.barn).length,1);console.log('DELAYED_DIRECT_REST_TIMING',JSON.stringify(requestTimes));assert.ok(Date.now()-recordStarted>=16000);
 }finally{globalThis.fetch=original;f.db.close()}
});
