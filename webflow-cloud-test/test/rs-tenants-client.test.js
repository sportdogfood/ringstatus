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
