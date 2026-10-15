import test from 'node:test';
import assert from 'node:assert/strict';
import {build} from 'esbuild';
import {createPersistence} from '../src/assets/rs-inputs/persistence.js';
const compiled=await build({entryPoints:['src/components/rs-onboarding/api.ts'],bundle:true,write:false,format:'esm',platform:'browser'});
const {createInputsApi}=await import('data:text/javascript;base64,'+Buffer.from(compiled.outputFiles[0].text).toString('base64'));
function memory(){const data=new Map();return {data,async read(k){return structuredClone(data.get(k)||null)},async mutate(k,fn){const out=fn(structuredClone(data.get(k)||null));data.set(k,structuredClone(out.state));return structuredClone(out.result)}}}
const draft=name=>({name,email:'',userId:'',riderId:'',locationId:'',address:''});
test('legacy failed Rider survives independent add/edit and reload; same retry keeps original request ID',async()=>{
 const storage=memory(),old=createPersistence({scope:{actorId:'test',resource:'native-riders',tenantId:'editor'},storage});
 const body={kind:'riders',draft:draft('Old failed Rider'),barnId:'barn'};
 await old.edit(body);const oldOp=await old.prepare({...body,requestId:'old-request'});await old.fail(oldOp,'offline');
 const before=JSON.stringify((await old.load()).pending);const posts=[];
 const state={version:1,barns:[{id:'barn',barnId:'barn',name:'Barn',revision:1}],users:[],riders:[],horses:[],locations:[],selectedBarn:'barn'};
 const original=globalThis.fetch;globalThis.fetch=async(url,init)=>{
  if(url.endsWith('/access'))return Response.json({ok:true,actor:{id:'test',profile:{personUid:'test',name:'Test',email:''}}});
  if(url.endsWith('/state'))return Response.json({ok:true,state});
  const b=JSON.parse(init.body);posts.push(b);const record={...b.draft,id:b.draft.id||'new-rider',barnId:b.barnId,revision:(b.expectedRevision||0)+1};
  return Response.json({ok:true,record});
 };
 try{
  const api=createInputsApi('/api',{durableBarn:true,storage});await api.access();await api.state();
  const added=await api.record('riders',draft('New Rider'),'barn');assert.equal(added.record.name,'New Rider');assert.equal(JSON.stringify((await old.load()).pending),before);
  await api.record('riders',{...draft('New Rider edited'),id:added.record.id,revision:1},'barn');assert.equal(JSON.stringify((await old.load()).pending),before);
  const reopened=createInputsApi('/api',{durableBarn:true,storage});await reopened.access();await reopened.state();const restored=await reopened.pendingDraft('riders');assert.equal(restored.requestId,'old-request');
  await reopened.record(restored.kind,restored.draft,restored.barnId);assert.equal(posts.at(-1).requestId,'old-request');assert.equal(await reopened.pendingDraft('riders'),null);assert.equal((await old.load()).dirty,false);assert.equal((await old.load()).draft.name,'Old failed Rider');
 }finally{globalThis.fetch=original}
});
test('independent queue keeps failures, isolates same record and removes only confirmed operation',async()=>{
 const storage=memory(),scope={actorId:'a',resource:'native-riders',tenantId:'editor'},p=createPersistence({scope,storage});
 const a=await p.prepareIndependent({kind:'riders',barnId:'b',draft:{id:'a',name:'A'},requestId:'1'});
 const b=await p.prepareIndependent({kind:'riders',barnId:'b',draft:{id:'b',name:'B'},requestId:'2'});
 await p.fail(a,'lost response');await p.fail(b,'offline');
 const retry=await p.prepareIndependent({requestId:'ignored',draft:{name:'B',id:'b'},barnId:'b',kind:'riders'});assert.equal(retry.requestId,'2');
 const later=await p.prepareIndependent({kind:'riders',barnId:'b',draft:{id:'a',name:'Changed A'},requestId:'3'});
 assert.equal(later.requestId,'3');
 await p.confirm(b,{id:'b',name:'B'},1);assert.equal((await p.load()).pending.requestId,'1');assert.equal((await p.load()).pending.error,'lost response');
 const other=createPersistence({scope:{...scope,actorId:'other'},storage});assert.equal((await other.load()).pending,null);
 const p2=createPersistence({scope,storage});assert.equal((await p2.load()).pending.requestId,'1');await p2.edit({name:'Later unsent edit'});await p2.confirm(a,{id:'a',name:'A'},1);assert.equal((await p2.load()).dirty,true);assert.equal((await p2.load()).draft.name,'Later unsent edit');assert.equal((await p2.load()).pending.body.expectedRevision,1);
});

test('new empty profile controls preserve a legacy User retry; blank PIN preserves retained secret only for unchanged retry',async()=>{
 const original=globalThis.fetch;
 try{for(const pin of [undefined,'1234']){
  const storage=memory(),p=createPersistence({scope:{actorId:'test',resource:'native-users',tenantId:'editor'},storage});
  const body={kind:'users',barnId:'barn',draft:{...draft('Existing User'),id:'user',revision:1,...(pin?{pin}:{})},expectedRevision:1,requestId:'retained-request'};
  const operation=await p.prepareIndependent(body);await p.fail(operation,'offline');let posted;
  globalThis.fetch=async(url,init)=>url.endsWith('/access')?Response.json({ok:true,actor:{id:'test',profile:{personUid:'test',name:'Test',email:''}}}):((posted=JSON.parse(init.body)),Response.json({ok:true,record:{id:'user',barnId:'barn',name:'Existing User',revision:2},state:{version:1,barns:[],users:[],riders:[],horses:[],locations:[],selectedBarn:'barn'}}));
  const api=createInputsApi('/api',{durableBarn:true,storage});await api.access();
  await api.record('users',{...body.draft,displayName:'',sms:'',pin:'',profileImg:'',roles:'',tags:''},'barn');
  assert.deepEqual(posted,body);assert.equal((await p.load()).pending,null);
 }}finally{globalThis.fetch=original}
});
