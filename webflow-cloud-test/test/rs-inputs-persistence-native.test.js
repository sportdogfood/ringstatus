import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import {createPersistence} from '../src/assets/rs-inputs/persistence.js';
const source=process.env.RS_TEST_BASELINE?execFileSync('git',['show','18e6e993aeaff6fd53168dac4d6f1b7b46baf195:webflow-cloud-test/src/assets/rs-input-subscriptions/native-client.js'],{encoding:'utf8'}):readFileSync(new URL('../src/assets/rs-input-subscriptions/native-client.js',import.meta.url),'utf8');
const flush=async()=>{for(let i=0;i<30;i++)await new Promise(r=>setImmediate(r))};
function device(){const rows=new Map();let chain=Promise.resolve();return {async read(k){return structuredClone(rows.get(k)||null)},async write(k,v){rows.set(k,structuredClone(v))},mutate(k,fn){const next=chain.then(()=>{const changed=fn(structuredClone(rows.get(k)||null));rows.set(k,structuredClone(changed.state));return structuredClone(changed.result)});chain=next.catch(()=>{});return next;}}}
async function page(storage,{lost=false,switchActor=false,expired=false,accountChangedAtPost=false,pendingAsync=false,staleAfterFirst=false}={}){
 const nodes=new Map(),events={},requests=[],scopes=[],timers=[];let accesses=0;
 function node(selector){if(!nodes.has(selector))nodes.set(selector,{value:'',checked:false,dataset:{},style:{},textContent:'',classList:{toggle(){}},setAttribute(){},hasAttribute(){return false},closest(){return null},tagName:'INPUT'});return nodes.get(selector)}
 const form={dataset:{},querySelectorAll(selector){return selector==='[type="submit"]'?[node('submit')]:[node('[data-rs-field="phone"]'),node('[data-rs-sms]'),node('submit')]}};
 const root={dataset:{theme:'dark'},style:{},classList:{toggle(){}},querySelector(s){return s==='[data-rs-form="alerts"]'?form:node(s)},querySelectorAll(){return []},addEventListener(type,fn){events[type]=fn}};
 const document={documentElement:{getAttribute(){return '6ac459b4506821bd349ed047'}},getElementById(id){return id==='rs-component-drafts'?root:{}}};
 const context=vm.createContext({document,URL,URLSearchParams,Intl,AbortSignal,crypto:globalThis.crypto,runSilentRecognition:async()=>true,createPersistence:options=>createPersistence({...options,storage}),setTimeout(fn){timers.push(fn);return timers.length},clearTimeout(){},window:{location:{search:''},matchMedia(){return {matches:false,addEventListener(){}}},addEventListener(){}},async fetch(url,options){
  if(String(url).endsWith('/access')){accesses++;if(expired&&accesses>1)return {ok:false,async json(){return {ok:false,error:'authentication_required'}}};return {ok:true,async json(){return {ok:true,actor:{id:switchActor&&accesses>1?'person-two':'person-one',permissions:['inputs:write']}}}}}
  if(options.method==='POST'){
   const body=JSON.parse(options.body);requests.push(body);scopes.push(options.headers?.['X-RS-Actor-Scope']);
   const persisted=await storage.read(JSON.stringify(['https://example.test/rs-inputs/subscriptions','personal','person-one']));
   assert.equal(persisted?.pending?.body.requestId,body.requestId,'request must be committed before network');
   if(staleAfterFirst&&requests.length===2)return {ok:false,async json(){return {ok:false,error:'record_changed'}}};
   if(pendingAsync)return {ok:true,async json(){return {ok:true,state:'Pending',requestId:body.requestId,actorId:'person-one'}}};
   if(accountChangedAtPost)return {ok:false,async json(){return {ok:false,error:'account_changed'}}};
   if(lost)throw new TypeError('lost response');
   return {ok:true,async json(){return {ok:true,preferences:body.preferences,revision:1}}};
  }
  if(pendingAsync&&new URL(url).searchParams.get('view')==='submission'){const latest=requests.at(-1);assert.equal(new URL(url).searchParams.get('request_id'),latest.requestId);assert.equal(options.headers['X-RS-Actor-Scope'],'person-one');return {ok:true,async json(){return {ok:true,state:'Applied',requestId:latest.requestId,actorId:'person-one',preferences:latest.preferences,revision:latest.expectedRevision+1}}};}
  if(staleAfterFirst&&requests.length>=2)return {ok:true,async json(){return {ok:true,preferences:requests[0].preferences,revision:2}}};
  return {ok:true,async json(){return {ok:true,preferences:null,revision:0}}};
 }});
 vm.runInContext(source.replace(/^\uFEFF?import [^\r\n]*\r?\n/gm,'').replaceAll('import.meta.url',"'https://example.test/rs-inputs/native-alerts.js'"),context);await flush();
 return {requests,scopes,node,async poll(){timers.shift()?.();await flush()},async edit(value){const phone=node('[data-rs-field="phone"]');phone.value=value;events.input({target:phone});await flush()},async save(){events.submit({target:form,preventDefault(){},stopImmediatePropagation(){}});await flush()}};
}
test('native typing persists draft but cannot submit consent; explicit Save persists exact request first',async()=>{
 const storage=device(),p=await page(storage);await p.edit('+12025550148');assert.equal(p.requests.length,0);
 const state=await storage.read(JSON.stringify(['https://example.test/rs-inputs/subscriptions','personal','person-one']));assert.equal(state?.draft?.phone,'+12025550148');assert.equal(state.pending,null);
 await p.save();assert.equal(p.requests.length,1);assert.equal(p.requests[0].preferences.enabled,false);
});

test('native Save recovers a rejected revision after a prior async success without resending the rejected operation',async()=>{
 const storage=device(),p=await page(storage,{pendingAsync:true,staleAfterFirst:true});
 const key=JSON.stringify(['https://example.test/rs-inputs/subscriptions','personal','person-one']);
 await p.edit('+12025550148');await p.save();await p.poll();assert.equal((await storage.read(key)).pending,null);
 await p.edit('+12025550149');await p.save();assert.equal(p.requests.length,2);
 const rejected=p.requests[1];assert.equal((await storage.read(key)).pending.error,'intake_record_changed');
 await p.save();assert.equal(p.requests.length,3);assert.notEqual(p.requests[2].requestId,rejected.requestId);
 assert.equal(p.requests[2].expectedRevision,2);assert.equal(p.requests[2].preferences.phone,'+12025550149');
 await p.poll();assert.equal((await storage.read(key)).pending,null);assert.equal((await storage.read(key)).confirmed.revision,3);
});
test('native reload retries only explicitly saved operation with original ID',async()=>{
 const storage=device(),a=await page(storage,{lost:true});await a.edit('+12025550148');await a.save();assert.equal(a.requests.length,1);
 const b=await page(storage);assert.equal(b.requests.length,1);assert.equal(b.requests[0].requestId,a.requests[0].requestId);assert.deepEqual(b.requests[0],a.requests[0]);
});
test('native account switch stops before transmission and hides prior actor draft',async()=>{
 const p=await page(device(),{switchActor:true});await p.edit('+12025550148');await p.save();assert.equal(p.requests.length,0);assert.equal(p.node('[data-rs-field="phone"]').value,'');
});


for(const scenario of [{switchActor:true},{expired:true}])test('native access loss refuses edits to prior actor ledger '+JSON.stringify(scenario),async()=>{
 const storage=device(),p=await page(storage,scenario);await p.edit('+12025550148');await p.save();
 const key=JSON.stringify(['https://example.test/rs-inputs/subscriptions','personal','person-one']);const before=await storage.read(key);
 assert.equal(p.node('[data-rs-field="phone"]').disabled,true);assert.equal(p.node('[data-rs-sms]').disabled,true);await p.edit('+12025550149');assert.deepEqual(await storage.read(key),before);assert.equal(p.requests.length,0);
});

test('native saved operation carries fixed actor scope and account_changed stops without clearing pending',async()=>{
 const storage=device(),p=await page(storage,{accountChangedAtPost:true});await p.edit('+12025550148');await p.save();
 assert.deepEqual(p.scopes,['person-one']);assert.equal(p.node('[data-rs-field="phone"]').value,'');assert.equal(p.node('[data-rs-field="phone"]').disabled,true);
 const key=JSON.stringify(['https://example.test/rs-inputs/subscriptions','personal','person-one']);const before=await storage.read(key);assert.equal(before.pending.body.requestId,p.requests[0].requestId);assert.equal(before.pending.body.preferences.phone,'+12025550148');
 await p.edit('+12025550149');await p.save();assert.deepEqual(await storage.read(key),before);assert.equal(p.requests.length,1);
});

test('Pending intake cannot confirm; exact operation polling preserves a newer explicitly unsaved edit',async()=>{
 const storage=device(),p=await page(storage,{pendingAsync:true});await p.edit('+12025550148');await p.save();const key=JSON.stringify(['https://example.test/rs-inputs/subscriptions','personal','person-one']);
 assert.equal((await storage.read(key)).pending.body.requestId,p.requests[0].requestId);await p.edit('+12025550149');await p.poll();const state=await storage.read(key);assert.equal(state.pending,null);assert.equal(state.confirmed.data.phone,'+12025550148');assert.equal(state.draft.phone,'+12025550149');assert.equal(state.dirty,true);assert.equal(p.requests.length,1);
});

test('confirmed async save displays Saved on status and button',async()=>{
 const p=await page(device(),{pendingAsync:true});await p.edit('+12025550148');await p.save();
 assert.equal(p.node('[data-rs-status]').textContent,'Saving');await p.poll();
 assert.equal(p.node('[data-rs-status]').textContent,'Saved');assert.equal(p.node('submit').textContent,'Saved');
});
