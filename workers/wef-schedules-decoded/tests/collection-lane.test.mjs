import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const source=fs.readFileSync(new URL('../shared-worker.mjs',import.meta.url),'utf8').replace(/^import .*;\r?\n/gm,'').split('export class SglPacer')[0].replace('export const sourceHandler =','return');
const path='/classes/123?customer_id=15&show_id=78';
const good=(id=123)=>Response.json({class:{class_id:id},trips:[]},{headers:{'X-RS-Decoded':'1'}});
function fixture({primary=()=>good(),fallback=()=>good(),storage=new Map()}={}){
 const calls=[],pauses=[];let renewals=0;
 const app=new Function('fetch','fetchFallback','publicRoute','scheduleHealth','renewFallbackToken','setTimeout',source)(async()=>{calls.push('runner');return primary();},async(r,e,o)=>{calls.push(o.profile);return fallback(o.profile,r,o);},()=>null,()=>{throw Error('health not invoked');},async()=>{renewals++;},(fn,ms)=>{pauses.push(ms);fn();});
 const env={RUNNER_ORIGIN:'https://runner.example',RUNNER_KEY:'fixture',SGL_LANE_STORAGE:{get:async k=>storage.get(k),put:async(k,v)=>storage.set(k,v)}};
 return {calls,pauses,storage,renewals:()=>renewals,run:(mode='defer',route=path)=>app.fetch(new Request('https://decoder.example'+route,{headers:{'X-RS-Recovery':mode}}),env)};
}
test('fast class failure is one attempt with no discovery or renewal',async()=>{const f=fixture({primary:()=>Response.json({})});const r=await f.run();assert.equal(r.status,502);assert.deepEqual(f.calls,['runner']);assert.equal(f.renewals(),0);assert.equal(r.headers.get('X-RS-Failure-Reason'),'response-undecoded');});
test('discovery learns Windows and reuses it across reconstructed handler and isolated failures',async()=>{
 const storage=new Map(),f=fixture({storage,primary:()=>Response.json({}),fallback:p=>p==='windows-clean'?good():Response.json({})});
 assert.equal((await f.run('trouble')).status,200);assert.deepEqual(f.calls,['runner','original-header','profile-fb-header1','profile-fb-header2','windows-clean']);assert.ok(f.pauses.every(x=>x>=1000));
 const g=fixture({storage,primary:()=>{throw Error('runner must not run');},fallback:()=>Response.json({})});
 assert.equal((await g.run()).status,502);assert.equal((await g.run('trouble')).status,502);assert.deepEqual(g.calls,['windows-clean','runner']);assert.equal(g.renewals(),0);
 const h=fixture({storage,primary:()=>{throw Error('wrong lane');}});assert.equal((await h.run()).status,200);assert.deepEqual(h.calls,['windows-clean']);
});
test('exhausted discovery is not repeated next run',async()=>{const f=fixture({primary:()=>Response.json({}),fallback:()=>Response.json({})});assert.equal((await f.run('trouble')).headers.get('X-RS-Recovery-State'),'exhausted');assert.equal(f.calls.length,11);assert.equal(f.renewals(),1);await f.run();await f.run('trouble');assert.equal(f.calls.length,13);assert.equal(f.renewals(),1);});
test('family hint is not proof for a different class or show',async()=>{const f=fixture();await f.run();const r=await f.run('defer','/classes/124?customer_id=15&show_id=78');assert.equal(r.status,502);assert.equal(f.storage.size,2);assert.equal((await f.run('trouble','/classes/124?customer_id=15&show_id=78')).status,502);assert.equal(f.calls.length,13);});
test('zero schedule rings valid, malformed schedule and wrong class rejected',async()=>{const f=fixture({primary:()=>Response.json({show:{show_id:78},rings:[]},{headers:{'X-RS-Decoded':'1'}})});assert.equal((await f.run('defer','/schedule?customer_id=15&show_id=78&date=2026-11-14')).status,200);assert.equal((await f.run('defer','/schedule?customer_id=15&show_id=78&date=2026-11-15')).status,200);assert.equal(f.storage.size,2);const g=fixture({primary:()=>good(999)});assert.equal((await g.run()).status,502);assert.equal(g.storage.size,0);});
test('invalid request rejected before storage or source',async()=>{const f=fixture();assert.equal((await f.run('defer',path+'&unapproved=1')).status,400);assert.equal(f.calls.length,0);assert.equal(f.storage.size,0);});

test('trouble progresses through alternatives without clearing known success or repeating discovery',async()=>{
 const storage=new Map(),first=fixture({storage});await first.run();
 const broken=fixture({storage,primary:()=>Response.json({}),fallback:()=>Response.json({})});await broken.run();await broken.run('trouble');await broken.run('trouble');
 assert.deepEqual(broken.calls,['runner','original-header','profile-fb-header1']);assert.equal(broken.renewals(),0);
 const recovered=fixture({storage,fallback:()=>good()});assert.equal((await recovered.run('trouble')).status,200);assert.deepEqual(recovered.calls,['profile-fb-header2']);assert.equal((await recovered.run()).status,200);assert.deepEqual(recovered.calls,['profile-fb-header2','profile-fb-header2']);
});
test('normalized trailing slash keeps class identity; wrong schedule show rejected',async()=>{const f=fixture();assert.equal((await f.run('defer','/classes/123/?customer_id=15&show_id=78')).status,200);const g=fixture({primary:()=>Response.json({show:{show_id:999},rings:[]},{headers:{'X-RS-Decoded':'1'}})});const r=await g.run('defer','/schedule?customer_id=15&show_id=78&date=2026-11-14');assert.equal(r.status,502);assert.equal(r.headers.has('X-RS-Decoded'),false);});

test('failure retains actual fallback report cause and observed upstream status',async()=>{const f=fixture({primary:()=>Response.json({}),fallback:(p,r,o)=>{o.report({schedule_upstream_status:200,reason:'Schedule response could not be decoded'});return Response.json({message:'decode failed'},{status:502});}});const r=await f.run('trouble');assert.equal(r.status,502);assert.equal(r.headers.get('X-RS-Upstream-Status'),'200');assert.equal(r.headers.get('X-RS-Failure-Reason'),'Schedule response could not be decoded');});
