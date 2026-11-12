import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const source=fs.readFileSync(new URL('../../../shared-worker.mjs',import.meta.url),'utf8').replace(/^import .*;\r?\n/gm,'').split('export class SglPacer')[0].replace('export const sourceHandler =','return');
const path='/classes/123?customer_id=15&show_id=78';
const good=(id=123)=>Response.json({class:{class_id:id},trips:[]},{headers:{'X-RS-Decoded':'1'}});
function fixture({primary=()=>good(),fallback=()=>good(),storage=new Map()}={}){
 const calls=[],pauses=[];let renewals=0;
 const app=new Function('fetch','fetchFallback','publicRoute','scheduleHealth','renewFallbackToken','setTimeout',source)(async()=>{calls.push('runner');return primary();},async(r,e,o)=>{calls.push(o.profile);return fallback(o.profile,r);},()=>null,()=>{throw Error('health not invoked');},async()=>{renewals++;},(fn,ms)=>{pauses.push(ms);fn();});
 const env={RUNNER_ORIGIN:'https://runner.example',RUNNER_KEY:'fixture',SGL_LANE_STORAGE:{get:async k=>storage.get(k),put:async(k,v)=>storage.set(k,v)}};
 return {calls,pauses,storage,renewals:()=>renewals,run:(mode='defer',route=path)=>app.fetch(new Request('https://decoder.example'+route,{headers:{'X-RS-Recovery':mode}}),env)};
}

test('wrong schedule show/date rejected without learning and empty matching schedule accepted',async()=>{for(const payload of [{show:{show_id:999},rings:[]},{show:{show_id:78},date:'2026-11-15',rings:[]}]){const f=fixture({primary:()=>Response.json(payload,{headers:{'X-RS-Decoded':'1'}})});assert.equal((await f.run('defer','/schedule?customer_id=15&show_id=78&date=2026-11-14')).status,502);assert.equal(f.storage.size,0);}});
test('rejection strips decoded marker and all arbitrary runner headers',async()=>{const f=fixture({primary:()=>Response.json({class:{class_id:999}},{headers:{'X-RS-Decoded':'1','X-Runner-Internal':'internal-fixture','Content-Encoding':'gzip'}})});const r=await f.run();assert.equal(r.status,502);for(const h of ['X-RS-Decoded','X-Runner-Internal','Content-Encoding'])assert.equal(r.headers.has(h),false);assert.equal((await r.json()).message,'Source request deferred');});
test('known runner outage finds healthy fallback in one later trouble attempt',async()=>{const storage=new Map();await fixture({storage}).run();const f=fixture({storage,primary:()=>new Response('offline',{status:503})});assert.equal((await f.run()).status,503);assert.equal((await f.run('trouble')).status,200);assert.deepEqual(f.calls,['runner','original-header']);assert.equal(f.renewals(),0);assert.equal((await f.run()).status,200);assert.deepEqual(f.calls,['runner','original-header','original-header']);});
test('exhausted outage can recover using single alternatives on later trouble passes',async()=>{const storage=new Map();const f=fixture({storage,primary:()=>Response.json({}),fallback:()=>Response.json({})});await f.run('trouble');const g=fixture({storage,primary:()=>Response.json({})});assert.equal((await g.run('trouble')).status,502);assert.equal((await g.run('trouble')).status,200);assert.deepEqual(g.calls,['runner','original-header']);assert.equal(g.renewals(),0);});
test('normalization and redirect remain safe',async()=>{assert.equal((await fixture().run('defer','/classes/123/?customer_id=15&show_id=78')).status,200);const f=fixture({primary:()=>new Response(null,{status:302,headers:{Location:'https://fixture.invalid'}})});const r=await f.run();assert.equal(r.status,502);assert.equal(r.headers.has('Location'),false);});
test('actual DO wrapper supplies durable storage and completes response under existing queue',async()=>{
const full=fs.readFileSync(new URL('../../../shared-worker.mjs',import.meta.url),'utf8').replace(/^import .*;\r?\n/gm,'').split('export default')[0].replace('export const sourceHandler','const sourceHandler').replace('export class SglPacer','class SglPacer')+'\nreturn SglPacer;';
const {createPacer}=await import('../../../sgl-pacing.mjs');
const DO=new Function('DurableObject','createPacer','fetch','fetchFallback','publicRoute','scheduleHealth','renewFallbackToken',full)(class {constructor(ctx,env){this.ctx=ctx;this.env=env;}},createPacer,async()=>good(),async()=>good(),()=>null,()=>{},()=>{});
const map=new Map();const ctx={storage:{get:async k=>map.get(k),put:async(k,v)=>map.set(k,v)},blockConcurrencyWhile:fn=>fn()};const obj=new DO(ctx,{RUNNER_ORIGIN:'https://runner.example',RUNNER_KEY:'fixture'});const r=await obj.fetch(new Request('https://decoder.example'+path,{headers:{'X-RS-Recovery':'defer'}}));assert.equal(r.status,200);assert.equal((await r.json()).class.class_id,123);assert.ok(map.has('nextAllowed'));assert.equal(map.get('lane|class|15|78|/classes/123|').lane,'runner');assert.ok(r.headers.has('X-RS-Queue-End'));
});
