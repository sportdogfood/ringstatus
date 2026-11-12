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

test('counterexample: wrong show schedule accepted',async()=>{const f=fixture({primary:()=>Response.json({show:{show_id:999},rings:[]},{headers:{'X-RS-Decoded':'1'}})});const r=await f.run('defer','/schedule?customer_id=15&show_id=78&date=2026-11-14');assert.equal(r.status,200);assert.equal(f.storage.size,2);});
test('counterexample: rejected class retains decoded marker and arbitrary runner headers',async()=>{const f=fixture({primary:()=>Response.json({class:{class_id:999}},{headers:{'X-RS-Decoded':'1','X-Runner-Internal':'internal-fixture'}})});const r=await f.run();assert.equal(r.status,502);assert.equal(r.headers.get('X-RS-Decoded'),'1');assert.equal(r.headers.get('X-Runner-Internal'),'internal-fixture');});
test('counterexample: learned runner outage never reaches healthy fallback in trouble',async()=>{const storage=new Map();const first=fixture({storage});await first.run();const later=fixture({storage,primary:()=>new Response('offline',{status:503})});for(let i=0;i<3;i++)assert.equal((await later.run('trouble')).status,503);assert.deepEqual(later.calls,['runner','runner','runner']);});
test('counterexample: after outage exhausts discovery recovered fallback remains unused',async()=>{const storage=new Map();const f=fixture({storage,primary:()=>Response.json({}),fallback:()=>Response.json({})});await f.run('trouble');const g=fixture({storage,primary:()=>Response.json({})});assert.equal((await g.run('trouble')).status,502);assert.deepEqual(g.calls,['runner']);});
test('counterexample: normalized valid trailing slash class rejected',async()=>{const f=fixture();assert.equal((await f.run('defer','/classes/123/?customer_id=15&show_id=78')).status,502);assert.deepEqual(f.calls,['runner']);});
