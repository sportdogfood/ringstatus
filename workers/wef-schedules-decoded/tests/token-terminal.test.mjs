import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {healthStore,recordRenewal} from '../health-records.mjs';
import {scheduleHealth} from '../schedule-health.mjs';
const root=new URL('../',import.meta.url);
const shared=fs.readFileSync(new URL('shared-worker.mjs',root),'utf8').replace(/^import .*;\r?\n/gm,'').split('export class SglPacer')[0];
const fallback=fs.readFileSync(new URL('fallback.mjs',root),'utf8').replace(/^import .*;\r?\n/gm,'').replaceAll('export async function ','async function ');
const decoder=new Function(fs.readFileSync(new URL('worker.js',root),'utf8').replace('export default','return'))();
const token=()=>['eyJhbGciOiJIUzI1NiJ9',btoa(JSON.stringify({anon:true,exp:Math.floor(Date.now()/1000)+2592000})),'fixture'].join('.');
function fixture({body=()=>Response.json({message:'issuance refused',_t:'',cookie:'fixture-cookie'}),limit=50,scheduleGood=false,wrongSchedule=false}={}){
 const calls=[],rows=new Map(),renewals=[];let count=0;
 const env={RUNNER_ORIGIN:'https://runner.invalid',RUNNER_KEY:'fixture-runner-key',SGL_COOKIE:'fixture-cookie',WEF_SHOWS_AIRTABLE_TOKEN:'fixture-airtable'};
 const tick=kind=>{calls.push(kind);if(++count>limit)throw Error('Too many subrequests by single Worker invocation');};
 const api=async(input,options={})=>{
  const u=new URL(input),table=u.pathname.split('/').pop(),method=options.method||'GET',filter=u.searchParams.get('filterByFormula'),identity=table+filter;tick(method+' '+table);
  if(method==='GET'){
   if(rows.has(identity))return Response.json({records:[rows.get(identity)]});
   if(table==='tblcYgKFwKtILSdU8')return Response.json({records:[{id:'health',fields:{status:'Healthy'}}]});
   if(table==='tblJA5CUon5s1sWib')return Response.json({records:[{id:'show',fields:{show_id:200000078}}]});
   if(table==='tbl14MaCCDw6dAywH')return Response.json({records:[]});
   return Response.json({records:[]});
  }
  const fields=JSON.parse(options.body).records[0].fields,key=table==='tbl8pJcHIaRchfDTP'?'refresh-id':table==='tblki6YL1Fe3nnfT0'?'warning-id':'health-key';
  const row={id:'row'+count,fields};rows.set(table+`{${key}}='${fields[key]}'`,row);return Response.json({records:[row]});
 };
 const tcp=async()=>{tick('token');return body(calls.filter(c=>c==='token').length);};
 const fakeDecoder={...decoder,fetch:async()=>{tick('data');return Response.json({show:{show_id:200000078},show_date:'2026-11-12',rings:[]},{headers:{'X-RS-Decoded':'1'}});}};
 const f=new Function('tcpFetch','decoder','setTimeout','publicRoute','fetchPublic','recordRenewal',fallback+';return {fetchFallback,renewFallbackToken};')(tcp,fakeDecoder,fn=>{fn();return 0;},()=>null,null,async(env,fields)=>{renewals.push(fields);return recordRenewal(env,fields,api);});
 const primary=async input=>{tick('runner');return Response.json(wrongSchedule?{show:{show_id:999},show_date:'2026-11-12',rings:[]}:scheduleGood&&new URL(input).pathname==='/schedule'?{show:{show_id:200000078},show_date:'2026-11-12',rings:[{classes:[{class_id:123}]}]}:{},{headers:{'X-RS-Decoded':'1'}});};
 const health=(req,env,opts)=>scheduleHealth(req,env,{...opts,store:healthStore(env,api),sleep:async()=>{}});
 const app=new Function('fetch','fetchFallback','publicRoute','scheduleHealth','renewFallbackToken','setTimeout','decoder',shared.replace('export const sourceHandler =','return'))(primary,f.fetchFallback,()=>null,health,f.renewFallbackToken,fn=>{fn();return 0;},decoder);
 return {calls,rows,renewals,env,app,f,get count(){return count;}};
}
test('integrated failed health stops at three token requests and saves warning/health below quota',async()=>{
 const f=fixture();const response=await f.app.fetch(new Request('https://fixture.invalid/health'),f.env),body=await response.json();
 assert.equal(response.status,503);assert.equal(body.ok,false);assert.equal(body.attempts.length,1);
 assert.equal(f.calls.filter(c=>c==='token').length,3);assert.equal(f.calls.includes('data'),false);assert.equal(f.count,22);
 assert.deepEqual(body.errors,[]);assert.equal(body.attempts[0].stage,'token-before-schedule');
 const rows=[...f.rows.values()];assert.equal(rows.filter(r=>r.fields.kind==='Outage').length,1);assert.equal(rows.filter(r=>r.fields['health-key']==='decoder'&&r.fields.status==='Outage').length,1);
 assert.equal(f.renewals.length,3);for(const row of f.renewals){assert.match(row.error,/Config response \(redacted\):/);assert.match(row.error,/issuance refused/);assert.doesNotMatch(row.error,/fixture-cookie/);}
});
test('root schedule fallback stops instead of repeating token acquisition across data profiles',async()=>{
 const f=fixture();const r=await f.app.fetch(new Request('https://fixture.invalid/schedule?customer_id=15&show_id=200000078&date=2026-11-14'),f.env);
 assert.equal(r.status,502);assert.equal((await r.json()).stage,'token-before-schedule');assert.equal(f.calls.filter(c=>c==='token').length,3);
});
test('health stops when token acquisition fails in a later alternate profile',async()=>{
 const f=fixture({wrongSchedule:true});const r=await f.app.fetch(new Request('https://fixture.invalid/health'),f.env),body=await r.json();
 assert.equal(r.status,503);assert.equal(body.attempts.length,2);assert.equal(body.attempts[1].profile,'profile-fb-header1');assert.equal(f.calls.filter(c=>c==='token').length,3);assert.deepEqual(body.errors,[]);
});
test('unproven saved-lane discovery stops at terminal token failure',async()=>{
 const f=fixture();const memory=new Map();const r=await f.app.fetch(new Request('https://fixture.invalid/schedule?customer_id=15&show_id=200000078&date=2026-11-14',{headers:{'X-RS-Recovery':'trouble'}}),{...f.env,SGL_LANE_STORAGE:{get:async k=>memory.get(k),put:async(k,v)=>memory.set(k,v)}});
 assert.equal(r.status,502);assert.equal((await r.json()).stage,'token-before-schedule');assert.equal(f.calls.filter(c=>c==='token').length,3);
});
test('class health terminal token failure does not repeat class data profiles',async()=>{
 const f=fixture({scheduleGood:true});const r=await f.app.fetch(new Request('https://fixture.invalid/health'),f.env),b=await r.json();
 assert.equal(r.status,503);assert.equal(b.schedule_status,200);assert.equal(b.class_check.ok,false);assert.equal(b.attempts.length,2);assert.equal(f.calls.filter(c=>c==='token').length,3);assert.ok(f.count<50);
});
test('first successful token still continues to usable data and stops renewal',async()=>{
 const f=fixture({body:n=>Response.json(n===1?{}:{_t:token()})});
 const r=await f.app.fetch(new Request('https://fixture.invalid/health'),f.env);assert.equal(r.status,200);assert.equal((await r.json()).ok,true);
 assert.equal(f.calls.filter(c=>c==='token').length,2);assert.equal(f.calls.filter(c=>c==='data').length,1);
});
test('rejected config JSON and invalid JSON retain redacted content without trimming',async()=>{
 for(const response of [
  ()=>Response.json({message:'x'.repeat(5000),_t:'bad-token',nested:{authorization:'private-authorization',password:'private-password'},detail:'Bearer private-bearer fixture-cookie'}),
  ()=>new Response('<html>denied _t="private-token" cookie="private-cookie" '+ 'x'.repeat(5000),{status:403}),
  ()=>new Response('Cookie: first=private-one; second=private-two\napi-key=private-key\n'+'x'.repeat(5000),{status:403})
 ]){
  const f=fixture({body:response});const r=await f.f.fetchFallback(new Request('https://fixture.invalid/schedule?customer_id=15&show_id=200000078&date=2026-11-14'),f.env);
  assert.equal(r.status,502);assert.equal(f.renewals.length,1);const error=f.renewals[0].error;
  assert.ok(error.includes('x'.repeat(5000)));assert.doesNotMatch(error,/bad-token|private-authorization|private-password|private-bearer|fixture-cookie|private-token|private-cookie|private-one|private-two|private-key/);
 }
});

