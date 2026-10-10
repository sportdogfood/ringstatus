import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, readFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {build} from 'esbuild';
import {Miniflare} from 'miniflare';

// Actual route, separate Workers, shared persistent local D1. Provider is synthetic.
// No live Airtable, consent, SMS, deployment or remote migration is exercised.
const BASE='app9kOZdIaGyKk5uG',SUB='tblbpALv7flu3NNEE';
const AUDIT='tblwts3huk3w1ACjh',origin='https://example.invalid';
const token='A'.repeat(43),hash=createHash('sha256').update(token).digest('hex');
const named=JSON.parse(await readFile(new URL('../test-support/rs-subscription-schema.json',import.meta.url),'utf8'));
const source=`import {ALL} from './src/pages/rs-inputs/subscriptions.js';
import {createInputAccess} from './src/lib/rs-inputs-access.js';
export default {async fetch(request,env){
 // Miniflare's Node service proxy rejects external Origin headers. Restore the
 // fixture's header inside the Worker before invoking the unchanged route.
 if(request.headers.has('X-Fixture-Origin')){
  const headers=new Headers(request.headers);headers.set('Origin',headers.get('X-Fixture-Origin'));headers.delete('X-Fixture-Origin');request=new Request(request,{headers});
 }
 if(new URL(request.url).pathname.endsWith('/access')){
  const result=await createInputAccess({env}).redeem(request,(await request.json()).token);
  return Response.json({ok:true},{headers:{'Set-Cookie':result.cookie}});
 }
 return ALL({request});
}};`;
const {outputFiles}=await build({stdin:{contents:source,resolveDir:new URL('../',import.meta.url).pathname.replace(/^\/(.:)/,'$1')},bundle:true,format:'esm',platform:'browser',external:['cloudflare:workers'],write:false});
const schemas=await Promise.all(['0001_claims.sql','0002_subscriptions.sql'].map(file=>readFile(new URL('../migrations/recognize-control/'+file,import.meta.url),'utf8')));
const clean=sql=>sql.replace(/--[^\n]*/g,'').replace(/\s+/g,' ');
const revoke={action:'revoke',barnId:'barn_1',subscriptionId:'sub_1',expectedRevision:1,requestId:'runtime_revoke_001'};
async function fixture(options={}){
 const persist=await mkdtemp(join(tmpdir(),'subscription-runtime-'));
 const tables={rs_people_test:[{id:'recPerson',fields:{person_uid:'person_1',person_name:'Synthetic',status:'Active',input_access:'approved',input_session_version:'cd'.repeat(16),input_invite_hash:hash,input_invite_expires_at:new Date(Date.now()+3600000).toISOString()}}],
 tblRvTwo3HYPUkZou:[{id:'recBarn',fields:{entity_uid:'barn_1',barn_uid:'barn_1',name:'Synthetic barn',owner_uid:'person_1',revision:1}}],
 tblYgoeLEey05xgw9:[{id:'recUser',fields:{entity_uid:'user_1',barn_uid:'barn_1',name:'Synthetic user',owner_uid:'person_1',recognition_person_uid:'person_1',revision:1}}],
 [SUB]:[{id:'recSub',fields:{entity_uid:'sub_1',barn_uid:'barn_1',user_uid:'user_1',engine_scope:'WEC',target_type:'Barn',target_uid:'barn_1',alert_types:'existing_started\nexisting_completed',phone_e164:'+12025550148',status:'Draft',consent_state:'Granted',consent_at:'2026-10-08T00:00:00.000Z',consent_source:'synthetic-grant',notice_version:'synthetic-v1',owner_uid:'person_1',revision:1,request_uid:'seed',request_hash:'a'.repeat(64),record_mode:'Test'}}],[AUDIT]:[]};
 let mutations=0,audits=0,loseResponse=!!options.loseResponse,failAudit=!!options.failAudit,seq=0;
 const provider=async request=>{
  const url=new URL(request.url),parts=url.pathname.split('/').filter(Boolean);
  assert.equal(url.origin,'https://api.airtable.com');assert.equal(parts[1],BASE);
  const table=decodeURIComponent(parts[2]);let rows=tables[table];assert.ok(rows,'unexpected provider destination '+table);
  if(request.method==='GET'){
   const formula=url.searchParams.get('filterByFormula');
   if(formula){const terms=[...formula.matchAll(/\{([^}]+)\}\s*=\s*'([^']*)'/g)];assert.ok(terms.length,formula);rows=rows.filter(row=>terms.every(([,key,value])=>row.fields[key]===value));}
   await new Promise(resolve=>setTimeout(resolve,15));
   return Response.json({records:structuredClone(rows)});
  }
  assert.equal(request.method,'PATCH');const body=await request.json();
  if(table==='rs_people_test'){Object.assign(rows[0].fields,body.records[0].fields);return Response.json({records:rows});}
  const schema=named.find(t=>t.id===table);assert.ok(schema);
  const fields=Object.fromEntries(Object.entries(body.records[0].fields).map(([key,value])=>{const field=schema.fields.find(f=>f.id===key||f.name===key);assert.ok(field,'unknown field '+key);return[field.name,value]}));
  const keys=body.performUpsert.fieldsToMergeOn.map(key=>schema.fields.find(f=>f.id===key||f.name===key).name);
  if(table===SUB){mutations++;if(options.rejectWrite)return Response.json({error:'synthetic unavailable'},{status:502});}
  if(table===AUDIT){audits++;if(failAudit)return Response.json({error:'synthetic audit unavailable'},{status:502});}
  let row=rows.find(row=>keys.every(key=>row.fields[key]===fields[key]));
  if(!row){row={id:'recNew'+(++seq),fields:{}};rows.push(row);}Object.assign(row.fields,fields);
  for(const key of Object.keys(row.fields))if(row.fields[key]==='')delete row.fields[key];
  if(table===SUB&&loseResponse){loseResponse=false;return Response.json({error:'synthetic lost response'},{status:502});}
  return Response.json({records:[structuredClone(row)]});
 };
 const bindings={AIRTABLE_TOKEN:'synthetic',RS_INPUTS_BASE_ID:BASE,RS_INPUTS_RECOGNITION_BASE_ID:BASE,RS_INPUTS_WRITE_MODE:'isolated-trial',RS_INPUTS_SESSION_SECRET:'ab'.repeat(32)};
 let mf,cookie;
 async function start(){
  mf=new Miniflare({d1Persist:persist,workers:['a','b'].map(name=>({name,modules:true,script:outputFiles[0].text,compatibilityDate:'2025-03-03',compatibilityFlags:['nodejs_compat'],bindings,d1Databases:{RS_RECOGNITION_CONTROL_DB:'recognize-control'},outboundService:provider}))});
  const db=await mf.getD1Database('RS_RECOGNITION_CONTROL_DB','a');
  await db.exec(clean(schemas[0]));if(!options.noMigration)await db.exec(clean(schemas[1]));
 }
 await start();
 const access=await(await mf.getWorker('a')).fetch(origin+'/test/rs-inputs/access',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token})});
 assert.equal(access.status,200);cookie=access.headers.get('Set-Cookie').split(';')[0];
 return{tables,get mutations(){return mutations},get audits(){return audits},auditAvailable(){failAudit=false},
 send:async(worker,body)=> (await mf.getWorker(worker)).fetch(origin+'/test/rs-inputs/subscriptions?barn_id=barn_1',{method:body?'POST':'GET',headers:{Cookie:cookie,...(body?{'X-Fixture-Origin':origin,'Content-Type':'application/json'}:{})},...(body?{body:JSON.stringify(body)}:{})}),
 restart:async()=>{await mf.dispose();await start()},close:()=>mf.dispose()};
}
async function status(response,expected){const body=await response.json();assert.equal(response.status,expected,JSON.stringify(body));return body;}
test('installed route reads, revokes once across Workers, and replays after restart',async()=>{
 const f=await fixture();try{
  const read=await status(await f.send('a'),200);assert.equal(read.subscriptions[0].user_uid,'user_1');
  const results=await Promise.all(['a','b'].map(worker=>f.send(worker,revoke)));
  assert.deepEqual(results.map(r=>r.status).sort(),[200,409]);assert.equal(f.mutations,1);assert.equal(f.tables[AUDIT].length,1);
  await f.restart();await status(await f.send('b',revoke),200);assert.equal(f.mutations,1);
 }finally{await f.close()}
});
test('installed route permits existing-alert removal but no grant or catalog expansion',async()=>{
 const f=await fixture();try{
  const update={action:'update',barnId:'barn_1',subscriptionId:'sub_1',expectedRevision:1,requestId:'runtime_update_001',alertTypes:[]};
  const result=await status(await f.send('a',update),200);assert.equal(result.subscription.alert_types,'');assert.equal(result.subscription.consent_state,'Granted');
  await status(await f.send('b',{...update,requestId:'runtime_expand_001',expectedRevision:2,alertTypes:['new_alert']}),409);
  await status(await f.send('b',{action:'grant',barnId:'barn_1',engineScope:'WEC',requestId:'runtime_grant_001',phone:'+12025550149',alertKeys:['class_started']}),409);
  assert.equal(f.mutations,1);
 }finally{await f.close()}
});
test('lost subscription response reconciles after restart without another mutation',async()=>{
 const f=await fixture({loseResponse:true});try{
  await status(await f.send('a',revoke),503);await f.restart();await status(await f.send('b',revoke),200);
  assert.equal(f.mutations,1);assert.equal(f.tables[AUDIT].length,1);
 }finally{await f.close()}
});
test('audit failure reserves actor until same request repairs audit after restart',async()=>{
 const f=await fixture({failAudit:true});try{
  await status(await f.send('a',revoke),503);await f.restart();f.auditAvailable();
  await status(await f.send('b',{...revoke,requestId:'different_request_001'}),503);
  await status(await f.send('b',revoke),200);assert.equal(f.mutations,1);assert.equal(f.tables[AUDIT].length,1);
 }finally{await f.close()}
});
test('uncertain absent provider write never repeats after restart',async()=>{
 const f=await fixture({rejectWrite:true});try{
  await status(await f.send('a',revoke),503);await f.restart();await status(await f.send('b',revoke),503);
  assert.equal(f.mutations,1);assert.equal(f.tables[AUDIT].length,0);
 }finally{await f.close()}
});
test('missing proposed migration allows scoped reads but rejects mutation',async()=>{
 const f=await fixture({noMigration:true});try{
  await status(await f.send('a'),200);const result=await status(await f.send('b',revoke),503);
  assert.equal(result.error,'subscription_control_unavailable');assert.equal(f.mutations,0);
 }finally{await f.close()}
});
