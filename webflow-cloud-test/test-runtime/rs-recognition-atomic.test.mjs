import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { build } from 'esbuild';
import { Miniflare } from 'miniflare';
import { createHash } from 'node:crypto';

// Actual independent Workers and durable SQLite; Airtable is intercepted.
// No deployment, real user, network SMS or production workflow is exercised.
const token = 'a'.repeat(43);
const hash = createHash('sha256').update(token).digest('hex');
const source = `
import {recordRecognitionSession} from './src/lib/rs-recognition-session.js';
import {createInputAccess} from './src/lib/rs-inputs-access.js';
import {createRecoverySms} from './src/lib/rs-recognition-sms.js';
export default {async fetch(request, env) {
 try {
  const body=await request.json();
  const path=new URL(request.url).pathname;
  const result=path.endsWith('/access') ? await createInputAccess({env}).redeem(request,body.token)
   :path.endsWith('/sms-request') ? await createRecoverySms({env}).request(body)
   :path.endsWith('/sms-prepare') ? await createRecoverySms({env}).prepare(body.id)
   :await recordRecognitionSession({env,request,payload:body});
  return Response.json(result);
 } catch(e) { return Response.json({error:e.code||e.message},{status:e.status||503}); }
}};`;
const {outputFiles} = await build({stdin:{contents:source,resolveDir:new URL('../',import.meta.url).pathname.replace(/^\/(.:)/,'$1')},bundle:true,format:'esm',platform:'browser',write:false});
const script = outputFiles[0].text;
const schema = (await readFile(new URL('../migrations/recognize-control/0001_claims.sql',import.meta.url),'utf8')).replace(/--[^\n]*/g,'').replace(/\s+/g,' ');
const event = {session_uid:'session-atomic',session_event_uid:'event-atomic',event_type:'recognition',event_result:'matched',idempotency_key:'same-event'};
const post = body => ({method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});

async function fixture(options={}) {
 const persist = await mkdtemp(join(tmpdir(),'recognize-atomic-'));
 const rows=[],smsRequests=[],smsEvents=[]; let creates=0,clears=0,reads=0;
 const person={id:'recSynthetic00001',fields:{person_uid:'invited',email:'synthetic@example.invalid',primary_phone_e164:'+12025550199',status:'Active',input_access:'invited',input_invite_hash:hash,input_invite_expires_at:new Date(Date.now()+3600000).toISOString(),input_session_version:'a'.repeat(32)}};
 const provider=async request=>{
  const url=new URL(request.url);
  assert.equal(url.origin,'https://api.airtable.com');
  assert.equal(url.pathname.split('/')[2],'app9kOZdIaGyKk5uG');
  const table=url.pathname.split('/')[3],id=url.pathname.split('/')[4];
  const people=['rs_people_test','tbly1PM5iFYqVzKSm'].includes(table);
  const sms=table==='tblxC4SYtzYhJNcJS'?smsRequests:table==='tblF1Hdqi3yoKTZPV'?smsEvents:null;
  if(sms){
   if(request.method==='GET'){
    if(id){const row=structuredClone(sms.find(r=>r.id===id));if(table==='tblxC4SYtzYhJNcJS')row.fields['primary_phone_e164 (from person_uid)']=[person.fields.primary_phone_e164];return Response.json(row);}
    const filter=url.searchParams.get('filterByFormula')?.match(/\{([^}]+)\} = '([^']*)'/);
    const snapshot=structuredClone(filter?sms.filter(r=>r.fields[filter[1]]===filter[2]):sms);
    await new Promise(r=>setTimeout(r,15));return Response.json({records:snapshot});
   }
   const entry=(await request.json()).records[0];
   let row=sms.find(r=>r.id===entry.id);
   if(!row){row={id:'rec'+String(200+sms.length).padStart(14,'0'),fields:{}};sms.push(row);}
   Object.assign(row.fields,entry.fields);return Response.json({records:[row]});
  }
  if(request.method==='GET'){
   reads++;
   const snapshot=people?[structuredClone(person)]:structuredClone(rows);
   await new Promise(r=>setTimeout(r,15));
   return Response.json(people&&id?snapshot[0]:{records:snapshot});
  }
  if(people){
   clears++;
   Object.assign(person.fields,(await request.json()).records[0].fields);
   if(options.loseInvitationResponse) throw new Error('synthetic lost response');
   return Response.json({records:[person]});
  }
  creates++;
  const row={id:'rec'+String(creates).padStart(14,'0'),fields:(await request.json()).records[0].fields};
  if(!options.rejectEvent) rows.push(row);
  if(options.loseEventResponse) throw new Error('synthetic lost response');
  return options.rejectEvent?Response.json({}, {status:502}):Response.json({records:[row]});
 };
 const bindings={AIRTABLE_TOKEN:'fixture',RS_INPUTS_BASE_ID:'app9kOZdIaGyKk5uG',RS_INPUTS_RECOGNITION_BASE_ID:'app9kOZdIaGyKk5uG',RS_INPUTS_WRITE_MODE:'isolated-trial',RS_INPUTS_SESSION_SECRET:'ab'.repeat(32),RS_RECOGNITION_SMS_QUEUE_MODE:'ready-gated',RS_RECOGNITION_SMS_RECORD_MODE:'test',RS_RECOGNITION_SMS_TEST_RECIPIENT:'+12025550199',RS_INPUTS_ONBOARDING_URL:'https://example.invalid/test/onboarding'};
 let mf;
 async function start(){
  mf=new Miniflare({d1Persist:persist,workers:['a','b'].map(name=>({name,modules:true,script,compatibilityDate:'2025-03-03',compatibilityFlags:['nodejs_compat'],bindings,d1Databases:options.noDatabase?{}:{RS_RECOGNITION_CONTROL_DB:'recognize-control'},outboundService:provider}))});
  if(!options.noDatabase) await (await mf.getD1Database('RS_RECOGNITION_CONTROL_DB','a')).exec(schema);
 }
 await start();
 return {rows,person,smsRequests,smsEvents,get creates(){return creates;},get clears(){return clears;},get reads(){return reads;},
  send:async(name,path,body)=>(await mf.getWorker(name)).fetch('https://example.invalid/test/rs-inputs/'+path,post(body)),
  restart:async()=>{await mf.dispose();await start();},close:()=>mf.dispose()};
}

test('two independent Workers create only one Airtable event and replay after restart',async()=>{
 const f=await fixture();try{
  const first=await Promise.all(['a','b'].map(w=>f.send(w,'event',event)));
  assert.ok(first.some(r=>r.status===200));assert.equal(f.creates,1);
  await f.restart();const retry=await f.send('b','event',event);assert.equal(retry.status,200);assert.equal(f.creates,1);
 }finally{await f.close();}
});
test('one invitation yields exactly one cookie across independent Workers',async()=>{
 const f=await fixture();try{
  const responses=await Promise.all(['a','b'].map(w=>f.send(w,'access',{token})));
  assert.equal(responses.filter(r=>r.status===200).length,1);assert.equal(f.clears,1);
  await f.restart();assert.notEqual((await f.send('a','access',{token})).status,200);assert.equal(f.clears,1);
 }finally{await f.close();}
});
test('lost event response is reconciled by readback after restart without another POST',async()=>{
 const f=await fixture({loseEventResponse:true});try{
  assert.ok((await f.send('a','event',event)).status >= 500);
  await f.restart();assert.equal((await f.send('b','event',event)).status,200);assert.equal(f.creates,1);
 }finally{await f.close();}
});
test('uncertain event absence cannot cause blind repeated POST after restart',async()=>{
 const f=await fixture({rejectEvent:true});try{
  await f.send('a','event',event);await f.restart();assert.equal((await f.send('b','event',event)).status,503);assert.equal(f.creates,1);
 }finally{await f.close();}
});
test('lost invitation response never issues a cookie or clears twice',async()=>{
 const f=await fixture({loseInvitationResponse:true});try{
  assert.equal((await f.send('a','access',{token})).status,503);await f.restart();
  assert.notEqual((await f.send('b','access',{token})).status,200);assert.equal(f.clears,1);
 }finally{await f.close();}
});
test('missing durable binding fails closed before business writes',async()=>{
 const f=await fixture({noDatabase:true});try{
  assert.equal((await f.send('a','event',event)).status,503);
  assert.equal((await f.send('b','access',{token})).status,503);
  assert.equal(f.creates+f.clears,0);
 }finally{await f.close();}
});
test('two independent Workers queue and prepare only one SMS, including after restart',async()=>{
 const f=await fixture();try{
  const body={session_event_uid:'sms-concurrent-recovery-0001',email:'synthetic@example.invalid'};
  const results=await Promise.all(['a','b'].map(w=>f.send(w,'sms-request',body)));
  assert.ok(results.some(r=>r.status===200));assert.equal(f.smsRequests.length,1);
  const id=f.smsRequests[0].id;
  const prepares=await Promise.all(['a','b'].map(w=>f.send(w,'sms-prepare',{id})));
  assert.equal(prepares.filter(r=>r.status===200).length,1);assert.equal(f.clears,1);
  assert.equal(f.smsEvents.filter(r=>r.fields.event_type==='requested').length,1);
  assert.equal(f.smsEvents.filter(r=>r.fields.event_type==='attempt_started').length,1);
  await f.restart();assert.notEqual((await f.send('b','sms-prepare',{id})).status,200);assert.equal(f.clears,1);
 }finally{await f.close();}
});
