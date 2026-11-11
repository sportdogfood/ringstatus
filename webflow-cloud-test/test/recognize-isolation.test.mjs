import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';
import {refreshIam,handleIamRecognition} from '../src/lib/rs-recognition-iam.js';
import {iamActor} from '../src/lib/rs-inputs-iam-policy.js';
const origin='https://ringstatus.com';
function database(){
 const sql=new DatabaseSync(':memory:');
 for(const name of ['0001_claims.sql','0003_iam.sql'])sql.exec(readFileSync(new URL('../migrations/recognize-control/'+name,import.meta.url),'utf8'));
 const db={prepare(text){let args=[];return {bind(...values){args=values;return this;},async run(){const r=sql.prepare(text).run(...args);return {meta:{changes:Number(r.changes)}};},async first(){return sql.prepare(text).get(...args)||null;},async all(){return {results:sql.prepare(text).all(...args)};}};},async batch(rows){sql.exec('BEGIN');try{const result=[];for(const row of rows)result.push(await row.run());sql.exec('COMMIT');return result;}catch(error){sql.exec('ROLLBACK');throw error;}},close(){sql.close();}};
 return db;
}
async function fixture(){
 const db=database(),env={RS_RECOGNITION_CONTROL_DB:db,RS_INPUTS_SESSION_SECRET:'1'.repeat(64)};
 const input={env,identities:[{id:'recIdentity000001',fields:{person_uid:'independent',status:'Active',person_name:'Independent',primary_phone_e164:'+12025550101',member_pin:'0101'}},{id:'recIdentity000002',fields:{person_uid:'member',status:'Active',person_name:'Member',primary_phone_e164:'+12025550102',input_access:'approved',member_pin:'0102'}}],people:[{id:'person_member',fields:{recognition_person_uid:'member',status:'Active',phone_e164:'+12025550102'}}],memberships:[{fields:{Person:['person_member'],Tenant:['barn'],Status:'Enabled',Roles:['role']}}],tenants:[{id:'barn',fields:{tenant_active:true}}],roles:[{id:'role',fields:{rec_name:'user'}}]};
 await refreshIam(input);return {db,env,input};
}
async function call(f,operation,body={},cookie='',ip='198.51.100.10'){
 const headers={'Content-Type':'application/json',Origin:origin,Cookie:cookie};if(ip)headers['CF-Connecting-IP']=ip;
 const tasks=[],request=new Request(origin+'/test/rs-inputs/native-recognition?operation='+operation,{method:'POST',headers,body:JSON.stringify({session_uid:crypto.randomUUID(),...body})});
 const response=await handleIamRecognition(request,f.env,{waitUntil:p=>tasks.push(p)});await Promise.all(tasks);
 return {response,body:await response.json(),request};
}
async function credential(f,phone='2025550101',ip){return call(f,'action',{action:'credential',kind:'phone',value:phone,request_id:crypto.randomUUID()},'',ip);}
const cookieOf=result=>'__Host-rs_recognition_device='+result.body.device_token;

test('independent identity, cookie renewal and separate private access',async()=>{
 const f=await fixture();try{
  const login=await credential(f);assert.equal(login.body.verified,true);
  await assert.rejects(iamActor(new Request(origin,{headers:{Cookie:cookieOf(login)}}),f.env),/authentication_required/);
  await f.db.prepare('UPDATE recognize_iam_devices SET expires_at=?').bind(Date.now()+60000).run();
  const known=await call(f,'recognize',{},cookieOf(login));assert.equal(known.body.person_uid,'independent');assert.equal(known.body.matched_by,'device_token');assert.equal(known.body.identity_only,false);assert.equal(known.body.person_name,'Independent');assert.equal(known.response.headers.get('X-RS-Airtable-Calls'),'0');
  assert.match(known.response.headers.get('set-cookie'),/Max-Age=31536000/);
  const device=await f.db.prepare('SELECT expires_at FROM recognize_iam_devices').first();assert.ok(device.expires_at>Date.now()+364*86400000);
  const member=await credential(f,'2025550102','203.0.113.2');assert.equal((await iamActor(new Request(origin,{headers:{Cookie:cookieOf(member)}}),f.env)).id,'member');
 }finally{f.db.close();}
});

test('unique exact edge IP recognizes, persists through refresh, never grants private access',async()=>{
 const f=await fixture();try{
  await credential(f,'2025550102');await refreshIam(f.input);
  const geo=await call(f,'recognize');assert.equal(geo.body.person_uid,'member');assert.equal(geo.body.matched_by,'geoip');assert.equal(geo.body.identity_only,true);
  const cookie=geo.response.headers.get('set-cookie').split(';')[0];
  await assert.rejects(iamActor(new Request(origin,{headers:{Cookie:cookie}}),f.env),/authentication_required/);
  const repeat=await call(f,'recognize',{},cookie);assert.equal(repeat.body.person_uid,'member');
  const other=await call(f,'recognize',{},'','203.0.113.99');assert.equal(other.body.recognized,false);
  const forged=await call(f,'recognize',{geoip:'198.51.100.10'},'','');assert.equal(forged.body.recognized,false);
 }finally{f.db.close();}
});

test('shared IP stays ambiguous; GeoIP-only cookies cannot teach another IP',async()=>{
 const f=await fixture();try{
  await credential(f);
  const geo=await call(f,'recognize'),cookie=geo.response.headers.get('set-cookie').split(';')[0];
  await call(f,'recognize',{},cookie,'203.0.113.88');assert.equal((await call(f,'recognize',{},'','203.0.113.88')).body.recognized,false);
  await credential(f,'2025550102');assert.equal((await call(f,'recognize')).body.recognized,false);
 }finally{f.db.close();}
});

test('retirement suppresses GeoIP return; retired and expired devices are not renewed',async()=>{
 const f=await fixture();try{
  const login=await credential(f),retired=await call(f,'action',{action:'retire_device',session_event_uid:crypto.randomUUID()},cookieOf(login));
  assert.match(retired.response.headers.get('set-cookie'),/__Host-rs_recognition_signed_out=1/);
  const after=await call(f,'recognize',{},'__Host-rs_recognition_signed_out=1');assert.equal(after.body.recognized,false);
  assert.equal((await call(f,'recognize',{},cookieOf(login))).body.recognized,false);
  const fresh=await credential(f);assert.match(fresh.response.headers.get('set-cookie'),/__Host-rs_recognition_signed_out=;.*Max-Age=0/);
  await f.db.prepare('UPDATE recognize_iam_devices SET expires_at=?').bind(Date.now()-1).run();
  const expired=await call(f,'recognize',{},cookieOf(fresh),'');assert.equal(expired.body.recognized,false);assert.equal(expired.response.headers.get('set-cookie'),null);
 }finally{f.db.close();}
});
