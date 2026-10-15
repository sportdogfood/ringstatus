import test from 'node:test';
import assert from 'node:assert/strict';
import {recognizeTenantsFixture} from '../test-support/recognize-tenants-fixture.mjs';
import {accessHash} from '../src/lib/rs-inputs-access.js';
import {safeState,validateDecision,redactMessage} from '../recognize-agent/policy.mjs';
const url='https://ringstatus.com/test/rs-inputs/native-recognition?operation=action';
const send=async(f,body,headers={})=>{const response=await f.dispatch(new Request(url,{method:'POST',headers:{Origin:'https://ringstatus.com','Content-Type':'application/json',...headers},body:JSON.stringify({action:'credential',request_id:crypto.randomUUID(),...body})}));return {status:response.status,data:await response.json(),cookie:response.headers.get('set-cookie')};};

test('phone lookup binds exactly once, normalizes +1, enters existing Tenants access; no SMS',async()=>{
 const f=await recognizeTenantsFixture();try{
  const request_id=crypto.randomUUID(),body={kind:'phone',value:'(631) 875-2160',request_id};
  const a=await send(f,body);assert.equal(a.data.verified,true);assert.equal(f.code,null);
  const b=await send(f,body);assert.equal(b.data.device_token,a.data.device_token);assert.equal(f.rows.tblfkRSJAEMzuzApR.length,1);
  const access=await f.dispatch(new Request('https://ringstatus.com/test/rs-inputs/access',{headers:{Cookie:a.cookie.split(';')[0]}}));assert.equal(access.status,200);assert.equal((await access.json()).actor.id,f.actor);
  assert.equal(f.rows.tblxC4SYtzYhJNcJS.length,0);
 }finally{f.db.close();}
});
test('duplicate PIN asks for phone; explicit PIN has leading zero and never uses phone suffix',async()=>{
 const f=await recognizeTenantsFixture();try{
  f.person.fields.member_pin='0123';f.insert('tbly1PM5iFYqVzKSm',{...f.person.fields,person_uid:'other_person',primary_phone_e164:'+16318752161'});
  assert.equal((await send(f,{kind:'pin',value:'0123'})).data.reason,'phone_required');assert.equal(f.rows.tblfkRSJAEMzuzApR.length,0);
  assert.equal((await send(f,{kind:'pin',value:'0123',phone:'6318752160'})).data.verified,true);
  assert.equal((await send(f,{kind:'pin',value:'2160'})).data.verified,false);
 }finally{f.db.close();}
});
test('seven-digit invitation uses existing hash expiry and one-use history; replay stays same operation',async()=>{
 const f=await recognizeTenantsFixture();try{
  Object.assign(f.person.fields,{input_invite_hash:await accessHash('0123456'),input_invite_expires_at:new Date(Date.now()+600000).toISOString()});
  const body={kind:'invite',value:'0123456',request_id:crypto.randomUUID()};const a=await send(f,body);assert.equal(a.data.verified,true,JSON.stringify(a));
  assert.equal(f.person.fields.input_invite_hash,'');assert.equal((await send(f,body)).data.device_token,a.data.device_token);
  assert.equal((await send(f,{kind:'invite',value:'0123456'})).data.reason,'invalid_invitation');assert.equal(f.rows.tblfkRSJAEMzuzApR.length,1);
 }finally{f.db.close();}
});
test('inactive, invalid, duplicate phones and bounded failed guesses never bind or send',async()=>{
 const f=await recognizeTenantsFixture();try{
  f.person.fields.status='Inactive';assert.equal((await send(f,{kind:'phone',value:'6318752160'})).data.verified,false);
  assert.equal((await send(f,{kind:'invite',value:'123'})).status,400);
  f.person.fields.status='Test';f.insert('tbly1PM5iFYqVzKSm',{...f.person.fields,person_uid:'duplicate'});assert.equal((await send(f,{kind:'phone',value:'6318752160'})).status,409);
  for(let i=0;i<4;i++)await send(f,{kind:'phone',value:'2025550199'});
  assert.equal((await send(f,{kind:'phone',value:'2025550199'})).data.reason,'retry_limit');assert.equal(f.rows.tblfkRSJAEMzuzApR.length,0);assert.equal(f.code,null);
 }finally{f.db.close();}
});
test('model cannot select sending, credential verification, data access or an unavailable action',()=>{
 const state={stage:'entry',allowedActions:['show_phone','show_pin','show_invite','show_sms'],busy:false,halted:false};
 for(const action of ['otp_start','submit_credential','fetch','grant_access'])assert.throws(()=>validateDecision({message:'Go',action},state));
 assert.equal(validateDecision({message:'Try',action:'retry'},state).action,'none');
 assert.equal(validateDecision({message:'Use invitation',action:'show_invite'},state).action,'show_invite');
 assert.equal(validateDecision({message:'Go',action:'show_sms'},{...state,busy:true}).action,'none');
 assert.deepEqual(safeState({...state,phone:'6318752160',pin:'0123'}),state);
 assert.doesNotMatch(redactMessage('0123 0123456 https://example.com/#invite=private 6318752160'),/0123|0123456|https:|6318752160/);
});
