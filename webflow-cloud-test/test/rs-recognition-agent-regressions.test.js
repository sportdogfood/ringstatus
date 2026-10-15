import test from 'node:test';
import assert from 'node:assert/strict';
import {recognizeTenantsFixture} from '../test-support/recognize-tenants-fixture.mjs';
import {createInputAccess,accessHash} from '../src/lib/rs-inputs-access.js';
import {runSilentRecognition} from '../src/assets/rs-recognition/native-client.js';
const origin='https://ringstatus.com';
const req=(action,payload,cookie)=>new Request(origin+'/test/rs-inputs/native-recognition?operation=action',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json',...(cookie?{Cookie:cookie}:{})},body:JSON.stringify({action,...payload})});
test('new credential clears old Inputs cookie; retirement retry confirms the same retired device',async()=>{
 const f=await recognizeTenantsFixture();try{
  const r=await f.dispatch(req('credential',{kind:'phone',value:'6318752160',request_id:crypto.randomUUID()}));const data=await r.json();
  assert.equal(data.verified,true);assert.ok(r.headers.getSetCookie().some(c=>c.startsWith('__Secure-rs_input_access=;')&&c.includes('Max-Age=0')));
  const cookie='__Host-rs_recognition_device='+data.device_token;const body={session_event_uid:crypto.randomUUID(),device_token:data.device_token};
  const retired=await f.dispatch(req('retire_device',body,cookie));assert.equal(retired.status,200,await retired.clone().text());
  const replay=await f.dispatch(req('retire_device',body,cookie));assert.equal(replay.status,200,await replay.clone().text());assert.equal((await replay.json()).retired,true);
  assert.equal(f.rows.tblfkRSJAEMzuzApR.length,1);assert.equal(f.rows.tblfkRSJAEMzuzApR[0].fields.status,'Retired');
 }finally{f.db.close();}
});
test('blocked local/session storage still sends a valid-cookie recognition request',async()=>{
 const storage={getItem(){throw new Error('disabled');},setItem(){throw new Error('disabled');},removeItem(){throw new Error('disabled');}};let called=false;
 const result=await runSilentRecognition({endpoint:origin+'/test/rs-inputs/native-recognition',persistentStorage:storage,sessionStorageImpl:storage,redirectOnRecognized:false,fetchImpl:async(url,options)=>{called=true;assert.equal(options.credentials,'same-origin');return Response.json({ok:true,recognized:true});}});
 assert.equal(result,true);assert.equal(called,true);
});

