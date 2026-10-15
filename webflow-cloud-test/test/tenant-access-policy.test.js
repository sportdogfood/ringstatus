import test from 'node:test';
import assert from 'node:assert/strict';
import {profileFixture} from '../test-support/tenant-profile-fixture.mjs';
import {TABLE} from '../src/lib/rs-tenant-barn.js';
import {createInputAccess,accessHash} from '../src/lib/rs-inputs-access.js';
const origin='https://ringstatus.com';

test('enabled role-less User has own-profile access but no tenant management',{timeout:30000},async()=>{
 const f=await profileFixture();try{
  f.membership.fields.Roles=[];
  const identity=await f.recognize('phone','6318752160');assert.equal(identity.verified,true);
  const state=await f.client(identity)('state');assert.equal(state.status,200);assert.equal(state.state.canManage,false);assert.equal(state.state.users.length,1);
 }finally{f.db.close()}
});

test('native recognition rejects SMS and OTP operations without calling delivery',{timeout:30000},async()=>{
 const f=await profileFixture();try{for(const action of ['otp_start','otp_check','recovery','phone_login']){
  const response=await f.dispatch(new Request(origin+'/test/rs-inputs/native-recognition?operation=action',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify({action})}));
  assert.equal(response.status,400);assert.equal((await response.json()).error,'unsupported_operation');
 }}finally{f.db.close()}
});

test('inactive Barn denies fresh credentials and an existing device without changing records',{timeout:30000},async()=>{
 const f=await profileFixture();try{
  f.tenant.fields.tenant_active=false;
  assert.equal((await f.recognize('phone','6318752160')).verified,false);
  assert.equal((await f.request('state')).status,401);
 }finally{f.db.close()}
});

test('prepopulated inactive User becomes editable; demotion closes saved-result replay and invite-cookie access',{timeout:90000},async()=>{
 const f=await profileFixture();try{
  const person=f.insert(TABLE.people,{entity_uid:'prepopulated-user',name:'Prepopulated User',phone_e164:'+12025550103',status:'Active'});
  const membership=f.insert(TABLE.memberships,{Person:[person.id],Tenant:[f.tenant.id],Roles:[f.userRole.id],Status:'Disabled'});
  const initial=await f.request('state'),row=initial.state.users.find(r=>r.id==='prepopulated-user');assert.equal(row.membershipStatus,'Disabled');
  f.insert(TABLE.optins,{entity_uid:'existing-prepopulated-optin',Membership:[membership.id],status:'Revoked',consent_state:'Revoked'});
  const preferences=await f.request(`user-optins?barn_id=${f.barnId}&user_id=prepopulated-user`);assert.equal(preferences.status,200,JSON.stringify(preferences));assert.equal(preferences.subscriptions[0].id,'existing-prepopulated-optin');
  const body={kind:'users',barnId:f.barnId,draft:{...row,membershipStatus:'Enabled',roles:f.staffRole.id,pin:'0007'},expectedRevision:0,requestId:'prepopulated-enable'};
  const saved=await f.request('record',body);assert.equal(saved.status,200,JSON.stringify(saved));assert.equal(f.rows[TABLE.people].filter(p=>p.fields.entity_uid===person.fields.entity_uid).length,1);assert.equal(membership.fields.Status,'Enabled');
  const pin=await f.recognize('pin','0007');assert.equal(pin.verified,true);const ownState=await f.client(pin,saved.record.recognitionPersonId)('state');assert.deepEqual(ownState.state.users.map(r=>r.id),['prepopulated-user']);assert.equal(ownState.state.canManage,false);
  // Existing signed invitation access is rechecked against the same memberships.
  const identity=f.rows.tbly1PM5iFYqVzKSm.find(p=>p.fields.person_uid===f.actor);identity.fields.input_invite_hash=await accessHash('1112345');identity.fields.input_invite_expires_at=new Date(Date.now()+600000).toISOString();
  const accessResponse=await f.dispatch(new Request(origin+'/test/rs-inputs/access',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify({token:'1112345'})}));assert.equal(accessResponse.status,200);const cookie=accessResponse.headers.get('set-cookie').split(';')[0];
  // A formerly permitted saved request cannot disclose another User after demotion.
  f.membership.fields.Roles=[f.userRole.id];const replay=await f.request('record',body);assert.equal(replay.status,403,JSON.stringify(replay));assert.equal(replay.record,undefined);
  f.membership.fields.Status='Disabled';const denied=await f.dispatch(new Request(origin+'/test/rs-inputs/access',{headers:{Cookie:cookie}}));assert.equal(denied.status,401);
  assert.equal(f.rows[TABLE.submissions].length,0);
 }finally{f.db.close()}
});

test('successful credential replay cannot resurrect a deactivated User',{timeout:30000},async()=>{
 const f=await profileFixture();try{
  const value={action:'credential',kind:'phone',value:'6318752160',request_id:crypto.randomUUID()};
  const send=()=>f.dispatch(new Request(origin+'/test/rs-inputs/native-recognition?operation=action',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify(value)}));
  assert.equal((await (await send()).json()).verified,true);
  f.membership.fields.Status='Disabled';assert.equal((await (await send()).json()).verified,false);
 }finally{f.db.close()}
});
