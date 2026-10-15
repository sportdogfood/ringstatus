import test from 'node:test';
import assert from 'node:assert/strict';
import {profileFixture} from '../test-support/tenant-profile-fixture.mjs';
import {TABLE,SOURCE} from '../src/lib/rs-tenant-barn.js';

test('one owner in two Barns: assigned state, two Users, edit/reload and same-Barn ambiguity protection',async()=>{
 const f=await profileFixture();try{
  const otherId='rs_'+'b'.repeat(32),other=f.insert(TABLE.barn,{tenant_official_name:'Second Barn',tenant_active:true});
  const second=f.insert(TABLE.memberships,{Person:[f.person.id],Tenant:[other.id],Roles:[f.ownerRole.id],Status:'Enabled'});
  const originalMemberships=structuredClone(f.rows[TABLE.memberships]);
  f.insert(TABLE.sources,{mapping_uid:`${SOURCE}|barn|${otherId}`,entity_uid:otherId,source_system:SOURCE,source_entity_type:'barn',source_scope:otherId,external_id:other.id});
  f.insert(TABLE.events,{event_uid:'second-barn',entity_uid:otherId,kind:'barn',outcome:'committed',result_json:JSON.stringify({id:otherId,barnId:otherId,name:'Second Barn',revision:1,ownerUid:f.actor})});
  const initial=await f.request('state');assert.equal(initial.status,200,JSON.stringify(initial));assert.equal(initial.state.selectedBarn,f.barnId);assert.equal(initial.state.users.filter(r=>r.id===f.person.fields.entity_uid).length,1);assert.ok(initial.state.users.every(r=>r.barnId===f.barnId));
  const save=(draft,requestId)=>f.request('record',{kind:'users',barnId:f.barnId,draft,expectedRevision:draft.revision,requestId});
  const one=await save({name:'Release User One'},'multi-barn-user-one');assert.equal(one.status,200,JSON.stringify(one));
  const two=await save({name:'Release User Two'},'multi-barn-user-two');assert.equal(two.status,200,JSON.stringify(two));
  const back=await f.request('state');assert.equal(back.status,200);assert.ok(back.state.users.some(r=>r.id===two.record.id));
  const edited=await save({...one.record,name:'Release User One edited'},'multi-barn-user-edit');assert.equal(edited.status,200,JSON.stringify(edited));
  const reload=await f.request('state');assert.equal(reload.state.users.find(r=>r.id===one.record.id).name,'Release User One edited');
  assert.deepEqual(f.rows[TABLE.memberships].filter(m=>originalMemberships.some(o=>o.id===m.id)),originalMemberships);
  // The same owner can be read in the other Barn without changing identity.
  const alternate=await f.request(`state?barn_id=${otherId}`);assert.equal(alternate.status,200,JSON.stringify(alternate));assert.equal(alternate.state.selectedBarn,otherId);assert.deepEqual(alternate.state.users.map(r=>[r.id,r.barnId]),[[f.person.fields.entity_uid,otherId]]);
  f.insert(TABLE.memberships,structuredClone(f.membership.fields));
  const duplicate=await f.request('state');assert.equal(duplicate.status,409);assert.equal(duplicate.error,'ambiguous_tenant_membership');
 }finally{f.db.close()}
});
