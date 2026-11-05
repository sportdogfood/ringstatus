import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const snapshot=uid=>({uid,deviceFingerprint:'retired-device',state:{identity:{currentId:uid},profiles:[{id:uid,first:'Cached',revision:1}],memberships:[]}});
test('confirmed re-login reuses only that person cache before refresh, rebinds identity, and clears on access rejection',async()=>{
 const original={fetch:globalThis.fetch,localStorage:globalThis.localStorage};
 const values=new Map(),store={getItem:k=>values.get(k)||null,setItem:(k,v)=>values.set(k,String(v)),removeItem:k=>values.delete(k)};
 values.set('rs_profile_snapshot_v1:person_a',JSON.stringify(snapshot('person_a')));
 values.set('rs_recognition_device_token_v1','new-token');
 let release,calls=0;
 globalThis.localStorage=store;globalThis.fetch=()=>{calls++;return new Promise(r=>release=r);};
 let source=readFileSync(new URL('../src/assets/rs-profile/service.js',import.meta.url),'utf8').replace("import {recognitionAdapter} from '../rs-recognition/widget.js';","const recognitionAdapter={current:async()=> 'person_a',profileDeviceFingerprint:()=> 'new-device'};").replace("'../rs-recognition/native-client.js'",JSON.stringify(new URL('../src/assets/rs-recognition/native-client.js',import.meta.url).href));
 try{
  const c=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
  let shown=null;c.subscribeProfile(next=>shown=next);
  const result=await c.loadProfile('person_a');
  assert.equal(result.profiles[0].first,'Cached');assert.equal(shown.profiles[0].first,'Cached');assert.equal(calls,1);
  assert.equal(JSON.parse(store.getItem('rs_profile_snapshot_v1:person_a')).deviceFingerprint,'new-device');
  assert.equal(JSON.parse(store.getItem('rs_recognition_identity_v1')).deviceToken,'new-token');
  release(Response.json({ok:false,error:'access_denied'},{status:403}));
  await new Promise(r=>setImmediate(r));assert.equal(c.profileState(),null);assert.equal(store.getItem('rs_profile_snapshot_v1:person_a'),null);
  c.clearProfile();values.set('rs_profile_snapshot_v1:person_a',JSON.stringify(snapshot('person_a')));
  const other=c.loadProfile('person_b');assert.equal(c.profileState(),null);
  c.clearProfile();release(Response.json({ok:true,person_uid:'person_b',state:snapshot('person_b').state}));await assert.rejects(other,/changed while loading/);assert.equal(c.profileState(),null);
 }finally{globalThis.fetch=original.fetch;globalThis.localStorage=original.localStorage;}
});
