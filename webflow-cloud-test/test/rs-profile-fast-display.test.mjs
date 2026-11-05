import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
test('returning device displays its Profile before recognition completes and clears rejected access',async()=>{
 const prior={fetch:globalThis.fetch,localStorage:globalThis.localStorage};const data=new Map();let release;
 const snapshot={identity:{currentId:'person'},profiles:[{id:'person',first:'Saved'}],memberships:[]};
 globalThis.localStorage={getItem:k=>data.get(k)||null,setItem:(k,v)=>data.set(k,String(v)),removeItem:k=>data.delete(k)};
 globalThis.__profileFastIdentity=new Promise(r=>release=r);
 globalThis.fetch=async()=>{throw Error('No network request before identity');};
 data.set('rs_recognition_device_token_v1','fixture-token');data.set('rs_recognition_identity_v1',JSON.stringify({personUid:'person',deviceToken:'fixture-token',expiresAt:Date.now()+60000}));data.set('rs_profile_snapshot_v1:person',JSON.stringify({uid:'person',deviceFingerprint:'fixture-fingerprint',state:snapshot}));
 try{
 let source=readFileSync(new URL('../src/assets/rs-profile/service.js',import.meta.url),'utf8').replace("import {recognitionAdapter} from '../rs-recognition/widget.js';","const recognitionAdapter={current:()=>globalThis.__profileFastIdentity,profileDeviceFingerprint:()=>null};").replace("'../rs-recognition/native-client.js'",JSON.stringify(new URL('../src/assets/rs-recognition/native-client.js',import.meta.url).href));
 const service=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));let shown;service.subscribeProfile(value=>shown=value);const loading=service.loadProfile();assert.equal(shown.profiles[0].first,'Saved');release(null);await loading;assert.equal(service.profileState(),null);
 data.set('rs_recognition_pending_signout_v1','pending');const again=service.loadProfile();assert.equal(service.profileState(),null);await again;
 }finally{delete globalThis.__profileFastIdentity;globalThis.fetch=prior.fetch;globalThis.localStorage=prior.localStorage;}
});
