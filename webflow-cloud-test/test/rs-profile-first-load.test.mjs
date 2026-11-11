import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const tick=()=>new Promise(r=>setImmediate(r));
const full={identity:{currentId:'a'},profiles:[{id:'a',first:'Actual',last:'Person',email:'a@example.test',revision:3,tags:[]}],teams:[],memberships:[],invitations:[],loaded:{profile:true,roster:false,team:false}};
async function fixture(identity={uid:'a',name:'Recognized Person'}){
 const values=new Map(),requests=[];
 globalThis.localStorage={getItem:k=>values.get(k)||null,setItem:(k,v)=>values.set(k,String(v)),removeItem:k=>values.delete(k)};
 globalThis.fetch=(url,options)=>new Promise(resolve=>requests.push({url:String(url),options,resolve}));
 let source=readFileSync(new URL('../src/assets/rs-profile/service.js',import.meta.url),'utf8').replace("import {recognitionAdapter} from '../rs-recognition/widget.js';",`const recognitionAdapter={current:async()=> 'a',profileDeviceFingerprint:()=> 'device-a',profileIdentity:()=> (${JSON.stringify(identity)})};`).replace("'../rs-recognition/native-client.js'",JSON.stringify(new URL('../src/assets/rs-recognition/native-client.js',import.meta.url).href));
 const service=await import('data:text/javascript;base64,'+Buffer.from(source+'\n//'+crypto.randomUUID()).toString('base64'));
 return {service,values,requests};
}
test('first load paints authenticated name before Airtable-backed Profile completes and then hydrates',async()=>{
 const old={fetch:globalThis.fetch,localStorage:globalThis.localStorage};
 try{const {service:s,requests,values}=await fixture();let shown;s.subscribeProfile(next=>shown=next);
 const ready=await s.loadProfile();assert.equal(ready.profiles[0].first,'Recognized Person');assert.equal(shown.loaded.profile,false);assert.equal(requests.length,1);assert.match(requests[0].url,/view=profile$/);assert.equal(ready.profiles[0].revision,undefined);
 assert.equal(JSON.parse(values.get('rs_profile_snapshot_v1:a')).state.loaded.profile,false);
 let editorReady=false;const editor=s.loadProfileView('profile',{priority:true}).then(()=>editorReady=true);await tick();assert.equal(editorReady,false);assert.equal(requests.length,1);
 requests[0].resolve(Response.json({ok:true,person_uid:'a',state:full}));await editor;assert.equal(s.profileState().profiles[0].email,'a@example.test');assert.equal(shown.loaded.profile,true);s.clearProfile();
 }finally{Object.assign(globalThis,old);}
});
test('returning device paints its full snapshot before silent recognition resolves',async()=>{
 const old={fetch:globalThis.fetch,localStorage:globalThis.localStorage};
 try{const {service:s,values}=await fixture();values.set('rs_recognition_device_token_v1','token');values.set('rs_recognition_identity_v1',JSON.stringify({personUid:'a',deviceToken:'token',expiresAt:Date.now()+60000}));values.set('rs_profile_snapshot_v1:a',JSON.stringify({uid:'a',deviceFingerprint:'device-a',state:full}));const work=s.loadProfile();assert.equal(s.profileState().profiles[0].first,'Actual');await work;assert.equal(s.profileState().loaded.profile,true);s.clearProfile();
 }finally{Object.assign(globalThis,old);}
});
test('missing or different authenticated identity cannot seed a Profile; logout fences delayed hydration',async()=>{
 const old={fetch:globalThis.fetch,localStorage:globalThis.localStorage};
 try{for(const identity of [null,{uid:'other',name:'Other'}]){const {service:s,requests}=await fixture(identity);const work=s.loadProfile();await tick();assert.equal(s.profileState(),null);s.clearProfile();requests[0].resolve(Response.json({ok:true,person_uid:'a',state:full}));await assert.rejects(work,/changed/);}
 const {service:s,requests}=await fixture();await s.loadProfile();s.clearProfile();requests[0].resolve(Response.json({ok:true,person_uid:'a',state:full}));await tick();assert.equal(s.profileState(),null);
 }finally{Object.assign(globalThis,old);}
});
test('Profile rejection removes the initial identity display and cached shell',async()=>{
 const old={fetch:globalThis.fetch,localStorage:globalThis.localStorage};
 try{const {service:s,requests,values}=await fixture();await s.loadProfile();requests[0].resolve(Response.json({ok:false,error:'profile_inactive'},{status:403}));await tick();assert.equal(s.profileState(),null);assert.equal(values.has('rs_profile_snapshot_v1:a'),false);s.clearProfile();}finally{Object.assign(globalThis,old);}
});

