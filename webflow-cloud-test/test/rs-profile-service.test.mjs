import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const storage=()=>{const values=new Map();return {getItem:k=>values.get(k)||null,setItem:(k,v)=>values.set(k,String(v)),removeItem:k=>values.delete(k),values};};
const state=()=>({identity:{currentId:'person_a'},profiles:[{id:'person_a',first:'First',last:'User',revision:0}],memberships:[]});
const ok=()=>Response.json({ok:true,person_uid:'person_a',state:state()});
async function client(store,fetchImpl){
 globalThis.localStorage=store;globalThis.fetch=fetchImpl;
 let source=readFileSync(new URL('../src/assets/rs-profile/service.js',import.meta.url),'utf8');
 source=source.replace("import {recognitionAdapter} from '../rs-recognition/widget.js';","const recognitionAdapter={current:async()=> 'person_a'};").replace("'../rs-recognition/native-client.js'",JSON.stringify(new URL('../src/assets/rs-recognition/native-client.js',import.meta.url).href));
 return import('data:text/javascript;base64,'+Buffer.from(source+'\n// '+crypto.randomUUID()).toString('base64'));
}
test('PIN reload retry preserves request ID, excludes plaintext PIN and survives a wrong re-entry',{concurrency:false},async()=>{
 const oldFetch=globalThis.fetch,oldStorage=globalThis.localStorage,store=storage(),sent=[];
 try{
  const first=await client(store,async(_url,options)=>{if(options?.method==='POST'){sent.push(JSON.parse(options.body));throw Error('lost reply');}return ok();});
  await first.loadProfile();await assert.rejects(first.saveProfile('profile',{fields:{first:'First',last:'User',pin:'9012'},expectedRevision:0}),/lost reply/);
  const retained=JSON.parse([...store.values.values()][0]);assert.equal(retained.body.fields.pin,undefined);assert.equal(retained.needsPin,true);assert.match(retained.fingerprint,/^[a-f0-9]{64}$/);
  const second=await client(store,async(_url,options)=>{if(options?.method==='POST')sent.push(JSON.parse(options.body));return ok();});
  assert.equal((await second.loadProfile()).pendingNeedsPin,true);
  await assert.rejects(second.saveProfile('profile',{fields:{first:'First',last:'User',pin:'3456'},expectedRevision:0}),/pending save/);
  await second.saveProfile('profile',{fields:{first:'First',last:'User',pin:'9012'},expectedRevision:0});
  assert.equal(sent.length,2);assert.equal(sent[0].requestId,sent[1].requestId);assert.equal(sent[1].fields.pin,'9012');assert.equal(store.values.size,0);
 }finally{globalThis.fetch=oldFetch;globalThis.localStorage=oldStorage;}
});
test('pending membership action resumes the exact operation after reload',{concurrency:false},async()=>{
 const oldFetch=globalThis.fetch,oldStorage=globalThis.localStorage,store=storage(),sent=[];
 try{
  const first=await client(store,async(_url,options)=>{if(options?.method==='POST'){sent.push(JSON.parse(options.body));throw Error('lost reply');}return ok();});
  await first.loadProfile();await assert.rejects(first.saveProfile('member_status',{membershipId:'membership',active:false,expectedRevision:0}),/lost reply/);
  const second=await client(store,async(_url,options)=>{if(options?.method==='POST')sent.push(JSON.parse(options.body));return ok();});
  await second.loadProfile();assert.equal(sent.length,2);assert.deepEqual(sent[0],sent[1]);assert.equal(store.values.size,0);
 }finally{globalThis.fetch=oldFetch;globalThis.localStorage=oldStorage;}
});
test('logout fences a delayed Profile load and a save awaiting its fingerprint',{concurrency:false},async()=>{
 const oldFetch=globalThis.fetch,oldStorage=globalThis.localStorage,store=storage();let release;
 try{
  const first=await client(store,()=>new Promise(resolve=>{release=()=>resolve(ok());}));
  const loading=first.loadProfile();await new Promise(resolve=>setImmediate(resolve));first.clearProfile();release();await assert.rejects(loading,/changed while loading/);
  let posts=0;const second=await client(store,async(_url,options)=>{if(options?.method==='POST')posts++;return ok();});await second.loadProfile();
  const saving=second.saveProfile('profile',{fields:{email:'new@example.test'},expectedRevision:0});second.clearProfile();await assert.rejects(saving,/changed before/);assert.equal(posts,0);
 }finally{globalThis.fetch=oldFetch;globalThis.localStorage=oldStorage;}
});

for(const [operation,values] of [['team_create',{name:'Test Team'}],['invitation_create',{userId:'existing_user'}],['invitation_response',{invitationId:'invitation',accept:true,expectedRevision:0}]])test('pending '+operation+' preserves the same request across reload',{concurrency:false},async()=>{
 const oldFetch=globalThis.fetch,oldStorage=globalThis.localStorage,store=storage(),sent=[];
 try{const first=await client(store,async(_url,options)=>{if(options?.method==='POST'){sent.push(JSON.parse(options.body));throw Error('lost reply');}return ok();});await first.loadProfile();await assert.rejects(first.saveProfile(operation,values),/lost reply/);
 const next=await client(store,async(_url,options)=>{if(options?.method==='POST')sent.push(JSON.parse(options.body));return ok();});await next.loadProfile();assert.deepEqual(sent[1],sent[0]);assert.equal(store.values.size,0);
 }finally{globalThis.fetch=oldFetch;globalThis.localStorage=oldStorage;}
});
