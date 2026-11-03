import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const storage=()=>{const values=new Map();return {getItem:k=>values.get(k)||null,setItem:(k,v)=>values.set(k,String(v)),removeItem:k=>values.delete(k),values};};
const state=()=>({identity:{currentId:'person_a'},profiles:[{id:'person_a',first:'First',last:'User',revision:0}],memberships:[]});
const pendingKey='rs_profile_pending_v1:person_a';
async function settled(store){for(let i=0;i<1000&&store.getItem(pendingKey);i++)await new Promise(resolve=>setImmediate(resolve));assert.equal(store.getItem(pendingKey),null);}
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
  const saved=JSON.parse(store.getItem(pendingKey)),retained=Array.isArray(saved)?saved[0]:saved;assert.equal(retained.body.fields.pin,undefined);assert.equal(retained.needsPin,true);assert.match(retained.fingerprint,/^[a-f0-9]{64}$/);
  const second=await client(store,async(_url,options)=>{if(options?.method==='POST')sent.push(JSON.parse(options.body));return ok();});
  assert.equal((await second.loadProfile()).pendingNeedsPin,true);
  await assert.rejects(second.saveProfile('profile',{fields:{first:'First',last:'User',pin:'3456'},expectedRevision:0}),/pending save/);
  await second.saveProfile('profile',{fields:{first:'First',last:'User',pin:'9012'},expectedRevision:0});
  assert.equal(sent.length,2);assert.equal(sent[0].requestId,sent[1].requestId);assert.equal(sent[1].fields.pin,'9012');await settled(store);
 }finally{globalThis.fetch=oldFetch;globalThis.localStorage=oldStorage;}
});
test('pending membership action resumes the exact operation after reload',{concurrency:false},async()=>{
 const oldFetch=globalThis.fetch,oldStorage=globalThis.localStorage,store=storage(),sent=[];
 try{
  const first=await client(store,async(_url,options)=>{if(options?.method==='POST'){sent.push(JSON.parse(options.body));throw Error('lost reply');}return ok();});
  await first.loadProfile();await assert.rejects(first.saveProfile('member_status',{membershipId:'membership',active:false,expectedRevision:0}),/lost reply/);
  const second=await client(store,async(_url,options)=>{if(options?.method==='POST')sent.push(JSON.parse(options.body));return ok();});
  await second.loadProfile();assert.equal(sent.length,2);assert.deepEqual(sent[0],sent[1]);await settled(store);
 }finally{globalThis.fetch=oldFetch;globalThis.localStorage=oldStorage;}
});
test('logout fences a delayed Profile load and a save awaiting its fingerprint',{concurrency:false},async()=>{
 const oldFetch=globalThis.fetch,oldStorage=globalThis.localStorage,store=storage();let release;
 try{
  const first=await client(store,()=>new Promise(resolve=>{release=()=>resolve(ok());}));
  const loading=first.loadProfile();await new Promise(resolve=>setImmediate(resolve));first.clearProfile();release();await assert.rejects(loading,/changed while loading/);
  let posts=0;const second=await client(store,async(_url,options)=>{if(options?.method==='POST')posts++;return ok();});await second.loadProfile();
  const saving=second.saveProfile('profile',{fields:{email:'new@example.test'},expectedRevision:0});second.clearProfile();await assert.rejects(saving,/Profile changed (before|during) this save\./);assert.equal(posts,0);
 }finally{globalThis.fetch=oldFetch;globalThis.localStorage=oldStorage;}
});

for(const [operation,values] of [['team_create',{name:'Test Team'}],['invitation_create',{userId:'existing_user'}],['invitation_response',{invitationId:'invitation',accept:true,expectedRevision:0}]])test('pending '+operation+' preserves the same request across reload',{concurrency:false},async()=>{
 const oldFetch=globalThis.fetch,oldStorage=globalThis.localStorage,store=storage(),sent=[];
 try{const first=await client(store,async(_url,options)=>{if(options?.method==='POST'){sent.push(JSON.parse(options.body));throw Error('lost reply');}return ok();});await first.loadProfile();await assert.rejects(first.saveProfile(operation,values),/lost reply/);
 const next=await client(store,async(_url,options)=>{if(options?.method==='POST')sent.push(JSON.parse(options.body));return ok();});await next.loadProfile();assert.deepEqual(sent[1],sent[0]);await settled(store);
 }finally{globalThis.fetch=oldFetch;globalThis.localStorage=oldStorage;}
});

test('scoped loading preserves prepared roster and pending own edit',{concurrency:false},async()=>{const oldFetch=globalThis.fetch,oldStorage=globalThis.localStorage,store=storage(),urls=[];let releasePost;try{const own=()=>({...state(),teams:[{id:'team'}],memberships:[{id:'own',userId:'person_a',teamId:'team',active:true,role:'Owner'}],invitations:[],loaded:{profile:true,roster:false,team:false}});const roster=()=>({...own(),profiles:[...own().profiles,{id:'other',first:'Other'}],memberships:[...own().memberships,{id:'other-member',userId:'other',teamId:'team',active:true}],loaded:{profile:true,roster:true,team:false}});const c=await client(store,async(url,options)=>{urls.push(String(url));if(options?.method==='POST')return new Promise(resolve=>{releasePost=()=>resolve(Response.json({ok:true,state:own()}));});return Response.json({ok:true,person_uid:'person_a',state:String(url).includes('view=roster')?roster():own()});});await c.loadProfile();assert.ok(urls[0].endsWith('?view=profile'));await c.loadProfileView('roster');assert.equal(c.profileState().loaded.roster,true);const saving=c.saveProfile('profile',{fields:{first:'Pending'},expectedRevision:0});for(let wait=0;wait<1000&&!releasePost;wait++)await new Promise(resolve=>setImmediate(resolve));assert.equal(typeof releasePost,'function');await c.loadProfile();assert.equal(c.profileState().profiles.find(p=>p.id==='person_a').first,'Pending');assert.ok(c.profileState().profiles.some(p=>p.id==='other'));assert.equal(c.profileState().loaded.roster,true);releasePost();await saving;c.clearProfile();}finally{globalThis.fetch=oldFetch;globalThis.localStorage=oldStorage;}});

test('authoritative Team and roster refresh remove retired people and invitation candidates',{concurrency:false},async()=>{const oldFetch=globalThis.fetch,oldStorage=globalThis.localStorage,store=storage();let current;try{const own={...state(),teams:[],memberships:[],invitations:[],loaded:{profile:true,roster:false,team:false}};current={...own,profiles:[...own.profiles,{id:'retired_candidate'},{id:'former_member'}],memberships:[{id:'member',userId:'former_member',teamId:'t'}],loaded:{profile:true,roster:true,team:true}};const c=await client(store,async()=>Response.json({ok:true,person_uid:'person_a',state:current}));await c.loadProfile();current={...own,loaded:{profile:true,roster:true,team:true}};await c.loadProfileView('team');assert.deepEqual(c.profileState().profiles.map(p=>p.id),['person_a']);current={...own,profiles:[...own.profiles,{id:'candidate'}],loaded:{profile:true,roster:true,team:true}};await c.loadProfileView('team');assert.ok(c.profileState().profiles.some(p=>p.id==='candidate'));current={...own,loaded:{profile:true,roster:true,team:false}};await c.loadProfileView('roster');assert.deepEqual(c.profileState().profiles.map(p=>p.id),['person_a']);assert.equal(c.profileState().loaded.team,false);c.clearProfile();}finally{globalThis.fetch=oldFetch;globalThis.localStorage=oldStorage;}});
test('scope started before a save confirmation refreshes instead of remaining not loaded',{concurrency:false},async()=>{const oldFetch=globalThis.fetch,oldStorage=globalThis.localStorage,store=storage();let releaseScope,readyPost,scopeReads=0;try{const own={...state(),teams:[],memberships:[],invitations:[],loaded:{profile:true,roster:false,team:false}};const reply=value=>Response.json({ok:true,person_uid:'person_a',state:value});const c=await client(store,async(url,options)=>{if(options?.method==='POST'){readyPost=true;return reply({...own,profiles:[{...own.profiles[0],first:'Saved',revision:1}]});}if(String(url).includes('view=team')){scopeReads++;if(scopeReads===1)return new Promise(resolve=>{releaseScope=()=>resolve(reply({...own,loaded:{profile:true,roster:true,team:true}}));});return reply({...own,profiles:[{...own.profiles[0],first:'Saved',revision:1}],loaded:{profile:true,roster:true,team:true}});}return reply(own);});await c.loadProfile();const opening=c.loadProfileView('team');await c.saveProfile('profile',{fields:{first:'Saved'},expectedRevision:0});assert.equal(readyPost,true);releaseScope();await opening;assert.equal(scopeReads,2);assert.equal(c.profileState().loaded.team,true);assert.equal(c.profileState().profiles[0].first,'Saved');c.clearProfile();}finally{globalThis.fetch=oldFetch;globalThis.localStorage=oldStorage;}});

test('lazy Opt-ins finishes for the current actor after an account switch',async()=>{const source=readFileSync(new URL('../src/assets/rs-profile/client.js',import.meta.url),'utf8');assert.ok(!/^import .*optins\.js/m.test(source));assert.ok(!source.includes('\ufffd'));const fn=source.split('\n').find(line=>line.startsWith('function prepareOptins()')).replace("import('./optins.js')",'fakeImport()');const factory=new Function(`let optinsModule=null,optinsLoading=null,screen='optins',panelOpen=true,error='',renders=0,resolve,actor='old';const p=()=>actor,render=()=>renders++,profileMessage=e=>e.message,fakeImport=()=>new Promise(r=>resolve=r);${fn};return {prepareOptins,switchActor:()=>actor='new',release:()=>resolve({}),state:()=>({renders,actor,ready:!!optinsModule})}`);const c=factory();c.prepareOptins();c.switchActor();c.prepareOptins();c.release();await new Promise(resolve=>setImmediate(resolve));assert.deepEqual(c.state(),{renders:1,actor:'new',ready:true});});
