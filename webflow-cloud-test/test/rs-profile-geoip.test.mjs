import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {profileGeoip} from '../src/lib/rs-profile-geoip.js';

test('request coordinates avoid lookup; GeoJS fallback validates ranges, omits IP and tolerates failure',async()=>{
 const request=new Request('https://ringstatus.com/test/rs-profile/data?view=geoip',{headers:{'X-Forwarded-For':'203.0.113.1'}});
 Object.defineProperty(request,'cf',{value:{latitude:'0',longitude:'-80.5'}});
 const supplied=await profileGeoip(request,()=>{throw Error('unnecessary lookup');});assert.equal(supplied.latitude,0);assert.equal(supplied.source,'cloudflare');
 const fallback=new Request(request.url,{headers:request.headers});let calls=0;
 const result=await profileGeoip(fallback,async(url,options)=>{calls++;assert.ok(url.startsWith('https://get.geojs.io/'));assert.ok(options.signal);assert.notEqual(options.redirect,'error','Cloudflare Workers rejects error redirect mode');return Response.json({latitude:'26.65',longitude:'-80.29',ip:'203.0.113.1'});});
 assert.equal(calls,1);assert.equal(result.longitude,-80.29);assert.ok(Number.isFinite(Date.parse(result.observedAt)));assert.equal(result.ip,undefined);
 assert.equal(await profileGeoip(fallback,async()=>Response.json({latitude:300,longitude:0})),null);
 assert.equal(await profileGeoip(fallback,async()=>{throw Error('timeout');}),null);
 assert.equal(await profileGeoip(new Request(request.url),()=>{throw Error('no address');}),null);
});

test('slow GeoIP never holds Profile; late account responses cannot contaminate device data',{timeout:3000},async()=>{
 const prior={fetch:globalThis.fetch,localStorage:globalThis.localStorage};const saved=new Map(),geo=new Map();let uid='a',geoCalls=0;
 globalThis.localStorage={getItem:k=>saved.get(k)||null,setItem:(k,v)=>saved.set(k,String(v)),removeItem:k=>saved.delete(k)};globalThis.__geoActor=()=>uid;
 globalThis.fetch=async(url,options)=>{const actor=options.headers['X-RS-Actor-Scope'];if(String(url).includes('view=geoip')){geoCalls++;return new Promise(resolve=>geo.set(actor,()=>resolve(Response.json({ok:true,saved:true,person_uid:actor,geoip:{latitude:26,longitude:-80,source:'geoip',observedAt:new Date().toISOString()}}))));}return Response.json({ok:true,person_uid:actor,state:{identity:{currentId:actor},profiles:[{id:actor,first:actor}],memberships:[],loaded:{profile:true}}});};
 try{
  const source=readFileSync(new URL('../src/assets/rs-profile/service.js',import.meta.url),'utf8').replace("import {recognitionAdapter} from '../rs-recognition/widget.js';","const recognitionAdapter={current:async()=>globalThis.__geoActor(),profileDeviceFingerprint:()=>globalThis.__geoActor()+'-device'};").replace("'../rs-recognition/native-client.js'",JSON.stringify(new URL('../src/assets/rs-recognition/native-client.js',import.meta.url).href));
  const c=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));await c.loadProfile();assert.equal(c.profileState().identity.currentId,'a');assert.equal(c.profileState().geoip,undefined);await new Promise(r=>setImmediate(r));assert.equal(geoCalls,1);
  await c.loadProfile();assert.equal(geoCalls,1);c.clearProfile();uid='b';await c.loadProfile();assert.equal(geoCalls,1);geo.get('a')();await new Promise(r=>setImmediate(r));assert.equal(saved.has('rs_profile_geoip_v1:a'),false);
  geo.get('b')();await new Promise(r=>setImmediate(r));assert.equal(c.profileState().geoip.latitude,26);const value=JSON.parse(saved.get('rs_profile_geoip_v1:b'));assert.equal(value.deviceFingerprint,'b-device');assert.equal(value.uid,'b');c.clearProfile();assert.equal(c.profileState(),null);
 }finally{globalThis.fetch=prior.fetch;globalThis.localStorage=prior.localStorage;delete globalThis.__geoActor;}
});


test('GPS takes precedence and invalid GPS uses the existing fallback',async()=>{
 const request=new Request('https://ringstatus.com/test/rs-profile/data?view=geoip');
 Object.defineProperty(request,'cf',{value:{latitude:20,longitude:30}});
 const noLookup=()=>{throw Error('unexpected lookup');};
 const gps=await profileGeoip(request,noLookup,{latitude:0,longitude:-80});
 assert.equal(gps.source,'gps');assert.equal(gps.latitude,0);
 for(const bad of [{latitude:91,longitude:0},{latitude:null,longitude:0},{latitude:'20',longitude:30}]){
  const result=await profileGeoip(request,noLookup,bad);assert.equal(result.source,'cloudflare');
 }
});

test('GPS goes through Astro; opt-out clears precision and defeats late GPS responses',async()=>{
 const prior={fetch:globalThis.fetch,localStorage:globalThis.localStorage};const saved=new Map();let resolveGps;
 globalThis.localStorage={getItem:k=>saved.get(k)||null,setItem:(k,v)=>saved.set(k,String(v)),removeItem:k=>saved.delete(k)};
 globalThis.fetch=async(url,options)=>{
  if(String(url).includes('view=geoip')){
   assert.equal(options.headers['X-RS-Actor-Scope'],'gps-user');
   if(JSON.parse(options.body).latitude!==undefined){
    assert.equal(JSON.parse(options.body).latitude,10);assert.ok(JSON.parse(options.body).requestId);
    return new Promise(resolve=>{resolveGps=()=>resolve(Response.json({saved:true,person_uid:'gps-user',geoip:{latitude:10,longitude:20,source:'gps',observedAt:new Date().toISOString()}}));});
   }
   return Response.json({saved:true,person_uid:'gps-user',geoip:{latitude:1,longitude:2,source:'geoip',observedAt:new Date().toISOString()}});
  }
  return Response.json({ok:true,person_uid:'gps-user',state:{identity:{currentId:'gps-user'},profiles:[{id:'gps-user'}],memberships:[],loaded:{profile:true}}});
 };
 try{
  const source=readFileSync(new URL('../src/assets/rs-profile/service.js',import.meta.url),'utf8').replace("import {recognitionAdapter} from '../rs-recognition/widget.js';","const recognitionAdapter={current:async()=>'gps-user',profileDeviceFingerprint:()=> 'gps-device'};").replace("'../rs-recognition/native-client.js'",JSON.stringify(new URL('../src/assets/rs-recognition/native-client.js',import.meta.url).href));
  const c=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));await c.loadProfile();await new Promise(r=>setImmediate(r));
  const gps=c.updateProfileLocation({latitude:10,longitude:20});await new Promise(r=>setImmediate(r));resolveGps();await gps;assert.equal(c.profileState().geoip.source,'gps');
  const stale=c.updateProfileLocation({latitude:10,longitude:20});await new Promise(r=>setImmediate(r));const fallback=c.updateProfileLocation(null);assert.equal(saved.has('rs_profile_geoip_v1:gps-user'),false);resolveGps();await stale;await fallback;
  assert.equal(c.profileState().geoip.source,'geoip');assert.equal(JSON.parse(saved.get('rs_profile_geoip_v1:gps-user')).deviceFingerprint,'gps-device');
  c.clearProfile();
 }finally{globalThis.fetch=prior.fetch;globalThis.localStorage=prior.localStorage;}
});


test('failed location logs survive reload and retain their request ID until confirmed',async()=>{
 const old={fetch:globalThis.fetch,localStorage:globalThis.localStorage};const saved=new Map();let fail=true;const sent=[];
 globalThis.localStorage={getItem:k=>saved.get(k)||null,setItem:(k,v)=>saved.set(k,String(v)),removeItem:k=>saved.delete(k)};
 globalThis.fetch=async(url,options)=>{
  if(String(url).includes('view=geoip')){const body=JSON.parse(options.body);sent.push(body.requestId);if(fail)throw Error('offline');return Response.json({person_uid:'pending-user',saved:true,geoip:{latitude:1,longitude:2,source:'geoip',observedAt:new Date().toISOString()}});}
  return Response.json({ok:true,person_uid:'pending-user',state:{identity:{currentId:'pending-user'},profiles:[{id:'pending-user'}],memberships:[],loaded:{profile:true}}});
 };
 const source=readFileSync(new URL('../src/assets/rs-profile/service.js',import.meta.url),'utf8').replace("import {recognitionAdapter} from '../rs-recognition/widget.js';","const recognitionAdapter={current:async()=>'pending-user',profileDeviceFingerprint:()=> 'same-device'};").replace("'../rs-recognition/native-client.js'",JSON.stringify(new URL('../src/assets/rs-recognition/native-client.js',import.meta.url).href));
 try{
  const first=await import('data:text/javascript;base64,'+Buffer.from(source+'\n// first').toString('base64'));await first.loadProfile();await new Promise(r=>setImmediate(r));
  const pending=JSON.parse(saved.get('rs_profile_location_pending_v1:pending-user'));assert.equal(pending.length,1);assert.equal(pending[0].body.requestId,sent[0]);first.clearProfile();fail=false;
  const next=await import('data:text/javascript;base64,'+Buffer.from(source+'\n// reload').toString('base64'));await next.loadProfile();await new Promise(r=>setImmediate(r));
  assert.equal(sent[1],sent[0]);assert.deepEqual(JSON.parse(saved.get('rs_profile_location_pending_v1:pending-user')),[]);next.clearProfile();
 }finally{globalThis.fetch=old.fetch;globalThis.localStorage=old.localStorage;}
});
