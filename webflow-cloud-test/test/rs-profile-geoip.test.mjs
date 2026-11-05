import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {profileGeoip} from '../src/lib/rs-profile-geoip.js';

test('request coordinates avoid lookup; GeoJS fallback validates ranges, omits IP and tolerates failure',async()=>{
 const request=new Request('https://ringstatus.com/test/rs-profile/data?view=geoip',{headers:{'X-Forwarded-For':'203.0.113.1'}});
 Object.defineProperty(request,'cf',{value:{latitude:'0',longitude:'-80.5'}});
 const supplied=await profileGeoip(request,()=>{throw Error('unnecessary lookup');});assert.equal(supplied.latitude,0);assert.equal(supplied.source,'cloudflare');
 const fallback=new Request(request.url,{headers:request.headers});let calls=0;
 const result=await profileGeoip(fallback,async(url,options)=>{calls++;assert.ok(url.startsWith('https://get.geojs.io/'));assert.ok(options.signal);return Response.json({latitude:'26.65',longitude:'-80.29',ip:'203.0.113.1'});});
 assert.equal(calls,1);assert.equal(result.longitude,-80.29);assert.ok(Number.isFinite(Date.parse(result.observedAt)));assert.equal(result.ip,undefined);
 assert.equal(await profileGeoip(fallback,async()=>Response.json({latitude:300,longitude:0})),null);
 assert.equal(await profileGeoip(fallback,async()=>{throw Error('timeout');}),null);
 assert.equal(await profileGeoip(new Request(request.url),()=>{throw Error('no address');}),null);
});

test('slow GeoIP never holds Profile; late account responses cannot contaminate device data',{timeout:3000},async()=>{
 const prior={fetch:globalThis.fetch,localStorage:globalThis.localStorage};const saved=new Map(),geo=new Map();let uid='a',geoCalls=0;
 globalThis.localStorage={getItem:k=>saved.get(k)||null,setItem:(k,v)=>saved.set(k,String(v)),removeItem:k=>saved.delete(k)};globalThis.__geoActor=()=>uid;
 globalThis.fetch=async(url,options)=>{const actor=options.headers['X-RS-Actor-Scope'];if(String(url).includes('view=geoip')){geoCalls++;return new Promise(resolve=>geo.set(actor,()=>resolve(Response.json({ok:true,person_uid:actor,geoip:{latitude:26,longitude:-80,source:'geoip',observedAt:new Date().toISOString()}}))));}return Response.json({ok:true,person_uid:actor,state:{identity:{currentId:actor},profiles:[{id:actor,first:actor}],memberships:[],loaded:{profile:true}}});};
 try{
  const source=readFileSync(new URL('../src/assets/rs-profile/service.js',import.meta.url),'utf8').replace("import {recognitionAdapter} from '../rs-recognition/widget.js';","const recognitionAdapter={current:async()=>globalThis.__geoActor(),profileDeviceFingerprint:()=>globalThis.__geoActor()+'-device'};").replace("'../rs-recognition/native-client.js'",JSON.stringify(new URL('../src/assets/rs-recognition/native-client.js',import.meta.url).href));
  const c=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));await c.loadProfile();assert.equal(c.profileState().identity.currentId,'a');assert.equal(c.profileState().geoip,undefined);assert.equal(geoCalls,1);
  await c.loadProfile();assert.equal(geoCalls,1);c.clearProfile();uid='b';await c.loadProfile();assert.equal(geoCalls,2);geo.get('a')();await new Promise(r=>setImmediate(r));assert.equal(saved.has('rs_profile_geoip_v1:a'),false);
  geo.get('b')();await new Promise(r=>setImmediate(r));assert.equal(c.profileState().geoip.latitude,26);const value=JSON.parse(saved.get('rs_profile_geoip_v1:b'));assert.equal(value.deviceFingerprint,'b-device');assert.equal(value.uid,'b');c.clearProfile();assert.equal(c.profileState(),null);
 }finally{globalThis.fetch=prior.fetch;globalThis.localStorage=prior.localStorage;delete globalThis.__geoActor;}
});
