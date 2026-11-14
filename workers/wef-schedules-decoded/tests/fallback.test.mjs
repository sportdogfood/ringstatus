import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const shared=fs.readFileSync(new URL('../shared-worker.mjs',import.meta.url),'utf8').replace(/^import .*;\r?\n/gm,'').split('export class SglPacer')[0];
const fallbackSource=fs.readFileSync(new URL('../fallback.mjs',import.meta.url),'utf8').replace(/^import .*;\r?\n/gm,'');
const actualDecoder=new Function(fs.readFileSync(new URL('../worker.js',import.meta.url),'utf8').replace('export default','return'))();
const route='/schedule?date=2026-11-06&show_id=200000077&customer_id=15';
const classRoute='/classes/200031233?show_id=200000077&customer_id=15&cgid=-1';
const env={RUNNER_ORIGIN:'https://runner.example',RUNNER_KEY:'offline-verification-key',SGL_COOKIE:'offline-cookie'};
const decoded=p=>Response.json(p,{headers:{'X-RS-Decoded':'1'}});
const request=(path=route,key)=>new Request('https://worker.example'+path,{headers:key?{'X-RS-Fallback-Test':key}:{}});
function load(primary,fallback,classify=()=>null,renew=async()=>{},sleep=fn=>{fn();return 0;}){return new Function('fetch','fetchFallback','publicRoute','scheduleHealth','renewFallbackToken','setTimeout','decoder',shared.replace('export const sourceHandler =','return'))(primary,fallback,classify,async()=>{throw Error('Health requires its separate integration fixture');},renew,sleep,actualDecoder);}

test('healthy local response is returned without invoking fallback',async()=>{
 let primary=0,backup=0;const p={show:{show_id:200000077},rings:[]};
 const app=load(async(url,options)=>{primary++;assert.equal(String(url),'https://runner.example'+route);assert.deepEqual(options.headers,{Authorization:'Bearer '+env.RUNNER_KEY});return decoded(p);},async()=>{backup++;throw Error('Must not run');});
 const r=await app.fetch(request(),env);assert.equal(r.status,200);assert.equal(r.headers.get('x-rs-source-lane'),'runner');assert.deepEqual(await r.json(),p);assert.equal(primary,1);assert.equal(backup,0);
});
test('HTTP errors, exception, undecoded, malformed and empty primary invoke fallback once',async()=>{
 const cases=[()=>new Response('upstream',{status:401}),()=>new Response('upstream',{status:403}),()=>new Response('upstream',{status:429}),()=>new Response('upstream',{status:502}),()=>{throw Error('connection');},()=>Response.json({show:{show_id:1}}),()=>decoded({}),()=>new Response('bad-json',{headers:{'x-rs-decoded':'1'}}),()=>decoded(null),()=>decoded([])];
 for(const failure of cases){let primary=0,backup=0;const app=load(async()=>{primary++;return failure();},async()=>{backup++;return decoded({rings:[{ring_id:25}]});});const r=await app.fetch(request(),env);assert.equal(r.status,200);assert.equal(r.headers.get('x-rs-source-lane'),'cloudflare-fallback');assert.equal((await r.json()).rings.length,1);assert.equal(primary,1);assert.equal(backup,1);}
});
test('secret-authorized verification bypasses local for both routes; invalid key does not',async()=>{
 for(const path of [route,classRoute]){let primary=0,backup=0;const app=load(async()=>{primary++;return decoded({ok:true});},async(req)=>{backup++;assert.equal(new URL(req.url).pathname,new URL('https://w'+path).pathname);return decoded({ok:true});});
  assert.equal((await app.fetch(request(path,env.RUNNER_KEY),env)).headers.get('x-rs-source-lane'),'cloudflare-fallback');assert.equal(primary,0);assert.equal(backup,1);
  assert.equal((await app.fetch(request(path,'wrong'),env)).headers.get('x-rs-source-lane'),'runner');assert.equal(primary,1);assert.equal(backup,1);
 }
});
test('fallback failure remains an error and invalid routes do not enter fallback',async()=>{
 let backup=0;const app=load(async()=>new Response('unavailable',{status:502}),async()=>{backup++;return Response.json({message:'failed'},{status:502});});
 const r=await app.fetch(request(),env);assert.equal(r.status,502);assert.equal(r.headers.has('x-rs-decoded'),false);assert.equal(backup,10);
 assert.equal((await app.fetch(request('/people/1?pid=1&show_id=1&customer_id=15',env.RUNNER_KEY),env)).status,502);assert.equal(backup,11);
 assert.equal((await app.fetch(request(route+'&extra=1',env.RUNNER_KEY),env)).status,400);assert.equal(backup,11);
});
function loadFallback(tcp,decoder,classify=()=>null,publicFetch){return new Function('tcpFetch','decoder','setTimeout','publicRoute','fetchPublic','recordRenewal',fallbackSource.replaceAll('export async function ','async function ')+';return fetchFallback;')(tcp,{...decoder,diagnostic:actualDecoder.diagnostic},fn=>{fn();return 0;},classify,publicFetch,async()=>true);}
test('actual dated schedule and class routes recover with header2 inside the same queue operation',async()=>{
 for(const path of [route,classRoute]){
  const profiles=[],pauses=[];let primary=0,renewals=0;
  const app=load(async()=>{primary++;return new Response('',{status:502});},async(req,config,options)=>{
   const profile=options?.profile||'original-header';profiles.push(profile);
   return profile==='profile-fb-header2'?decoded({result:'usable'}):new Response('',{status:502});
  },()=>null,async()=>{renewals++;},(fn,ms)=>{pauses.push(ms);fn();});
  const r=await app.fetch(request(path),env);assert.equal(r.status,200);assert.equal(r.headers.get('x-rs-header-profile'),'profile-fb-header2');assert.equal(primary,1);assert.equal(renewals,0);assert.deepEqual(profiles,['original-header','profile-fb-header1','profile-fb-header2']);assert.deepEqual(pauses,[1000,1000]);
 }
});
test('all profiles fail: renew once, repeat the five once, then return the failure',async()=>{
 const profiles=[];let renewals=0;
 const app=load(async()=>new Response('',{status:502}),async(req,config,options)=>{profiles.push(options?.profile||'original-header');return new Response('',{status:502});},()=>null,async()=>{renewals++;});
 const r=await app.fetch(request(),env);assert.equal(r.status,502);assert.equal(renewals,1);assert.deepEqual(profiles,['original-header','profile-fb-header1','profile-fb-header2','windows-clean','android-clean','original-header','profile-fb-header1','profile-fb-header2','windows-clean','android-clean']);
});
test('each added profile can recover either route and stops before renewal',async()=>{
 for(const path of [route,classRoute]){
  for(const winner of ['windows-clean','android-clean']){
   const profiles=[],pauses=[];
   let renewals=0;
   const app=load(async()=>new Response('',{status:502}),async(req,config,options)=>{
    const profile=options?.profile||'original-header';
    profiles.push(profile);
    return profile===winner?decoded({result:'usable'}):new Response('',{status:502});
   },()=>null,async()=>{renewals++;},(fn,ms)=>{pauses.push(ms);fn();});
   const response=await app.fetch(request(path),env);
   assert.equal(response.status,200);
   assert.equal(response.headers.get('x-rs-header-profile'),winner);
   const expected=['original-header','profile-fb-header1','profile-fb-header2','windows-clean','android-clean'];
   assert.deepEqual(profiles,expected.slice(0,expected.indexOf(winner)+1));
   assert.equal(renewals,0);
   assert.ok(pauses.every(ms=>ms>=1000));
  }
 }
});
test('health probes retain their separate recovery owner; empty 200 never becomes collection success',async()=>{
 let calls=0;const app=load(async()=>new Response('',{status:502}),async()=>{calls++;return decoded({});});
 assert.equal((await app.fetch(request(),{...env,SGL_HEALTH_PROBE:true})).status,200);assert.equal(calls,1);
 assert.equal((await app.fetch(request(),env)).status,502);assert.equal(calls,11);
});
const anonymous=exp=>'header.'+btoa(JSON.stringify({anon:true,exp})) +'.signature';
test('fallback renews anonymously inside Cloudflare and reuses token without runner calls',async()=>{
 let renewals=0,reads=0;const tcp=async(url,options)=>{assert.equal(url,'https://sglapi.wellingtoninternational.com/utilities/get-config');assert.equal(options.headers.Cookie,env.SGL_COOKIE);assert.equal(options.headers['Sec-Fetch-Site'],'same-site');renewals++;return Response.json({_t:anonymous(Math.floor(Date.now()/1000)+2592000)});};
 const fallback=loadFallback(tcp,{fetch:async(req,config)=>{reads++;assert.equal(config.SGL_FETCH,tcp);assert.equal(config.SGL_COOKIE,env.SGL_COOKIE);assert.ok(config.SGL_TOKEN.startsWith('header.'));return decoded({class:{class_id:1}});}});
 assert.equal((await fallback(request(),env)).status,200);assert.equal((await fallback(request(classRoute),env)).status,200);assert.equal(renewals,1);assert.equal(reads,2);
});
test('renewal failures and expired tokens cannot become decoded success',async()=>{
 for(const tokenResponse of [()=>new Response('secret upstream body',{status:403}),()=>Response.json({_t:anonymous(1)}),()=>Response.json({})]){
  let reads=0;const fallback=loadFallback(async()=>tokenResponse(),{fetch:async()=>{reads++;return decoded({ok:true});}});const r=await fallback(request(),env);assert.equal(r.status,502);assert.equal(r.headers.has('x-rs-decoded'),false);assert.equal(reads,0);assert.doesNotMatch(await r.text(),/offline-cookie|secret upstream body/);
 }
});

test('data-response token replaces the existing fallback cache even when that response fails',async()=>{
 const first=anonymous(Math.floor(Date.now()/1000)+2592000),next=anonymous(Math.floor(Date.now()/1000)+2592010);
 let renewals=0,reads=0;
 const fallback=loadFallback(async()=>{renewals++;return Response.json({_t:first});},{fetch:async(req,config)=>{
  if(!reads++){assert.equal(config.SGL_TOKEN,first);await config.SGL_ON_TOKEN(next,(Math.floor(Date.now()/1000)+2592010)*1000);return new Response('',{status:502});}
  assert.equal(config.SGL_TOKEN,next);return decoded({class:{class_id:123},trips:[]});
 }});
 assert.equal((await fallback(request(classRoute),env)).status,502);assert.equal((await fallback(request(classRoute),env)).status,200);assert.equal(renewals,1);assert.equal(reads,2);
});
test('public false and empty arrays remain valid without a decoded marker',async()=>{
 for(const payload of [false,[]]){let backups=0;const app=load(async()=>Response.json(payload),async()=>{backups++;},()=>({valid:true,origin:'https://sglapi.wellingtoninternational.com'}));const r=await app.fetch(request('/homepage/getLiveClassStatus?customer_id=15&datetimestamp=1'),env);assert.equal(r.status,200);assert.equal(r.headers.get('x-rs-source-lane'),'runner');assert.deepEqual(await r.json(),payload);assert.equal(backups,0);}
});
test('public fallback uses existing public fetch over TLS without token renewal or decoding',async()=>{
 let called=0;const tcp=async()=>{throw Error('No renewal call expected');};const fallback=loadFallback(tcp,{fetch(){throw Error('No decoder expected');}},()=>({valid:true,origin:'https://sgl.wellingtoninternational.com'}),async(url,origin,transport)=>{called++;assert.equal(origin,'https://sgl.wellingtoninternational.com');assert.equal(transport,tcp);return Response.json(false);});const r=await fallback(request('/iphone.php/esp/webservice/LiveScoreWidget'),env);assert.equal(r.status,200);assert.equal(await r.json(),false);assert.equal(r.headers.has('x-rs-decoded'),false);assert.equal(called,1);
});

test('missing token uses existing Windows then Android profiles and stops on first valid anonymous token',async()=>{
 for(const failures of [1,2]){
  const agents=[];let reads=0;
  const fallback=loadFallback(async(url,options)=>{agents.push(options.headers['User-Agent']);assert.equal(options.headers['Sec-Fetch-Site'],'same-site');return Response.json(agents.length<=failures?{}:{_t:anonymous(Math.floor(Date.now()/1000)+2592000)});},{fetch:async()=>{reads++;return decoded({rings:[]});}});
  assert.equal((await fallback(request(),env)).status,200);assert.equal(agents.length,failures+1);assert.equal(reads,1);
  assert.match(agents[0],/iPhone/);assert.match(agents[1],/Windows NT/);if(failures===2)assert.match(agents[2],/Android 16/);
  assert.equal((await fallback(request(),env)).status,200);assert.equal(agents.length,failures+1);
 }
});
test('all missing tokens stop after the three existing profiles without a schedule call',async()=>{
 let tokens=0,reads=0;const fallback=loadFallback(async()=>{tokens++;return Response.json({});},{fetch:async()=>{reads++;}});
 const response=await fallback(request(),env);assert.equal(response.status,502);assert.equal(tokens,3);assert.equal(reads,0);
});
test('an invalid token or config HTTP error does not trigger extra profile attempts',async()=>{
 for(const response of [()=>Response.json({_t:anonymous(1)}),()=>new Response('',{status:403})]){
  let calls=0;const fallback=loadFallback(async()=>{calls++;return response();},{fetch:async()=>{throw Error('Data fetch forbidden');}});
  assert.equal((await fallback(request(),env)).status,502);assert.equal(calls,1);
 }
});
test('saved-lane failure retains the runner error reason with secrets redacted',async()=>{
 const app=load(async()=>Response.json({reason:'ECONNRESET '+env.RUNNER_KEY+' Bearer private-value'},{status:502}),async()=>{throw Error('Fallback forbidden in defer');});
 const storage={get:async()=>({lane:'runner'}),put:async()=>{}};
 const response=await app.fetch(new Request('https://worker.example'+route,{headers:{'X-RS-Recovery':'defer'}}),{...env,SGL_LANE_STORAGE:storage});
 const body=await response.json();assert.equal(response.status,502);assert.match(body.reason,/ECONNRESET/);assert.doesNotMatch(body.reason,/offline-verification-key|private-value/);
});

test('multiline Unicode runner reasons keep their JSON stage/status and a valid one-line header',async()=>{
 const app=load(async()=>Response.json({reason:'Socket fermé\nECONNRESET',stage:'decode',upstreamStatus:200},{status:502}),async()=>{throw Error('No fallback');});
 const response=await app.fetch(new Request('https://worker.example'+route,{headers:{'X-RS-Recovery':'defer'}}),{...env,SGL_LANE_STORAGE:{get:async()=>({lane:'runner'}),put:async()=>{}}});
 const body=await response.json();assert.equal(response.status,502);assert.match(body.reason,/Socket fermé\nECONNRESET/);assert.equal(body.stage,'decode');assert.equal(body.upstreamStatus,200);assert.equal(response.headers.get('x-rs-upstream-status'),'200');assert.doesNotMatch(response.headers.get('x-rs-failure-reason'),/[\r\n]/);
});
test('config transport cause is retained without retry or secrets',async()=>{
 let calls=0;const fallback=loadFallback(async()=>{calls++;throw new Error('config fetch failed',{cause:new Error('ECONNRESET')});},{fetch:async()=>{throw Error('No schedule');}});
 let details;const response=await fallback(request(),env,{report:d=>{details=d;}});assert.equal(response.status,502);assert.equal(calls,1);assert.match(details.reason,/ECONNRESET/);
});

test('fallback response decoding cause survives its report callback',async()=>{
 const fallback=loadFallback(async()=>Response.json({_t:anonymous(Math.floor(Date.now()/1000)+2592000)}),{fetch:async(req,env)=>{await env.SGL_FETCH('https://source.example');return Response.json({message:'Decode failed',reason:'OperationError: invalid AES tag',stage:'decode',upstreamStatus:200},{status:502});}});
 let detail;const response=await fallback(request(),env,{report:x=>{detail=x;}});assert.equal(response.status,502);assert.equal(detail.reason,'OperationError: invalid AES tag');assert.equal(detail.stage,'decode');assert.equal(detail.schedule_upstream_status,200);
});
test('non-200 missing-token responses do not add profile attempts',async()=>{
 let calls=0;const fallback=loadFallback(async()=>{calls++;return Response.json({},{status:201});},{fetch:async()=>{throw Error('No data');}});
 assert.equal((await fallback(request(),env)).status,502);assert.equal(calls,1);
});

test('concrete fallback JSON reason overrides a generic report and retains decode stage/status',async()=>{
 const app=load(async()=>{throw Error('Runner must not be selected');},async(req,env,options)=>{options.report({reason:'Schedule response could not be decoded'});return Response.json({message:'Decode failed',reason:'OperationError: invalid AES tag',stage:'decode',upstreamStatus:200},{status:502});});
 const r=await app.fetch(new Request('https://worker.example'+route,{headers:{'X-RS-Recovery':'trouble'}}),{...env,SGL_LANE_STORAGE:{get:async()=>({lane:'runner',next:0}),put:async()=>{}}});
 const body=await r.json();assert.equal(r.status,502);assert.match(body.reason,/invalid AES tag/);assert.equal(body.stage,'decode');assert.equal(body.upstreamStatus,200);
});
