import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {cadenceSlot,localTime,defaultShow,healthFields,observe,run,airtable,TABLES} from '../show-time-health.mjs';

test('Eastern daytime boundaries, overnight slots and daylight saving conversion',()=>{
  const cases=[['2026-11-06T11:59:00Z',false],['2026-11-06T12:00:00Z',true],['2026-11-06T12:01:00Z',false],['2026-11-06T12:03:00Z',true],['2026-11-06T23:57:00Z',true],['2026-11-07T00:00:00Z',true],['2026-11-07T00:03:00Z',false],['2026-11-07T03:00:00Z',true],['2026-11-07T06:00:00Z',true],['2026-11-07T09:00:00Z',true],['2026-07-06T11:00:00Z',true],['2026-07-06T23:00:00Z',true]];
  for(const [now,due] of cases)assert.equal(Boolean(cadenceSlot(now)),due,now);
  assert.equal(localTime('2026-07-06T11:00:00Z').hour,7);
  assert.equal(localTime('2026-11-06T12:00:00Z').hour,7);
  for(const day of ['2026-11-06','2026-07-06']){let count=0;for(let m=0;m<1440;m++){const n=new Date(Date.parse(day+'T05:00:00Z')+m*60000);if(cadenceSlot(n))count++;}assert.equal(count,244);}
});

test('reject missing or ambiguous canonical default rather than choosing saved first row',()=>{
  assert.throws(()=>defaultShow([]));
  const row={id:'recShow',fields:{is_default:true,customer_id:15,show_id:200000077,'show-key':'15|200000077'}};
  assert.equal(defaultShow([row]).id,'recShow');assert.throws(()=>defaultShow([row,row]));
});

test('keeps original outage time; records recovery without fixed ring requirements',()=>{
  const old={status:'Outage','outage-started-at':'2026-11-06T12:00:00Z','last-success-at':'2026-11-05T12:00:00Z'};
  const fail=healthFields('decoder','url',{ok:false,status:503,detail:'failed'},old,{},'2026-11-06T13:00:00Z');
  assert.equal(fail['outage-started-at'],old['outage-started-at']);assert.equal(fail['last-success-at'],old['last-success-at']);
  const recovered=healthFields('decoder','url',{ok:true,status:200,detail:'ok'},old,{},'2026-11-06T14:00:00Z');
  assert.equal(recovered['recovered-at'],'2026-11-06T14:00:00Z');assert.equal(recovered['outage-started-at'],null);
});

test('reads Date header, not builtAt; clock only with no proxy request or credentials',async()=>{
  const calls=[];const r=await observe({now:'2026-11-06T20:13:32Z',request:async(url,options)=>{calls.push({url,options});return url.includes('version.json')?new Response('{"version":"0.1.44","builtAt":"2020-01-01T00:00:00Z"}',{headers:{Date:'Fri, 06 Nov 2026 20:13:32 GMT'}}):new Response('{"ok":false,"rings":0,"schedule_status":502}',{status:503});}});
  assert.equal(r.clock.sourceTime,'2026-11-06T20:13:32.000Z');assert.equal(r.clock.freshness,'Unverified');assert.equal(r.clock.version,'0.1.44');assert.equal(calls[0].options.headers['Sec-Fetch-Site'],'cross-site');assert.equal(r.decoder,undefined);assert.equal(calls.length,1);assert.ok(calls.every(c=>new URL(c.url).hostname==='sglapp.showgroundslive.com'));
  for(const c of calls){assert.equal(c.options.headers.Authorization,undefined);assert.equal(c.options.headers.Cookie,undefined);}
  assert.equal(calls[0].options.headers['User-Agent'],'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1');
  const one=await observe({now:'2026-11-06T20:13:32Z',request:async(url)=>url.includes('version.json')?new Response('{}',{headers:{Date:'Fri, 06 Nov 2026 20:13:32 GMT',Age:'60'}}):new Response('{"ok":true,"rings":7}')});
  assert.equal(one.clock.freshness,'Stale');assert.equal(one.decoder,undefined);
});

test('missing Date fails without retry or proxy request',async()=>{
  let calls=0;const r=await observe({now:'2026-11-06T20:13:32Z',request:async()=>{calls++;return new Response('not-json');}});
  assert.equal(calls,1);assert.equal(r.clock.ok,false);assert.equal(r.decoder,undefined);
});

test('writes only clock/health, links current clock date, avoids replay of same slot',async()=>{
  const dir=await fs.mkdtemp(path.join(os.tmpdir(),'wef-clock-test-'));
  const calls=[],writes=[];
  const api={list:async t=>t===TABLES.shows?[{id:'recShow',fields:{is_default:true,customer_id:15,show_id:77,'show-key':'15|77',selected_date:'2026-11-07'}}]:t===TABLES.days?[{id:'recDay',fields:{show:['recShow'],date:'2026-11-06'}}]:[],upsert:async(t,k,f)=>{assert.ok([TABLES.times,TABLES.health].includes(t));writes.push({t,k,f});}};
  const options={now:'2026-11-06T20:00:00Z',api,statePath:path.join(dir,'state.json'),request:async url=>{calls.push(url);return url.includes('version.json')?new Response('{}',{headers:{Date:'Fri, 06 Nov 2026 20:00:00 GMT'}}):new Response('{"ok":true,"rings":7}');}};
  try{assert.equal((await run(options)).status,'Recorded');assert.equal(writes.length,2);assert.equal(writes[1].f['health-key'],'clock');assert.deepEqual(writes[0].f['show-day'],['recDay']);assert.equal((await run(options)).status,'Skipped');assert.equal(calls.length,1);assert.equal((await run({...options,now:'2026-11-06T20:01:00Z'})).status,'Skipped');assert.equal(calls.length,1);}finally{await fs.rm(dir,{recursive:true});}
});

test('Airtable adapter preserves ambiguous-write failure with no automatic retry',async()=>{
  let calls=0;const api=airtable({token:'offline-placeholder',pause:async()=>{},request:async(url,options)=>{calls++;if(options.method==='GET')return new Response('{"records":[]}');throw Error('unknown create result');}});
  await assert.rejects(api.upsert(TABLES.health,'health-key',{'health-key':'clock'}));assert.equal(calls,2);
});
test('pre-show clock links the selected show-day while preserving the actual source date',async()=>{
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'wef-clock-link-test-'));
 const writes=[];
 const api={
  list:async table=>table===TABLES.shows
   ?[{id:'recShow',fields:{is_default:true,customer_id:15,show_id:78,'show-key':'15|78',selected_date:'2026-11-11'}}]
   :table===TABLES.days?[{id:'recDay',fields:{show:['recShow'],date:'2026-11-11'}}]:[],
  upsert:async(table,key,fields)=>{writes.push({table,fields});}
 };
 try{
  const result=await run({now:'2026-11-10T22:57:00Z',api,statePath:path.join(dir,'state.json'),request:async()=>Response.json({version:'test'},{headers:{Date:'Tue, 10 Nov 2026 22:57:00 GMT'}})});
  assert.equal(result.status,'Recorded');
  const clock=writes.find(row=>row.table===TABLES.times).fields;
  assert.deepEqual(clock.show,['recShow']);
  assert.deepEqual(clock['show-day'],['recDay']);
  assert.equal(clock['show-time-key'],'15|78|2026-11-10');
  assert.equal(clock['source-time'],'2026-11-10T22:57:00.000Z');
  assert.match(clock.notes,/selected show-day 2026-11-11/);
 }finally{await fs.rm(dir,{recursive:true});}
});
test('clock observation is not saved without a matching or selected show-day',async()=>{
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'wef-clock-missing-day-'));
 const writes=[];
 const api={list:async table=>table===TABLES.shows?[{id:'recShow',fields:{is_default:true,customer_id:15,show_id:78,'show-key':'15|78',selected_date:'2026-11-11'}}]:[],upsert:async(table,key,fields)=>writes.push({table,fields})};
 try{
  const result=await run({now:'2026-11-10T22:57:00Z',api,statePath:path.join(dir,'state.json'),request:async()=>Response.json({version:'test'},{headers:{Date:'Tue, 10 Nov 2026 22:57:00 GMT'}})});
  assert.equal(result.status,'Failed');
  assert.equal(writes.some(row=>row.table===TABLES.times),false);
 }finally{await fs.rm(dir,{recursive:true});}
});

test('successful outage reporting is recorded with explicit false source health',async()=>{
  const dir=await fs.mkdtemp(path.join(os.tmpdir(),'wef-outage-test-'));const writes=[];
  const api={list:async t=>t===TABLES.shows?[{id:'recShow',fields:{is_default:true,customer_id:15,show_id:77,'show-key':'15|77'}}]:[],upsert:async(t,k,f)=>writes.push(f)};
  try{const result=await run({now:'2026-11-06T20:00:00Z',api,statePath:path.join(dir,'state.json'),request:async()=>new Response('unavailable',{status:503})});assert.equal(result.status,'Recorded');assert.equal(result.clock,false);assert.equal(result.decoder,undefined);assert.equal(writes.length,1);assert.ok(writes.every(f=>f.status==='Outage'));}finally{await fs.rm(dir,{recursive:true});}
});

test('Airtable adapter verifies persisted values',async()=>{
  let gets=0;const saved={id:'recHealth',fields:{'health-key':'clock',status:'Healthy'}};
  const api=airtable({token:'offline-placeholder',pause:async()=>{},request:async(url,options)=>{if(options.method==='GET'){gets++;return new Response(JSON.stringify({records:gets===1?[]:[saved]}));}return new Response(JSON.stringify({records:[saved]}));}});
  assert.equal((await api.upsert(TABLES.health,'health-key',saved.fields)).id,'recHealth');assert.equal(gets,2);
});

test('Airtable readback accepts equivalent timestamps and omitted empty links',async()=>{
  let gets=0;const fields={'health-key':'clock','checked-at':'2026-11-06T20:00:00Z','show-day':[],'outage-started-at':null};
  const saved={id:'recHealth',fields:{'health-key':'clock','checked-at':'2026-11-06T20:00:00.000Z'}};
  const api=airtable({token:'offline-placeholder',pause:async()=>{},request:async(url,options)=>new Response(JSON.stringify(options.method==='GET'?{records:++gets===1?[]:[saved]}:{records:[saved]}))});
  assert.equal((await api.upsert(TABLES.health,'health-key',fields)).id,'recHealth');
});

test('Airtable adapter follows read pagination without dropping default show',async()=>{
  let count=0;const api=airtable({token:'offline-placeholder',pause:async()=>{},request:async(url)=>{count++;if(!url.includes('offset='))return new Response('{"records":[{"id":"recOld","fields":{}}],"offset":"next"}');return new Response('{"records":[{"id":"recCurrent","fields":{}}]}');}});
  assert.equal((await api.list(TABLES.shows)).length,2);assert.equal(count,2);
});
