"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {schedulePace,intervalPace,evaluate,ageReason}=require('./pace');
const {verifyRows}=require('./runner');
const {api,ENGINE,SCHEDULES,TABLES}=require('./api');
const now='2026-10-29T09:25:00Z';
const row={'stage-key':'76|2026-10-30|g_c','class-id':30,'start-time':'08:06:00','end-time':'08:39:00','total-trips':11,type:'Jumpers'};
const identity={customer:15,show:76,date:'2026-10-30',ring:6,group:19};
test('saved 33-minute eleven-trip schedule yields three minutes; no invented actual',()=>{
  const r=evaluate({identity,rows:[row],observedAt:now,inputRefs:[],now});
  assert.equal(r.fields['pace-minutes-per-trip'],3);assert.equal(r.fields.availability,'partial');
  assert.equal(JSON.parse(r.fields['payload-json']).observed_interval_pace_minutes_per_trip,null);
});
test('zero, missing, invalid windows, blocks and multi-class groups never divide or double-count',()=>{
  for(const rows of [[{...row,'total-trips':0}],[{...row,'total-trips':null}],[{...row,'end-time':'08:00:00'}],[{...row,type:'schooling'}],[row,row]])assert.equal(schedulePace(rows).value,null);
});
test('interval throughput uses compatible dated observations, including holds',()=>{
  const prior={at:'2026-10-29T09:20:00Z',total:30,gone:10,membership:'m'};
  const next={at:'2026-10-29T09:24:00Z',total:30,gone:12,membership:'m'};
  const r=intervalPace(next,prior,now);assert.equal(r.value,2);assert.equal(r.completed,2);
});
test('reset/membership change/out-of-order/gap/stale/future/count-conflict cannot produce pace',()=>{
  const prior={at:'2026-10-29T09:20:00Z',total:30,gone:10,membership:'m'};
  for(const patch of [{gone:9},{membership:'n'},{at:'2026-10-29T09:19:00Z',gone:12},{at:'2026-10-29T09:36:00Z',gone:12},{gone:31},{at:prior.at,gone:12}])assert.equal(intervalPace({...prior,...patch},prior,now).value,null);
  assert.equal(ageReason('2026-10-29T09:15:00Z',now,600000),'stale_observation');
});
test('no-progress does not advance baseline; repeated source preserves measured interval without new measurement',()=>{
  const before={at:'2026-10-29T09:20:00Z',total:30,gone:10,membership:'m'};
  const noProgress=intervalPace({...before,at:'2026-10-29T09:21:00Z'},before,now);assert.deepEqual(noProgress.baseline,before);
  const sample={...before,at:'2026-10-29T09:24:00Z',gone:12};
  const first=evaluate({identity,rows:[row],observedAt:now,inputRefs:[],live:sample,previous:before,now});
  const repeat=evaluate({identity,rows:[row],observedAt:now,inputRefs:[],live:sample,previous:first.baseline,now:'2026-10-29T09:26:00Z'});
  assert.equal(first.fields['content-hash'],repeat.fields['content-hash']);assert.equal(repeat.fields['observed-at'],sample.at);
});
test('fresh evaluation clock alone does not renew stale observation',()=>{
  const sample={at:'2026-10-29T09:14:00Z',total:30,gone:12,membership:'m'};
  assert.equal(intervalPace(sample,null,now).reason,'stale_observation');
});
test('a new observation without progress never represents the previous interval as a new measurement',()=>{
  const sample={at:'2026-10-29T09:24:00Z',total:30,gone:12,membership:'m'};
  const previous={...sample,at:'2026-10-29T09:20:00Z',gone:10};
  const measured=evaluate({identity,rows:[row],observedAt:now,inputRefs:[],live:sample,previous,now});
  const held=evaluate({identity,rows:[row],observedAt:now,inputRefs:[],live:{...sample,at:'2026-10-29T09:25:00Z'},previous:measured.baseline,now});
  const p=JSON.parse(held.fields['payload-json']);assert.equal(p.observed_interval_pace_minutes_per_trip,null);assert(p.reasons.includes('no_new_completions'));
});
test('source-invalid scope does not publish numeric schedule fallback',()=>{
  const r=evaluate({identity,rows:[row],observedAt:now,inputRefs:[],sourceReason:'schedule_consistency_unproven',now});
  assert.equal(r.fields.availability,'unavailable');assert(!('pace-minutes-per-trip' in r.fields));
});
test('readback rejects duplicate identities and mismatched content',()=>{
  assert.throws(()=>verifyRows([{fields:{key:'x',value:3}},{fields:{key:'x',value:3}}],[{key:'x',value:3}],'key'),/Duplicate/);
  assert.throws(()=>verifyRows([{fields:{key:'x',value:4}}],[{key:'x',value:3}],'key'),/mismatch/);
});
test('transport forbids source writes and unsupported targets before any network action',async()=>{
  let calls=0;const a=api({token:'test',deadline:Date.now()+10000,request:async()=>{calls++;throw Error('should not run');}});
  await assert.rejects(a.upsert('schedules','key',[{key:'x'}]),/Forbidden/);
  await assert.rejects(a.list(SCHEDULES,'entries'),/Forbidden/);assert.equal(calls,0);
});
test('transport uses existing-key performUpsert and validates server clock',async()=>{
  let seen;const a=api({token:'test',deadline:Date.now()+10000,request:async(url,options)=>{seen={url,options};return{ok:true,headers:new Headers({Date:new Date().toUTCString()}),json:async()=>({records:[]})};}});
  await a.upsert(TABLES.answers,'answer-key',[{'answer-key':'one'}]);
  assert(seen.url.startsWith('https://api.airtable.com/v0/'+ENGINE));assert.equal(seen.options.method,'PATCH');
  assert.deepEqual(JSON.parse(seen.options.body).performUpsert.fieldsToMergeOn,['answer-key']);assert(a.clockVerified);
});
