'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
globalThis.fetch=async()=>{throw Error('Offline test: no network');};
const {decision,check}=require('./candidate/wef-live/live-cadence');
const scope={show:77,customer:15,date:'2026-11-06'};
function fixture(t,previous){const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-status-retry-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));if(previous)fs.writeFileSync(path.join(runtime,'live-cadence-state.json'),JSON.stringify(previous));let now='2026-11-06T14:03:04Z',statusCalls=0;const writes=[];const api={list:async table=>table==='shows'?[{id:'recShow',fields:{show_id:77,customer_id:15,is_default:true}}]:[{fields:{'show-day-key':'15|77|2026-11-06','day-priority':0,date:'2026-11-06'}}],call:async(base,table,method,body)=>{writes.push({table,fields:body.records[0].fields});return{records:body.records.map(x=>({id:'recLog',fields:x.fields}))};}};return{runtime,writes,setNow:v=>now=v,calls:()=>statusCalls,options:{runtime,api,clock:()=>now,getStatus:async()=>{statusCalls++;throw Error('HTTP403');}}};}
test('status failures use3minutes; confirmedfalse/date/window rules stay unchanged',()=>{
 assert.equal(decision(undefined,scope,'2026-11-06T14:00:00Z').minutes,3);assert.equal(decision(null,scope,'2026-11-06T04:00:00Z').minutes,3);
 assert.equal(decision(false,scope,'2026-11-06T14:00:00Z').minutes,60);assert.equal(decision(true,{...scope,date:'2026-11-07'},'2026-11-06T14:00:00Z').minutes,60);assert.equal(decision(true,scope,'2026-11-06T09:00:00Z').minutes,60);assert.equal(decision(true,scope,'2026-11-06T14:00:00Z').collect,true);
});
test('legacy hourly error checkpoint is naturally due at3minutes without manual state change',async t=>{
 const a=fixture(t,{checked_at:'2026-11-06T14:00:04Z',next_check_at:'2026-11-06T15:00:00Z',reason:'live-status-unavailable',live_status:null});a.setNow('2026-11-06T14:02:59Z');assert.equal((await check(a.options)).due,false);assert.equal(a.calls(),0);
 a.setNow('2026-11-06T14:03:01Z');const r=await check(a.options);assert.equal(r.due,true);assert.equal(r.minutes,3);assert.equal(r.next_check_at,'2026-11-06T14:06:00.000Z');assert(a.writes.some(x=>x.table==='errs-log'&&x.fields.message.includes('HTTP403')));assert.equal(a.calls(),1);
});
test('repeated status failures wait3minutes,log individually,and do not retry early',async t=>{
 const a=fixture(t);let r=await check(a.options);assert.equal(r.reason,'live-status-unavailable');a.setNow('2026-11-06T14:05:59Z');assert.equal((await check(a.options)).due,false);a.setNow('2026-11-06T14:06:02Z');r=await check(a.options);assert.equal(r.due,true);assert.equal(a.calls(),2);assert.equal(a.writes.filter(x=>x.table==='errs-log').length,2);
});
test('recovery to confirmedfalse records hourly checkpoint,not3minutes',async t=>{
 const a=fixture(t,{checked_at:'2026-11-06T14:00:04Z',next_check_at:'2026-11-06T15:00:00Z',reason:'live-status-unavailable'});const r=await check({...a.options,getStatus:async()=>false});assert.equal(r.reason,'live-false');assert.equal(r.next_check_at,'2026-11-06T15:03:00.000Z');assert(!a.writes.some(x=>x.table==='errs-log'));a.setNow('2026-11-06T14:06:00Z');assert.equal((await check(a.options)).due,false);
});
test('recovery to true/today resumes collection on normal3minute cadence',async t=>{const a=fixture(t,{checked_at:'2026-11-06T14:00:04Z',next_check_at:'2026-11-06T15:00:00Z',reason:'live-status-unavailable'});const r=await check({...a.options,getStatus:async()=>true});assert.equal(r.collect,true);assert.equal(r.next_check_at,'2026-11-06T14:06:00.000Z');});
