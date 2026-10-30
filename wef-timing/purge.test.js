"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {authority,eligible,plan,localDay}=require('./purge');
const {api,TABLES,ENGINE}=require('./api');
const now='2026-10-30T00:10:00Z'; // Still October 29 in New York.
const show={id:'show',fields:{'show-key':'15|76',show_id:76,customer_id:15,is_default:true,selected_date:'2026-10-29',start_date:'2026-10-29',end_date:'2026-11-01',last_checked:'2026-10-30T00:09:00Z'}};
const scopes=new Map([['2026-10-29',null]]);
const context={customer:15,show:76,day:'2026-10-29',show_record:'show',retired_shows:[75]};
function snapshot(sid,date,customer=15){const scope=['schedules_live',sid,date,6,19,'base'].join('|');return{id:'rec00000000000001',fields:{'answer-kind':'ring-pace','source-lane':'schedules_live','show-id':sid,date,'ring-number':6,'group-id':19,'scope-key':scope,'answer-key':scope+'|run|hash','content-hash':'hash','payload-json':JSON.stringify({identity:{customer,show:sid,date,ring:6,group:19}})}};}
test('authority uses New York date, not UTC midnight; requires fresh completed scope',()=>{
  assert.equal(localDay(now),'2026-10-29');assert.equal(authority(show,scopes,now).reason,null);
  assert.equal(authority(show,new Map(),now).reason,'purge_new_scope_not_verified');
  assert.equal(authority(show,new Map([['2026-10-29','schedule_consistency_unproven']]),now).reason,'purge_new_scope_not_verified');
  assert.equal(authority({...show,fields:{...show.fields,last_checked:'2026-10-29T20:00:00Z'}},scopes,now).reason,'purge_show_not_fresh');
});
test('discovering a future working day does not retire current day',()=>{
  const future={...show,fields:{...show.fields,selected_date:'2026-10-30'}};
  assert.equal(authority(future,new Map([['2026-10-30',null]]),now).reason,'purge_working_day_not_today');
});
test('only owned prior-show and past-day pace snapshots qualify',()=>{
  assert(eligible(snapshot(75,'2026-11-01'),context));assert(eligible(snapshot(76,'2026-10-28'),context));
  assert(!eligible(snapshot(77,'2026-11-01'),context));assert(!eligible(snapshot(74,'2026-10-01'),context));
  assert(!eligible(snapshot(76,'2026-10-29'),context));assert(!eligible(snapshot(76,'2026-10-30'),context));
  assert(!eligible(snapshot(75,'2026-10-28',99),context));
  for(const patch of [{'answer-kind':'other'},{'source-lane':'wef_data'},{date:'bad-date'},{'scope-key':'unrelated'}])assert(!eligible({...snapshot(75,'2026-10-28'),fields:{...snapshot(75,'2026-10-28').fields,...patch}},context));
});
test('two distinct normal invocations confirm authority; first observation never deletes',()=>{
  const verified=authority(show,scopes,now),rows=[snapshot(75,'2026-10-28')];verified.context.retired_shows=[75];
  const first=plan(rows,verified,{},'one');assert.equal(first.rows.length,0);
  assert.equal(plan(rows,verified,{purge_authority:first.next},'one').rows.length,0);
  const second=plan(rows,verified,{purge_authority:first.next},'two');assert.equal(second.rows.length,1);
  const different=authority({...show,fields:{...show.fields,show_id:77,'show-key':'15|77'}},scopes,now);
  assert.equal(plan(rows,different,{purge_authority:first.next},'two').rows.length,0);
});
test('delete transport only targets exact Engine snapshot IDs and reads back absence',async()=>{
  const calls=[];const a=api({token:'test',deadline:Date.now()+10000,request:async(url,options)=>{calls.push({url,method:options.method});return{ok:true,headers:new Headers({Date:new Date().toUTCString()}),json:async()=>options.method==='DELETE'?{records:[{id:'rec00000000000001',deleted:true}]}:{records:[]}};}});
  await a.removeSnapshots(['rec00000000000001']);assert.equal(calls.length,2);assert.equal(calls[0].method,'DELETE');
  assert(calls[0].url.includes('/'+ENGINE+'/'+TABLES.answers));assert(calls[0].url.includes('records%5B%5D=rec00000000000001'));
  assert.equal(calls[1].method,'GET');assert(calls[1].url.includes('RECORD_ID'));
  await assert.rejects(a.removeSnapshots(['bad']),/Invalid/);await assert.rejects(a.removeSnapshots(['rec00000000000001','rec00000000000001']),/Invalid/);
});
test('lost delete response is not retried; a retained row fails verification',async()=>{
  let calls=0;const a=api({token:'test',deadline:Date.now()+10000,request:async()=>{calls++;throw Error('Unknown delete response');}});
  await assert.rejects(a.removeSnapshots(['rec00000000000001']),/Unknown/);assert.equal(calls,1);
  const b=api({token:'test',deadline:Date.now()+10000,request:async(url,o)=>({ok:true,headers:new Headers({Date:new Date().toUTCString()}),json:async()=>o.method==='DELETE'?{records:[{id:'rec00000000000001',deleted:true}]}:{records:[{id:'rec00000000000001',fields:{}}]}})});
  await assert.rejects(b.removeSnapshots(['rec00000000000001']),/readback failed/);
});
test('duplicated delete response IDs cannot masquerade as an exact successful batch',async()=>{
  let calls=0;const a=api({token:'test',deadline:Date.now()+10000,request:async()=>{calls++;return{ok:true,headers:new Headers({Date:new Date().toUTCString()}),json:async()=>({records:[{id:'rec00000000000001',deleted:true},{id:'rec00000000000001',deleted:true}]})};}});
  await assert.rejects(a.removeSnapshots(['rec00000000000001','rec00000000000002']),/response mismatch/);assert.equal(calls,1);
});
