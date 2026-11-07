'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
globalThis.fetch=async()=>{throw Error('Offline network forbidden');};
const {context,decision}=require('./candidate/wef-live/live-cadence');
const scope={show:77,customer:15,date:'2026-11-07',eastern_time:'2026-11-07 07:33:11'};
test('Shows published Eastern date and time decide eligibility,not a conflicting host clock',()=>{
 const r=decision(true,scope,'2026-11-08T04:30:00Z');assert.equal(r.collect,true);assert.equal(r.today,'2026-11-07');assert.equal(r.minutes,3);
 assert.equal(decision(true,{...scope,eastern_time:'2026-11-07 04:59:00'},'2026-11-07T12:00:00Z').reason,'outside-collection-window');
 assert.equal(decision(true,{...scope,eastern_time:'2026-11-07 19:01:00'},'2026-11-07T12:00:00Z').reason,'outside-collection-window');
 assert.equal(decision(true,{...scope,date:'2026-11-08'},'2026-11-07T12:00:00Z').reason,'priority-date-mismatch');
});
test('false and failed status rules remain60/3minutes with published time',()=>{
 assert.equal(decision(false,scope,'2026-11-07T12:00:00Z').minutes,60);assert.equal(decision(null,scope,'2026-11-07T12:00:00Z').minutes,3);
});
test('context reads existing canonical lookup without another table or request',async()=>{
 const calls=[];const api={list:async(t,b)=>{calls.push(t);return t==='shows'?[{id:'show',fields:{customer_id:15,is_default:true,show_id:77}}]:[{fields:{'show-day-key':'15|77|2026-11-07','day-priority':0,date:'2026-11-07','eastern-time (from show-times)':['2026-11-07 07:33:11']}}];}};
 const r=await context(api);assert.equal(r.eastern_time,scope.eastern_time);assert.deepEqual(calls,['shows','show-days']);
});
test('missing published time keeps existing Eastern calculation without blocking collection',()=>{
 assert.equal(decision(true,{show:77,date:'2026-11-07'},'2026-11-07T12:00:00Z').collect,true);
});
