import test from 'node:test';
import assert from 'node:assert/strict';
import { runDueFeeds } from '../src/lib/wef-schedule/cadence.mjs';
test('normal ticks give Day 0 priority and run full only every fifteen minutes',async()=>{
 const state={},calls=[],saved=[];
 const refresh=async(_,options)=>{calls.push(options);return {log:{result:'Success'},loggingErrors:[]};};
 for(const minutes of [0,3,6,9,12,15])await runDueFeeds({state,save:async s=>saved.push(structuredClone(s)),now:new Date(Date.UTC(2026,10,11,18,minutes)),refresh});
 assert.deepEqual(calls.map(c=>c.lane),['day0','full','day0','day0','day0','day0','day0','full']);
 assert.equal(saved.length,8);
 await runDueFeeds({state,save:async()=>assert.fail(),now:new Date('2026-11-11T18:15:30Z'),refresh});
 assert.equal(calls.length,8);
});
test('failed workflow durably stops both lanes without a substitute or retry',async()=>{
 const state={};let calls=0;
 const refresh=async()=>{calls++;return {log:{result:'Error'},loggingErrors:[]};};
 const args={state,save:async()=>{},now:new Date('2026-11-11T18:00:00Z'),refresh};
 assert.equal((await runDueFeeds(args)).stopped.lane,'day0');
 await runDueFeeds({...args,now:new Date('2026-11-11T18:03:00Z')});
 assert.equal(calls,1);
});
test('logging failure also stops the writer and retains the successful lane receipt',async()=>{
 const state={};
 const result=await runDueFeeds({state,save:async()=>{},now:new Date('2026-11-11T18:00:00Z'),refresh:async()=>({log:{result:'Success'},loggingErrors:[{table:'runs'}]})});
 assert.equal(result.stopped.lane,'day0');assert.equal(result.outcomes.length,1);
});
