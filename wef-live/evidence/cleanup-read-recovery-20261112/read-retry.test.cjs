'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {cleanupApi}=require('./candidate/wef-live/cleanup');
const base='appgzosUcVqA4vZdf';
test('cleanup GET transport failure retries once with a fresh timeout and same URL',async()=>{
 const calls=[],delays=[];
 const api=cleanupApi(async(url,options)=>{calls.push({url,options});if(calls.length===1)throw TypeError('fetch failed');return{ok:true,json:async()=>({records:[]})};},async ms=>delays.push(ms));
 assert.deepEqual(await api.list('shows','appCaSmL819KK7UFJ'),[]);
 assert.equal(calls.length,2);assert.equal(calls[0].url,calls[1].url);assert.equal(calls[1].options.method,'GET');
 assert.notEqual(calls[0].options.signal,calls[1].options.signal);assert.deepEqual(delays,[500]);
});
test('two failed GET attempts return the failure without an unbounded retry',async()=>{
 let count=0;const api=cleanupApi(async()=>{count++;throw TypeError('fetch failed');},async()=>{});
 await assert.rejects(api.list('cleanup'),/fetch failed/);assert.equal(count,2);
});
test('cleanup POST and PATCH failures are never repeated',async()=>{
 for(const method of ['POST','PATCH']){let count=0;const api=cleanupApi(async()=>{count++;throw TypeError('fetch failed');},async()=>{});
  await assert.rejects(api.call(base,'cleanup',method,{records:[]}),/fetch failed/);assert.equal(count,1);
 }
});
test('HTTP authentication failures are returned without transport retry',async()=>{
 let count=0;const api=cleanupApi(async()=>{count++;return{ok:false,status:403};},async()=>{});
 await assert.rejects(api.list('cleanup'),/HTTP 403/);assert.equal(count,1);
});
test('uncertain cleanup DELETE is attempted once and never repeated',async()=>{
 const row={id:'recOldLog',fields:{result:'Success',completed:'2026-11-10T12:00:00Z'}};
 let deletes=0;
 const api=cleanupApi(async(url,options)=>{
  if(options.method==='DELETE'){deletes++;throw TypeError('fetch failed');}
  return{ok:true,json:async()=>({records:url.includes('/shows?')?[{id:'recShow',fields:{customer_id:15,is_default:true,show_id:77}}]:[row]})};
 },async()=>{});
 await assert.rejects(api.retireCleanup('heartbeat',[row],{show:77,trigger:'daily-clear',protectedRuns:new Set(),deletedIds:new Set()}),/fetch failed/);
 assert.equal(deletes,1);
});
