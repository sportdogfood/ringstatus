"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {fetchSourceText}=require('../source-fetch');
const url='https://wef-schedules-decoded.gombcg.workers.dev/ring?customer_id=15';
const good=()=>({ok:true,headers:{get:()=> '1'},text:async()=>'{"show":{"show_id":76}}'});
const network=()=>new TypeError('fetch failed',{cause:Object.assign(Error('private diagnostics'),{code:'ECONNRESET'})});
test('transient source failure recovers on same URL with bounded GETs',async()=>{
 let calls=0;const waits=[];const r=await fetchSourceText(url,{request:async(u,o)=>{assert.equal(u,url);assert.equal(o.method,'GET');assert.equal(o.headers.Accept,'application/json');calls++;if(calls<3)throw network();return good();},pause:async ms=>waits.push(ms)});
 assert.equal(calls,3);assert.deepEqual(waits,[500,1000]);assert.match(r.text,/show_id/);
});
test('exhausted source reads stop after three and retain safe nested cause',async()=>{
 let calls=0;await assert.rejects(fetchSourceText(url+'&private=SECRET',{request:async()=>{calls++;throw network();},pause:async()=>{}}),e=>{assert.match(e.message,/GET \/ring.*3 attempts.*ECONNRESET/);assert.ok(!/SECRET|private|customer_id/.test(e.message));return true;});assert.equal(calls,3);
});
test('HTTP 503 recovers but 401 and 404 do not retry',async()=>{
 for(const status of [503,401,404]){let calls=0,cancelled=0;const p=fetchSourceText(url,{request:async()=>{calls++;return calls===1?{ok:false,status,body:{cancel:async()=>{cancelled++;}}}:good();},pause:async()=>{}});if(status===503){await p;assert.equal(calls,2);}else{await assert.rejects(p,new RegExp('HTTP '+status));assert.equal(calls,1);}assert.equal(cancelled,1);}
});
test('body transport failure is recovered; invalid JSON and missing decode proof are not retried',async()=>{
 let calls=0;await fetchSourceText(url,{request:async()=>{calls++;return calls===1?{ok:true,text:async()=>{throw network();}}:good();},pause:async()=>{}});assert.equal(calls,2);
 const {fetchSource}=require('../collector'),original=globalThis.fetch;
 try{for(const response of [{ok:true,headers:{get:()=>null},text:async()=>'{}'},{ok:true,headers:{get:()=> '1'},text:async()=>'invalid'}]){calls=0;globalThis.fetch=async()=>{calls++;return response;};await assert.rejects(fetchSource(url));assert.equal(calls,1);}}finally{globalThis.fetch=original;}
});
test('unapproved endpoints and certificate errors never fall back or retry',async()=>{
 let calls=0;await assert.rejects(fetchSourceText('https://sglapi.wellingtoninternational.com/ring',{request:async()=>{calls++;}}),/Unapproved/);assert.equal(calls,0);
 await assert.rejects(fetchSourceText(url,{request:async()=>{calls++;throw new TypeError('fetch failed',{cause:Object.assign(Error('bad certificate'),{code:'CERT_HAS_EXPIRED'})});}}),/1 attempts.*CERT_HAS_EXPIRED/);assert.equal(calls,1);
});
