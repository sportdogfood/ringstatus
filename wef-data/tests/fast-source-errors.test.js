'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const source=require('../fast-source');
const params={customer_id:15,show_id:200000078,date:'2026-11-14'};
test('both preparation attempts retain their own cause, route, scope and safe headers',async()=>{
 let calls=0;
 const r=await source.read('/schedule',params,true,null,async()=>{
  calls++;return Response.json({message:'Source request deferred',reason:calls===1?'runner-http-502: ECONNRESET':'Config response is missing _t',statusCode:502},{status:502,headers:{'X-RS-Source-Lane':calls===1?'runner':'cloudflare-fallback','X-RS-Header-Profile':'windows-clean','X-RS-Failure-Reason':calls===1?'ECONNRESET':'Config response is missing _t','Set-Cookie':'private-value'}});
 });
 assert.equal(calls,2);assert.equal(r.ok,false);assert.equal(r.attempts.length,2);
 assert.match(r.attempts[0].failure.reason,/ECONNRESET/);assert.match(r.attempts[1].failure.reason,/missing _t/);
 assert.equal(r.attempts[0].headers['x-rs-source-lane'],'runner');assert.equal(r.metric.params.date,'2026-11-14');assert.doesNotMatch(JSON.stringify(r),/private-value|set-cookie/);
});
test('network exception retains nested code and cause without a new request',async()=>{
 let calls=0;const cause=Object.assign(new Error('socket closed'),{code:'ECONNRESET'});
 const r=await source.once('/classes/123',{customer_id:15,show_id:200000078},'iphone',null,async()=>{calls++;throw new Error('fetch failed',{cause});});
 assert.equal(calls,1);assert.equal(r.ok,false);assert.match(r.error,/ECONNRESET/);assert.match(r.error,/socket closed/);assert.equal(r.metric.http,null);
});
test('HTTP failure metadata and its message redact bearer credentials',async()=>{
 const r=await source.once('/schedule',params,null,'defer',async()=>Response.json({message:'Source request deferred',reason:'Runner error Bearer private-token',_t:'secret-token'},{status:502}));
 assert.equal(r.ok,false);assert.doesNotMatch(JSON.stringify(r),/private-token|secret-token/);assert.match(r.error,/Runner error/);
});

test('plain cookies, returned-token text and configured secrets do not enter new diagnostics',async t=>{
 const old=process.env.SGL_TOKEN;process.env.SGL_TOKEN='configured-private-value';t.after(()=>{if(old===undefined)delete process.env.SGL_TOKEN;else process.env.SGL_TOKEN=old;});
 const reason='ECONNRESET configured-private-value\n_t="returned-private"\nCookie: session=private-cookie';
 const r=await source.once('/schedule',params,null,'defer',async()=>Response.json({message:'Source request deferred',reason},{status:502,headers:{'x-rs-failure-reason':'_t=returned-private'}}));
 assert.doesNotMatch(JSON.stringify(r),/configured-private-value|returned-private|private-cookie/);assert.match(r.error,/ECONNRESET/);
});

test('quoted cookie and returned token spellings are redacted from structured reason text',async()=>{
 const r=await source.once('/schedule',params,null,'defer',async()=>Response.json({message:'Source failed',reason:'ECONNRESET "cookie":"private-cookie", "_t":"private-returned"'},{status:502}));
 assert.doesNotMatch(JSON.stringify(r),/private-cookie|private-returned/);assert.match(r.error,/ECONNRESET/);
});
