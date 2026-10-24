const test=require('node:test'),assert=require('node:assert/strict'),{readSource}=require('../../lib/wef-source-read');
const url='https://wef-schedules-decoded.gombcg.workers.dev/people/50121';
const response=(status,text='{}',decoded='1')=>({response:{ok:status===200,status,headers:{get:()=>decoded}},text});
test('one transient source retry recovers; no retry for authentication or malformed success',async()=>{
 let calls=0;const events=[];const payload=await readSource(url,{decoded:true,pause:async()=>{},onRetry:e=>events.push(e),request:async()=>++calls===1?response(502):response(200,'{"trips":[]}')});assert.equal(calls,2);assert.deepEqual(payload,{trips:[]});assert.equal(events.length,1);
 for(const r of [response(401),response(200,'bad'),response(200,'{}',null)]){calls=0;await assert.rejects(readSource(url,{decoded:true,request:async()=>{calls++;return r;}}));assert.equal(calls,1);}
});
test('persistent 502 is bounded to two attempts and fails rather than empty success',async()=>{let calls=0;await assert.rejects(readSource(url,{pause:async()=>{},onRetry:()=>{},request:async()=>{calls++;return response(502);}}),/HTTP 502/);assert.equal(calls,2);});
test('timeout may retry once; arbitrary programming errors never retry',async()=>{let calls=0;assert.deepEqual(await readSource(url,{pause:async()=>{},onRetry:()=>{},request:async()=>{if(++calls===1)throw Object.assign(Error('timeout'),{name:'TimeoutError'});return response(200);}}),{});assert.equal(calls,2);calls=0;await assert.rejects(readSource(url,{request:async()=>{calls++;throw Error('bug');}}),/bug/);assert.equal(calls,1);});
