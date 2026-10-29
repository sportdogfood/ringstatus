const test=require('node:test'),assert=require('node:assert/strict');
const {read}=require('../source-reader');
test('reports explicit timeout for scheduled recovery without retrying',async()=>{
 let calls=0;const value=await read('/classes/123',{customer_id:15,show_id:76},{transport:async()=>{calls++;const error=new Error('fixture');error.name='TimeoutError';throw error;}});
 assert.equal(calls,1);assert.equal(value.ok,false);assert.equal(value.metric.timeout,true);
});
test('uses existing transport with bounded decoded source and preserves response bytes',async()=>{
 let calls=0;
 const value=await read('/entries',{customer_id:15,show_id:200000076,page:1,sort_on:'number',sort_type:'asc',search_text:''},{transport:async(url,fallback,options)=>{calls++;assert.equal(new URL(url).hostname,'wef-schedules-decoded.gombcg.workers.dev');assert.equal(options.env.SGL_FETCH_TRANSPORT,'node');return {response:{status:200,headers:new Headers({'x-rs-decoded':'1'})},body:Buffer.from('{"horse":"HÖRSE"}')};}});
 assert.equal(calls,1);assert.equal(value.ok,true);assert.equal(Buffer.from(value.body,'base64').toString(),'{"horse":"HÖRSE"}');
});
test('original HTTP byte receipt includes UTF-8 BOM without text decoding loss',async()=>{
 const body=Buffer.concat([Buffer.from([0xef,0xbb,0xbf]),Buffer.from('{"value":1}')]);
 const prior=global.fetch;global.fetch=async()=>new Response(body,{headers:{'x-rs-decoded':'1'}});
 try{
  const result=await read('/classes/123',{customer_id:15,show_id:76},{transport:async(url,fallback)=>fallback(url)});
  assert.equal(result.ok,true);assert.deepEqual(Buffer.from(result.body,'base64'),body);
  assert.equal(result.metric.bytes,body.length);assert.equal(result.metric.sha256,require('node:crypto').createHash('sha256').update(body).digest('hex'));
 }finally{global.fetch=prior;}
});
test('non-200 produces safe diagnostics and never retries or returns response content',async()=>{
 let calls=0;const value=await read('/schedule',{date:'2026-10-29',show_id:200000076,customer_id:15},{transport:async()=>{calls++;return {response:{status:403,headers:new Headers({'content-type':'text/html','cf-ray':'example-ray','set-cookie':'private-cookie'})},text:'private source token'};}});
 assert.equal(calls,1);assert.equal(value.ok,false);assert.equal(value.metric.http,403);assert.equal(value.metric.headers['content-type'],'text/html');assert.doesNotMatch(JSON.stringify(value),/private|set-cookie/);
});
test('rejects arbitrary routes and missing decoded proof',async()=>{
 let calls=0;const transport=async()=>{calls++;return {response:{status:200,headers:new Headers()},text:'{}'};};
 await assert.rejects(()=>read('/auth/login',{customer_id:15,show_id:76},{transport}));assert.equal(calls,0);
 const value=await read('/classes/123',{customer_id:15,show_id:76},{transport});assert.equal(value.ok,false);assert.match(value.error,/decoding/);
});
