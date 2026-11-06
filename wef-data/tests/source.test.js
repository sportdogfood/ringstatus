const test=require('node:test'),assert=require('node:assert/strict');
const {read}=require('../source-reader');

test('transport logs preserve original message, nested cause codes and aggregate children',async()=>{
 const first=Object.assign(new Error('getaddrinfo ENOTFOUND example.invalid'),{code:'ENOTFOUND',syscall:'getaddrinfo',hostname:'example.invalid'});
 const second=Object.assign(new Error('connection reset'),{code:'ECONNRESET',address:'192.0.2.1',port:443});
 const cause=Object.assign(new AggregateError([first,second],'all connection attempts failed'),{code:'ECONNFAIL'});
 let calls=0;const value=await read('/classes/123',{customer_id:15,show_id:76},{transport:async()=>{calls++;throw new TypeError('fetch failed',{cause});}});
 assert.equal(calls,1);assert.equal(value.ok,false);assert.match(value.error,/TypeError: fetch failed/);
 assert.match(value.error,/Cause:\nAggregateError: all connection attempts failed/);assert.match(value.error,/code: ECONNFAIL/);
 assert.match(value.error,/code: ENOTFOUND/);assert.match(value.error,/hostname: example.invalid/);
 assert.match(value.error,/connection reset/);assert.match(value.error,/port: 443/);
});

test('message and cause reach existing durable error log with existing secret redaction',async()=>{
 const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),log=require('../attempt-log');
 const folder=fs.mkdtempSync(path.join(os.tmpdir(),'wef-error-offline-'));const old=process.env.AIRTABLE_TOKEN;
 process.env.AIRTABLE_TOKEN='fixture-secret-only';
 try{
  const cause=Object.assign(new Error('connect refused fixture-secret-only https://example.invalid/?token=fixture-query-secret'),{code:'ECONNREFUSED'});
  const value=await read('/classes/123',{customer_id:15,show_id:76},{transport:async()=>{throw new TypeError('fetch failed Authorization: Bearer fixture-secret-only',{cause});}});
  log.begin(folder,{workflow:'wef-data',base:'fixture'},'error-test');
  log.recordError(folder,'error-test',{message:'SourceFailure: '+value.error,category:'Collection',target:'class123'},'Collection');
  const text=fs.readFileSync(path.join(folder,'err-logs/error-test-0001.json'),'utf8');
  assert.match(text,/fetch failed/);assert.match(text,/connect refused/);assert.match(text,/ECONNREFUSED/);assert.match(text,/\[redacted\]/);assert.doesNotMatch(text,/fixture-secret-only|fixture-query-secret/);
 }finally{if(old===undefined)delete process.env.AIRTABLE_TOKEN;else process.env.AIRTABLE_TOKEN=old;assert.equal(path.dirname(fs.realpathSync(folder)),fs.realpathSync(os.tmpdir()));assert.ok(path.basename(folder).startsWith('wef-error-offline-'));fs.rmSync(folder,{recursive:true,force:true});}
});

test('a circular cause cannot prevent the source error response',async()=>{
 const error=new Error('original failure');error.cause=error;
 const value=await read('/classes/123',{customer_id:15,show_id:76},{transport:async()=>{throw error;}});
 assert.equal(value.ok,false);assert.match(value.error,/original failure/);assert.match(value.error,/circular cause/);
});
test('reports explicit timeout for scheduled recovery without retrying',async()=>{
 let calls=0;const value=await read('/classes/123',{customer_id:15,show_id:76},{transport:async()=>{calls++;const error=new Error('fixture');error.name='TimeoutError';throw error;}});
 assert.equal(calls,1);assert.equal(value.ok,false);assert.equal(value.metric.timeout,true);
});
test('uses existing transport with bounded decoded source and preserves response bytes',async()=>{
 let calls=0;
 const value=await read('/entries',{customer_id:15,show_id:200000076,page:1,sort_on:'number',sort_type:'asc',search_text:''},{transport:async(url,fallback,options)=>{calls++;assert.equal(new URL(url).hostname,'sglapi.wellingtoninternational.com');assert.equal(options.env.SGL_FETCH_TRANSPORT,'node');return {response:{status:200,headers:new Headers({'x-rs-decoded':'1'})},body:Buffer.from('{"horse":"HÖRSE"}')};}});
 assert.equal(calls,1);assert.equal(value.ok,true);assert.equal(Buffer.from(value.body,'base64').toString(),'{"horse":"HÖRSE"}');
});
test('local decoder fetches Wellington once and hashes the decoded bytes passed to Data',async()=>{
 const oldFetch=global.fetch,oldToken=process.env.SGL_TOKEN,oldCookie=process.env.SGL_COOKIE;
 const payload={rings:[{ring_name:'H�RSE'}]},expected=Buffer.from(JSON.stringify(payload));
 let calls=0;
 process.env.SGL_TOKEN='fixture';process.env.SGL_COOKIE='sgl=fixture';
 global.fetch=async(url,options)=>{
  calls++;assert.equal(new URL(url).hostname,'sglapi.wellingtoninternational.com');
  assert.equal(options.headers.Authorization,'Bearer fixture');assert.equal(options.headers.Cookie,'sgl=fixture');
  return new Response(JSON.stringify(payload));
 };
 try{
  const result=await read('/schedule',{date:'2026-11-06',customer_id:15,show_id:76});
  assert.equal(calls,1);assert.equal(result.ok,true);assert.deepEqual(Buffer.from(result.body,'base64'),expected);
  assert.equal(result.metric.bytes,expected.length);assert.equal(result.metric.sha256,require('node:crypto').createHash('sha256').update(expected).digest('hex'));
 }finally{global.fetch=oldFetch;for(const [k,v] of Object.entries({SGL_TOKEN:oldToken,SGL_COOKIE:oldCookie})){if(v===undefined)delete process.env[k];else process.env[k]=v;}}
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
