"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {webcrypto,randomBytes}=require('node:crypto');
const root=path.resolve(__dirname,'../..');
// Historical shared-proxy compatibility fixtures use its archived candidate.
// The active tenant route is tested below against the isolated Worker endpoint.
const source=fs.readFileSync(path.join(__dirname,'../evidence/proxy-decode-20261021/broad-tooth-candidate.js'),'utf8');
const baseline=fs.readFileSync(path.join(__dirname,'../evidence/proxy-decode-20261021/broad-tooth-before.js'),'utf8');
const salt='5874a2ec29e1d30258abe7a00220fd5d',token='offline-fixture-token';
async function protect(fields,{auth='Bearer '+token,minute='29877080'}={}){
 const key=await webcrypto.subtle.importKey('raw',await webcrypto.subtle.digest('SHA-256',new TextEncoder().encode(salt+'|'+auth+'|'+minute)),{name:'AES-GCM'},false,['encrypt']);
 const iv=randomBytes(12),cipher=await webcrypto.subtle.encrypt({name:'AES-GCM',iv},key,new TextEncoder().encode(JSON.stringify(fields)));
 return Buffer.concat([Buffer.from(minute),iv,Buffer.from(cipher)]).toString('base64');
}
function worker(code,fetch){return new Function('fetch','crypto',code.replace('export default','return'))(fetch,webcrypto);}
async function run(url,payload,{code=source,status=200,env={SGL_TOKEN:token}}={}){
 const calls=[];const app=worker(code,async(url,options)=>{calls.push({url:String(url),options});return new Response(typeof payload==='string'?payload:JSON.stringify(payload),{status,headers:{'content-type':'application/json'}});});
 const response=await app.fetch(new Request('https://proxy.test'+url),env);return {response,text:await response.text(),calls};
}
test('opt-in restores nested protected IDs with the exact upstream authorization',async()=>{
 const ids={entry_id:200245506,class_id:200030347,horse_id:200002670,rider_id:50121};
 const payload={people:{people_id:50121},trips:[{entry_id:null,horse_id:null,_:await protect(ids)}]};
 const r=await run('/people/50121?pid=50121&show_id=200000075&customer_id=15&decode=1',payload);
 assert.equal(r.response.status,200);assert.deepEqual(JSON.parse(r.text).trips[0],ids);
 assert.equal(r.response.headers.get('x-rs-decoded'),'1');assert.equal(r.calls.length,1);
 assert.equal(r.calls[0].options.headers.Authorization,'Bearer '+token);
 assert.equal(new URL(r.calls[0].url).searchParams.has('decode'),false);
 assert.doesNotMatch(r.text,/offline-fixture-token/);
});
test('without opt-in the deployed baseline body, headers and upstream requests stay identical',async()=>{
 const body={trips:[{entry_id:null,_:await protect({entry_id:42})}]};
 for(const url of ['/people/50121?pid=50121&show_id=75&customer_id=15','/classes/123?show_id=75&customer_id=15&cgid=-1','/ring?customer_id=15','/schedule?date=10%2F22%2F2026&show_id=75&customer_id=15','/people/50121?pid=50121&decode=0']){
  const old=await run(url,body,{code:baseline}),now=await run(url,body);
  assert.equal(now.response.status,old.response.status);assert.equal(now.text,old.text);assert.deepEqual([...now.response.headers],[...old.response.headers]);assert.deepEqual(now.calls,old.calls);
 }
});
test('opt-in accepts identified empty responses without inventing records',async()=>{
 const payload={people:{people_id:8778},trips:[],total_trips:0};const r=await run('/people/8778?decode=1',payload);
 assert.equal(r.response.status,200);assert.deepEqual(JSON.parse(r.text),payload);assert.equal(r.response.headers.get('x-rs-decoded'),'1');
});
test('opt-in recursively restores objects and arrays and rejects unsafe field maps',async()=>{
 const r=await run('/people/50121?decode=1',{people:{people_id:50121,_:await protect({name:'Isabela'})},trips:[{_:await protect({entry_id:42})}]});
 assert.deepEqual(JSON.parse(r.text),{people:{people_id:50121,name:'Isabela'},trips:[{entry_id:42}]});
 for(const fields of [[],null,JSON.parse('{"__proto__":{"polluted":true}}'),{_: 'nested'}]){
  const bad=await run('/people/50121?decode=1',{trips:[{_:await protect(fields)}]});assert.equal(bad.response.status,502);
 }
});
test('wrong key, corrupt blobs and invalid JSON stop decoding without partial records',async()=>{
 const blob=await protect({entry_id:42});
 for(const payload of [{trips:[{_:blob},{_:'broken'}]},{trips:[{_:await protect({entry_id:42},{auth:'Bearer other-token'})}]},'not-json']){
  const r=await run('/people/50121?decode=1',payload);assert.equal(r.response.status,502);assert.match(r.text,/decod/i);assert.doesNotMatch(r.text,/offline-fixture-token|"entry_id":42/);
 }
});
test('upstream failures remain failures and decode is confined to individual people requests',async()=>{
 const r=await run('/people/50121?decode=1',{message:'upstream unavailable'},{status:503});assert.equal(r.response.status,503);
 const other=await run('/classes/123?decode=1',{});assert.equal(other.response.status,400);assert.equal(other.calls.length,0);
});
test('existing 401-to-full retry remains identical without opt-in; opt-in authenticates initially',async()=>{
 async function request(code,url){const calls=[];const app=worker(code,async(u,o)=>{calls.push({url:String(u),headers:o.headers});return new Response(JSON.stringify(o.headers.Authorization?{people:{people_id:50121},trips:[]}:{message:'unauthorized'}),{status:o.headers.Authorization?200:401});});const r=await app.fetch(new Request('https://proxy.test'+url),{SGL_TOKEN:token,SGL_COOKIE:'fixture-cookie'});return {status:r.status,text:await r.text(),headers:[...r.headers],calls};}
 assert.deepEqual(await request(source,'/people/50121?pid=50121'),await request(baseline,'/people/50121?pid=50121'));
 const opt=await request(source,'/people/50121?pid=50121&decode=1');assert.equal(opt.status,200);assert.equal(opt.calls.length,1);
 const refresh=await run('/people/50121?decode=1',{_t:'returned-token',trips:[]});assert.equal(refresh.response.status,502);assert.doesNotMatch(refresh.text,/returned-token/);
});
test('tenant rejects an unconfirmed proxy response and wrong person without any fallback request',async()=>{
 const code=fs.readFileSync(process.env.WEF_TENANT_SOURCE_UNDER_TEST||path.join(__dirname,'../source.js'),'utf8');
 for(const [headers,payload,message] of [[new Headers(),{people:{people_id:50121},trips:[]},/confirm/],[new Headers({'x-rs-decoded':'1'}),{people:{people_id:80970},trips:[]},/identity/]]){
  let calls=0;const module={exports:{}};new Function('require','module','exports',code)((name)=>name==='./settings'?{}:name==='./lib/sgl_fetch_adapter'?{fetchTextWithConfiguredTransport:async()=>{calls++;return {response:{ok:true,headers},text:JSON.stringify(payload)};}}:require(path.join(__dirname,'..',name)),module,module.exports);
  await assert.rejects(()=>module.exports.fetchPeople(50121,75,15),message);assert.equal(calls,1);
 }
});
test('tenant requests the opt-in proxy route and retains people identity validation',async()=>{
 const code=fs.readFileSync(process.env.WEF_TENANT_SOURCE_UNDER_TEST||path.join(__dirname,'../source.js'),'utf8');let url;
 const transport=async(endpoint)=>{url=endpoint;return {response:{ok:true,headers:new Headers({'x-rs-decoded':'1'})},text:JSON.stringify({people:{people_id:50121},show_id:75,trips:[],total_trips:0})};};
 const module={exports:{}};new Function('require','module','exports',code)((name)=>name==='./settings'?{}:name==='./lib/sgl_fetch_adapter'?{fetchTextWithConfiguredTransport:transport}:require(path.join(__dirname,'..',name)),module,module.exports);
 await module.exports.fetchPeople(50121,75,15);
 const endpoint=new URL(url);assert.equal(endpoint.origin,'https://wef-schedules-decoded.gombcg.workers.dev');assert.equal(endpoint.pathname,'/people/50121');assert.equal(endpoint.searchParams.has('decode'),false);assert.equal(endpoint.searchParams.get('pid'),'50121');
 await assert.rejects(()=>module.exports.fetchPeople(0,75,15),/trainer ID/);
});
