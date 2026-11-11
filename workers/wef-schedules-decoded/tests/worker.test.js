const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {webcrypto,randomBytes}=require('node:crypto');
const source=fs.readFileSync(require('node:path').join(__dirname,'../worker.js'),'utf8');
const salt='5874a2ec29e1d30258abe7a00220fd5d',token='offline-token';
test('cleaned profiles send tested browser headers without ordinary protocol fields',async()=>{
 for(const profile of ['windows-clean','android-clean']){
  for(const route of ['/schedule?date=2026-11-14&show_id=200000078&customer_id=15','/classes/123?show_id=200000078&customer_id=15']){
   const result=await run(route,{show:{show_id:200000078},rings:[]},{env:{SGL_TOKEN:token,SGL_COOKIE:'test-cookie',SGL_HEADER_PROFILE:profile}});
   assert.equal(result.status,200);
   const headers=result.calls[0].options.headers;
   for(const name of ['authority','method','path','scheme'])assert.equal(Object.hasOwn(headers,name),false);
   assert.match(headers['User-Agent'],profile==='windows-clean'?/Windows NT.*Edg\/158/:/Android 16.*Edg\/157/);
   if(profile==='windows-clean'){
    assert.equal(headers['sec-ch-ua-mobile'],'?0');
    assert.equal(headers['sec-ch-ua-platform'],'"Windows"');
   }
   assert.equal(headers.Authorization,'Bearer '+token);
   assert.equal(headers.Cookie,'test-cookie');
  }
 }
});
async function blob(fields,auth='Bearer '+token){const key=await webcrypto.subtle.importKey('raw',await webcrypto.subtle.digest('SHA-256',new TextEncoder().encode(salt+'|'+auth+'|29877080')),{name:'AES-GCM'},false,['encrypt']);const iv=randomBytes(12),cipher=await webcrypto.subtle.encrypt({name:'AES-GCM',iv},key,new TextEncoder().encode(JSON.stringify(fields)));return Buffer.concat([Buffer.from('29877080'),iv,Buffer.from(cipher)]).toString('base64');}
async function run(route,payload,{status=200,method='GET',env={SGL_TOKEN:token,SGL_COOKIE:'test-cookie'}}={}){const calls=[];const app=new Function('fetch','crypto',source.replace('export default','return'))(async(url,options)=>{calls.push({url:String(url),options});return new Response(typeof payload==='string'?payload:JSON.stringify(payload),{status});},webcrypto);const r=await app.fetch(new Request('https://isolated.test'+route,{method}),env);return {status:r.status,headers:r.headers,text:await r.text(),calls};}
test('schedule restores class IDs and preserves source evidence and schedule keys',async()=>{const encoded=await blob({class_id:123,estimated_start_time:'08:00:00'});const p={show:{show_id:75},show_date:'2026-10-25',classes:[{class_group_id:99,class_number:770,class_name:'Class',class_id:null,_:encoded}]};const r=await run('/schedule?date=2026-10-25&show_id=75&customer_id=15',p);assert.equal(r.status,200);const decoded=JSON.parse(r.text);assert.equal(decoded.classes[0].class_id,123);assert.equal(decoded.classes[0]._,encoded);const {normalizeSchedulePayload}=require('../../../wef-schedules/normalizer');const opts={scope:{app_show_idv2:75,app_sql_datev2:'2026-10-25'},source:'fixture'};const before=normalizeSchedulePayload(p,opts).rows,after=normalizeSchedulePayload(decoded,opts).rows;assert.deepEqual(before.map(r=>r.key),after.map(r=>r.key));assert.equal(after[0].fields.class_id,123);assert.equal(r.headers.get('x-rs-decoded'),'1');assert.equal(r.headers.get('cache-control'),'no-store');assert.equal(r.calls.length,1);assert.equal(r.calls[0].options.headers.Authorization,'Bearer '+token);assert.equal(new URL(r.calls[0].url).hostname,'sglapi.wellingtoninternational.com');});
test('people restores IDs and removes protected blobs without returning credentials',async()=>{const r=await run('/people/50121?pid=50121&show_id=75&customer_id=15',{people:{people_id:50121},trips:[{entry_id:null,_:await blob({entry_id:42,horse_id:43,rider_id:50121,class_id:123})}]});assert.equal(r.status,200);assert.equal(JSON.parse(r.text).trips[0].entry_id,42);assert.equal(Object.hasOwn(JSON.parse(r.text).trips[0],'_'),false);assert.doesNotMatch(r.text,/offline-token|test-cookie/);});
test('empty show day remains empty and ring discovery is supported',async()=>{for(const route of ['/ring?customer_id=15','/schedule?date=00%2F00%2F00&show_id=75&customer_id=15']){const p={show:{show_id:75},rings:[]};const r=await run(route,p);assert.equal(r.status,200);assert.deepEqual(JSON.parse(r.text),p);}});
test('empty source objects fail without a decoded-success marker on every data route',async()=>{
 for(const route of ['/schedule?date=2026-11-06&show_id=75&customer_id=15','/classes/123?show_id=75&customer_id=15','/classes?show_id=75&customer_id=15','/people/50121?pid=50121&show_id=75&customer_id=15','/ring?customer_id=15','/ring/25?show_date=2026-11-06&show_id=75&customer_id=15','/entries?show_id=75&customer_id=15&page=1']){
  const r=await run(route,' \n{}\n ');assert.equal(r.status,502);assert.equal(r.headers.has('x-rs-decoded'),false);assert.equal(JSON.parse(r.text).statusCode,502);assert.equal(r.calls.length,1);assert.doesNotMatch(r.text,/offline-token|test-cookie/);
 }
});
test('a protected object that decodes to an empty object also fails',async()=>{
 const r=await run('/classes/123?show_id=75&customer_id=15',{_:await blob({})});assert.equal(r.status,502);assert.equal(r.headers.has('x-rs-decoded'),false);assert.equal(r.calls.length,1);
});
test('identified classes and directory pages may legitimately contain no rows',async()=>{
 for(const [route,payload] of [['/classes/123?show_id=75&customer_id=15',{class:{class_id:123},trips:[]}],['/entries?show_id=75&customer_id=15&page=1',{show_id:75,entries:[],page:1,total_entries:0}]]){
  const r=await run(route,payload);assert.equal(r.status,200);assert.equal(r.headers.get('x-rs-decoded'),'1');assert.deepEqual(JSON.parse(r.text),payload);
 }
});
test('bad key, damaged blobs, invalid JSON, refreshed token and missing auth fail closed',async()=>{for(const p of [{trips:[{_:await blob({entry_id:42},'Bearer wrong')}]},{trips:[{_:'broken'}]},'not-json',{_t:'new-token',trips:[]}]){const r=await run('/people/50121?pid=50121&show_id=75&customer_id=15',p);assert.equal(r.status,502);assert.doesNotMatch(r.text,/new-token|offline-token|test-cookie/);}const r=await run('/ring?customer_id=15',{}, {env:{}});assert.equal(r.status,503);assert.equal(r.calls.length,0);});
test('unapproved routes, methods and query parameters cannot cause upstream actions',async()=>{for(const [route,method]of [['/auth/login','GET'],['/people/50121?pid=50121&show_id=75&customer_id=15','POST'],['/schedule?date=2026-10-25&show_id=75&customer_id=15&url=https://other.test','GET']]){const r=await run(route,{}, {method});assert.ok([400,404,405].includes(r.status));assert.equal(r.calls.length,0);}});
test('upstream failures stay failures, without retries or leaking the body',async()=>{const r=await run('/ring?customer_id=15','private upstream error',{status:401});assert.equal(r.status,401);assert.equal(r.calls.length,1);assert.doesNotMatch(r.text,/private upstream error/);});

const returnedToken=(claims={anon:true,exp:Math.floor(Date.now()/1000)+2592000})=>'header.'+Buffer.from(JSON.stringify(claims)).toString('base64url')+'.signature';
async function tokenSequence(responses){
 const calls=[],pauses=[],saved=[],logs=[];
 const app=new Function('fetch','crypto','setTimeout','console',source.replace('export default','return'))(async(url,options)=>{
  calls.push({url:String(url),headers:{...options.headers}});
  const response=responses[calls.length-1];assert.ok(response,'Unexpected extra source request');
  return response instanceof Response?response:Response.json(response);
 },webcrypto,(fn,ms)=>{pauses.push(ms);fn();},{error:message=>logs.push(message)});
 const response=await app.fetch(new Request('https://isolated.test/classes/123?show_id=75&customer_id=15&cgid=-1'),{SGL_TOKEN:token,SGL_COOKIE:'test-cookie',SGL_HEADER_PROFILE:'windows-clean',SGL_ON_TOKEN:(next,expires)=>saved.push({token:next,expires})});
 return {response,text:await response.text(),calls,pauses,saved,logs};
}
test('returned anonymous token is saved and the identical class GET is replayed and decoded',async()=>{
 const next=returnedToken(),payload={class:{class_id:123},trips:[{_:await blob({trip_id:42,entry_id:7},'Bearer '+next)}]};
 const r=await tokenSequence([{_t:next,class:{class_id:123},trips:[{trip_id:null}]},payload]);
 assert.equal(r.response.status,200);assert.equal(r.response.headers.get('x-rs-decoded'),'1');
 assert.deepEqual(JSON.parse(r.text),{class:{class_id:123},trips:[{trip_id:42,entry_id:7}]});
 assert.equal(r.calls.length,2);assert.equal(r.calls[0].url,r.calls[1].url);assert.equal(r.calls[1].headers.Authorization,'Bearer '+next);
 assert.deepEqual({...r.calls[0].headers,Authorization:'Bearer '+next},r.calls[1].headers);
 assert.deepEqual(r.pauses,[1000]);assert.equal(r.saved[0].token,next);assert.ok(r.saved[0].expires>Date.now());
 assert.doesNotMatch(r.text,/_t|signature/);assert.equal(r.logs.length,0);
});
test('invalid, expired or account returned tokens cannot replay or become success',async()=>{
 for(const next of [null,'','broken',returnedToken({anon:false,exp:Math.floor(Date.now()/1000)+2592000}),returnedToken({anon:true,exp:1})]){
  const r=await tokenSequence([{_t:next,trips:[]}]);assert.equal(r.response.status,502);assert.equal(r.calls.length,1);assert.equal(r.saved.length,0);assert.equal(r.pauses.length,0);assert.doesNotMatch(r.text,/offline-token|test-cookie|signature/);
 }
});
test('a second returned token is saved without a third request or a decoded success',async()=>{
 const first=returnedToken(),second=returnedToken({anon:true,exp:Math.floor(Date.now()/1000)+2592001});
 const r=await tokenSequence([{_t:first},{_t:second}]);assert.equal(r.response.status,502);assert.equal(r.calls.length,2);assert.deepEqual(r.saved.map(x=>x.token),[first,second]);assert.deepEqual(r.pauses,[1000]);assert.equal(r.response.headers.has('x-rs-decoded'),false);assert.doesNotMatch(r.logs.join('\n'),/header\.|signature/);
});
test('replay redirects and HTTP failures remain failures after adopting the token',async()=>{
 for(const status of [302,503]){const r=await tokenSequence([{_t:returnedToken()},new Response('private',{status})]);assert.equal(r.response.status,status===302?502:status);assert.equal(r.calls.length,2);assert.equal(r.saved.length,1);assert.equal(r.response.headers.has('x-rs-decoded'),false);assert.doesNotMatch(r.text,/private|signature/);}
});
test('uses Workers-supported manual redirects and blocks them without forwarding credentials',async()=>{const r=await run('/ring?customer_id=15','redirect body',{status:302});assert.equal(r.calls[0].options.redirect,'manual');assert.equal(r.status,502);assert.equal(r.calls.length,1);assert.doesNotMatch(r.text,/redirect body|offline-token|test-cookie/);});
test('entries decodes a single explicitly scoped directory page',async()=>{
 const route='/entries?customer_id=15&show_id=76&page=2&sort_on=number&sort_type=asc&search_text=';
 const payload={entries:[{entry_id:null,_:await blob({entry_id:123,horse_id:456})}],show_id:'76',page:2,total_entries:20,records_per_page:10,sort:{sort_on:'number',sort_type:'asc'},term:''};
 const r=await run(route,payload);assert.equal(r.status,200);assert.equal(r.headers.get('x-rs-decoded'),'1');
 assert.equal(JSON.parse(r.text).entries[0].entry_id,123);assert.equal(JSON.parse(r.text).entries[0].horse_id,456);
 assert.equal(Object.hasOwn(JSON.parse(r.text).entries[0],'_'),false);assert.equal(r.calls.length,1);
 assert.equal(new URL(r.calls[0].url).searchParams.get('page'),'2');assert.doesNotMatch(r.text,/offline-token|test-cookie/);
});
test('entries rejects unbounded, malformed or different-filter requests without upstream calls',async()=>{
 for(const suffix of ['page=0','page=-1','page=1.5','page=1&all=1','page=1&sort_on=horse','page=1&sort_type=desc','page=1&search_text=abc']){
  const route='/entries?customer_id=15&show_id=76&'+suffix;
  const r=await run(route,{});assert.equal(r.status,400);assert.equal(r.calls.length,0);
 }
});

test('runner and fallback omit copied authority/method/scheme while preserving other headers',async()=>{for(const filename of ["C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\wef-data\\runtime\\wef-decoder-runner\\worker.js","C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\workers\\wef-schedules-decoded\\worker.js"]){const code=fs.readFileSync(filename,'utf8');const calls=[];const app=new Function('fetch','crypto',code.replace('export default','return'))(async(url,options)=>{calls.push(options.headers);return Response.json({class:{class_id:123},trips:[]});},webcrypto);for(const route of ['/schedule?date=2026-11-07&show_id=75&customer_id=15','/classes/123?show_id=75&customer_id=15']){const response=await app.fetch(new Request('https://fixture.test'+route),{SGL_TOKEN:token,SGL_COOKIE:'test-cookie'});assert.equal(response.status,200);}for(const [index,headers] of calls.entries()){for(const name of ['authority','method','scheme'])assert.equal(Object.hasOwn(headers,name),false);assert.equal(headers.Authorization,'Bearer '+token);assert.equal(headers.Cookie,'test-cookie');assert.equal(headers['sgl-request-origin'],'SGL-API');if(filename.includes('runtime')&&index===0){assert.equal(headers.path,undefined);}else{assert.ok(headers.path.startsWith('/'));}}}});
