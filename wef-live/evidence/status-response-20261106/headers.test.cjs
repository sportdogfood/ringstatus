'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const activity=require('./candidate/wef-live/activity'),source=require('./candidate/wef-live/source');
const UA='Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1';
const originalHeaders={Accept:'application/json','Cache-Control':'no-cache'};
test('status request preserves exact owner User-Agent,URL/query/timeout and uses proven public website headers',async()=>{
 const old=Date.now;Date.now=()=>123456789;let captured;try{
  assert.equal(await activity.readStatus({customer:15},async(url,options)=>{captured={url,options};return{ok:true,json:async()=>true};}),true);
 }finally{Date.now=old;}
 assert.equal(captured.url,'https://sglapi.wellingtoninternational.com/homepage/getLiveClassStatus?customer_id=15&datetimestamp=123456789');const h=captured.options.headers;assert.equal(h['User-Agent'],UA);assert.equal(h.Accept,'application/json, text/plain, */*');assert.equal(h['sgl-request-origin'],'SGL-API');assert.equal(h.Origin,'https://www.wellingtoninternational.com');assert.equal(h.Referer,'https://www.wellingtoninternational.com/');assert.equal(h.authority,'sglapi.wellingtoninternational.com');assert.equal(h.path,new URL(captured.url).pathname+new URL(captured.url).search);assert.equal(h['Cache-Control'],'no-cache');assert.equal(h['sec-fetch-site'],'same-site');assert(!h.Cookie&&!h.Authorization);assert(captured.options.signal instanceof AbortSignal);assert.equal(captured.options.method,undefined);
});

test('explicit true/false and existing boolean wrappers remain valid; empty/unknown shapes never become false',()=>{
 for(const b of [true,false]){assert.equal(activity.statusValue(b),b);for(const k of ['status','is_live','live','payload'])assert.equal(activity.statusValue({[k]:b}),b);}
 for(const p of [{},[],null,1,0,'true','false',{status:'true'},{error:'unavailable'}])assert.throws(()=>activity.statusValue(p),/expected explicit boolean/);
 assert.throws(()=>activity.statusValue({}),/object keys \[\]/);
});

test('HTTP or unreadable status fails once; next-check retry remains owned by existing cadence',async()=>{
 let calls=0;await assert.rejects(activity.readStatus({customer:15},async()=>{calls++;return{ok:false,status:403};}),/HTTP 403/);assert.equal(calls,1);
 await assert.rejects(activity.readStatus({customer:15},async()=>({ok:true,json:async()=>({})})),/object keys \[\]/);
});
for(const [name,url]of [['ListAjax','https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/ListAjax?from_wp_api=true'],['LiveScoreWidget','https://sgl.wellingtoninternational.com/iphone.php/esp/webservice/LiveScoreWidget'],['getLiveClassData','https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/getLiveClassData?show_id=200000077&cid=200030963&t=123456789']]){
 test(name+' request preserves exact URL/query/other headers and adds only User-Agent',async()=>{const old=global.fetch;let captured;global.fetch=async(target,options)=>{captured={target,options};return{ok:true,text:async()=> '[]'};};try{assert.deepEqual(await source.get(url),[]);}finally{global.fetch=old;}
  assert.equal(captured.target,url);assert.deepEqual(captured.options.headers,{...originalHeaders,'User-Agent':UA});assert(captured.options.signal instanceof AbortSignal);assert.equal(captured.options.method,undefined);
 });
}
test('decoded Cloudflare request retains original headers,URL and decode confirmation check',async()=>{
 const old=global.fetch,url='https://wef-schedules-decoded.gombcg.workers.dev/classes/200030963?show_id=200000077&customer_id=15&cgid=-1';let captured;global.fetch=async(target,options)=>{captured={target,options};return{ok:true,text:async()=>'{"class":{}}',headers:{get:()=> '1'}};};try{assert.deepEqual(await source.get(url,true),{class:{}});}finally{global.fetch=old;}assert.equal(captured.target,url);assert.deepEqual(captured.options.headers,originalHeaders);
});
test('other Wellington paths do not receive the added header',async()=>{const old=global.fetch;let captured;global.fetch=async(target,options)=>{captured=options;return{ok:true,text:async()=> '[]'};};try{await source.get('https://sgl.wellingtoninternational.com/other');}finally{global.fetch=old;}assert.deepEqual(captured.headers,originalHeaders);});
test('direct-feed retry retains existing one-retry behavior and identical new headers',async()=>{const old=global.fetch;const calls=[];global.fetch=async(target,options)=>{calls.push(options.headers);return{ok:calls.length>1,status:calls.length===1?503:200,text:async()=> '[]'};};try{assert.deepEqual(await source.get('https://sgl.wellingtoninternational.com/iphone.php/esp/webservice/LiveScoreWidget'),[]);}finally{global.fetch=old;}assert.equal(calls.length,2);assert(calls.every(h=>h['User-Agent']===UA));});
