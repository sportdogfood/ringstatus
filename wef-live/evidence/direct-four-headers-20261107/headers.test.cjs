"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const activity=require('./candidate/wef-live/activity'),source=require('./candidate/wef-live/source');
const expected={Accept:'application/json','Cache-Control':'no-cache','User-Agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1','Sec-Fetch-Site':'cross-site'};
globalThis.fetch=async()=>{throw Error('Network forbidden');};
test('status uses direct SGL API, original query, exactly four approved headers and existing boolean parser',async()=>{
 let url;const value=await activity.readStatus({customer:15},async(u,o)=>{url=new URL(u);assert.deepEqual(o.headers,expected);assert.ok(o.signal instanceof AbortSignal);return new Response('true');});
 assert.equal(value,true);assert.equal(url.origin,'https://sglapi.wellingtoninternational.com');assert.equal(url.pathname,'/homepage/getLiveClassStatus');assert.equal(url.searchParams.get('customer_id'),'15');assert.match(url.searchParams.get('datetimestamp'),/^\d+$/);assert.equal([...url.searchParams].length,2);
 assert.equal(await activity.readStatus({customer:15},async()=>new Response('false')),false);
 await assert.rejects(activity.readStatus({customer:15},async()=>new Response('{}')),/Unreadable live status/);
 await assert.rejects(activity.readStatus({customer:15},async()=>new Response('',{status:403})),/HTTP 403/);
});
test('groups/widget/class requests use direct SGL, exact parameters and four headers; decoded URL unchanged',async()=>{
 const old=globalThis.fetch,seen=[];globalThis.fetch=async(u,o)=>{seen.push(u);assert.deepEqual(o.headers,expected);assert.ok(o.signal instanceof AbortSignal);return new Response('[{"show_id":200000077}]');};
 try{for(const u of [source.endpoints.groups,source.endpoints.widget,source.endpoints.detail({show:200000077},200031255)])assert.deepEqual(await source.get(u),[{show_id:200000077}]);}finally{globalThis.fetch=old;}
 assert.equal(seen[0],'https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/ListAjax?from_wp_api=true');assert.equal(seen[1],'https://sgl.wellingtoninternational.com/iphone.php/esp/webservice/LiveScoreWidget');assert.match(seen[2],/^https:\/\/sgl\.wellingtoninternational\.com\/iphonev2\/index\.php\/esp\/liveclassv2\/getLiveClassData\?show_id=200000077&cid=200031255&t=\d+$/);
 assert.equal(source.endpoints.decoded({show:200000077,customer:15},200031255),'https://wef-schedules-decoded.gombcg.workers.dev/classes/200031255?show_id=200000077&customer_id=15&cgid=-1');
});
test('shared read error handling remains: invalid JSON and 403 rejected, 503 gets existing bounded retry',async()=>{
 const old=globalThis.fetch;let n=0;
 try{globalThis.fetch=async()=>new Response('bad');await assert.rejects(source.get(source.endpoints.groups),/Unreadable source JSON/);
 globalThis.fetch=async()=>new Response('',{status:403});await assert.rejects(source.get(source.endpoints.groups),/HTTP 403/);
 globalThis.fetch=async(u,o)=>{assert.deepEqual(o.headers,expected);return ++n===1?new Response('',{status:503}):new Response('[]');};assert.deepEqual(await source.get(source.endpoints.groups),[]);assert.equal(n,2);
 }finally{globalThis.fetch=old;}
});
