const test=require('node:test'),assert=require('node:assert/strict');
const {endpoints,get}=require('../source'),{readStatus}=require('../activity');
const origin='https://wef-schedules-decoded.gombcg.workers.dev';
test('all Live endpoint builders target shared proxy with unchanged route parameters',()=>{
 const urls=[endpoints.groups,endpoints.widget,endpoints.detail({show:200000077},200031203),endpoints.decoded({show:200000077,customer:15},200031233)];
 for(const url of urls)assert.equal(new URL(url).origin,origin);
 assert.equal(new URL(urls[0]).searchParams.get('from_wp_api'),'true');assert.equal(new URL(urls[2]).searchParams.get('cid'),'200031203');assert.equal(new URL(urls[3]).searchParams.get('cgid'),'-1');
});
test('activity false remains false and request uses proxy',async()=>{
 let calls=0;const status=await readStatus({customer:15},async url=>{calls++;const u=new URL(url);assert.equal(u.origin,origin);assert.equal(u.pathname,'/homepage/getLiveClassStatus');assert.equal(u.searchParams.get('customer_id'),'15');return Response.json(false);});assert.equal(status,false);assert.equal(calls,1);
});
test('public and decoded Live reads use hosted proxy without source credentials',async()=>{
 const before=global.fetch;const calls=[];global.fetch=async(url,options)=>{calls.push(url);assert.equal(new URL(url).origin,origin);assert.equal(options.headers.Authorization,undefined);assert.equal(options.headers.Cookie,undefined);return Response.json({trips:[]},{headers:{'x-rs-decoded':'1'}});};
 try{assert.deepEqual(await get(endpoints.groups),{trips:[]});assert.deepEqual(await get(endpoints.decoded({show:200000077,customer:15},200031233),true),{trips:[]});assert.equal(calls.length,2);}finally{global.fetch=before;}
});
