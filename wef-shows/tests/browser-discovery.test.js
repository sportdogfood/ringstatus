"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {normalize,capture,PAGE}=require('../browser-discovery');
const payload=require('./fixtures/browser-schedule.json');
function withoutBlobs(value){if(Array.isArray(value))return value.map(withoutBlobs);if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).filter(([k])=>k!=='_').map(([k,v])=>[k,withoutBlobs(v)]));return value;}
const evidence=()=>({url:PAGE+'?date=2026-11-07&sid=200000077',defaultShow:200000077,payload:structuredClone(payload)});
test('captured browser metadata validates without either proxy endpoint',()=>{
 const f=normalize(evidence());assert.equal(f.show.fields.show_id,200000077);assert.equal(f.show.fields.start_date,'2026-11-05');assert.equal(f.show.fields.end_date,'2026-11-08');assert.equal(f.show.fields.sgl_default_day,'2026-11-07');assert.deepEqual(f.payload,payload);
});
test('browser evidence rejects mismatched default, URL, day, incomplete metadata and encoded payload',()=>{
 for(const alter of [x=>x.defaultShow=1,x=>x.url=PAGE+'?sid=1&date=2026-11-07',x=>x.payload.show_date='2026-11-09',x=>x.payload.show.start_date='2026-02-30',x=>x.payload.show.facility_id=0,x=>x.payload.show.show_name='',x=>x.payload._t='encoded',x=>delete x.payload.rings,x=>x.url='https://example.com/?sid=200000077&date=2026-11-07']){const e=evidence();alter(e);assert.throws(()=>normalize(e),/Browser/);}
});
function mock({hang=false,malformed=false,late=false}={}){
 const state={closed:0,launch:null,context:null,navigations:[]};let handler;
 const page={on:(name,fn)=>{assert.equal(name,'response');handler=fn;},goto:async url=>{
  state.navigations.push(url);if(hang)return new Promise(()=>{});
  const send=()=>{for(const [endpoint,body] of [['shows',{default_show:200000077}],['schedule',malformed?{}:payload]])handler({url:()=>`https://sglapi.wellingtoninternational.com/${endpoint}?customer_id=15`,status:()=>200,request:()=>({headerValue:async()=>null}),json:async()=>withoutBlobs(body)});};if(late)setTimeout(send,10);else send();
 },waitForURL:async fn=>{assert(fn(new URL(evidence().url)));},url:()=>evidence().url};
 const chromium={launch:async opts=>{state.launch=opts;return{newContext:async opts=>{state.context=opts;return{newPage:async()=>page};},close:async()=>{state.closed++;}};}};
 return {state,chromium};
}
test('browser launch is headless with new context, clean page URL and cleanup; response-only acquisition',async()=>{
 const m=mock();assert.equal((await capture({chromium:m.chromium})).show.fields.show_id,200000077);assert.deepEqual(m.state.launch,{channel:'msedge',headless:true,timeout:15000});assert.deepEqual(m.state.context,{timezoneId:'America/New_York'});assert.deepEqual(m.state.navigations,[PAGE]);assert.equal(m.state.closed,1);
});
test('missing browser response and whole-attempt timeout close the browser and fail for fallback',async()=>{
 const bad=mock({malformed:true});await assert.rejects(capture({chromium:bad.chromium,timeoutMs:30}),/timed out/);assert.equal(bad.state.closed,1);
 const slow=mock({hang:true});await assert.rejects(capture({chromium:slow.chromium,timeoutMs:10}),/timed out/);assert.equal(slow.state.closed,1);
});

test('browser discovery updates focus evidence and preserves scoped overrides',async t=>{
 const fs=require('node:fs'),path=require('node:path'),os=require('node:os');const dir=fs.mkdtempSync(path.join(os.tmpdir(),'browser-focus-test-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));const statePath=path.join(dir,'focus.json');const previous={version:1,show_id:200000076,fields:{},overrides:{'200000077':{focus_day:'2026-11-06'}},default_show:{current:{show_id:200000076}}};fs.writeFileSync(statePath,JSON.stringify(previous));await require('../browser-discovery').discoverBrowser({statePath,now:'2026-11-07T23:00:00Z',capturePage:async()=>normalize(evidence())});const state=JSON.parse(fs.readFileSync(statePath));assert.equal(state.default_show.current.show_id,200000077);assert.equal(state.default_show.schedule_day.current,'2026-11-07');assert.equal(state.fields.focus_day,'2026-11-06');assert.deepEqual(state.overrides,previous.overrides);
});

test('response parsing after URL update still supplies primary evidence instead of falling back',async()=>{const m=mock({late:true});assert.equal((await capture({chromium:m.chromium,timeoutMs:1000})).show.fields.show_id,200000077);assert.equal(m.state.closed,1);});

test('browser budget plus unchanged fallback pair leaves publication reserve within existing task limit',()=>{const {BROWSER_TIMEOUT_MS}=require('../browser-discovery');assert.equal(BROWSER_TIMEOUT_MS,20000);assert.equal(240000-BROWSER_TIMEOUT_MS-2*(3*30000+1500),37000);});
