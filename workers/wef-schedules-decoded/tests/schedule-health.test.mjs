import test from 'node:test';
import assert from 'node:assert/strict';
import {scheduleHealth} from '../schedule-health.mjs';
import {HEALTH_TABLES as T,recordRenewal} from '../health-records.mjs';
const good=()=>Response.json({show:{show_id:88},show_date:'2026-11-07',rings:[]},{headers:{'x-rs-decoded':'1'}});
function fixture({prior,defaults=[{id:'recShow',fields:{show_id:88}}],original=good,alternate=good}={}){
 const writes=[],calls=[],pauses=[];
 const store={list:async(t)=>t===T.health?(prior?[{id:'recHealth',fields:prior}]:[]):t===T.shows?defaults:t===T.days?[{id:'recDay',fields:{show:['recShow'],date:'2026-11-07'}}]:[],save:async(t,k,f)=>{writes.push({t,f});return{id:'recWarning',fields:f};}};
 const options={store,now:()=> '2026-11-07T23:30:00.000Z',sleep:async ms=>pauses.push(ms),original:async req=>{calls.push(['original',new URL(req.url).searchParams.get('show_id')]);return original();},alternate:async(req,p)=>{calls.push([p]);return alternate(p);},renew:async()=>calls.push(['renew'])};
 return {writes,calls,pauses,run:()=>scheduleHealth(new Request('https://worker.example/health'),{},options)};
}
test('uses the default show, accepts legitimate zero rings, links correct day and never touches clock records',async()=>{
 const f=fixture();const r=await f.run();assert.equal(r.ok,true);assert.equal(r.show_id,'88');assert.deepEqual(f.calls,[['original','88']]);assert.equal(f.writes.length,1);assert.equal(f.writes[0].f['health-key'],'decoder');assert.deepEqual(f.writes[0].f.show,['recShow']);assert.deepEqual(f.writes[0].f['show-day'],['recDay']);
});
test('profile1 recovery stops immediately without token renewal',async()=>{
 const f=fixture({original:()=>Response.json({},{status:503})});assert.equal((await f.run()).ok,true);assert.deepEqual(f.calls.map(x=>x[0]),['original','profile-fb-header1']);assert.deepEqual(f.pauses,[1000]);
});
test('health can recover with either cleaned profile without renewing',async()=>{
 for(const winner of ['windows-clean','android-clean']){
  const f=fixture({
   original:()=>Response.json({},{status:502}),
   alternate:profile=>profile===winner?good():Response.json({},{status:502})
  });
  const result=await f.run();
  assert.equal(result.ok,true);
  assert.equal(result.attempts.at(-1).profile,winner);
  assert.equal(f.calls.some(call=>call[0]==='renew'),false);
  assert.ok(f.pauses.every(ms=>ms>=1000));
 }
});
test('exhaustion renews once then repeats exactly five, warns once and preserves last success',async()=>{
 const f=fixture({prior:{status:'Healthy','last-success-at':'2026-11-07T20:00:00.000Z'},original:()=>Response.json({}),alternate:()=>Response.json({})});const r=await f.run();assert.equal(r.ok,false);assert.deepEqual(f.calls.map(x=>x[0]),['original','profile-fb-header1','profile-fb-header2','windows-clean','android-clean','renew','original-header','profile-fb-header1','profile-fb-header2','windows-clean','android-clean']);assert.equal(r.attempts.length,10);assert.ok(f.pauses.every(ms=>ms>=1000));assert.equal(f.writes[0].t,T.warnings);assert.equal(f.writes[0].f['delivery-status'],'Pending');assert.equal(f.writes[1].f['last-success-at'],'2026-11-07T20:00:00.000Z');
});
test('repeated outage does not duplicate warnings; successful recovery warns once',async()=>{
 const prior={status:'Outage','outage-started-at':'2026-11-07T22:00:00.000Z'};
 const f=fixture({prior,original:()=>Response.json({}),alternate:()=>Response.json({})});await f.run();assert.equal(f.writes.filter(x=>x.t===T.warnings).length,0);
 const g=fixture({prior});assert.equal((await g.run()).ok,true);assert.equal(g.writes[0].f.kind,'Recovery');
});
test('missing or ambiguous defaults never invoke schedule or invent a fallback show ID',async()=>{
 for(const defaults of [[],[{fields:{show_id:1}},{fields:{show_id:2}}]]){const f=fixture({defaults});assert.equal((await f.run()).ok,false);assert.deepEqual(f.calls,[]);}
});
test('wrong show, malformed payload, missing decode proof are never healthy',async()=>{
 for(const original of [()=>Response.json({show:{show_id:99},show_date:'2026-11-07',rings:[]},{headers:{'x-rs-decoded':'1'}}),()=>Response.json({show:{show_id:88},show_date:'2026-11-07',rings:[]}),()=>new Response('bad')]){const f=fixture({original,alternate:()=>Response.json({})});assert.equal((await f.run()).ok,false);}
});
test('renewal log redacts unknown fields and logging failure is visible without throwing',async()=>{
 let body;
 const r=await recordRenewal({WEF_SHOWS_AIRTABLE_TOKEN:'offline'},{'refresh-id':'test',result:'Failed',token:'DO-NOT-LOG',Cookie:'DO-NOT-LOG'},async(url,opts)=>{if(opts.method==='POST'){body=opts.body;return Response.json({records:[{id:'r'}]});}return Response.json({records:body?[{id:'r',fields:{'refresh-id':'test',result:'Failed'}}]:[]});});assert.equal(r,true);assert.ok(!body.includes('DO-NOT-LOG'));
 assert.equal(await recordRenewal({}, {'refresh-id':'failed'}),false);
});

function dualFixture({classResponse,prior}={}){
 const writes=[],calls=[],pauses=[];
 const schedule=()=>Response.json({show:{show_id:88},show_date:'2026-11-07',rings:[{classes:[{class_id:123}]}]},{headers:{'x-rs-decoded':'1','x-rs-source-lane':'runner'}});
 const validClass=()=>Response.json({class:{class_id:123},trips:[]},{headers:{'x-rs-decoded':'1','x-rs-source-lane':'runner'}});
 const store={list:async t=>t===T.health?(prior?[{fields:prior}]:[]):t===T.shows?[{id:'recShow',fields:{show_id:88}}]:t===T.days?[{id:'recDay',fields:{show:['recShow'],date:'2026-11-07'}}]:[],save:async(t,k,f)=>{writes.push({t,f});return{id:'recWarning'};}};
 const call=async(req,profile)=>{calls.push({path:new URL(req.url).pathname,profile});return new URL(req.url).pathname==='/schedule'?schedule():(classResponse?classResponse(profile):validClass());};
 return {writes,calls,pauses,run:()=>scheduleHealth(new Request('https://preview.example/health'),{},{store,original:req=>call(req,'runner'),alternate:call,renew:async()=>calls.push({profile:'renew'}),sleep:async ms=>pauses.push(ms),now:()=> '2026-11-07T23:30:00.000Z'})};
}
test('dual health samples current schedule class and accepts zero trips',async()=>{
 const f=dualFixture(),r=await f.run();assert.equal(r.ok,true);assert.equal(r.class_check.class_id,123);assert.equal(r.class_check.trips,0);assert.deepEqual(f.calls.map(x=>x.path),['/schedule','/classes/123']);assert.deepEqual(f.pauses,[1000]);assert.equal(JSON.parse(f.writes[0].f.details).class_check.ok,true);
});
test('class failure makes health fail despite usable schedule and alerts once',async()=>{
 const f=dualFixture({classResponse:()=>Response.json({},{status:502}),prior:{status:'Healthy'}}),r=await f.run();assert.equal(r.ok,false);assert.equal(r.schedule_status,200);assert.equal(r.rings,1);assert.equal(r.class_check.ok,false);assert.equal(r.attempts.length,11);assert.equal(f.calls.filter(x=>x.profile==='renew').length,1);assert.match(f.writes[0].f.message,/class-detail.*class 123/);assert.equal(f.writes[1].f.status,'Outage');
});
test('class recovery uses header profiles, stops first usable result and preserves spacing',async()=>{
 const f=dualFixture({classResponse:p=>p==='android-clean'?Response.json({class:{class_id:123},trips:[{}]},{headers:{'x-rs-decoded':'1'}}):Response.json({})}),r=await f.run();assert.equal(r.ok,true);assert.equal(r.class_check.trips,1);assert.equal(r.attempts.at(-1).profile,'android-clean');assert.equal(f.calls.some(x=>x.profile==='renew'),false);assert.ok(f.pauses.every(ms=>ms>=1000));
});
test('wrong class or missing trips never passes and repeat outage does not warn twice',async()=>{
 for(const payload of [{class:{class_id:999},trips:[]},{class:{class_id:123}}]){
 const f=dualFixture({classResponse:()=>Response.json(payload,{headers:{'x-rs-decoded':'1'}}),prior:{status:'Outage'}}),r=await f.run();assert.equal(r.ok,false);assert.equal(f.writes.filter(x=>x.t===T.warnings).length,0);
 }
});
