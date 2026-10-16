"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const s=require('../collect'),legacy=require('../collector-core');
const {fixture,payload,NOW}=require('./fixtures/schedule-collector');
const {runStagedCollector}=require('../store');
global.fetch=async()=>{throw Error('Network forbidden');};
function temp(t){const dir=fs.mkdtempSync(path.join(os.tmpdir(),'wef-schedules-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));return {statePath:path.join(dir,'focus.json'),lockPath:path.join(dir,'collector.lock')};}
function discovery(id=74){return {fetchJson:async url=>new URL(url).pathname==='/ring'?{show:{show_id:id}}:{show:{show_id:id,start_date:'2026-10-16',end_date:'2026-10-18',show_name:'Fixture'},show_date:'2026-10-16'}};}
test('discovers new/current/new show and persists focus independently',async t=>{
 const files=temp(t);const first=await s.discoverFocus({...files,io:discovery(),now:NOW});
 assert.equal(first.state.discovery,'new');assert.equal(first.show.fields.focus_day,'2026-10-15');assert.deepEqual(s.selectedDays(first.show.fields),['2026-10-16','2026-10-17','2026-10-18']);
 assert.equal((await s.discoverFocus({...files,io:discovery(),now:NOW})).state.discovery,'current');
 assert.equal((await s.discoverFocus({...files,io:discovery(75),now:NOW})).state.discovery,'new');
 assert.equal(JSON.parse(fs.readFileSync(files.statePath)).show_id,75);
});
test('focus rollover does not restrict the complete show range',()=>{
 assert.equal(s.localDay('2026-10-16T02:00:00Z'),'2026-10-15');assert.equal(s.localDay('2026-10-16T05:00:00Z'),'2026-10-16');
 assert.deepEqual(s.selectedDays({focus_day:'2026-10-18',start_date:'2026-10-16',end_date:'2026-10-18'}),['2026-10-16','2026-10-17','2026-10-18']);
 assert.deepEqual(s.selectedDays({focus_day:'2026-10-19',start_date:'2026-10-16',end_date:'2026-10-18'}),['2026-10-16','2026-10-17','2026-10-18']);
});
test('manual focus and attachment settings persist only for their own show',async t=>{
 const files=temp(t),overrides={'74':{focus_day:'2026-10-17',has_full_schedule_payload:true,full_schedule_payload_file:[{url:'fixture'}]}};
 fs.writeFileSync(files.statePath,JSON.stringify({overrides}));
 const a=await s.discoverFocus({...files,io:discovery(),now:NOW});assert.equal(a.show.fields.focus_day,'2026-10-17');assert.deepEqual(a.state.overrides,overrides);
 const b=await s.discoverFocus({...files,io:discovery(75),now:NOW});assert.equal(b.show.fields.focus_day,'2026-10-15');assert.deepEqual(b.state.overrides,overrides);
});
test('invalid discovery leaves last saved focus intact',async t=>{
 const files=temp(t);await s.discoverFocus({...files,io:discovery(),now:NOW});const before=fs.readFileSync(files.statePath,'utf8');
 await assert.rejects(s.discoverFocus({...files,io:{fetchJson:async()=>({})},now:NOW}),/show ID/);
 assert.equal(fs.readFileSync(files.statePath,'utf8'),before);
});
test('mismatched default response is rejected',async t=>{
 const files=temp(t);await assert.rejects(s.discoverFocus({...files,io:{fetchJson:async u=>new URL(u).pathname==='/ring'?{show:{show_id:74}}:{show:{show_id:75}}},now:NOW}),/show mismatch/);
 assert.equal(fs.existsSync(files.statePath),false);
});
test('collector transport disables legacy audit on both fetch paths',async()=>{
 const original={...legacy.scheduleIO};const seen=[];
 try {legacy.scheduleIO.fetchJson=async(u,a)=>seen.push(a);legacy.scheduleIO.fetchClassListJson=async(u,a)=>seen.push(a);
 const io=s.collectorIO();await io.fetchJson('fixture',{runtimeAudit:true});await io.fetchClassListJson('fixture',{runtimeAudit:true});assert.deepEqual(seen,[{runtimeAudit:false},{runtimeAudit:false}]);
 }finally{Object.assign(legacy.scheduleIO,original);}
});
test('overlapping collector is rejected without invoking SGL',async t=>{
 const files=temp(t),release=s.acquireLock(files.lockPath);try{await assert.rejects(runStagedCollector({...files,io:{fetchJson:()=>{throw Error('Must not fetch');}}}),/already running/);}finally{release();}
});
test('complete scheduled entry discovers once, collects, then repeats without duplicate datasets',async t=>{
 const files=temp(t),f=fixture(),attempts=[],datasets=new Map();let writes=0,pings=0;
 const store={async begin(fields){const r={fields:{...fields}};attempts.push(r);return r;},async finish(r,fields){Object.assign(r.fields,fields);},async latest(id,date){return attempts.filter(r=>r.fields.status==='complete'&&r.fields.date===date).at(-1)||null;},async writeRows(id,show,date,rows){writes++;datasets.set(id,structuredClone(rows));},async loadRows(id){return datasets.get(id)||[];}};
 const source=f.io.fetchJson;
 f.io.fetchJson=async u=>{const url=new URL(u);if(url.pathname==='/ring'){pings++;return {show:{show_id:74}};}const p=await source(u);if(url.searchParams.get('date')==='00/00/00') p.show={show_id:74,start_date:'2026-10-15',end_date:'2026-10-17'};return p;};
 const first=await runStagedCollector({...files,store,io:f.io,now:NOW});assert.equal(first.ok,true,JSON.stringify(first));assert.equal(writes,3);assert.equal(pings,1);
 const second=await runStagedCollector({...files,store,io:f.io,now:NOW});assert.equal(second.ok,true);assert.equal(writes,3);assert.equal(pings,2);assert.equal(second.discovery,'current');assert.equal(fs.existsSync(files.lockPath),false);
});

test('default changes before next show without inventing contiguous days',async t=>{
 const files=temp(t);
 await s.discoverFocus({...files,io:discovery(74),now:NOW});
 const io={fetchJson:async url=>new URL(url).pathname==='/ring'?{show:{show_id:75}}:{show:{show_id:75,start_date:'2026-10-21',end_date:'2026-10-25'},show_date:'2026-10-21'}};
 const next=await s.discoverFocus({...files,io,now:'2026-10-19T16:00:00Z'});
 assert.equal(next.state.default_show.code,'default_show_changed');
 assert.equal(next.state.default_show.previous.show_id,74);
 assert.equal(next.state.default_show.previous.end_date,'2026-10-18');
 assert.equal(next.state.default_show.phase,'upcoming');
 assert.deepEqual(s.selectedDays(next.show.fields),['2026-10-21','2026-10-22','2026-10-23','2026-10-24','2026-10-25']);
 const again=await s.discoverFocus({...files,io,now:'2026-10-20T16:00:00Z'});
 assert.equal(again.state.default_show.code,'default_show_current');assert.equal(again.state.default_show.phase,'upcoming');
});


test('priority uses New York calendar day and preserves every show date',()=>{
 const fields={start_date:'2026-10-14',end_date:'2026-10-18'};
 assert.deepEqual(s.selectedDays(fields,'2026-10-16T01:00:00Z'),['2026-10-15','2026-10-16','2026-10-17','2026-10-18','2026-10-14']);
 assert.deepEqual(s.selectedDays(fields,'2026-10-17T12:00:00Z'),['2026-10-17','2026-10-18','2026-10-14','2026-10-15','2026-10-16']);
});
