"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {shouldRun}=require('../run-cadence');
function state(at='2026-11-02T16:04:00Z'){
 return{focus:{show_id:77,checked_at:'2026-11-02T06:04:00Z',fields:{show_id:77,start_date:'2026-11-05',end_date:'2026-11-08'}},last:{ok:true,show_id:77,selected_date:'2026-11-05',started:at,completed:new Date(Date.parse(at)+10000).toISOString()},discovery:{version:1,cycle:require('../discovery-policy').sunday(at),baseline:76,complete:true,next:require('../discovery-policy').nextSunday(at),failed:false},priority:{version:2,date:'2026-11-02',anchor:'2026-11-05',evening:false}};
}
test('outside show dates: hourly elapsed cadence; selected future day0 does not enable five minutes',()=>{
 const s=state();for(let minutes=5;minutes<60;minutes+=5)assert.equal(shouldRun(new Date(Date.parse(s.last.started)+minutes*60000).toISOString(),s),false);
 assert.equal(shouldRun('2026-11-02T17:04:00Z',s),true);
});
test('after show ends hourly cadence also applies',()=>{const s=state('2026-11-09T16:04:00Z');s.focus.checked_at='2026-11-02T06:04:00Z';s.discovery.cycle='2026-11-09T03:00:00.000Z';s.discovery.next='2026-11-16T03:00:00Z';assert.equal(shouldRun('2026-11-09T16:09:00Z',s),false);assert.equal(shouldRun('2026-11-09T17:04:00Z',s),true);});
test('inside range including first and last dates every scheduled five-minute wake runs',()=>{
 for(const date of ['2026-11-05','2026-11-06','2026-11-08']){const s=state(date+'T16:04:00Z');assert.equal(shouldRun(date+'T16:09:00Z',s),true);}
});
test('calendar TODAY uses Eastern midnight, not UTC or planning day0',()=>{
 const s=state('2026-11-05T04:04:00Z');s.priority={...s.priority,date:'2026-11-04',evening:true};assert.equal(shouldRun('2026-11-05T04:09:00Z',s),false);assert.equal(shouldRun('2026-11-05T05:00:00Z',s),true);
 const end=state('2026-11-09T04:54:00Z');end.priority={...end.priority,date:'2026-11-08',evening:true};assert.equal(shouldRun('2026-11-09T04:59:00Z',end),true);assert.equal(shouldRun('2026-11-09T05:04:00Z',end),false);
});
test('Sunday22 and three-hour retries bypass an hourly skip when due',()=>{
 const s=state('2026-11-09T02:54:00Z');s.priority={...s.priority,date:'2026-11-08',evening:true};s.focus.fields.end_date='2026-11-07';
 assert.equal(shouldRun('2026-11-09T03:04:00Z',s),true);
 s.last={...s.last,started:'2026-11-09T05:54:00Z',completed:'2026-11-09T05:54:10Z'};s.discovery={...s.discovery,cycle:'2026-11-09T03:00:00Z',baseline:77,complete:false,next:'2026-11-09T06:04:00Z'};
 assert.equal(shouldRun('2026-11-09T05:59:00Z',s),false);assert.equal(shouldRun('2026-11-09T06:04:00Z',s),true);
});
test('first 19:00 evening decision runs between hours; subsequent unchanged wakes skip',()=>{
 const s=state('2026-11-02T23:54:00Z');assert.equal(shouldRun('2026-11-03T00:04:00Z',s),true);s.priority.evening=true;assert.equal(shouldRun('2026-11-03T00:09:00Z',s),false);
});
test('DST fallback hourly interval uses elapsed time, without duplicate clock-hour assumptions',()=>{
 const s=state('2026-11-01T05:34:00Z');s.focus.checked_at='2026-10-31T12:00:00Z';s.priority.date='2026-11-01';assert.equal(shouldRun('2026-11-01T06:29:00Z',s),false);assert.equal(shouldRun('2026-11-01T06:34:00Z',s),true);
});
test('unknown, malformed, changed or failed cache falls through to existing collector validation',()=>{
 for(const change of [s=>s.focus=null,s=>s.last=null,s=>s.last.ok=false,s=>s.last.show_id=76,s=>s.focus.fields.start_date='bad',s=>s.focus.checked_at='2026-11-02T16:05:00Z',s=>s.discovery=null,s=>s.discovery.next='bad',s=>s.priority=null,s=>s.priority.version=1,s=>s.last.completed='2026-11-03T00:00:00Z']){const s=state();change(s);assert.equal(shouldRun('2026-11-02T16:09:00Z',s),true);}
});

test('entry skips without invoking collector or changing last-run; due execution retains lock/report behavior',async t=>{
 const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'shows-cadence-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
 const paths={settings:require.resolve('../settings'),collector:require.resolve('../collector'),collect:require.resolve('../../wef-schedules/collect'),entry:require.resolve('../entry')};
 const saved=Object.fromEntries(Object.entries(paths).map(([k,p])=>[k,require.cache[p]]));let calls=0,locks=0,releases=0,writes=0,at='2026-11-02T16:09:00Z';
 const realDate=Date,realLog=console.log,realExit=process.exitCode;
 t.after(()=>{global.Date=realDate;console.log=realLog;process.exitCode=realExit;for(const[k,p]of Object.entries(paths)){if(saved[k])require.cache[p]=saved[k];else delete require.cache[p];}});
 global.Date=class extends realDate{constructor(...args){super(...(args.length?args:[at]));}};console.log=()=>{};
 require.cache[paths.settings]={id:paths.settings,filename:paths.settings,loaded:true,exports:{AIRTABLE_TOKEN:'offline-fixture',RUNTIME:dir}};
 require.cache[paths.collector]={id:paths.collector,filename:paths.collector,loaded:true,exports:{runShows:async()=>{calls++;return{ok:true,started:at};}}};
 require.cache[paths.collect]={id:paths.collect,filename:paths.collect,loaded:true,exports:{acquireLock:()=>{locks++;return()=>{releases++;};},atomicReport:async()=>{writes++;}}};
 delete require.cache[paths.entry];const {scheduled}=require('../entry'),s=state();
 const names={focus:'focus.json',last:'last-run.json',discovery:'discovery-policy.json',priority:'day-priority.json'};for(const[k,name]of Object.entries(names))fs.writeFileSync(path.join(dir,name),JSON.stringify(s[k]));
 const before=fs.readFileSync(path.join(dir,'last-run.json'),'utf8');await scheduled();assert.equal(calls,0);assert.equal(writes,0);assert.equal(locks,1);assert.equal(releases,1);assert.equal(fs.readFileSync(path.join(dir,'last-run.json'),'utf8'),before);
 at='2026-11-02T17:04:00Z';await scheduled();assert.equal(calls,1);assert.equal(writes,1);assert.equal(locks,2);assert.equal(releases,2);assert.equal(process.exitCode,0);
 // Failed collector reports still publish evidence/exit1 through the established path.
 require.cache[paths.collector].exports.runShows=async()=>({ok:false});await scheduled();assert.equal(process.exitCode,1);assert.equal(releases,3);
});
