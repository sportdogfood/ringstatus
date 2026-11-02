'use strict';
globalThis.fetch=async()=>{throw Error('Offline review: network forbidden');};
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os');
const {cleanup,LOGS}=require('./candidate/cleanup');
function setup(t,previous={show:77,day:'2026-11-02'}){
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-day-trigger-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));
 fs.writeFileSync(path.join(runtime,'cleanup-state.json'),JSON.stringify(previous));
 let now='2026-11-02T18:00:00.000Z',seq=0;const reads=[],writes=[],deleted=[];
 const db={shows:[{id:'recShow',fields:{show_id:77,customer_id:15,is_default:true}}],cleanup:[...LOGS,'live-groups'].map(Name=>({id:'recControl'+(++seq),fields:{Name,cleanup:true}}))};
 function rows(table,n,show=77){db[table]=Array.from({length:n},(_,i)=>({id:table+'Rec'+i,createdTime:new Date(Date.UTC(2026,10,1,0,i)).toISOString(),fields:{'show-id':show,'run-id':table+'Run'+i,result:'Success',completed:'2026-11-02T12:00:00Z','occurred-at':'2026-11-02T12:00:00Z'}}));}
 const api={list:async table=>{reads.push(table);return structuredClone(db[table]||[]);},upsert:async(table,key,fields)=>{writes.push(table);for(const f of fields){let old=(db[table]||=[]).find(r=>r.fields[key]===f[key]);if(old)Object.assign(old.fields,f);else db[table].push({id:'rec'+(++seq),fields:structuredClone(f)});}return db[table];},call:async(base,table,method,body)=>{writes.push(table);return{records:body.records.map(p=>{const r=db[table].find(r=>r.id===p.id);Object.assign(r.fields,p.fields);return r;})};},retireCleanup:async(table,chosen,policy,receipt,onDeleted)=>{assert.deepEqual(require('./candidate/log-retention').verified(receipt),chosen);deleted.push({table,count:chosen.length});db[table]=db[table].filter(r=>!chosen.some(c=>c.id===r.id));onDeleted(chosen.map(r=>r.id));}};
 return{runtime,db,reads,writes,deleted,rows,setNow:v=>{now=v;},options:{runtime,api,clock:()=>now}};
}
test('same show and day:1000+ logs do not cause table reads, archive or deletion',async t=>{
 const a=setup(t);for(const table of LOGS)a.rows(table,1200);const r=await cleanup(a.options);
 assert.equal(r.result,'Skipped');assert.equal(r.new_show,false);assert.equal(r.new_day,false);assert.deepEqual(a.reads,['shows']);assert.equal(a.writes.length,0);assert.equal(a.deleted.length,0);assert(!fs.existsSync(path.join(a.runtime,'history')));
});
test('new Eastern day:1000 logs trigger exactly500 oldest removals per approved log table',async t=>{
 const a=setup(t,{show:77,day:'2026-11-01'});for(const table of LOGS)a.rows(table,1000);a.rows('live-groups',1000);
 const r=await cleanup(a.options);assert.equal(r.result,'Success');assert.equal(r.new_day,true);assert.equal(r.new_show,false);assert.equal(a.deleted.length,3);assert(a.deleted.every(d=>d.count===500&&LOGS.includes(d.table)));assert(!a.reads.includes('live-groups'));assert.equal(a.db['live-groups'].length,1000);
 for(const table of LOGS){assert.equal(a.db[table].length,500);assert.equal(a.db[table][0].id,table+'Rec500');}
});
test('same-day repeats after a day cleanup do not read logs or repeat500 deletions',async t=>{
 const a=setup(t,{show:77,day:'2026-11-01'});for(const table of LOGS)a.rows(table,1500);await cleanup(a.options);a.reads.length=0;a.writes.length=0;
 const r=await cleanup(a.options);assert.equal(r.result,'Skipped');assert.deepEqual(a.reads,['shows']);assert.equal(a.writes.length,0);assert.equal(a.deleted.length,3);for(const table of LOGS)assert.equal(a.db[table].length,1000);
});
test('a later new day can run the500-record rule again',async t=>{
 const a=setup(t,{show:77,day:'2026-11-01'});for(const table of LOGS)a.rows(table,1500);await cleanup(a.options);a.setNow('2026-11-03T18:00:00Z');const r=await cleanup(a.options);assert.equal(r.new_day,true);assert.equal(a.deleted.length,6);
});
test('new show on same day only runs new-show cleanup, never quick-cleanup on1000 current logs',async t=>{
 const a=setup(t,{show:76,day:'2026-11-02'});for(const table of LOGS)a.rows(table,1100,77);a.rows('live-groups',1,76);
 const r=await cleanup(a.options);assert.equal(r.new_show,true);assert.equal(r.new_day,false);assert(r.actions.every(x=>x.trigger==='new-show'));assert.deepEqual(a.deleted,[{table:'live-groups',count:1}]);for(const table of LOGS)assert.equal(a.db[table].length,1100);
});
test('UTC midnight is not Eastern midnight; Eastern midnight triggers a day change',async t=>{
 const a=setup(t);a.setNow('2026-11-03T00:01:00Z');let r=await cleanup(a.options);assert.equal(r.new_day,false);assert.deepEqual(a.reads,['shows']);
 a.setNow('2026-11-03T05:00:00Z');r=await cleanup(a.options);assert.equal(r.new_day,true);assert.equal(r.day,'2026-11-03');
});
test('legacy checkpoint derives its day from original completed time without forced same-day cleanup',async t=>{
 const previous={show:77,run_id:'old-run',completed:'2026-11-02T14:30:36.679Z'},a=setup(t,previous);for(const table of LOGS)a.rows(table,1200);
 const r=await cleanup(a.options);assert.equal(r.result,'Skipped');assert.deepEqual(a.reads,['shows']);const migrated=JSON.parse(fs.readFileSync(path.join(a.runtime,'cleanup-state.json')));assert.deepEqual(migrated,{...previous,day:'2026-11-02'});assert.equal(a.deleted.length,0);
});
test('legacy checkpoint from previous Eastern day allows the first normal new-day check',async t=>{
 const a=setup(t,{show:77,completed:'2026-11-02T04:30:00Z'});for(const table of LOGS)a.rows(table,1000);const r=await cleanup(a.options);assert.equal(r.new_day,true);assert.equal(a.deleted.length,3);
});
test('new-day counts below1000 are logged Skipped and checkpointed once',async t=>{
 const a=setup(t,{show:77,day:'2026-11-01'});for(const table of LOGS)a.rows(table,999);const r=await cleanup(a.options);assert.equal(r.result,'Success');assert.equal(a.deleted.length,0);assert.equal(r.actions.length,3);assert(r.actions.every(x=>x.result==='Skipped'&&x.deleted===0));
 assert.equal(JSON.parse(fs.readFileSync(path.join(a.runtime,'cleanup-state.json'))).day,'2026-11-02');
});
test('when both show and day change, each trigger runs only its own approved action',async t=>{
 const a=setup(t,{show:76,day:'2026-11-01'});for(const table of LOGS)a.rows(table,1000,77);a.rows('live-groups',1,76);
 const r=await cleanup(a.options);assert.equal(r.new_show,true);assert.equal(r.new_day,true);assert.equal(a.db['live-groups'].length,0);assert.equal(a.deleted.filter(d=>d.table==='live-groups').length,1);assert.equal(a.deleted.filter(d=>LOGS.includes(d.table)&&d.count===500).length,3);
});
