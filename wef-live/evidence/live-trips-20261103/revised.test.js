'use strict';
globalThis.fetch=async()=>{throw Error('Offline network forbidden');};
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {cleanup,TABLES,LOGS,eligible,protectedRun}=require('./candidate/cleanup');
function setup(t,previous={show:77,day:'2026-11-03',quick_checked_at:'2026-11-03T13:00:00Z'}){
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-revised-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));
 fs.writeFileSync(path.join(runtime,'cleanup-state.json'),JSON.stringify(previous));
 let seq=0,now='2026-11-03T14:00:00Z';const reads=[],deletes=[];
 const db={shows:[{id:'recShow',fields:{customer_id:15,is_default:true,show_id:77}}],cleanup:TABLES.map(Name=>({id:'recControl'+(++seq),fields:{Name,cleanup:true}}))};
 const row=(table,fields,createdTime='2026-11-01T00:00:00Z')=>{const r={id:'rec'+(++seq),fields,createdTime};(db[table]||=[]).push(r);return r;};
 const rows=(table,n)=>{for(let i=0;i<n;i++)row(table,{'run-id':String(i),'show-id':77,result:'Success',completed:'2026-11-03T12:00:00Z','occurred-at':'2026-11-03T12:00:00Z'},new Date(Date.UTC(2026,10,1,0,i)).toISOString());};
 const api={list:async name=>{reads.push(name);return structuredClone(db[name]||[]);},upsert:async(name,key,values)=>{for(const f of values){const r=(db[name]||[]).find(r=>r.fields[key]===f[key]);if(r)Object.assign(r.fields,f);else row(name,structuredClone(f));}},call:async(base,name,method,body)=>({records:body.records.map(p=>{const r=db[name].find(r=>r.id===p.id);Object.assign(r.fields,p.fields);return r;})}),retireCleanup:async(name,values,policy,onDeleted)=>{assert(values.every(r=>eligible(name,r,policy.trigger,policy.show)&&!protectedRun(r,policy.protectedRuns)));deletes.push({table:name,count:values.length,trigger:policy.trigger});db[name]=db[name].filter(r=>!values.some(x=>x.id===r.id));onDeleted(values.map(r=>r.id));}};
 return{runtime,db,rows,row,reads,deletes,setNow:v=>now=v,options:{runtime,api,clock:()=>now,archive:()=>{throw Error('No archive allowed');}}};
}
test('new calendar day clears even fewer than1000 logs and leaves show rows',async t=>{
 const a=setup(t,{show:77,day:'2026-11-02',quick_checked_at:'2026-11-03T13:00:00Z'});for(const name of LOGS)a.rows(name,3);a.row('live-groups',{'show-id':77});
 const r=await cleanup(a.options);assert.equal(r.result,'Success');assert(r.actions.every(x=>x.trigger==='calendar-day'&&x.deleted===3));for(const name of LOGS)assert.equal(a.db[name].length,0);assert.equal(a.db['live-groups'].length,1);assert(!fs.existsSync(path.join(a.runtime,'history')));
});
test('daily clear protects running logs, current invocation and its diff children',async t=>{
 const a=setup(t,{show:77,day:'2026-11-02'});a.rows('heartbeat',1);a.row('heartbeat',{result:'Running'});a.row('heartbeat',{'run-id':'current',result:'Success',completed:'2026-11-03T13:00:00Z'});a.row('run-logs',{'run-id':'current|diff|1',result:'Success',completed:'2026-11-03T13:00:00Z'});a.options.protectedRuns=['current'];
 await cleanup(a.options);assert.equal(a.db.heartbeat.length,2);assert.equal(a.db['run-logs'].length,1);
});
test('three hour check deletes oldest500 at1000 and only logging tables',async t=>{
 const a=setup(t);a.setNow('2026-11-03T16:00:00Z');for(const name of LOGS)a.rows(name,1000);a.row('live-groups',{'show-id':77});
 const r=await cleanup(a.options);assert.equal(r.quick_due,true);assert.equal(a.deletes.length,3);assert(a.deletes.every(x=>x.count===500&&x.trigger==='quick-cleanup'));assert.equal(a.db.heartbeat[0].fields['run-id'],'500');assert.equal(a.db['live-groups'].length,1);
});
test('below1000 is logged Skipped at3hours and checked once',async t=>{
 const a=setup(t);a.setNow('2026-11-03T16:00:00Z');for(const name of LOGS)a.rows(name,999);let r=await cleanup(a.options);assert.equal(r.actions.length,3);assert(r.actions.every(x=>x.result==='Skipped'));a.reads.length=0;r=await cleanup({...a.options,checkShow:false});assert.equal(r.result,'Skipped');assert.equal(a.reads.length,0);assert.equal(a.deletes.length,0);
});
test('same day before3hours reads no log inventory even at1000+',async t=>{
 const a=setup(t);a.rows('heartbeat',1200);let r=await cleanup(a.options);assert.equal(r.result,'Skipped');assert.deepEqual(a.reads,['shows']);a.reads.length=0;r=await cleanup({...a.options,checkShow:false});assert.equal(r.result,'Skipped');assert.equal(a.reads.length,0);
});
test('new show clears ALL owned show rows including matching new-show rows and eligible logs',async t=>{
 const a=setup(t,{show:76,day:'2026-11-03',quick_checked_at:'2026-11-03T13:00:00Z'});for(const name of TABLES){if(LOGS.includes(name))a.rows(name,2);else{a.row(name,{'show-id':76});a.row(name,{'show-id':77});}}a.row('collections',{'show-id':76});
 const r=await cleanup(a.options);assert.equal(r.result,'Success');assert.equal(r.actions.length,TABLES.length);assert(r.actions.every(x=>x.trigger==='new-show'&&x.deleted===2));assert.equal(a.db.collections.length,1);assert.equal(a.db['cleanup-logs'].length,TABLES.length);
});
test('new show plus calendar day executes full clear once and resets3hour clock',async t=>{
 const a=setup(t,{show:76,day:'2026-11-02'});a.rows('heartbeat',1100);const r=await cleanup(a.options);assert.equal(a.deletes[0].count,1100);assert(r.actions.every(x=>x.trigger==='new-show'));assert.equal(JSON.parse(fs.readFileSync(path.join(a.runtime,'cleanup-state.json'))).quick_checked_at,r.started);
});
test('planning-date change has no separate cleanup trigger',async t=>{
 const a=setup(t);a.db.shows[0].fields.selected_date='2026-11-06';a.rows('heartbeat',1100);const r=await cleanup(a.options);assert.equal(r.result,'Skipped');assert.equal(a.deletes.length,0);
});
test('UTC midnight does not clear logs until Eastern calendar day changes',async t=>{
 const a=setup(t,{show:77,day:'2026-11-03',quick_checked_at:'2026-11-04T00:00:00Z'});a.setNow('2026-11-04T00:01:00Z');a.rows('heartbeat',3);let r=await cleanup(a.options);assert.equal(r.new_day,false);assert.equal(a.deletes.length,0);a.setNow('2026-11-04T05:00:00Z');r=await cleanup(a.options);assert.equal(r.new_day,true);assert.equal(a.db.heartbeat.length,0);
});
test('quiet scheduled dispatcher can perform3hour cleanup without source/status/logging collection',async t=>{
 const a=setup(t);a.setNow('2026-11-03T16:00:00Z');a.rows('heartbeat',999);fs.writeFileSync(path.join(a.runtime,'live-cadence-state.json'),JSON.stringify({checked_at:'2026-11-03T15:30:00Z',next_check_at:'2026-11-03T16:30:00Z',reason:'live-false',live_status:false}));
 const r=await require('./candidate/lanes').scheduled({...a.options,getStatus:()=>{throw Error('must not call source');}});assert.equal(r.skipped,'next-check-not-due');assert.equal(r.cleanup.result,'Success');assert.equal(a.db['cleanup-logs'].length,3);assert(!a.db['run-logs']);
});
test('metadata receipts never save deleted fields or archive files',async t=>{
 const a=setup(t,{show:76,day:'2026-11-03'});a.row('live-groups',{'show-id':77,secret_marker:'original-fields'});await cleanup(a.options);const text=fs.readFileSync(path.join(a.runtime,'cleanup-last-run.json'),'utf8');assert(!text.includes('original-fields'));assert(!text.includes('archive-file'));assert(!fs.existsSync(path.join(a.runtime,'history')));
});
test('DELETE failure logs error and stops cleanup without retry',async t=>{
 const a=setup(t,{show:76,day:'2026-11-03'});a.row('live-groups',{'show-id':76});let n=0;a.options.api.retireCleanup=async()=>{n++;throw Error('uncertain DELETE');};assert.equal((await cleanup(a.options)).result,'Error');assert.equal((await cleanup(a.options)).result,'Blocked');assert.equal(n,1);assert.equal(a.db['errs-log'].length,1);
});
test('three-hour threshold repeats only after another full interval and protects current logs',async t=>{
 const a=setup(t);a.rows('heartbeat',1500);a.options.protectedRuns=['0'];a.setNow('2026-11-03T16:00:00Z');await cleanup(a.options);assert.equal(a.db.heartbeat.length,1000);assert(a.db.heartbeat.some(r=>r.fields['run-id']==='0'));
 a.setNow('2026-11-03T18:59:59Z');assert.equal((await cleanup(a.options)).result,'Skipped');a.setNow('2026-11-03T19:00:00Z');await cleanup(a.options);assert.equal(a.db.heartbeat.length,500);assert.equal(a.deletes.length,2);assert(a.db.heartbeat.some(r=>r.fields['run-id']==='0'));
});
