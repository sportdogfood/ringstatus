'use strict';
globalThis.fetch=async()=>{throw Error('Offline network forbidden');};
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {cleanup,TABLES,LOGS,eligible,protectedRun}=require('./candidate/wef-live/cleanup');
function setup(t,previous={show:77,day:'2026-11-03',quick_checked_at:'2026-11-03T13:00:00Z',daily_clear_day:'2026-11-02'}){
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-revised-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));
 fs.writeFileSync(path.join(runtime,'cleanup-state.json'),JSON.stringify(previous));
 let seq=0,now='2026-11-03T14:00:00Z';const reads=[],deletes=[];
 const db={shows:[{id:'recShow',fields:{customer_id:15,is_default:true,show_id:77}}],cleanup:TABLES.map(Name=>({id:'recControl'+(++seq),fields:{Name,cleanup:true}}))};
 const row=(table,fields,createdTime='2026-11-01T00:00:00Z')=>{const r={id:'rec'+(++seq),fields,createdTime};(db[table]||=[]).push(r);return r;};
 const rows=(table,n)=>{for(let i=0;i<n;i++)row(table,{'run-id':String(i),'show-id':77,result:'Success',completed:'2026-11-03T12:00:00Z','occurred-at':'2026-11-03T12:00:00Z'},new Date(Date.UTC(2026,10,1,0,i)).toISOString());};
 const api={list:async name=>{reads.push(name);return structuredClone(db[name]||[]);},upsert:async(name,key,values)=>{for(const f of values){const r=(db[name]||[]).find(r=>r.fields[key]===f[key]);if(r)Object.assign(r.fields,f);else row(name,structuredClone(f));}},call:async(base,name,method,body)=>({records:body.records.map(p=>{const r=db[name].find(r=>r.id===p.id);Object.assign(r.fields,p.fields);return r;})}),retireCleanup:async(name,values,policy,onDeleted)=>{assert(values.every(r=>eligible(name,r,policy.trigger,policy.show)&&!protectedRun(r,policy.protectedRuns)));deletes.push({table:name,count:values.length,trigger:policy.trigger});db[name]=db[name].filter(r=>!values.some(x=>x.id===r.id));onDeleted(values.map(r=>r.id));}};
 return{runtime,db,rows,row,reads,deletes,setNow:v=>now=v,options:{runtime,api,clock:()=>now,archive:()=>{throw Error('No archive allowed');}}};
}

const {dailyClearDay}=require('./candidate/wef-live/cleanup');
test('7pm Eastern event boundary follows winter and summer DST offsets',()=>{
 assert.equal(dailyClearDay('2026-11-07T23:59:59Z'),'2026-11-06');assert.equal(dailyClearDay('2026-11-08T00:00:00Z'),'2026-11-07');assert.equal(dailyClearDay('2026-10-30T22:59:59Z'),'2026-10-29');assert.equal(dailyClearDay('2026-10-30T23:00:00Z'),'2026-10-30');
});
test('7pm clears all completed logs including runs/diffs once;not every invocation',async t=>{
 const a=setup(t);for(const name of LOGS)a.rows(name,3);a.row('run-logs',{'run-id':'old|diff|1',result:'Success',completed:'2026-11-03T20:00:00Z'});
 a.setNow('2026-11-03T23:59:59Z');let r=await cleanup(a.options);assert.equal(r.daily_due,false);assert.equal(a.db['run-logs'].length,4);
 a.setNow('2026-11-04T00:00:00Z');r=await cleanup(a.options);assert.equal(r.daily_due,true);assert.equal(a.db['run-logs'].length,0);assert.equal(a.db.heartbeat.length,0);assert.equal(a.db['errs-log'].length,0);assert(r.actions.every(x=>x.trigger==='daily-clear'));
 a.setNow('2026-11-04T00:03:00Z');a.reads.length=0;r=await cleanup({...a.options,checkShow:false});assert.equal(r.result,'Skipped');assert(a.reads.includes('run-logs'));assert.equal(a.deletes.length,3);
});
test('midnight is not a daily-clear event',async t=>{
 const a=setup(t,{show:77,day:'2026-11-03',daily_clear_day:'2026-11-03',quick_checked_at:'2026-11-04T04:00:00Z'});a.rows('run-logs',3);a.setNow('2026-11-04T05:01:00Z');const r=await cleanup({...a.options,checkShow:false});assert.equal(r.result,'Skipped');assert.equal(a.deletes.length,0);assert.equal(a.db['run-logs'].length,3);
});
test('missed7pm clears on first available normal invocation and checkpoints only successful event',async t=>{
 const a=setup(t,{show:77,day:'2026-11-03',daily_clear_day:'2026-11-02',quick_checked_at:'2026-11-04T11:00:00Z'});a.rows('run-logs',2);a.setNow('2026-11-04T12:00:00Z');await cleanup(a.options);assert.equal(a.db['run-logs'].length,0);assert.equal(JSON.parse(fs.readFileSync(path.join(a.runtime,'cleanup-state.json'))).daily_clear_day,'2026-11-03');
});
test('threshold checked eachinvoke,1200drains700then500below1000,repeatnodeletes',async t=>{
 const a=setup(t);a.rows('run-logs',1200);let r=await cleanup({...a.options,checkShow:false});assert.equal(r.result,'Success');assert.equal(a.db['run-logs'].length,700);assert.equal(a.deletes[0].count,500);a.setNow('2026-11-03T14:03:00Z');r=await cleanup({...a.options,checkShow:false});assert.equal(a.db['run-logs'].length,500);assert.equal(a.deletes[1].count,200);a.setNow('2026-11-03T14:06:00Z');r=await cleanup(a.options);assert.equal(a.deletes.length,2);assert.equal(r.actions.length,0);
});
test('999untriggeredrowsnotdeleted;1000triggerremovesoldest500',async t=>{
 const a=setup(t);a.rows('run-logs',999);await cleanup(a.options);assert.equal(a.deletes.length,0);a.row('run-logs',{'run-id':'999',result:'Success',completed:'2026-11-03T12:00:00Z'},'2026-11-03T13:00:00Z');await cleanup(a.options);assert.equal(a.db['run-logs'].length,500);assert.equal(a.db['run-logs'][0].fields['run-id'],'500');
});
test('daily clear protects active/current/diff logs and does not alter show records',async t=>{
 const a=setup(t);for(const name of LOGS)a.rows(name,2);a.row('heartbeat',{result:'Running'});a.row('run-logs',{'run-id':'current|diff|1',result:'Success',completed:'2026-11-03T20:00:00Z'});a.row('live-groups',{'show-id':77});a.options.protectedRuns=['current'];a.setNow('2026-11-04T00:00:00Z');await cleanup(a.options);assert.equal(a.db.heartbeat.length,1);assert.equal(a.db['run-logs'].length,1);assert.equal(a.db['live-groups'].length,1);assert(!fs.existsSync(path.join(a.runtime,'history')));
});
test('newshow clears allcheckedownedrows and completedlogs;catalogs/references untouched',async t=>{
 const a=setup(t,{show:76,day:'2026-11-03',daily_clear_day:'2026-11-02',quick_checked_at:'2026-11-03T13:00:00Z'});a.rows('run-logs',2);a.row('live-trips',{'show-id':77});a.row('w-shows',{'show-id':77});a.row('collections',{'show-id':76});const r=await cleanup(a.options);assert.equal(r.new_show,true);assert.equal(a.db['run-logs'].length,0);assert.equal(a.db['live-trips'].length,0);assert.equal(a.db['w-shows'].length,1);assert.equal(a.db.collections.length,1);assert(r.actions.every(x=>x.trigger==='new-show'));
});
test('cleanup and cleanup-logs document table/trigger/start/completed/success/deleted/runID',async t=>{
 const a=setup(t);a.rows('run-logs',1);a.setNow('2026-11-04T00:00:00Z');const r=await cleanup(a.options),log=a.db['cleanup-logs'].find(x=>x.fields.table==='run-logs').fields,summary=a.db.cleanup.find(x=>x.fields.Name==='run-logs').fields;for(const f of [log,summary]){assert.equal(f.result,'Success');assert.equal(f['records-deleted'],1);assert.equal(f['run-id'],r.run_id);assert.ok(f.started);assert.ok(f.completed);}assert.equal(log.trigger,'daily-clear');
});
test('daily deletion failure retains failure evidence without stamping successful daily event',async t=>{
 const a=setup(t);a.rows('run-logs',1);a.setNow('2026-11-04T00:00:00Z');a.options.api.retireCleanup=async()=>{throw Error('offline delete failure');};const r=await cleanup(a.options);assert.equal(r.result,'Error');assert.equal(JSON.parse(fs.readFileSync(path.join(a.runtime,'cleanup-state.json'))).daily_clear_day,'2026-11-02');assert(fs.existsSync(path.join(a.runtime,'cleanup-stop.json')));
});

test('largeexistingbacklogisnotforgottenwhenbelow1000;normalcontinuationsreach500',async t=>{
 const a=setup(t);a.rows('errs-log',4300);for(const [i,r]of a.db['errs-log'].entries())r.createdTime=new Date(Date.UTC(2026,10,2,0,0,i)).toISOString();for(let i=0;i<8;i++){a.setNow(new Date(Date.UTC(2026,10,3,14,i*3)).toISOString());await cleanup(a.options);}assert.equal(a.db['errs-log'].length,500);assert.equal(a.deletes.reduce((n,x)=>n+x.count,0),3800);assert.equal(a.deletes.at(-1).count,300);assert.deepEqual(JSON.parse(fs.readFileSync(path.join(a.runtime,'cleanup-state.json'))).draining_logs,[]);
});
