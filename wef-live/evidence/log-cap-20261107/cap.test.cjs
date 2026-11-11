'use strict';
globalThis.fetch=async()=>{throw Error('Offline network forbidden');};
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {cleanup,TABLES,LOGS,eligible,protectedRun}=require('./candidate/wef-live/cleanup');
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

test('checks every invocation and persists continuation below1000 until500 then stops',async t=>{
 const a=setup(t);a.rows('run-logs',1200);let r=await cleanup({...a.options,checkShow:false});assert.equal(r.result,'Success');assert.equal(a.db['run-logs'].length,700);assert.equal(a.deletes[0].count,500);assert.equal(a.db['run-logs'][0].fields['run-id'],'500');
 assert.deepEqual(JSON.parse(fs.readFileSync(path.join(a.runtime,'cleanup-state.json'))).draining_logs,['run-logs']);
 a.setNow('2026-11-03T14:03:00Z');r=await cleanup({...a.options,checkShow:false});assert.equal(a.db['run-logs'].length,500);assert.equal(a.deletes[1].count,200);assert.deepEqual(JSON.parse(fs.readFileSync(path.join(a.runtime,'cleanup-state.json'))).draining_logs,[]);
 a.setNow('2026-11-03T14:06:00Z');r=await cleanup({...a.options,checkShow:false});assert.equal(r.result,'Skipped');assert.equal(r.actions.length,0);assert.equal(a.deletes.length,2);
});
test('below1000 without prior trigger does not delete;exact1000 drops to500',async t=>{
 const a=setup(t);a.rows('run-logs',999);let r=await cleanup(a.options);assert.equal(r.actions.length,0);assert.equal(a.deletes.length,0);a.rows('run-logs',1);r=await cleanup(a.options);assert.equal(a.db['run-logs'].length,500);assert.equal(a.deletes[0].count,500);
});
test('large backlog drains through repeated normal-shaped invocations,never over500 deletes per table',async t=>{
 const a=setup(t);a.rows('run-logs',5907);for(let i=0;i<11;i++){a.setNow(new Date(Date.UTC(2026,10,3,14,i*3)).toISOString());await cleanup({...a.options,checkShow:false});}assert.equal(a.db['run-logs'].length,500);assert.equal(a.deletes.reduce((n,x)=>n+x.count,0),5407);assert(a.deletes.every(x=>x.count<=500));assert.equal(a.deletes.at(-1).count,407);
});
test('running/current/diff records preserved and only logging tables threshold-cleaned',async t=>{
 const a=setup(t);a.rows('heartbeat',1000);a.row('heartbeat',{'run-id':'active',result:'Running'});a.row('heartbeat',{'run-id':'current',result:'Success',completed:'2026-11-03T13:00:00Z'});a.row('run-logs',{'run-id':'current|diff|1',result:'Success',completed:'2026-11-03T13:00:00Z'});a.row('live-groups',{'show-id':77});a.options.protectedRuns=['current'];await cleanup(a.options);await cleanup(a.options);assert.equal(a.db.heartbeat.length,500);assert(a.db.heartbeat.some(x=>x.fields['run-id']==='active'));assert(a.db.heartbeat.some(x=>x.fields['run-id']==='current'));assert.equal(a.db['run-logs'].length,1);assert.equal(a.db['live-groups'].length,1);
});
test('new calendar day still clears eligible logs below1000 and preserves active rows',async t=>{
 const a=setup(t,{show:77,day:'2026-11-02'});for(const name of LOGS)a.rows(name,3);a.row('heartbeat',{result:'Running'});const r=await cleanup(a.options);assert.equal(r.result,'Success');assert.equal(a.db.heartbeat.length,1);assert.equal(a.db['run-logs'].length,0);assert.equal(a.db['errs-log'].length,0);assert(r.actions.every(x=>x.trigger==='calendar-day'));assert(!fs.existsSync(path.join(a.runtime,'history')));
});
test('new show still clears checked owned records without reference deletion or archives',async t=>{
 const a=setup(t,{show:76,day:'2026-11-03'});a.rows('run-logs',2);a.row('live-groups',{'show-id':77});a.row('w-shows',{'show-id':77});const r=await cleanup(a.options);assert.equal(r.result,'Success');assert.equal(a.db['live-groups'].length,0);assert.equal(a.db['run-logs'].length,0);assert.equal(a.db['w-shows'].length,1);assert(!fs.existsSync(path.join(a.runtime,'history')));
});
