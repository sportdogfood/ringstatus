'use strict';
globalThis.fetch=async()=>{throw Error('Review offline network forbidden');};
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {cleanup,dailyClearDay,quickRows}=require('./candidate/wef-live/cleanup');
function fixture(t,state){
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'cleanup-independent-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));
 fs.writeFileSync(path.join(runtime,'cleanup-state.json'),JSON.stringify(state));
 const db={shows:[{id:'recShow',fields:{show_id:77,is_default:true,customer_id:15}}],cleanup:[{id:'recControl',fields:{Name:'run-logs',cleanup:true}}],'run-logs':[]};
 let now='2026-11-08T00:00:00Z',reads=0,deletes=0;
 const api={list:async table=>{reads++;return structuredClone(db[table]||[]);},upsert:async(table,key,values)=>{for(const fields of values){const rows=db[table]||=[];const found=rows.find(r=>r.fields[key]===fields[key]);if(found)Object.assign(found.fields,fields);else rows.push({id:'recLog'+rows.length,fields:structuredClone(fields)});}},call:async(base,table,method,body)=>({records:body.records.map(row=>{const found=db[table].find(r=>r.id===row.id);Object.assign(found.fields,row.fields);return found;})}),retireCleanup:async(table,rows,policy,done)=>{deletes+=rows.length;db[table]=db[table].filter(r=>!rows.some(d=>d.id===r.id));done(rows.map(r=>r.id));}};
 return{db,api,runtime,options:{api,runtime,clock:()=>now},setNow:v=>now=v,reads:()=>reads,deletes:()=>deletes,state:()=>JSON.parse(fs.readFileSync(path.join(runtime,'cleanup-state.json')))};
}
test('review: daily clear preserves independently not-due quick clock; exact 3h becomes due',async t=>{
 const a=fixture(t,{show:77,daily_clear_day:'2026-11-06',quick_checked_at:'2026-11-07T23:00:00Z'});
 a.db['run-logs'].push({id:'recCompleted',fields:{result:'Success',completed:'2026-11-07T23:30:00Z'}});
 let r=await cleanup(a.options);assert.equal(r.daily_due,true);assert.equal(r.quick_due,false);assert.equal(a.state().quick_checked_at,'2026-11-07T23:00:00Z');assert.equal(a.deletes(),1);
 a.setNow('2026-11-08T02:00:00Z');r=await cleanup(a.options);assert.equal(r.quick_due,true);assert.equal(r.daily_due,false);
});
test('review: exact threshold chooses oldest 500 and ignores earlier unfinished rows',()=>{
 const rows=Array.from({length:1000},(_,i)=>({id:'rec'+i,createdTime:new Date(Date.UTC(2026,10,1,0,i)).toISOString(),fields:{result:'Success',completed:'2026-11-07T23:00:00Z'}})).reverse();
 assert.equal(quickRows('run-logs',rows,999).length,0);const targets=quickRows('run-logs',rows,1000);assert.equal(targets.length,500);assert.equal(targets[0].id,'rec0');assert.equal(targets[499].id,'rec499');
 assert.equal(quickRows('live-trips',rows,1000).length,0);
});
test('review: installed checkpoint migration catches latest missed event once without old drain continuation',async t=>{
 const a=fixture(t,{show:77,day:'2026-11-07',completed:'2026-11-08T01:51:00Z',quick_checked_at:'2026-11-08T01:51:00Z',draining_logs:['run-logs']});a.setNow('2026-11-08T02:00:00Z');
 await cleanup(a.options);assert.equal(a.state().daily_clear_day,'2026-11-07');assert.equal(a.state().draining_logs,undefined);const reads=a.reads();await cleanup({...a.options,checkShow:false});assert.equal(a.reads(),reads);
});
test('review: spring/fall DST transition days retain 19h Eastern event',()=>{
 for(const [instant,day] of [['2026-03-08T22:59:59Z','2026-03-07'],['2026-03-08T23:00:00Z','2026-03-08'],['2026-11-01T23:59:59Z','2026-10-31'],['2026-11-02T00:00:00Z','2026-11-01']])assert.equal(dailyClearDay(instant),day);
});
test('review: a failed clear is stopped on repeat with no additional API work',async t=>{
 const a=fixture(t,{show:77,daily_clear_day:'2026-11-06',quick_checked_at:'2026-11-07T23:00:00Z'});a.db['run-logs'].push({id:'recFail',fields:{result:'Success',completed:'2026-11-07T23:30:00Z'}});a.api.retireCleanup=async()=>{throw Error('offline failure');};
 const first=await cleanup(a.options);assert.equal(first.result,'Error');const reads=a.reads();const second=await cleanup(a.options);assert.equal(second.result,'Blocked');assert.equal(a.reads(),reads);assert.equal(a.state().daily_clear_day,'2026-11-06');
});
