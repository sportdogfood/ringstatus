"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),zlib=require('node:zlib');
const {currentStore,columns}=require('../current-store'),{fingerprint,verifyDataset,collectStagedSchedules}=require('../store');
const {fixture,NOW}=require('./fixtures/schedule-collector');
global.fetch=async()=>{throw Error('Network forbidden');};
const row=(key='a',name='Class A')=>({key,fields:{show_id:74,show_date:'2026-10-15',ring_number:2,class_number:101,class_name:name,completed_trips:0}});
function setup(t){const root=fs.mkdtempSync(path.join(os.tmpdir(),'current-staging-'));t.after(()=>fs.rmSync(root,{recursive:true,force:true}));let next=0;const db={collections:[],schedules:[]},deleted=[];
 const api={async airtableList(table){return structuredClone(db[table]);},async airtableCreateRecords(table,records){for(const r of records)db[table].push({id:'rec'+(++next),fields:structuredClone(r.fields)});return {failedRows:[]};},async airtablePatchRecords(table,records){for(const r of records){const old=db[table].find(x=>x.id===r.id);if(!old)throw Error('Missing fixture');Object.assign(old.fields,structuredClone(r.fields));}return {failedRows:[]};}};
 const removeRecords=async(table,ids)=>{assert.ok(fs.readdirSync(root).length);deleted.push(...ids);db[table]=db[table].filter(r=>!ids.includes(r.id));};
 const make=()=>currentStore({api,historyRoot:root,removeRecords});return {root,db,api,deleted,make};}
function complete(rows,id='dataset1',show=74,date='2026-10-15'){return {'collection-id':id,'show-id':show,date,status:'complete',started:NOW,completed:NOW,'dataset-id':id,'row-count':rows.length,fingerprint:fingerprint(rows),message:'ok','context-json':'{}'};}
async function save(store,rows,id='dataset1'){const f=complete(rows,id);const r=await store.begin(f);await store.writeRows(id,74,'2026-10-15',rows);await store.finish(r,f);return f;}
test('migration archives exact old records, removes duplicate snapshots and leaves one current key',async t=>{
 const x=setup(t),a=row();x.db.schedules=[{id:'old1',fields:{'stage-key':'d1|0','dataset-id':'d1','row-json':JSON.stringify(a)}},{id:'old2',fields:{'stage-key':'d2|0','dataset-id':'d2','row-json':JSON.stringify(a)}}];
 x.db.collections=[{id:'c1',fields:complete([a],'d1')},{id:'c2',fields:{...complete([a],'d2'),completed:'2026-10-15T15:00:00.000Z'}}];const before=structuredClone(x.db);const s=x.make();await s.begin(complete([a],'attempt'));
 assert.deepEqual(JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(x.root,'initial-airtable.json.gz')))),before);
 const prior=await s.latest(74,'2026-10-15');assert.equal(prior.fields['dataset-id'],'d2');verifyDataset(prior,await s.loadRows('d2'),74,'2026-10-15');
 await s.finish({fields:complete([a],'attempt')},{...complete([a],'attempt'),'dataset-id':'d2'});
 assert.equal(x.db.schedules.length,1);assert.equal(x.db.collections.length,1);assert.equal(x.db.schedules[0].fields['stage-key'],'74|2026-10-15|a');assert.equal(x.db.schedules[0].fields['class-name'],'Class A');assert.equal(x.db.schedules[0].fields['completed-trips'],0);
 assert.ok(fs.existsSync(path.join(x.root,'datasets/d1.json.gz')));
});
test('repeat process and real edits update in place, preserve unrelated fields and archive history',async t=>{
 const x=setup(t);await save(x.make(),[row()]);const id=x.db.schedules[0].id;x.db.schedules[0].fields['owner-note']='preserve';
 await save(x.make(),[row('a','Changed')],'dataset2');assert.equal(x.db.schedules.length,1);assert.equal(x.db.schedules[0].id,id);assert.equal(x.db.schedules[0].fields['owner-note'],'preserve');assert.equal(x.db.schedules[0].fields['class-name'],'Changed');assert.equal(x.db.collections.length,1);assert.equal((await x.make().loadRows('dataset1'))[0].fields.class_name,'Class A');
});
test('failed fetch preserves current rows and last complete state',async t=>{
 const x=setup(t);await save(x.make(),[row()]);const before=structuredClone(x.db.schedules);const s=x.make(),r=await s.begin({...complete([], 'failed'),status:'running'});await s.finish(r,{status:'failed',message:'fetch failed'});
 assert.deepEqual(x.db.schedules,before);assert.equal((await x.make().latest(74,'2026-10-15')).fields.status,'complete');assert.equal(x.db.collections.length,1);assert.equal(x.db.collections[0].fields.status,'failed');
});
test('partial materialization does not promote checkpoint and subsequent normal invocation can converge',async t=>{
 const x=setup(t);await save(x.make(),[row()]);const patch=x.api.airtablePatchRecords;x.api.airtablePatchRecords=async()=>({failedRows:[{error:'fixture failure'}]});
 await assert.rejects(save(x.make(),[row('a','changed')],'dataset2'),/write failed/);assert.equal((await x.make().latest(74,'2026-10-15')).fields['dataset-id'],'dataset1');x.api.airtablePatchRecords=patch;await save(x.make(),[row('a','changed')],'dataset3');assert.equal(x.db.schedules.length,1);
});
test('successful default transition retires old show only through explicit finalize and preserves archive',async t=>{
 const x=setup(t),s=x.make();await save(s,[row()]);assert.equal(x.db.schedules.length,1);await s.finalize(75);assert.equal(x.db.schedules.length,0);assert.equal(x.db.collections.length,0);assert.ok(fs.readdirSync(path.join(x.root,'retired')).length===2);assert.equal((await s.loadRows('dataset1')).length,1);
});
test('current store integrates two collector repeats without growing either table',async t=>{
 const x=setup(t),f=fixture(),shows=[{fields:{show_id:74,customer_id:15,focus_day:'2026-10-15',start_date:'2026-10-15',end_date:'2026-10-17'}}];
 const first=await collectStagedSchedules({store:x.make(),io:f.io,shows,now:NOW});assert.equal(first.ok,true,JSON.stringify(first));assert.equal(x.db.schedules.length,3);assert.equal(x.db.collections.length,3);
 const second=await collectStagedSchedules({store:x.make(),io:f.io,shows,now:NOW});assert.equal(second.ok,true,JSON.stringify(second));assert.equal(x.db.schedules.length,3);assert.equal(x.db.collections.length,3);assert.ok(second.days.every(r=>r.unchanged));
});
test('projection exposes source values without invented times or transport duplicates',()=>{
 const f=columns(row(),74,'2026-10-15','d',NOW);assert.equal(f['start-time'],null);assert.equal(f['class-number'],101);assert.equal(f.status,'active');assert.equal(f.sgl_token_raw,undefined);
});
