"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os');
const {runShows}=require('../collector'),{archiveFile}=require('../../wef-tenants/archive');
const {readShowFocus}=require('../../wef-schedules/show-source');
const {fixture:collectionFixture}=require('../../wef-schedules/tests/fixtures/schedule-collector');
const now='2026-10-15T12:00:00Z';
function fixture(t){
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'wef-show-proof-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));const history=path.join(dir,'history'),statePath=path.join(dir,'shows-focus.json');
 const tables={shows:[],'run-logs':[],heartbeat:[]},writes=[],requests=[];let n=0;
 const api={list:async table=>structuredClone(tables[table]),upsert:async(table,key,fields)=>{writes.push(table);for(const f of fields){let r=tables[table].find(r=>r.fields[key]===f[key]);if(!r){r={id:'rec'+(++n),fields:{}};tables[table].push(r);}Object.assign(r.fields,structuredClone(f));}return structuredClone(fields.map(f=>tables[table].find(r=>r.fields[key]===f[key])));},call:async(base,table,method,body)=>{for(const r of body.records)Object.assign(tables[table].find(x=>x.id===r.id).fields,r.fields);}};
 let showId=74;const c=collectionFixture();
 const source=async url=>{requests.push(url);if(new URL(url).pathname==='/ring')return{show:{show_id:showId}};const payload=await c.io.fetchJson(url);return{...payload,show:{show_id:showId,facility_id:516,show_name:'Fixture',start_date:'2026-10-15',end_date:'2026-10-17'}};};
 const options={publishDays:async()=>({ok:true,days:[],errors:[]}),readSummaries:async()=>({days:[]}),dayStopPath:path.join(dir,'day-stop.json'),api,clock:()=>now,statePath,fetchJson:source,archive:(...args)=>archiveFile(history,...args)};
 const read=async()=>api.list('shows'),consume=extra=>readShowFocus({statePath:path.join(dir,'schedules-focus.json'),now,read,archiveRoot:history,...extra});
 return{dir,history,options,consume,tables,writes,requests,api,setShow:id=>showId=id};
}
test('producer owns two discovery requests; consumer reads verified show without source calls',async t=>{
 const f=fixture(t),r=await runShows(f.options);assert.equal(r.ok,true,r.errors.join(';'));assert.equal(f.requests.length,2);const row=f.tables.shows[0];assert.equal(row.fields.selected_date,'2026-10-15');
 const found=await f.consume();assert.equal(found.show.fields.show_id,74);assert.equal(found.state.show_source.record,row.id);assert.equal(f.requests.length,2);assert.equal(found.payload.show.show_id,74);assert.equal(r.archives.length,3);
});
test('repeat discovery preserves show record ID and records each run',async t=>{const f=fixture(t);await runShows(f.options);const id=f.tables.shows[0].id;assert.equal((await runShows(f.options)).ok,true);assert.equal(f.tables.shows.length,1);assert.equal(f.tables.shows[0].id,id);assert.equal(f.tables['run-logs'].length,2);assert.equal(f.tables.heartbeat.length,1);});
test('failed discovery preserves published show and records failure',async t=>{const f=fixture(t);await runShows(f.options);const previous=structuredClone(f.tables.shows);const r=await runShows({...f.options,fetchJson:async()=>{throw Error('Source unavailable');}});assert.equal(r.ok,false);assert.deepEqual(f.tables.shows,previous);assert.equal(f.tables.heartbeat[0].fields.result,'Failed');assert.equal(f.tables['run-logs'].at(-1).fields.result,'Failed');});
test('rollover retains old show and marks only new show default',async t=>{const f=fixture(t);await runShows(f.options);const id=f.tables.shows[0].id;f.setShow(75);assert.equal((await runShows(f.options)).ok,true);assert.equal(f.tables.shows.length,2);assert.equal(f.tables.shows[0].id,id);assert.equal(f.tables.shows[0].fields.is_default,false);assert.equal((await f.consume()).show.fields.show_id,75);});
test('archive failure prevents canonical show changes',async t=>{const f=fixture(t);await runShows(f.options);const before=structuredClone(f.tables.shows);const r=await runShows({...f.options,archive:async()=>{throw Error('Archive disk failure');}});assert.equal(r.ok,false);assert.deepEqual(f.tables.shows,before);});
test('consumer rejects stale, multiple defaults and tampered payload before saving state',async t=>{const f=fixture(t);await runShows(f.options);await assert.rejects(f.consume({now:'2026-10-15T12:31:00Z'}),/stale/);await assert.rejects(f.consume({read:async()=>[f.tables.shows[0],f.tables.shows[0]]}),/Exactly one/);const fields=f.tables.shows[0].fields;fields['default-payload-sha256']='0'.repeat(64);await assert.rejects(f.consume(),/hash mismatch/);assert.equal(fs.existsSync(path.join(f.dir,'schedules-focus.json')),false);});
test('consumer preserves show-specific manual inputs without changing canonical defaults',async t=>{const f=fixture(t);await runShows(f.options);const file=path.join(f.dir,'schedules-focus.json');fs.writeFileSync(file,JSON.stringify({overrides:{74:{focus_day:'2026-10-17',ring_collection:'existing'}}}));const found=await f.consume();assert.equal(found.show.fields.focus_day,'2026-10-17');assert.equal(found.show.fields.sgl_default_day,'2026-10-15');assert.equal(found.show.fields.ring_collection,'existing');assert.equal(f.tables.shows[0].fields.selected_date,'2026-10-15');});
test('schedules collect every day using shared metadata and never call discovery endpoints',async t=>{
 const f=fixture(t);await runShows(f.options);const c=collectionFixture(),datasets=new Map(),requests=[];
 const store={async begin(fields){return{fields};},async latest(){return null;},async writeRows(id,show,date,rows){datasets.set(id,rows);},async loadRows(id){return datasets.get(id)||[];},async finish(){}};
 const resolveShow=args=>f.consume({...args,statePath:path.join(f.dir,'schedule-run.json')});
 const io={...c.io,fetchJson:async u=>{requests.push(u);const p=new URL(u);assert.notEqual(p.pathname,'/ring');assert.notEqual(p.searchParams.get('date'),'00/00/00');return c.io.fetchJson(u);}};
 const result=await require('../../wef-schedules/store').runStagedCollector({store,io,resolveShow,now,lockPath:path.join(f.dir,'collector.lock'),statePath:path.join(f.dir,'schedule-run.json')});
 assert.equal(result.ok,true,JSON.stringify(result));assert.equal(result.days.length,3);assert.equal(requests.length,3);assert.equal(result.discovery,'new');
});
test('consumer rejects archive traversal and inconsistent dates',async t=>{const f=fixture(t);await runShows(f.options);const row=f.tables.shows[0];row.fields['default-payload-file']=path.join(f.dir,'outside.json.gz');await assert.rejects(f.consume(),/outside archive/);});
test('producer adapter blocks changes outside shows base and all deletes',async()=>{const api=require('../airtable').airtable({request:async()=>{throw Error('Network forbidden');}});await assert.rejects(api.call('appxKPBtdl2Rdq7CS','shows','PATCH',{}));await assert.rejects(api.call(require('../settings').BASE_ID,'shows','DELETE',{}));});

test('day failure preserves discovery and stops only day retries',async t=>{
 const f=fixture(t);let attempts=0;const options={...f.options,publishDays:async()=>{attempts++;throw Error('Day failed');}};
 const first=await runShows(options);assert.equal(first.discovery_ok,true);assert.equal(first.ok,false);assert.equal(f.tables.heartbeat[0].fields.result,'Partial');
 const second=await runShows(options);assert.equal(second.discovery_ok,true);assert.equal(attempts,1);assert.equal(f.requests.length,4);
});
