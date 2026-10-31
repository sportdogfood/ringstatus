"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os');
const {runShows}=require('../collector'),{archiveFile}=require('../../wef-tenants/archive');
const {readShowFocus}=require('../../wef-schedules/show-source');
const {fixture:collectionFixture}=require('../../wef-schedules/tests/fixtures/schedule-collector');
const now='2026-10-18T20:00:00Z';
function fixture(t){
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'wef-show-proof-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));const history=path.join(dir,'history'),statePath=path.join(dir,'shows-focus.json');
 const tables={shows:[],'run-logs':[],heartbeat:[],sources:[],'err-logs':[]},writes=[],requests=[];let n=0;
 const api={list:async table=>structuredClone(tables[table]),upsert:async(table,key,fields)=>{writes.push(table);if(table==='heartbeat')for(const f of fields){if(f['selected-date']==='')throw Error('Airtable PATCH heartbeat HTTP 422');}for(const f of fields){let r=tables[table].find(r=>r.fields[key]===f[key]);if(!r){r={id:'rec'+(++n),fields:{}};tables[table].push(r);}Object.assign(r.fields,structuredClone(f));}return structuredClone(fields.map(f=>tables[table].find(r=>r.fields[key]===f[key])));},call:async(base,table,method,body)=>{for(const r of body.records)Object.assign(tables[table].find(x=>x.id===r.id).fields,r.fields);}};
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
test('consumer rejects stale, multiple defaults and tampered payload before saving state',async t=>{const f=fixture(t);await runShows(f.options);await assert.rejects(f.consume({now:'2026-10-18T20:31:00Z'}),/stale/);await assert.rejects(f.consume({read:async()=>[f.tables.shows[0],f.tables.shows[0]]}),/Exactly one/);const fields=f.tables.shows[0].fields;fields['default-payload-sha256']='0'.repeat(64);await assert.rejects(f.consume(),/hash mismatch/);assert.equal(fs.existsSync(path.join(f.dir,'schedules-focus.json')),false);});
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

test('normal successes link attempted sources and create no error rows',async t=>{
 const f=fixture(t);const run=await runShows({...f.options,publishDays:async({readSummaries,show})=>{await readSummaries(show);return{ok:true,days:[],errors:[]};}});
 assert.equal(run.ok,true,run.errors.join(';'));assert.equal(f.tables.sources.length,3);assert.equal(f.tables['err-logs'].length,0);assert.equal(f.tables['run-logs'][0].fields.sources.length,3);assert.equal(f.tables.heartbeat[0].fields.result,'Success');
});
test('source failures create linked separate errors and do not invent a success time',async t=>{
 const f=fixture(t);const run=await runShows({...f.options,fetchJson:async()=>{throw Error('fixture source unavailable');}});assert.equal(run.ok,false);assert.equal(f.tables['err-logs'].length,1);
 const err=f.tables['err-logs'][0].fields;assert.deepEqual(err.run,[f.tables['run-logs'][0].id]);assert.deepEqual(err.source,[f.tables.sources[0].id]);assert.equal(err['run-id'],run.run_id);assert.equal(f.tables.sources[0].fields['last-success-at'],undefined);assert.equal(f.tables.sources[0].fields['last-result'],'Failed');
});
test('error-linked log rows are not reused; successful slot source links are cleared',async t=>{
 const f=fixture(t);f.tables['run-logs'].push({id:'oldErr',fields:{'run-id':'old-error',result:'Failed',started:'2026-10-14','err-logs':['error1']}},{id:'oldOk',fields:{'run-id':'old-ok',result:'Success',started:'2026-10-14',sources:['oldSource']}});
 const run=await runShows({...f.options,logLimit:2});assert.equal(run.ok,true,run.errors.join(';'));assert.equal(f.tables['run-logs'].find(r=>r.id==='oldErr').fields['run-id'],'old-error');assert.equal(f.tables['run-logs'].find(r=>r.id==='oldOk').fields['run-id'],run.run_id);assert.ok(!f.tables['run-logs'].find(r=>r.id==='oldOk').fields.sources.includes('oldSource'));
});
test('source-log failure does not undo completed discovery and is reported separately',async t=>{
 const f=fixture(t),upsert=f.api.upsert;f.api.upsert=async(table,...args)=>{if(table==='sources')throw Error('fixture source log failure');return upsert(table,...args);};const run=await runShows(f.options);assert.equal(run.discovery_ok,true);assert.equal(f.tables.shows.length,1);assert.equal(run.ok,true);assert.equal(run.observability.ok,false);assert.equal(f.tables['err-logs'][0].fields.stage,'source-log');assert.equal(f.tables.heartbeat[0].fields.result,'Success');assert.equal(f.tables.heartbeat[0].fields.errors,'');assert.match(f.tables['run-logs'][0].fields.errors,/Source logging/);
});

test('count-source rejection links the error to Season without clearing canonical discovery',async t=>{
 const f=fixture(t);const run=await runShows({...f.options,readSummaries:async()=>{throw Error('fixture retained hash mismatch');},publishDays:async({readSummaries,show})=>{try{await readSummaries(show);}catch(e){return{ok:false,days:[],errors:[e.message]};}}});
 assert.equal(run.discovery_ok,true);assert.equal(run.ok,false);const source=f.tables.sources.find(r=>r.fields['source-key']==='season-counts');assert.equal(source.fields['last-result'],'Failed');assert.deepEqual(f.tables['err-logs'][0].fields.source,[source.id]);assert.equal(f.tables.heartbeat[0].fields.result,'Partial');
});
test('archive errors remain separate linked errors without treating transport success as failure',async t=>{
 const f=fixture(t);const run=await runShows({...f.options,archive:async()=>{throw Error('fixture archive failure');}});assert.equal(run.ok,false);assert.equal(f.tables['err-logs'][0].fields.stage,'archive');assert.equal(f.tables.sources[0].fields['last-result'],'Success');assert.deepEqual(f.tables['err-logs'][0].fields.run,[f.tables['run-logs'][0].id]);
});

test('all protected error-linked log slots append a new attempt without blocking discovery',async t=>{
 const f=fixture(t);f.tables['run-logs'].push({id:'protected',fields:{'run-id':'prior-error',result:'Failed',started:'2026-10-14','err-logs':['prior-error-row']}});
 const run=await runShows({...f.options,logLimit:1});assert.equal(run.ok,true,JSON.stringify(run));assert.equal(f.requests.length,2);assert.equal(f.tables['run-logs'].length,2);assert.equal(f.tables['run-logs'][0].fields['run-id'],'prior-error');assert.equal(f.tables.heartbeat[0].fields.result,'Success');
});

test('completion evidence failure also marks telemetry health failed and saves separate error',async t=>{
 const f=fixture(t),upsert=f.api.upsert;f.api.upsert=async(table,...args)=>{if(table==='heartbeat')throw Error('fixture heartbeat write failure');return upsert(table,...args);};const run=await runShows(f.options);assert.equal(run.ok,false);assert.equal(run.observability.ok,false);assert.equal(f.tables['err-logs'][0].fields.stage,'completion-log');assert.deepEqual(f.tables['err-logs'][0].fields.run,[f.tables['run-logs'][0].id]);
});

test('failed discovery clears absent heartbeat date with null and records its real error',async t=>{
 const f=fixture(t);await runShows(f.options);const before=structuredClone(f.tables.shows);
 const run=await runShows({...f.options,fetchJson:async()=>{throw Error('Show source GET /ring failed after 3 attempts: request failed [TypeError > ECONNRESET]');}});
 assert.equal(run.ok,false);assert.equal(run.observability.ok,true);assert.deepEqual(f.tables.shows,before);
 const heartbeat=f.tables.heartbeat[0].fields;assert.equal(heartbeat.result,'Failed');assert.equal(heartbeat['selected-date'],null);assert.equal(heartbeat['show-id'],null);assert.match(heartbeat.errors,/ECONNRESET/);
 assert.equal(f.tables['err-logs'].length,1);assert.deepEqual(f.tables['err-logs'][0].fields.run,[f.tables['run-logs'].at(-1).id]);assert.equal(run.errors.length,1);
});


test('outside window updates logs and heartbeat, retains source timestamps and publishes counts without endpoint calls',async t=>{
 const f=fixture(t);await runShows(f.options);
 const before=structuredClone(f.tables.shows),sources=structuredClone(f.tables.sources);let publications=0;
 f.requests.length=0;f.writes.length=0;
 const run=await runShows({...f.options,clock:()=> '2026-10-19T12:00:00Z',publishDays:async({show})=>{publications++;assert.deepEqual(show,before[0]);return{ok:true,days:[],errors:[]};}});
 assert.equal(run.ok,true,JSON.stringify(run));assert.equal(run.discovery.show,'Skipped');assert.equal(run.discovery.show_day,'Skipped');
 assert.equal(f.requests.length,0);assert.deepEqual(f.tables.shows,before);assert.deepEqual(f.tables.sources,sources);assert.equal(f.writes.includes('shows'),false);
 assert.equal(publications,1);assert.equal(f.tables.heartbeat[0].fields['checked-at'],'2026-10-19T12:00:00Z');assert.match(f.tables['run-logs'].at(-1).fields.summary,/Discovery skipped/);
});

test('daily window queries only known show schedule and preserves canonical record identity',async t=>{
 const f=fixture(t);await runShows(f.options);const id=f.tables.shows[0].id;f.requests.length=0;
 const run=await runShows({...f.options,clock:()=> '2026-10-19T20:00:00Z'});
 assert.equal(run.ok,true,JSON.stringify(run));assert.equal(f.requests.length,1);assert.equal(new URL(f.requests[0]).pathname,'/schedule');
 assert.equal(new URL(f.requests[0]).searchParams.get('show_id'),'74');assert.equal(run.discovery.show,'Skipped');assert.equal(run.discovery.show_day,'Checked');assert.equal(f.tables.shows[0].id,id);
 assert.equal(run.archives.some(a=>a.kind==='default-ring'),false);
});

test('daily discovery cannot switch to a new show returned by source',async t=>{
 const f=fixture(t);await runShows(f.options);const before=structuredClone(f.tables.shows);f.setShow(75);f.requests.length=0;
 const run=await runShows({...f.options,clock:()=> '2026-10-19T20:00:00Z'});
 assert.equal(run.ok,false);assert.match(run.errors.join(';'),/disagrees/);assert.deepEqual(f.tables.shows,before);assert.equal(f.requests.length,1);
});

test('daily source-selected day advances from source, never computer date',async t=>{
 const f=fixture(t);await runShows(f.options);const source=f.options.fetchJson;
 const run=await runShows({...f.options,clock:()=> '2026-10-19T20:00:00Z',fetchJson:async url=>({...await source(url),show_date:'2026-10-16'})});
 assert.equal(run.ok,true,JSON.stringify(run));assert.equal(f.tables.shows[0].fields.selected_date,'2026-10-16');
});

test('missing canonical authority outside Sunday fails without substitute source discovery',async t=>{
 for(const at of ['2026-10-19T12:00:00Z','2026-10-19T20:00:00Z']){
  const f=fixture(t);const run=await runShows({...f.options,clock:()=>at});assert.equal(run.ok,false);assert.match(run.errors.join(';'),/Exactly one/);assert.equal(f.requests.length,0);assert.equal(f.tables['run-logs'].length,1);assert.equal(f.tables.heartbeat[0].fields.result,'Failed');
 }
});

test('outside window preserves existing count stop without retrying it',async t=>{
 const f=fixture(t);await runShows(f.options);const stopped='{"error":"existing failure"}';fs.writeFileSync(f.options.dayStopPath,stopped);let calls=0;f.requests.length=0;
 const run=await runShows({...f.options,clock:()=> '2026-10-19T12:00:00Z',publishDays:async()=>{calls++;throw Error('must not run');}});
 assert.equal(run.ok,false);assert.equal(run.discovery.show_day,'Skipped');assert.equal(calls,0);assert.equal(f.requests.length,0);assert.equal(fs.readFileSync(f.options.dayStopPath,'utf8'),stopped);assert.equal(f.tables.heartbeat[0].fields.result,'Partial');
});
