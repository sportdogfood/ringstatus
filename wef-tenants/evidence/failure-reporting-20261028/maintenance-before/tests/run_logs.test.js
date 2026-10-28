"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),zlib=require('node:zlib');
const {runStagedCollector}=require('../store');
const {fixture}=require('./fixtures/schedule-collector');
global.fetch=async()=>{throw Error('Network forbidden in run-log tests');};
function files(t){const dir=fs.mkdtempSync(path.join(os.tmpdir(),'wef-run-logs-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));return {statePath:path.join(dir,'focus.json'),lockPath:path.join(dir,'lock'),historyRoot:path.join(dir,'history')};}
function apiFixture(){const records=[];const result={records,api:{
 async airtableList(table,q,base){assert.equal(table,'run-logs');assert.equal(base,'appUq5Qz9kARqFija');return structuredClone(records);},
 async airtableCreateRecords(table,rows,base){assert.equal(table,'run-logs');assert.equal(base,'appUq5Qz9kARqFija');for(const row of rows)records.push({id:'rec'+records.length,fields:structuredClone(row.fields)});return {failedRows:[]};},
 async airtablePatchRecords(table,rows,base){assert.equal(table,'run-logs');assert.equal(base,'appUq5Qz9kARqFija');for(const row of rows)Object.assign(records.find(r=>r.id===row.id).fields,structuredClone(row.fields));return {failedRows:[]};}
 }};result.request=async(url,options)=>{const r=await result.api.airtableCreateRecords('run-logs',JSON.parse(options.body).records,'appUq5Qz9kARqFija');return {ok:!r.failedRows.length,status:r.failedRows.length?500:200};};return result;}
function logger(t,a,opts={}){return require('../run-logs').runLogs({api:a.api,request:a.request,historyRoot:files(t).historyRoot,...opts});}

test('verified show/ring totals are readable in the scheduled run log',async t=>{
 const a=apiFixture(),log=logger(t,a),record=await log.begin();await log.finish(record,{ok:true,days:[],references:{show_id:75,selected_date:'2026-10-22',rings:24,selected_rings:6,helpers:6,schedule_links:435}});assert.match(a.records[0].fields.summary,/source-selected date 2026-10-22; 24 daily rings \(6 selected\); 6 static helpers; 435 schedule links/);
});
test('real collector publishes one readable run log across all show days',async t=>{
 const locations=files(t),a=apiFixture(),log=logger(t,a),f=fixture();const fetch=f.io.fetchJson;
 f.io.fetchJson=async u=>{const url=new URL(u);if(url.pathname==='/ring')return {show:{show_id:74}};const p=await fetch(u);if(url.searchParams.get('date')==='00/00/00')p.show={show_id:74,start_date:'2026-10-15',end_date:'2026-10-17'};return p;};
 const datasets=new Map(),saved=[];
 const store={async begin(fields){return {fields};},async latest(show,date){return saved.find(r=>r.fields.date===date)||null;},async writeRows(id,show,date,rows){datasets.set(id,structuredClone(rows));},async loadRows(id){return datasets.get(id)||[];},async finish(record,fields){Object.assign(record.fields,fields);saved.push(record);}};
 const r=await runStagedCollector({...locations,store,runLog:log,io:f.io});assert.equal(r.ok,true);
 assert.equal(a.records.length,1);const out=a.records[0].fields;
 assert.equal(out.result,'Success');assert.equal(out.rows,3);assert.equal(out['days-complete'],3);assert.equal(out['days-failed'],0);
 assert.equal(out['show-id'],74);assert.ok(Date.parse(out.completed)>=Date.parse(out.started));
 assert.match(out.summary,/2026-10-15: 1 rows/);assert.match(out.summary,/2026-10-17: 1 rows/);assert.equal(out.errors,'');
});
test('discovery failure is recorded without rerunning collector',async t=>{
 const locations=files(t),a=apiFixture(),log=logger(t,a);let calls=0;
 await assert.rejects(runStagedCollector({...locations,store:{},runLog:log,io:{fetchJson:async()=>{calls++;throw Error('source unavailable');}}}),/source unavailable/);
 assert.equal(calls,1);assert.equal(a.records.length,1);assert.equal(a.records[0].fields.result,'Failed');assert.match(a.records[0].fields.errors,/source unavailable/);assert.ok(a.records[0].fields.completed);assert.equal(a.records[0].fields.rows,null);
 assert.equal(fs.existsSync(locations.lockPath),false);
});
test('warnings and partial failures preserve readable per-day results and observed counts',async t=>{
 const a=apiFixture(),log=logger(t,a);const r=await log.begin();
 await log.finish(r,{ok:false,days:[{show_id:74,date:'2026-10-15',ok:true,row_count:0,warnings:[{code:'empty_show_day',message:'No classes'}]},{show_id:74,date:'2026-10-16',ok:false,error:'fetch failed'}]});
 const f=a.records[0].fields;assert.equal(f.result,'Failed');assert.equal(f.rows,0);assert.equal(f['days-failed'],1);assert.match(f.summary,/empty_show_day/);assert.match(f.errors,/2026-10-16: fetch failed/);
 const next=await log.begin();await log.finish(next,{ok:true,days:[{show_id:74,date:'2026-10-15',ok:true,row_count:0,warnings:[{code:'empty_show_day',message:'No classes'}]}]});assert.equal(a.records[1].fields.result,'Warning');
});
test('bounded logs archive exact completed record before reusing its slot; running rows retained',async t=>{
 const a=apiFixture(),locations=files(t);const log=require('../run-logs').runLogs({api:a.api,request:a.request,historyRoot:locations.historyRoot,limit:2});
 const first=await log.begin();await log.finish(first,{ok:true,days:[]});const prior=structuredClone(a.records[0]);
 const second=await log.begin();await log.finish(second,{ok:true,days:[]});
 const third=await log.begin();assert.equal(a.records.length,2);assert.equal(third.id,prior.id);
 const archives=fs.readdirSync(locations.historyRoot);assert.equal(archives.length,1);
 assert.deepEqual(JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(locations.historyRoot,archives[0])))),prior);
 const fourth=await log.begin();assert.notEqual(fourth.id,third.id);
 await assert.rejects(log.begin(),/running/);
});
test('log write and readback failures are explicit',async t=>{
 const a=apiFixture();a.api.airtableCreateRecords=async()=>({failedRows:[{error:'denied'}]});await assert.rejects(logger(t,a).begin(),/log.*write/i);
 const b=apiFixture(),log=logger(t,b),r=await log.begin();b.api.airtablePatchRecords=async()=>({failedRows:[]});await assert.rejects(log.finish(r,{ok:true,days:[]}),/readback/);
});

test('omitted Airtable blank cells pass while corrupt populated values fail readback',async t=>{
 const a=apiFixture(),list=a.api.airtableList;
 a.api.airtableList=async(...args)=>(await list(...args)).map(r=>({...r,fields:Object.fromEntries(Object.entries(r.fields).filter(([,v])=>v!==''&&v!==null))}));
 const log=logger(t,a),record=await log.begin();
 await log.finish(record,{ok:true,days:[{show_id:74,date:'2026-10-16',ok:true,row_count:1}]});
 assert.equal(a.records[0].fields.result,'Success');assert.equal(a.records[0].fields.rows,1);
 const patch=a.api.airtablePatchRecords;
 a.api.airtablePatchRecords=async(...args)=>{const result=await patch(...args);a.records[0].fields.rows=999;return result;};
 await assert.rejects(log.finish(record,{ok:true,days:[{show_id:74,date:'2026-10-16',ok:true,row_count:1}]}),/readback mismatch: rows/);
});


test('committed create timeout is recovered by run ID without a second POST',async t=>{
 const a=apiFixture();let posts=0;
 const request=async(url,options)=>{posts++;assert.equal(options.method,'POST');assert.match(url,/appUq5Qz9kARqFija\/run-logs$/);const fields=JSON.parse(options.body).records[0].fields;a.records.push({id:'committed',fields});throw Error('response timeout');};
 const log=logger(t,a,{request}),r=await log.begin();assert.equal(posts,1);assert.equal(r.id,'committed');assert.equal(a.records.length,1);
});
test('uncommitted and ambiguous create outcomes fail without retrying POST',async t=>{
 for(const duplicates of [0,2]){const a=apiFixture();let posts=0;
 const log=logger(t,a,{request:async(url,options)=>{posts++;const fields=JSON.parse(options.body).records[0].fields;for(let i=0;i<duplicates;i++)a.records.push({id:'rec'+i,fields});throw Error('timeout');}});
 await assert.rejects(log.begin(),/Run log.*(unresolved|ambiguous)/);assert.equal(posts,1);}
});
test('archive events persist on owning run and cleanup updates same file entry',async t=>{
 const a=apiFixture(),log=logger(t,a),r=await log.begin();
 await log.archive(r,{file:'C:/private/history/retired/old.json.gz',kind:'retired',outcome:'Created',shows:[74],dates:['2026-10-15'],records:5,sha256:'a'.repeat(64)});
 await log.archive(r,{file:'C:/private/history/retired/old.json.gz',cleanup:'Complete'});
 assert.equal(a.records[0].fields['archive-count'],1);assert.match(a.records[0].fields.archives,/74/);assert.match(a.records[0].fields.archives,/5 records/);assert.match(a.records[0].fields.archives,/cleanup Complete/);
 await log.finish(r,{ok:true,days:[]});assert.match(a.records[0].fields.archives,/old.json.gz/);
});

test('heartbeat uses the verified terminal run log for success, warning and early failure',async t=>{
 const a=apiFixture(),seen=[];const log=logger(t,a,{heartbeat:{async update(fields,date){assert.notEqual(fields.result,'Running');assert.deepEqual(a.records.find(r=>r.fields['run-id']===fields['run-id']).fields,fields);seen.push({fields,date});}}});
 const good=await log.begin();await log.finish(good,{ok:true,days:[{show_id:75,date:'2026-10-22',ok:true,row_count:8}],references:{show_id:75,selected_date:'2026-10-22'}});
 const bad=await log.begin();await log.finish(bad,undefined,Error('Discovery failed'));
 assert.equal(seen.length,2);assert.equal(seen[0].fields.rows,8);assert.equal(seen[0].date,'2026-10-22');assert.equal(seen[1].fields.result,'Failed');assert.equal(seen[1].date,null);assert.match(seen[1].fields.errors,/Discovery failed/);
});
test('heartbeat failure is recorded in owning run-log and surfaces as failure',async t=>{const a=apiFixture(),log=logger(t,a,{heartbeat:{async update(){throw Error('Status unavailable');}}});const r=await log.begin();await assert.rejects(log.finish(r,{ok:true,days:[]}),/Heartbeat update failed/);assert.equal(a.records[0].fields.result,'Failed');assert.match(a.records[0].fields.errors,/Heartbeat update failed: Status unavailable/);});

test('catalog links verified terminal log before heartbeat publishes show relation',async t=>{const a=apiFixture(),seen=[];const log=logger(t,a,{catalog:{async sync(run){assert.equal(run.fields.result,'Success');seen.push('catalog');return{...run,fields:{...run.fields,shows:['showRecord']}};}},heartbeat:{async update(fields){seen.push('heartbeat');assert.deepEqual(fields.shows,['showRecord']);}}});await log.finish(await log.begin(),{ok:true,days:[]});assert.deepEqual(seen,['catalog','heartbeat']);});
test('catalog failure stops status publication and is recorded in run history',async t=>{const a=apiFixture();let calls=0;const log=logger(t,a,{catalog:{async sync(){throw Error('Archive invalid');}},heartbeat:{async update(){calls++;}}});await assert.rejects(log.finish(await log.begin(),{ok:true,days:[]}),/Show index update failed/);assert.equal(calls,0);assert.equal(a.records[0].fields.result,'Failed');assert.match(a.records[0].fields.errors,/Archive invalid/);});
