'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {runLogs}=require('../run-logs');
const {errorLogs}=require('../error-logs');
const {runAttempt,recoverAttempts,readReceipts}=require('../attempts');
function fixture(t){
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'schedule-attempt-'));t.after(()=>fs.rmSync(root,{recursive:true,force:true}));
 const db={'run-logs':[],'err-logs':[]};let id=0;
 const api={airtableList:async name=>structuredClone(db[name]),airtablePatchRecords:async(name,rows)=>{for(const r of rows)Object.assign(db[name].find(x=>x.id===r.id).fields,r.fields);return{failedRows:[]};}};
 const request=async(url,opts)=>{const name=decodeURIComponent(new URL(url).pathname.split('/').at(-1));for(const r of JSON.parse(opts.body).records)db[name].push({id:'rec'+(++id),fields:structuredClone(r.fields)});return{ok:true};};
 const logger=runLogs({api,request,historyRoot:path.join(root,'runs'),clock:()=> '2026-10-29T16:00:00.000Z'});
 const errors=errorLogs({api,request,historyRoot:path.join(root,'errors'),clock:()=> '2026-10-29T16:00:00.000Z'});
 return{root,db,api,request,logger,errors};
}
const context={runId:'00000000-0000-4000-8000-000000000001',started:'2026-10-29T15:59:00.000Z'};
test('attempt starts before startup/lock failure and writes a separate error linked by run ID',async t=>{
 const x=fixture(t);let called=0;
 await assert.rejects(runAttempt({context,logger:x.logger,errors:x.errors,run:async record=>{called++;assert.equal(x.db['run-logs'][0].fields.result,'Running');throw Error('Schedule collector already running');}}),/already running/);
 assert.equal(called,1);assert.equal(x.db['run-logs'].length,1);assert.equal(x.db['run-logs'][0].fields['run-id'],context.runId);assert.equal(x.db['run-logs'][0].fields.started,context.started);assert.equal(x.db['run-logs'][0].fields.result,'Failed');
 assert.equal(x.db['err-logs'].length,1);assert.equal(x.db['err-logs'][0].fields.stage,'lock');assert.equal(x.db['err-logs'][0].fields['run-id'],context.runId);
});
test('successful attempt uses the same run-log record, creates no error and does not repeat work',async t=>{
 const x=fixture(t);let calls=0;
 const result=await runAttempt({context,logger:x.logger,errors:x.errors,run:async record=>{calls++;await x.logger.finish(record,{ok:true,days:[]});return{ok:true};}});
 assert.equal(result.ok,true);assert.equal(calls,1);assert.equal(x.db['run-logs'].length,1);assert.equal(x.db['run-logs'][0].fields.result,'Success');assert.equal(x.db['err-logs'].length,0);
});
test('separate errors are idempotent and redact configured secrets',async t=>{
 const x=fixture(t),record=await x.logger.begin(context);
 const event={stage:'startup',message:'Failed credential SECRET-VALUE',secrets:['SECRET-VALUE']};
 await x.errors.record(record,event);await x.errors.record(record,event);
 assert.equal(x.db['err-logs'].length,1);assert(!JSON.stringify(x.db['err-logs']).includes('SECRET-VALUE'));assert.match(x.db['err-logs'][0].fields.message,/REDACTED/);
});
test('receipt replay records only new wrapper attempts, preserves completed logs and never collects',async t=>{
 const x=fixture(t),old={...context,runId:'00000000-0000-4000-8000-000000000002'},existing=await x.logger.begin(old);await x.logger.finish(existing,{ok:true,days:[]});const before=structuredClone(x.db['run-logs'][0]);
 const failed={...context,runId:'00000000-0000-4000-8000-000000000003',completed:'2026-10-29T15:59:30.000Z',exitCode:1,error:'Node could not start'};
 await recoverAttempts({receipts:[{...old,completed:'2026-10-29T15:59:30.000Z',exitCode:0},failed],logger:x.logger,errors:x.errors});
 await recoverAttempts({receipts:[failed],logger:x.logger,errors:x.errors});
 assert.deepEqual(x.db['run-logs'][0],before);assert.equal(x.db['run-logs'].length,2);assert.equal(x.db['run-logs'][1].fields.result,'Failed');assert.equal(x.db['err-logs'].length,1);
});
test('unclosed wrapper receipt remains unknown and never claims a completed collection',async t=>{
 const x=fixture(t);await recoverAttempts({receipts:[context],logger:x.logger,errors:x.errors});
 assert.equal(x.db['run-logs'][0].fields.result,'Running');assert.equal(x.db['err-logs'].length,1);assert.match(x.db['err-logs'][0].fields.message,/completion.*unknown/i);
});
test('receipt parser handles existing mixed runner encoding and ignores unrelated/historical lines',t=>{
 const x=fixture(t),event={version:1,...context,state:'started'},end={version:1,...context,state:'finished',completed:'2026-10-29T15:59:30.000Z',exitCode:1};
 const line=v=>'WEF_SCHEDULES_ATTEMPT '+Buffer.from(JSON.stringify(v)).toString('base64')+'\r\n';
 const file=path.join(x.root,'collector-2026-10-29.log');fs.writeFileSync(file,Buffer.concat([Buffer.from('old START\n'),Buffer.from(line(event),'utf16le'),Buffer.from(line(end))]));
 const r=readReceipts([file]);assert.equal(r.length,1);assert.equal(r[0].runId,context.runId);assert.equal(r[0].exitCode,1);
});
test('collection failures include dated error rows; informational empty warnings do not',async t=>{
 const x=fixture(t),record=await x.logger.begin(context);
 await x.errors.report(record,{days:[{show_id:76,date:'2026-10-29',ok:false,error:'fixture failed'},{show_id:76,date:'2026-10-30',ok:true,warnings:[{code:'empty_show_day',message:'empty'}]}]});
 assert.equal(x.db['err-logs'].length,1);assert.equal(x.db['err-logs'][0].fields.date,'2026-10-29');assert.equal(x.db['err-logs'][0].fields['show-id'],76);
});
test('failed attempt-log creation prevents source work and retains the failure',async t=>{
 const x=fixture(t);let calls=0;
 await assert.rejects(runAttempt({context,logger:{begin:async()=>{throw Error('Airtable unavailable');}},errors:x.errors,run:async()=>{calls++;}}),/Airtable unavailable/);
 assert.equal(calls,0);assert.equal(x.db['err-logs'].length,1);
});

test('seven-fraction wrapper timestamps normalize to Airtable ISO and replay identity',async t=>{
 const x=fixture(t),raw={...context,started:'2026-10-29T15:59:00.1234567Z'};
 const a=await x.logger.begin(raw),b=await x.logger.begin(raw);assert.equal(a.id,b.id);assert.equal(a.fields.started,'2026-10-29T15:59:00.123Z');
 await x.errors.record(null,{runId:context.runId,started:raw.started,stage:'startup',message:'fixture'});assert.equal(x.db['err-logs'][0].fields.started,'2026-10-29T15:59:00.123Z');
});
test('recovered failure keeps original wrapper completion rather than replay time',async t=>{
 const x=fixture(t),completed='2026-10-29T15:59:30.1234567Z';
 await recoverAttempts({receipts:[{...context,completed,exitCode:1}],logger:x.logger,errors:x.errors});assert.equal(x.db['run-logs'][0].fields.completed,'2026-10-29T15:59:30.123Z');
});
test('a terminal Failed run does not hide an error event whose earlier write failed',async t=>{
 const x=fixture(t),record=await x.logger.begin(context);await x.logger.finish(record,{ok:false,days:[]},Error('source fixture'));
 await recoverAttempts({receipts:[{...context,completed:'2026-10-29T15:59:30Z',exitCode:1,errors:[{stage:'collection',message:'source fixture'}]}],logger:x.logger,errors:x.errors});
 assert.equal(x.db['err-logs'].length,1);assert.equal(x.db['err-logs'][0].fields.message,'source fixture');
});
test('unknown acknowledgments prevent starvation, and later finish invalidates ACK',async t=>{
 const x=fixture(t),{appendReceipt,fingerprint}=require('../attempts');const files=[];
 for(let i=0;i<11;i++){const file=path.join(x.root,'receipt'+i+'.jsonl');appendReceipt(file,{version:1,state:'started',runId:'fixture-'+i,started:'2026-10-29T15:00:00Z'});files.push(file);}
 const recover=()=>recoverAttempts({receipts:readReceipts(files),logger:x.logger,errors:x.errors,acknowledge:(r,hash)=>appendReceipt(r.sourceFile,{version:1,state:'ack',runId:r.runId,started:r.started,fingerprint:hash})});
 await recover();await recover();assert.equal(x.db['run-logs'].length,11);
 appendReceipt(files[0],{version:1,state:'finished',runId:'fixture-0',started:'2026-10-29T15:00:00Z',completed:'2026-10-29T15:10:00Z',exitCode:1});await recover();assert.equal(x.db['run-logs'].find(r=>r.fields['run-id']==='fixture-0').fields.result,'Failed');
});
test('all retained receipt files remain recoverable regardless of age',async t=>{
 const x=fixture(t),{appendReceipt}=require('../attempts');const files=[];
 for(let i=20;i<=24;i++){const file=path.join(x.root,'receipt-'+i+'.jsonl');appendReceipt(file,{version:1,state:'started',runId:'aged-'+i,started:`2026-10-${i}T15:00:00Z`});appendReceipt(file,{version:1,state:'finished',runId:'aged-'+i,started:`2026-10-${i}T15:00:00Z`,completed:`2026-10-${i}T15:01:00Z`,exitCode:1});files.push(file);}
 await recoverAttempts({receipts:readReceipts(files),logger:x.logger,errors:x.errors});assert.equal(x.db['run-logs'].length,5);assert.equal(x.db['err-logs'].length,5);
});
test('partial final receipt line is ignored until completed, without losing earlier events',t=>{
 const x=fixture(t),file=path.join(x.root,'partial.jsonl'),{appendReceipt}=require('../attempts');appendReceipt(file,{version:1,state:'started',...context});fs.appendFileSync(file,'{"version":1,"state":"finished"');const receipts=readReceipts([file]);assert.equal(receipts.length,1);assert.equal(receipts[0].completed,undefined);
});
test('receipt archival verifies and logs before removing only fully acknowledged old receipts',async t=>{
 const x=fixture(t),{appendReceipt,archiveReceipts}=require('../attempts'),file=path.join(x.root,'old.jsonl');
 appendReceipt(file,{version:1,state:'started',...context});appendReceipt(file,{version:1,state:'finished',...context,completed:'2026-10-29T16:00:00Z',exitCode:1});
 await archiveReceipts({files:[file],historyRoot:path.join(x.root,'history'),now:Date.parse('2026-11-01T00:00:00Z')});assert(fs.existsSync(file));
 const r=readReceipts([file])[0];appendReceipt(file,{version:1,state:'ack',...context,fingerprint:r.fingerprint});let logged=0;
 await archiveReceipts({files:[file],historyRoot:path.join(x.root,'history'),now:Date.parse('2026-11-01T00:00:00Z'),onArchive:async e=>{logged++;assert(fs.existsSync(e.file));if(!e.cleanup)assert(fs.existsSync(file));}});assert(!fs.existsSync(file));assert.equal(logged,2);
});
test('shared logging lock serializes independent processes and supports safe reentry',async t=>{
 const x=fixture(t),{spawn}=require('node:child_process'),lock=path.join(x.root,'lock'),counter=path.join(x.root,'counter');fs.writeFileSync(counter,'0');
 const module=path.resolve(__dirname,'../logging-lock.js');
 const code=`const fs=require('fs'),s=require(${JSON.stringify(module)}).loggingLock(${JSON.stringify(lock)});s(async()=>{const n=Number(fs.readFileSync(${JSON.stringify(counter)}));await s(async()=>new Promise(r=>setTimeout(r,30)));fs.writeFileSync(${JSON.stringify(counter)},String(n+1));}).catch(e=>{console.error(e);process.exitCode=1});`;
 await Promise.all([1,2,3].map(()=>new Promise((resolve,reject)=>{const child=spawn(process.execPath,['-e',code]);let err='';child.stderr.on('data',x=>err+=x);child.on('exit',n=>n===0?resolve():reject(Error(err)));})));assert.equal(fs.readFileSync(counter,'utf8'),'3');assert(!fs.existsSync(lock));
});

test('lost error creation response resolves by key without repeating POST',async t=>{
 const x=fixture(t);let calls=0;const errors=errorLogs({api:x.api,historyRoot:path.join(x.root,'errors'),request:async(...args)=>{calls++;await x.request(...args);throw Error('fixture lost response');}}),record=await x.logger.begin(context);
 await errors.record(record,{stage:'startup',message:'fixture'});await errors.record(record,{stage:'startup',message:'fixture'});assert.equal(calls,1);assert.equal(x.db['err-logs'].length,1);
});
test('error retention archives exact record before reusing a bounded slot',async t=>{
 const x=fixture(t),events=[],errors=errorLogs({api:x.api,request:x.request,limit:1,historyRoot:path.join(x.root,'bounded-errors'),onArchive:async e=>events.push(e)}),record=await x.logger.begin(context);
 await errors.record(record,{stage:'startup',message:'old error'});const original=structuredClone(x.db['err-logs'][0]);await errors.record(record,{stage:'startup',message:'new error'});
 assert.equal(x.db['err-logs'].length,1);assert.equal(x.db['err-logs'][0].id,original.id);assert.deepEqual(JSON.parse(require('node:zlib').gunzipSync(fs.readFileSync(events[0].file))),original);
});

test('failed remote error write leaves durable intent recoverable after run finalization',async t=>{
 const x=fixture(t),{appendReceipt}=require('../attempts'),file=path.join(x.root,'outage.jsonl');appendReceipt(file,{version:1,state:'started',...context});
 const unavailable=errorLogs({api:{...x.api,airtableList:async()=>{throw Error('offline error table');}},request:x.request,onEvent:e=>appendReceipt(file,e)});
 await assert.rejects(runAttempt({context,logger:x.logger,errors:unavailable,run:async()=>{throw Error('fixture source failure');}}),/source failure/);
 appendReceipt(file,{version:1,state:'finished',...context,completed:'2026-10-29T16:00:00Z',exitCode:1});assert.equal(x.db['run-logs'][0].fields.result,'Failed');assert.equal(x.db['err-logs'].length,0);
 await recoverAttempts({receipts:readReceipts([file]),logger:x.logger,errors:x.errors});assert.equal(x.db['err-logs'].length,1);assert.match(x.db['err-logs'][0].fields.message,/source failure/);
});

test('explicit null show identity survives intent replay after final run gains a show',async t=>{
 const x=fixture(t),record=await x.logger.begin(context),event={stage:'collection',showId:null,message:'same event'};await x.errors.record(record,event);
 record.fields['show-id']=76;await x.errors.record(record,{...event,recovered:true});assert.equal(x.db['err-logs'].length,1);assert.equal(x.db['err-logs'][0].fields['show-id'],null);
});
test('dead logging lock fails visibly without deleting or reclaiming owner state',async t=>{
 const x=fixture(t),file=path.join(x.root,'dead-lock'),body=JSON.stringify({pid:2147483647,token:'retained-owner'});fs.writeFileSync(file,body);
 await assert.rejects(require('../logging-lock').loggingLock(file) (async()=>{}),/Dead logging lock/);assert.equal(fs.readFileSync(file,'utf8'),body);
});
