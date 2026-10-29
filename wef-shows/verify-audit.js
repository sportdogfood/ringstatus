"use strict";
// Read-only normal-run acceptance. Never starts collection or creates error fixtures.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const settings=require('./settings'),{definitions}=require('./audit');
async function verifyAudit(){
 const receiptFile=path.join(settings.RUNTIME,'last-run.json'),receipt=JSON.parse(fs.readFileSync(receiptFile));
 assert.equal(receipt.ok,true,JSON.stringify(receipt.errors));assert.equal(receipt.discovery_ok,true);assert.equal(receipt.show_days?.ok,true);assert.equal(receipt.observability?.ok,true,JSON.stringify(receipt.telemetry_errors));
 const api=require('./airtable').airtable(),filter=`{run-id}=${JSON.stringify(receipt.run_id)}`;
 const runs=await api.list('run-logs',settings.BASE_ID,filter);assert.equal(runs.length,1);const run=runs[0];assert.equal(run.fields.result,'Success');assert.equal(Date.parse(run.fields.completed),Date.parse(receipt.completed));
 const sources=await api.list('sources'),errors=await api.list('err-logs',settings.BASE_ID,filter),heartbeat=await api.list('heartbeat');
 assert.equal(sources.length,Object.keys(definitions).length);assert.equal(new Set(sources.map(r=>r.fields['source-key'])).size,sources.length);
 assert.deepEqual([...run.fields.sources].sort(),sources.map(r=>r.id).sort());
 for(const source of sources){const key=source.fields['source-key'],attempt=receipt.observability.sources.find(s=>s.key===key);assert.ok(attempt);assert.equal(source.fields['last-result'],'Success');assert.equal(Date.parse(source.fields['last-attempt-at']),Date.parse(attempt.at));assert.equal(Date.parse(source.fields['last-success-at']),Date.parse(attempt.successAt));assert.ok(source.fields['run-logs'].includes(run.id));assert.equal(source.fields.location,definitions[key].location);}
 assert.equal(errors.length,0,'Successful run must not fabricate error records');assert.equal(receipt.error_records.length,0);assert.equal(heartbeat.length,1);assert.equal(heartbeat[0].fields.result,'Success');assert.equal(Date.parse(heartbeat[0].fields['checked-at']),Date.parse(receipt.completed));
 const after=JSON.parse(fs.readFileSync(receiptFile));assert.equal(after.run_id,receipt.run_id,'Normal run changed during verification');
 const result={at:new Date().toISOString(),result:'PASS',run:receipt.run_id,started:receipt.started,completed:receipt.completed,runRecord:run.id,sourceRecords:sources.map(r=>({id:r.id,key:r.fields['source-key'],lastAttempt:r.fields['last-attempt-at'],lastSuccess:r.fields['last-success-at']})),errors:errors.length,heartbeat:heartbeat[0].id,showDays:receipt.show_days.days};
 const dir=path.join(__dirname,'evidence/sources-errors-build-20261029');fs.mkdirSync(dir,{recursive:true});fs.writeFileSync(path.join(dir,'normal-'+receipt.run_id+'.json'),JSON.stringify(result,null,2));return result;
}
if(require.main===module)verifyAudit().then(r=>console.log(JSON.stringify(r))).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={verifyAudit};
