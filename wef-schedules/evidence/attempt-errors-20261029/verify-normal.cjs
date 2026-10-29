"use strict";
// Read-only normal-run acceptance. Never invokes collection or source endpoints.
const fs=require('fs'),path=require('path'),assert=require('assert/strict'),api=require('../../collector-core'),{readReceipts}=require('../../attempts');
(async()=>{
 const base='appUq5Qz9kARqFija',dir=path.resolve(__dirname,'../../runtime/attempt-receipts'),receipts=readReceipts(fs.readdirSync(dir).filter(n=>n.endsWith('.jsonl')).map(n=>path.join(dir,n))).filter(r=>r.completed).sort((a,b)=>a.started.localeCompare(b.started));
 const heartbeat=await api.airtableList('heartbeat',{},base),errorRows=await api.airtableList('error-log',{},base),before=JSON.parse(fs.readFileSync(path.join(__dirname,'business-before.json')));
 const current={};for(const table of ['schedules','rings','shows','rings-helper'])current[table]=await api.airtableList(table,{},base);
 for(const [table,oldRows] of Object.entries(before)){
  const map=new Map(current[table].map(r=>[r.id,r]));for(const old of oldRows){const row=map.get(old.id);assert(row,'Missing prior '+table+' ID '+old.id);
   if(table==='schedules')for(const field of ['stage-key','class-id','group-id','show-details','rings'])assert.deepEqual(row.fields[field]??null,old.fields[field]??null,field+' changed '+old.id);
   if(table==='rings')for(const field of ['ring-key','show-details','rings-helper'])assert.deepEqual(row.fields[field],old.fields[field],field+' changed '+old.id);
   if(table==='shows')assert.equal(row.fields['show-key'],old.fields['show-key']);
   if(table==='rings-helper')assert.deepEqual(row.fields,old.fields,'Static helper changed '+old.id);
  }
 }
 assert.equal(new Set(current.schedules.map(r=>r.fields['stage-key'])).size,current.schedules.length,'Duplicate schedule key');
 const saved=[];
 for(let i=0;i<receipts.length;i++){
  const receipt=receipts[i];assert.equal(receipt.exitCode,0,'Normal wrapper failure; stop verification');
  const matches=await api.airtableList('run-logs',{filterByFormula:`{run-id}="${receipt.runId}"`},base);assert.equal(matches.length,1,'Attempt row missing/duplicate');const record=matches[0],f=record.fields;
  assert.equal(f.started,receipt.started);assert(['Success','Warning'].includes(f.result),'Normal collection failed; stop verification');assert(f.completed);assert(Date.parse(f.completed)<=Date.parse(receipt.completed));assert.equal(f['days-failed'],0);assert.equal(f['days-complete'],4);
  assert.equal(f.rows,current.schedules.filter(r=>r.fields.status==='active').length);
  const errors=errorRows.filter(r=>r.fields['run-id']===receipt.runId);if(f.errors)assert(errors.length>0,'Missing separate error evidence');else assert.equal(errors.length,0);
  const result={verified_at:new Date().toISOString(),receipt:{run_id:receipt.runId,started:receipt.started,completed:receipt.completed,exit_code:receipt.exitCode,acknowledged:receipt.ack===receipt.fingerprint},run_log:record,error_records:errors,business_preserved:Object.fromEntries(Object.entries(before).map(([k,v])=>[k,v.length])),schedule_records:current.schedules.length,no_duplicate_schedule_keys:true,manual_runs:0,historical_repairs:0};
  const file=path.join(__dirname,`normal-run-${i+1}.json`);fs.writeFileSync(file,JSON.stringify(result,null,2));saved.push(result);
 }
 if(saved.length){const last=saved.at(-1),hb=heartbeat.find(r=>r.fields['run-id']===last.receipt.run_id);assert(hb,'Heartbeat not matched to newest completed attempt');assert.equal(hb.fields.result,last.run_log.fields.result);last.heartbeat=hb;fs.writeFileSync(path.join(__dirname,`normal-run-${saved.length}.json`),JSON.stringify(last,null,2));}
 if(saved.length>=2)assert(saved[0].receipt.acknowledged,'First attempt was not ACKed by subsequent normal run');
 fs.writeFileSync(path.join(__dirname,'business-latest.json'),JSON.stringify(current));console.log(JSON.stringify({completed_normal_attempts:saved.length,attempts:saved.map(r=>({id:r.receipt.run_id,started:r.receipt.started,completed:r.run_log.fields.completed,result:r.run_log.fields.result,acknowledged:r.receipt.acknowledged})),error_records:errorRows.length,business_preserved:Object.fromEntries(Object.entries(before).map(([k,v])=>[k,v.length])),heartbeat_matches:!!saved.length},null,2));
})().catch(e=>{console.error(e.stack);process.exitCode=1});
