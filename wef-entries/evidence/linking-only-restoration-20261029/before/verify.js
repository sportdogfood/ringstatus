"use strict";
// Read-only verification of a task-produced receipt, never a collector invocation.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto'),zlib=require('node:zlib');
const settings=require('./settings'),{unique}=require('../wef-tenants/airtable');
async function verify(){
 const receipt=JSON.parse(fs.readFileSync(path.join(settings.RUNTIME,'last-run.json'),'utf8'));
 const api=require('./airtable').airtable();
 const [entries,collections,logs,heartbeats,schedules]=await Promise.all(['entries','collections','run-logs','heartbeat',settings.SYNC_TABLE].map(t=>api.list(t)));
 const log=logs.find(r=>r.fields['run-id']===receipt.run_id);
 assert.equal(receipt.ok,true,receipt.errors.join('; '));assert.equal(log?.fields.result,'Success');assert.equal(heartbeats.length,1);assert.equal(heartbeats[0].fields.result,'Success');assert.equal(heartbeats[0].fields['checked-at'],receipt.completed);
 unique(entries,'entry-key');unique(collections,'collection-id');unique(logs,'run-id');
 const active=entries.filter(r=>r.fields.status==='active');assert.equal(active.length,receipt.rows);
 for(const entry of active){assert.equal(entry.fields.schedules.length,1);const schedule=schedules.find(s=>s.id===entry.fields.schedules[0]);assert.equal(schedule?.fields['stage-key'],entry.fields['schedule-key']);assert.equal(Number(schedule.fields['class-id']),entry.fields['class-id']);assert.equal(schedule.fields.date,entry.fields.date);}
 const current=collections.filter(r=>Number(r.fields['show-id'])===receipt.show_id);assert.equal(current.length,receipt.dates.length*5);
 for(const c of current){assert.equal(c.fields.status,'complete');assert.equal(c.fields['row-count']??0,(c.fields.entries||[]).length);for(const id of c.fields.entries||[])assert.ok(active.some(r=>r.id===id));assert.ok(fs.existsSync(c.fields['archive-file']));}
 for(const event of receipt.archives){assert.ok(event.file);const content=zlib.gunzipSync(fs.readFileSync(event.file));assert.equal(crypto.createHash('sha256').update(content).digest('hex'),event.sha256);}
 const protectedHashes=JSON.parse(fs.readFileSync(path.join(__dirname,'evidence/protected-hashes.json')));
 for(const [file,hash]of Object.entries(protectedHashes))assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(__dirname,'..',file))).digest('hex'),hash,'Protected code changed: '+file);
 const proof={verifiedAt:new Date().toISOString(),run_id:receipt.run_id,started:receipt.started,completed:receipt.completed,result:'PASS',rows:active.length,collectionCount:current.length,archives:receipt.archives.length,entryIds:entries.map(r=>({key:r.fields['entry-key'],id:r.id})),collectionIds:current.map(r=>({key:r.fields['collection-id'],id:r.id})),sourceResults:receipt.collections,protectedFilesUnchanged:Object.keys(protectedHashes).length,receipt,entries,collections,log,heartbeat:heartbeats[0]};
 fs.writeFileSync(path.join(__dirname,'evidence','normal-run-'+receipt.run_id+'.json'),JSON.stringify(proof,null,2));
 console.log(JSON.stringify({run:proof.run_id,result:proof.result,started:proof.started,completed:proof.completed,rows:proof.rows,collections:proof.collectionCount,archives:proof.archives,sourceResults:proof.sourceResults,protectedFilesUnchanged:proof.protectedFilesUnchanged},null,2));return proof;
}
if(require.main===module)verify().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={verify};
