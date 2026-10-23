"use strict";
// Read-only acceptance: Airtable GETs and existing scheduled-run receipts/archives.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),zlib=require('node:zlib'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../../..'),{airtable}=require(path.join(root,'wef-tenants/airtable'));
const read=p=>JSON.parse(fs.readFileSync(path.join(__dirname,p)));
async function verify(){
 const deployed=read('deployment.json'),before=read('before.json'),report=JSON.parse(fs.readFileSync(path.join(root,'wef-tenants/runtime/last-run.json')));
 if(report.started<=deployed.at){console.log(JSON.stringify({pending:true,lastCompleted:report.completed}));return;}
 assert.equal(report.ok,true,'Normal scheduled run failed; stop verification path');assert.deepEqual(report.errors,[]);
 const api=airtable();const [trainers,entries,heartbeat,logs]=await Promise.all([api.list('trainers'),api.list('entries'),api.list('heartbeat'),api.list('run-logs',undefined,{filterByFormula:`{run-id}='${report.run_id}'`})]);
 assert.equal(logs.length,1);assert.equal(logs[0].fields.result,'Success');assert.equal(logs[0].fields.rows,6);assert.equal(heartbeat[0].fields.result,'Success');assert.ok(heartbeat[0].fields['checked-at']>=report.started);
 for(const event of report.archives){const body=zlib.gunzipSync(fs.readFileSync(event.file));assert.equal(crypto.createHash('sha256').update(body).digest('hex'),event.sha256);}
 const trainerSummary=[];
 for(const old of before.trainers){const current=trainers.find(t=>t.id===old.id);assert.ok(current);for(const k of ['trainer-id','trainer-key','role','slot','active'])assert.deepEqual(current.fields[k],old.fields[k]);
  const archive=report.archives.find(a=>a.kind==='people-'+old.fields['trainer-id']);assert.ok(archive);const source=JSON.parse(zlib.gunzipSync(fs.readFileSync(archive.file)));assert.equal(Number(source.people.people_id),old.fields['trainer-id']);assert.equal(current.fields['trainer-name'],source.people.name.trim());
  trainerSummary.push({record:current.id,id:current.fields['trainer-id'],name:current.fields['trainer-name'],slot:current.fields.slot,result:current.fields['last-result']});
 }
 assert.equal(trainers.length,before.trainers.length);assert.equal(trainerSummary.find(t=>t.id===80970).name,'TAYLOR COWLES');
 assert.equal(entries.length,6);assert.equal(new Set(entries.map(e=>e.fields['entry-key'])).size,6);
 for(const old of before.entries){const current=entries.find(e=>e.id===old.id);assert.ok(current);for(const key of ['entry-key','status','schedule-key','schedule-link','horse-id','rider-id','class-id','date','ring-number','trainer-ids','trainers','horses','riders','rings'])assert.deepEqual(current.fields[key],old.fields[key],key);}
 const hashes=read('protected-hashes.json');for(const [p,hash]of Object.entries(hashes))assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root,p))).digest('hex'),hash,p);
 const result={verifiedAt:new Date().toISOString(),runId:report.run_id,started:report.started,completed:report.completed,trainers:trainerSummary,entries:entries.length,entryIdsAndLinksPreserved:true,archivesVerified:report.archives.length,heartbeat:heartbeat[0].fields,log:logs[0],protectedFilesUnchanged:true};
 fs.writeFileSync(path.join(__dirname,'verified-'+report.run_id+'.json'),JSON.stringify(result,null,2));console.log(JSON.stringify(result));
}
verify().catch(e=>{console.error(e.message);process.exitCode=1;});
