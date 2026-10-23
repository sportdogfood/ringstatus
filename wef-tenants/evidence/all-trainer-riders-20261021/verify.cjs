"use strict";
// Read-only acceptance: existing scheduled receipts/archives and Airtable GETs.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),zlib=require('node:zlib'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../../..'),{airtable}=require(path.join(root,'wef-tenants/airtable')),{assertUniqueRoster}=require(path.join(root,'wef-tenants/identity-policy'));
const read=name=>JSON.parse(fs.readFileSync(path.join(__dirname,name)));
async function verify(){
 const deployment=read('deployment.json'),before=read('before.json'),report=JSON.parse(fs.readFileSync(path.join(root,'wef-tenants/runtime/last-run.json')));
 if(report.started<=deployment.at){console.log(JSON.stringify({pending:true,lastCompleted:report.completed}));return;}
 assert.equal(report.ok,true,'Normal scheduled run failed; stop this verification path');assert.deepEqual(report.errors,[]);
 const api=airtable(),state={};for(const table of ['trainers','horses','riders','entries','heartbeat'])state[table]=await api.list(table);
 const logs=await api.list('run-logs',undefined,{filterByFormula:`{run-id}='${report.run_id}'`});assert.equal(logs.length,1);assert.equal(logs[0].fields.result,'Success');assert.equal(logs[0].fields.rows,6);assert.equal(logs[0].fields['days-failed'],0);
 assert.equal(state.heartbeat[0].fields.result,'Success');assert.ok(state.heartbeat[0].fields['checked-at']>=report.started);
 const helperFields=['name','nick-name','aliases','notes','disabled','first-name','last-name','show-name','legacy-display-name','source-base','source-record'];
 for(const [table,prefix]of [['horses','horse'],['riders','rider']]){
  assertUniqueRoster(table,state[table].map(r=>r.fields));
  for(const old of before[table]){const current=state[table].find(r=>r.id===old.id);assert.ok(current,'Missing retained '+table+' '+old.id);for(const key of [prefix+'-key',prefix+'-id',prefix+'-name',...helperFields])assert.deepEqual(current.fields[key],old.fields[key],table+'.'+key+' '+old.id);}
 }
 assert.equal(state.horses.length,before.horses.length);assert.equal(state.riders.length,36);
 const expected=new Map([[50121,'ISABELA DE SOUSA'],[81017,'JACQUELINE ROSENTHAL'],[80970,'TAYLOR COWLES']]);
 for(const [id,name]of expected){const rider=state.riders.find(r=>r.fields['rider-id']===id);assert.ok(rider);assert.equal(rider.fields['rider-name'],name);assert.equal(rider.fields['tenant-id'],8778);assert.equal(rider.fields['trainer-ids'],String(id===81017?50121:id));}
 assert.equal(state.trainers.length,before.trainers.length);for(const old of before.trainers){const current=state.trainers.find(r=>r.id===old.id);assert.ok(current);for(const key of ['trainer-key','trainer-id','trainer-name','slot','role','active'])assert.deepEqual(current.fields[key],old.fields[key]);}
 assert.equal(state.entries.length,6);assert.equal(new Set(state.entries.map(r=>r.fields['entry-key'])).size,6);
 for(const old of before.entries){const current=state.entries.find(r=>r.id===old.id);assert.ok(current);for(const key of ['entry-key','entry-id','class-id','horse-id','rider-id','horse-name','rider-name','date','ring-number','status','trainer-ids','schedule-key','schedule-link','horses','rings','trainers'])assert.deepEqual(current.fields[key],old.fields[key],key);const rider=state.riders.find(r=>r.fields['rider-id']===current.fields['rider-id']);assert.ok(rider);assert.deepEqual(current.fields.riders,[rider.id]);for(const trainer of current.fields.trainers)assert.ok(rider.fields.trainers.includes(trainer));}
 for(const event of report.archives){const bytes=zlib.gunzipSync(fs.readFileSync(event.file));assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'),event.sha256);}
 for(const [file,hash]of Object.entries(read('protected-hashes.json')))assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root,file))).digest('hex'),hash,file);
 const prior=fs.readdirSync(__dirname).filter(n=>/^verified-.*\.json$/.test(n)).map(read).filter(r=>r.runId!==report.run_id).sort((a,b)=>b.started.localeCompare(a.started))[0];
 const ids=state.riders.map(r=>r.id).sort();if(prior)assert.deepEqual(ids,prior.riderRecordIds,'Repeated run changed rider helper identities');
 const result={verifiedAt:new Date().toISOString(),runId:report.run_id,started:report.started,completed:report.completed,result:logs[0].fields.result,entries:state.entries.length,riders:state.riders.length,newRiders:state.riders.filter(r=>expected.has(r.fields['rider-id'])).map(r=>({record:r.id,id:r.fields['rider-id'],name:r.fields['rider-name'],trainers:r.fields['trainer-ids']})),allEntryRiderLinksCorrect:true,entryIdsAndOtherLinksPreserved:true,existingHelpersPreserved:true,unique:true,archivesVerified:report.archives.length,riderRecordIds:ids,repeatStable:!!prior,protectedFilesUnchanged:true,heartbeat:state.heartbeat[0].fields,log:{id:logs[0].id,...logs[0].fields}};
 fs.writeFileSync(path.join(__dirname,'verified-'+report.run_id+'.json'),JSON.stringify(result,null,2));console.log(JSON.stringify({...result,riderRecordIds:undefined,heartbeat:undefined,log:undefined}));
}
verify().catch(e=>{console.error(e.message);process.exitCode=1;});
