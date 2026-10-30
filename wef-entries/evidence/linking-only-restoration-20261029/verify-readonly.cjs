"use strict";
// Airtable GET-only snapshots. Never imports or invokes a collector.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const root=path.resolve(__dirname,'../..');
for(const line of fs.readFileSync(path.join(root,'runtime/collector.env'),'utf8').split(/\r?\n/)){
 const m=line.match(/^([A-Z_][A-Z_0-9]*)=(.*)$/);if(m)process.env[m[1]]=m[2].replace(/^['"]|['"]$/g,'');
}
const settings=require(path.join(root,'settings')),api=require(path.join(root,'airtable')).airtable();
const scope=f=>[Number(f['show-id']),String(f.date).slice(0,10),Number(f['class-id']),Number(f['ring-number']??f.ring)].join('|');
async function inspect(){
 const label=process.argv[2];assert(/^[a-z0-9-]+$/.test(label));
 const report=JSON.parse(fs.readFileSync(path.join(root,'runtime/last-run.json'),'utf8'));
 const entries=await api.list('entries'),collections=await api.list('collections'),heartbeat=await api.list('heartbeat'),shared=await api.list(settings.SYNC_TABLE);
 const logs=await api.list('run-logs',settings.BASE_ID,`{run-id}=${JSON.stringify(report.run_id)}`);
 const data={at:new Date().toISOString(),report,entries,collections,heartbeat,shared,logs};
 assert.equal(new Set(entries.map(r=>r.fields['entry-key'])).size,entries.length,'Duplicate keys');
 fs.writeFileSync(path.join(__dirname,label+'.json'),JSON.stringify(data));
 const summary={label,run:report.run_id,started:report.started,completed:report.completed,ok:report.ok,result:report.result,rows:report.rows,accepted:report.accepted_rows,outside:report.out_of_scope_classes,entries:entries.length,errors:report.errors,checks:{uniqueKeys:true}};
 if(label!=='before'){
  const deployed=JSON.parse(fs.readFileSync(path.join(__dirname,'deployment.json'),'utf8'));
  assert(Date.parse(report.started)>Date.parse(deployed.at),'No new deployed scheduled receipt');
  assert.equal(report.ok,true,JSON.stringify(report.errors));assert.equal(report.errors.length,0);assert.equal(report.live,undefined,'Live execution evidence must be absent');
  const before=JSON.parse(fs.readFileSync(path.join(__dirname,'before.json'),'utf8')),byId=new Map(entries.map(r=>[r.id,r]));
  const keys=new Set(shared.filter(r=>r.fields.status==='active').map(r=>scope(r.fields)));
  let outside=0,native=0,historical=0;
  for(const old of before.entries){
   const now=byId.get(old.id);assert(now,'Missing original record '+old.id);assert.equal(now.fields['entry-key'],old.fields['entry-key']);
   for(const [name,value]of Object.entries(old.fields))if(Array.isArray(value)&&value.every(x=>typeof x==='string'&&/^rec/.test(x))){assert.deepEqual(now.fields[name],value,'Native/helper links changed '+old.id+'.'+name);native++;}
   const oldTime=Date.parse(old.fields['last-seen']),newTime=Date.parse(now.fields['last-seen']);if(Number.isFinite(oldTime))assert(Number.isFinite(newTime)&&newTime>=oldTime,'Source time missing/invalid/regressed');
   if(!keys.has(scope(old.fields))){assert.deepEqual(now.fields,old.fields,'Outside shared scope changed '+old.id);outside++;}
   let a,b;try{a=JSON.parse(old.fields['source-json']);}catch{continue;}
   if(!a||typeof a!=='object')continue;
   if(['laneAssociations','liveEvidence'].some(k=>Object.hasOwn(a,k))){b=JSON.parse(now.fields['source-json']);assert(b&&typeof b==='object','Historical source evidence missing');}else continue;
   for(const field of ['laneAssociations','liveEvidence'])if(Object.hasOwn(a,field)){assert.deepEqual(b[field],a[field],'Historical evidence changed '+field);historical++;}
  }
  for(const row of entries)if(!before.entries.some(r=>r.id===row.id))assert(keys.has(scope(row.fields)),'New row outside shared scope');
  let collectionLinks=0;const collectionIds=new Map(collections.map(r=>[r.id,r]));
  for(const old of before.collections){const now=collectionIds.get(old.id);assert(now,'Missing collection '+old.id);assert.equal(now.fields['collection-id'],old.fields['collection-id']);
   for(const [name,value]of Object.entries(old.fields))if(Array.isArray(value)&&value.every(x=>typeof x==='string'&&/^rec/.test(x))){assert.deepEqual(now.fields[name],value,'Collection links changed '+old.id+'.'+name);collectionLinks++;}
  }
  if(label==='run-2'){const first=JSON.parse(fs.readFileSync(path.join(__dirname,'run-1-summary.json')));assert.notEqual(report.run_id,first.run,'Same receipt cannot prove two runs');assert(Date.parse(report.started)>Date.parse(first.completed),'Scheduled runs not ordered');}
  const task=require('node:child_process').spawnSync('powershell',['-NoProfile','-Command','Export-ScheduledTask -TaskName wef-entries-cwf-collector'],{encoding:'utf8',windowsHide:true});assert.equal(task.status,0);assert.equal(crypto.createHash('sha256').update(task.stdout).digest('hex'),deployed.taskXmlSha256,'Task XML changed');
  const beat=heartbeat.find(r=>r.fields['heartbeat-key']==='wef-entries');assert(beat);assert.equal(Date.parse(beat.fields['checked-at']),Date.parse(report.completed));assert.equal(beat.fields.result,report.result);
  assert.equal(logs.length,1);assert.equal(logs[0].fields.result,report.result);
  const depHashes=deployed.files;for(const [name,hash]of Object.entries(depHashes))assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root,name))).digest('hex'),hash,'Deployed file changed');
  summary.checks={uniqueKeys:true,allOriginalRecords:true,nativeFields:native,outsideScopeUnchanged:outside,historicalEvidenceFields:historical,collectionIdsPreserved:before.collections.length,collectionNativeFields:collectionLinks,taskXmlUnchanged:true,sourceTimesNotRegressed:true,noNewOutsideScope:true,logsHeartbeat:true,deployedHashes:true,noLiveExecution:true};
 }
 fs.writeFileSync(path.join(__dirname,label+'-summary.json'),JSON.stringify(summary,null,2));console.log(JSON.stringify(summary));
}
inspect().catch(e=>{console.error(e.stack);process.exitCode=1;});
