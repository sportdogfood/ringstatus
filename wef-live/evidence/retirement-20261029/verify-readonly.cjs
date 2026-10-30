const fs=require('node:fs'),z=require('node:zlib'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const api=require('../../airtable').airtable();
(async()=>{
 const report=JSON.parse(fs.readFileSync('wef-live/runtime/last-run.json'));
 const deployment=JSON.parse(fs.readFileSync(__dirname+'/deployed.json'));
 assert(Date.parse(report.started)>Date.parse(deployment.at),'No post-deployment completed run yet');
 assert(report.ok,JSON.stringify(report.errors));assert.equal(report.logging_errors.length,0);
 const before=JSON.parse(fs.readFileSync(__dirname+'/before-records.json'));
 const current={};for(const t of Object.keys(before))current[t]=await api.list(t);
 const cats=current.collections.filter(r=>r.fields['source-kind']==='archive-catalog');
 const archived={};for(const cat of cats){const f=cat.fields;assert.equal(f.status,'Retired');const bytes=z.gunzipSync(fs.readFileSync(f['archive-file']));assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'),f.fingerprint);const rows=JSON.parse(bytes);assert.equal(rows.length,f['row-count']);archived[f['source-id']]||=[];archived[f['source-id']].push(...rows);}
 for(const t of ['live-rings','live-groups','live-classes','live-entries','collections']){
   const old=before[t].filter(r=>Number(r.fields['show-id'])===200000075&&['class','groups','rings'].includes(r.fields['source-kind']||'class'));
   for(const row of old){const saved=archived[t]?.find(r=>r.id===row.id);assert(saved,'Missing archived '+t+' '+row.id);assert.deepEqual(saved,row,'Archive must preserve exact old fields and links');}
   assert.equal(current[t].filter(r=>Number(r.fields['show-id'])===200000075&&r.fields['source-kind']!=='archive-catalog').length,0,'Old active rows remain '+t);
 }
 for(const t of ['wef-schedules.schedules','wef-entries.entries']){
   const nowIds=new Set(current[t].map(r=>r.id));for(const r of before[t])assert(nowIds.has(r.id),'Reference record removed '+r.id);
 }
 const filter='?'+new URLSearchParams({filterByFormula:`{run-id}="${report.run_id}"`});
 const logs={};for(const t of ['run-logs','heartbeat','errs-log'])logs[t]=(await api.call('appgzosUcVqA4vZdf',t,'GET',undefined,filter)).records;
 assert.equal(logs['run-logs'].length,1);assert.equal(logs.heartbeat.length,1);assert.equal(logs['errs-log'].length,0);
 for(const t of ['run-logs','heartbeat'])assert(['Success','Skipped'].includes(logs[t][0].fields.result));
 const result={run:report.run_id,started:report.started,completed:report.completed,counts:report.counts,retired:report.retired,catalogs:cats.length,archived:Object.fromEntries(Object.entries(archived).map(([k,v])=>[k,v.length])),activeCounts:Object.fromEntries(Object.entries(current).map(([k,v])=>[k,v.length])),exactArchivedFieldsAndLinks:true,referenceIdsPreserved:true,logs};
 fs.writeFileSync(__dirname+'/verified-'+report.run_id+'.json',JSON.stringify(result,null,2));console.log(JSON.stringify(result));
})().catch(e=>{console.error(e.stack);process.exitCode=1});
