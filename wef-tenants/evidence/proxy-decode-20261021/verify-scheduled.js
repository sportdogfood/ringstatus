// Read-only acceptance inspector. Does not import collector entrypoints or fetch SGL.
const fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib'),crypto=require('node:crypto');
const {airtable}=require('../../airtable');
(async()=>{
 const report=JSON.parse(fs.readFileSync(path.join(__dirname,'../../runtime/last-run.json'),'utf8'));
 if(report.started<'2026-10-21T23:42:00Z')throw Error('Await first post-switch normal run');
 const api=airtable();
 const logs=await api.list('run-logs',undefined,{filterByFormula:`{run-id}='${report.run_id}'`,fields:['run-id','started','completed','result','rows','days-complete','days-failed','summary','errors','archive-count']});
 const heartbeat=await api.list('heartbeat');
 const trainers=await api.list('trainers',undefined,{fields:['trainer-key','trainer-id','role','slot','active','last-checked','last-result','last-error']});
 const entries=await api.list('entries',undefined,{fields:['entry-key','entry-id','class-id','horse-id','rider-id','date','status','trainer-ids']});
 const archives=(report.archives||[]).map(a=>{const body=zlib.gunzipSync(fs.readFileSync(a.file)),sha=crypto.createHash('sha256').update(body).digest('hex');const p=JSON.parse(body);return {...a,hashMatches:sha===a.sha256,recordCountMatches:(Array.isArray(p)?p.length:1)===a.records,...(a.kind==='people-50121'?{decodedRows:p.trips.map(r=>({entry_id:r.entry_id,class_id:r.class_id,horse_id:r.horse_id,rider_id:r.rider_id,protectedBlobRemaining:Object.hasOwn(r,'_')}))}:{})};});
 const evidence={checkedAt:new Date().toISOString(),report:{ok:report.ok,run_id:report.run_id,started:report.started,completed:report.completed,rows:report.rows,trainers:report.trainers,errors:report.errors,days:report.days},logs,heartbeat,trainers,entries,archives};
 fs.writeFileSync(path.join(__dirname,'scheduled-'+report.run_id+'.json'),JSON.stringify(evidence,null,2));
 console.log(JSON.stringify({run:evidence.report,logs:logs.map(r=>r.fields),heartbeat:heartbeat.map(r=>r.fields),entries:entries.length,archiveCount:archives.length,archivesVerified:archives.every(a=>a.hashMatches&&a.recordCountMatches),decodedRows:archives.find(a=>a.kind==='people-50121')?.decodedRows}));
})().catch(e=>{console.error(e.message);process.exitCode=1});
