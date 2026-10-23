// Read-only acceptance: Airtable GETs and existing local archive reads only.
const fs=require('node:fs'),path=require('node:path'),z=require('node:zlib'),crypto=require('node:crypto');
const {airtable}=require('../../../wef-tenants/airtable'),settings=require('../../../wef-tenants/settings');
const out=__dirname,fields=['run-id','started','completed','result','rows','days-complete','days-failed','errors','summary','archives','archive-count'];
const digest=b=>crypto.createHash('sha256').update(b).digest('hex');
(async()=>{
 const api=airtable(),base=settings.SHARED_BASE_ID,before=JSON.parse(fs.readFileSync(path.join(out,'pre-switch-airtable.json'),'utf8'));
 const schedules=await api.list('schedules',base,{fields:['stage-key','class-id','class-number','date','show-id','status']});
 const logs={schedules:await api.list('run-logs',base,{fields,filterByFormula:"IS_AFTER({started}, '2026-10-21T23:59:30Z')"}),tenant:await api.list('run-logs',undefined,{fields,filterByFormula:"IS_AFTER({started}, '2026-10-21T23:59:30Z')"})};
 const heartbeat={schedules:await api.list('heartbeat',base),tenant:await api.list('heartbeat')};
 const entries=await api.list('entries',undefined,{fields:['entry-key','entry-id','class-id','horse-id','rider-id','date','status','schedule-key','schedule-link','trainer-ids','rings','horses','riders']});
 const active=entries.filter(r=>r.fields.status==='active');
 const linkChecks=active.map(r=>{const f=r.fields,s=schedules.find(s=>s.fields['stage-key']===f['schedule-key']);return {id:r.id,key:f['entry-key'],entryId:f['entry-id'],classId:f['class-id'],date:f.date,scheduleId:s?.id,matches:!!s&&s.fields['class-id']===f['class-id']&&s.fields.date===f.date&&f['schedule-link'].endsWith('/'+s.id)};});
 const preservedIds=before.tables.schedules.every(old=>schedules.some(r=>r.id===old.id&&r.fields['stage-key']===old.fields['stage-key']));
 const preservation={};
 for(const table of ['horses','riders','rings-helper','ring-walks']){
  const rows=await api.list(table),changes=[];
  const staticFields=table==='horses'?['horse-key','horse-id','horse-name','name','nick-name','show-name']:table==='riders'?['rider-key','rider-id','rider-name','name','nick-name','first-name','last-name']:null;
  for(const old of before.tables[table]){const current=rows.find(r=>r.id===old.id);if(!current){changes.push({id:old.id,missing:true});continue;}for(const[k,v]of Object.entries(old.fields)){if(staticFields&&!staticFields.includes(k))continue;if(JSON.stringify(current.fields[k])!==JSON.stringify(v))changes.push({id:old.id,field:k});}}
  preservation[table]={before:before.tables[table].length,after:rows.length,changes};
 }
 const archiveChecks=[];
 for(const [lane,records]of Object.entries(logs))for(const log of records)for(const m of (log.fields.archives||'').matchAll(/;\s+([^;\n]+\.json\.gz); SHA256 ([0-9a-f]{64})/g)){
  const bytes=z.gunzipSync(fs.readFileSync(m[1]));archiveChecks.push({lane,runId:log.fields['run-id'],file:m[1],matches:digest(bytes)===m[2]});
 }
 const result={checkedAt:new Date().toISOString(),logs,heartbeat,schedules:{count:schedules.length,classIds:schedules.filter(r=>r.fields['class-id']>0).length,preservedIds,uniqueKeys:new Set(schedules.map(r=>r.fields['stage-key'])).size},entries:linkChecks,entryCount:active.length,uniqueEntryKeys:new Set(active.map(r=>r.fields['entry-key'])).size,preservation,archiveChecks};
 const file='normal-'+result.checkedAt.replace(/[:.]/g,'-')+'.json';fs.writeFileSync(path.join(out,file),JSON.stringify(result,null,2));
 console.log(JSON.stringify({file,schedules:result.schedules,entryCount:active.length,entries:linkChecks,preservation,logs:Object.fromEntries(Object.entries(logs).map(([k,records])=>[k,records.map(r=>({id:r.fields['run-id'],started:r.fields.started,completed:r.fields.completed,result:r.fields.result,rows:r.fields.rows,errors:r.fields.errors}))])),archiveChecks:archiveChecks.length,archivesVerified:archiveChecks.every(a=>a.matches)}));
})().catch(e=>{console.error(e.message);process.exitCode=1});
