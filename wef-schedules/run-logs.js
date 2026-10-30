"use strict";
const crypto=require('node:crypto'),fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib');
const settings=require('./settings');

// One row per acquired collector invocation. Keep Airtable bounded without
// deleting records: archive the oldest finished log before reusing its slot.
function runLogs({api,request=globalThis.fetch,limit=1000,heartbeat,catalog,serialize=work=>work(),
  historyRoot=path.join(path.dirname(settings.SCHEDULE_FOCUS_FILE),'history','run-logs'),
  clock=()=>new Date().toISOString()}={}) {
 api ||= require('./collector-core');
 const base=settings.SCHEDULE_STAGING_BASE_ID,table='run-logs';
 const list=()=>api.airtableList(table,{},base);
 const checked=r=>{if(r.failedRows?.length)throw Error('Run log write failed');};
 const cell=value=>value===''||value==null?null:value;
 async function readback(id,fields){
  const matches=(await list()).filter(r=>r.fields['run-id']===id);
  if(matches.length!==1)throw Error('Run log readback missing or ambiguous');
  for(const [k,v] of Object.entries(fields))
   if(JSON.stringify(cell(matches[0].fields[k]))!==JSON.stringify(cell(v)))throw Error('Run log readback mismatch: '+k);
  return matches[0];
 }
 const archiveFields=events=>({'archive-count':events.length,archives:events.map(e=>`${e.at}: ${e.kind} ${e.outcome}; shows ${(e.shows||[]).join(',')||'none'}; dates ${(e.dates||[]).join(',')||'none'}; ${e.records} records; ${e.file}; SHA256 ${e.sha256||'unverified'}${e.cleanup?'; cleanup '+e.cleanup:''}${e.error?'; '+e.error:''}`).join('\n')});
 return {
  records:list,
  async archive(record,event){return serialize(async()=>{
   const events=record.archiveEvents||(record.archiveEvents=[]),index=events.findIndex(e=>e.file===event.file);
   if(index<0)events.push({at:clock(),...event});else events[index]={...events[index],...event};
   const fields=archiveFields(events);checked(await api.airtablePatchRecords(table,[{id:record.id,fields}],base));
   await readback(record.fields['run-id'],fields);
  });},
  async begin({runId=crypto.randomUUID(),started=clock()}={}){return serialize(async()=>{
   if(!/^[A-Za-z0-9_-]{1,80}$/.test(runId)||!Number.isFinite(Date.parse(started)))throw Error('Invalid attempt identity/time');
   started=new Date(started).toISOString();
   const previous=await list();
   const retainedAttempt=previous.filter(r=>r.fields['run-id']===runId);
   if(retainedAttempt.length){if(retainedAttempt.length!==1||retainedAttempt[0].fields.started!==started)throw Error('Ambiguous attempt identity');return retainedAttempt[0];}
   const fields={'run-id':runId,started,completed:null,result:'Running',
    'show-id':null,rows:null,'days-complete':0,'days-failed':0,summary:'Scheduled attempt started; collection has not begun.',errors:'',...archiveFields([])};
   let retained;
   if(previous.length>=limit){
    const oldest=previous.filter(r=>r.fields.result!=='Running').sort((a,b)=>String(a.fields.started).localeCompare(String(b.fields.started)))[0];
    if(!oldest)throw Error('Run log capacity occupied by running records; inspect interrupted runs');
    const key=oldest.fields['run-id'];if(!/^[A-Za-z0-9_-]+$/.test(key))throw Error('Invalid archived run ID');
    const file=path.join(historyRoot,key+'.json.gz'),body=Buffer.from(JSON.stringify(oldest));
    fs.mkdirSync(historyRoot,{recursive:true});
    const retainedExists=fs.existsSync(file);
    if(retainedExists){if(!zlib.gunzipSync(fs.readFileSync(file)).equals(body))throw Error('Run log archive collision');}
    else {fs.writeFileSync(file+'.tmp',zlib.gzipSync(body));if(!zlib.gunzipSync(fs.readFileSync(file+'.tmp')).equals(body))throw Error('Run log archive readback mismatch');fs.renameSync(file+'.tmp',file);}
    const data=Array.isArray(oldest)?oldest:[oldest];
    retained={at:clock(),kind:'run-logs',outcome:retainedExists?'Reused':'Created',file,shows:oldest.fields['show-id']?[oldest.fields['show-id']]:[],dates:[...new Set(String(oldest.fields.summary||'').match(/\d{4}-\d{2}-\d{2}/g)||[])],records:data.length,sha256:crypto.createHash('sha256').update(body).digest('hex')};
    Object.assign(fields,archiveFields([retained]));
    checked(await api.airtablePatchRecords(table,[{id:oldest.id,fields}],base));
   } else {
    // A creation request is never retried. A lost response is reconciled by
    // stable run ID using read-only lookup; uncertainty must not create twice.
    try {
     const response=await request(`https://api.airtable.com/v0/${base}/${table}`,{method:'POST',headers:{Authorization:`Bearer ${settings.AIRTABLE_TOKEN}`,'Content-Type':'application/json'},body:JSON.stringify({records:[{fields}]}),signal:AbortSignal.timeout(30000)});
     if(!response.ok)throw Error('HTTP '+response.status);
    } catch(error) {
     try {return await readback(fields['run-id'],fields);}
     catch(readError){throw Error('Run log write outcome unresolved: '+readError.message);}
    }
   }
   const record=await readback(fields['run-id'],fields);record.archiveEvents=retained?[retained]:[];return record;
  });},
  async finish(record,report,error,{completedAt}={}){return serialize(async()=>{
   const days=report?.days||[],successful=days.filter(d=>d.ok),failed=days.filter(d=>!d.ok);
   const warnings=days.flatMap(d=>d.warnings||[]);
   const errors=[...(record.archiveEvents||[]).filter(e=>e.error).map(e=>e.file+': '+e.error),...(error?[String(error.message||error)]:[]),...(report?.entries?.error?[report.entries.error]:[]),...(report?.entries?.errors||[]).map(e=>'Entries '+e.kind+' '+e.id+': '+e.error),...failed.map(d=>`${d.date}: ${d.error}${d.log_error?'; status save failed: '+d.log_error:''}`),
    ...days.flatMap(d=>(d.warnings||[]).filter(w=>w.error).map(w=>`${d.date}: ${w.error}`))];
   const lines=days.map(d=>d.ok?`${d.date}: ${d.row_count} rows${d.classes_count!==undefined?`; ${d.classes_count} classes, ${d.schedule_blocks_count||0} schooling/break blocks`:""}; added ${d.changes?.added.length||0}, changed ${d.changes?.changed.length||0}, dropped ${d.changes?.dropped.length||0}${d.unchanged?'; dataset reused':''}${d.warnings?.length?'; '+d.warnings.map(w=>w.code+': '+(w.message||w.error||`${w.unmatched||0} unmatched`)).join('; '):''}`:`${d.date}: FAILED; ${d.error}`);
   if(report?.references){const r=report.references;lines.push(`Show/rings verified: show ${r.show_id}; source-selected date ${r.selected_date}; ${r.rings} daily rings (${r.selected_rings} selected); ${r.helpers} static helpers; ${r.schedule_links} schedule links.${r.dropped_without_ring?' '+r.dropped_without_ring+' retained dropped schedules have no source/historical ring; link left blank.':''}`);}
   if(report?.entries){const e=report.entries;lines.push(e.error?'Entry/trip collection FAILED: '+e.error:`Entry/trip collection: ${e.entries||0} entries; ${e.trips||0} trips; ${e.completed_classes||0}/${e.classes||0} classes; ${e.conflicts||0} ambiguous schedule links; ${e.source_requests||0} decoded source reads; ${e.ok?'complete':'FAILED'}.`);}
   const fields={completed:new Date(completedAt||clock()).toISOString(),result:error||report?.ok===false?'Failed':warnings.length?'Warning':'Success',
    'show-id':report?.default_show?.current?.show_id??days[0]?.show_id??null,
    rows:days.length?successful.reduce((sum,d)=>sum+(d.row_count||0),0):null,
    'days-complete':successful.length,'days-failed':failed.length,
    summary:lines.join('\n')||(error?'Failed before day collection completed.':'No show days collected.'),errors:errors.join('\n')};
   checked(await api.airtablePatchRecords(table,[{id:record.id,fields}],base));
   let saved=await readback(record.fields['run-id'],fields);
   if(catalog)try{saved=await catalog.sync(saved);}
   catch(error){
    const failure={result:'Failed',errors:[fields.errors,'Show index update failed: '+error.message].filter(Boolean).join('\n')};
    checked(await api.airtablePatchRecords(table,[{id:record.id,fields:failure}],base));
    await readback(record.fields['run-id'],failure);
    throw Error('Show index update failed: '+error.message);
   }
   if(heartbeat)try{await heartbeat.update(saved.fields,report?.references?.selected_date??null);}
   catch(error){
    const failure={result:'Failed',errors:[fields.errors,'Heartbeat update failed: '+error.message].filter(Boolean).join('\n')};
    checked(await api.airtablePatchRecords(table,[{id:record.id,fields:failure}],base));
    await readback(record.fields['run-id'],failure);
    throw Error('Heartbeat update failed: '+error.message);
   }
   return saved;
  });}
 };
}
module.exports={runLogs};
