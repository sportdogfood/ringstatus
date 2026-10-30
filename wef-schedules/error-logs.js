'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),zlib=require('node:zlib'),settings=require('./settings');
const ERROR_FIELDS=[{name:'error-id',type:'singleLineText'},...['run-id','stage'].map(name=>({name,type:'singleLineText'})),...['recorded-at','started'].map(name=>({name,type:'dateTime',options:{dateFormat:{name:'iso'},timeFormat:{name:'24hour'},timeZone:'utc'}})),{name:'date',type:'date',options:{dateFormat:{name:'iso'}}},{name:'show-id',type:'number',options:{precision:0}},{name:'message',type:'multilineText'},{name:'run-log-url',type:'url'}];
function safeMessage(value,secrets=[]){let text=String(value?.message||value||'Unknown error');for(const secret of secrets)if(typeof secret==='string'&&secret.length>=4)text=text.split(secret).join('[REDACTED]');return text.replace(/Bearer\s+[^\s,;]+/gi,'Bearer [REDACTED]').slice(0,4000);}
function errorLogs({api=require('./attempt-api').attemptAPI(),request=globalThis.fetch,limit=1000,historyRoot=path.join(__dirname,'runtime/history/error-logs'),clock=()=>new Date().toISOString(),onArchive,onEvent,serialize=work=>work(),secrets=Object.entries(process.env).filter(([k])=>/WEF_SCHEDULES_.*(TOKEN|COOKIE|AUTHORIZATION|BEARER)/.test(k)).map(([,v])=>v)}={}){
 const table='err-logs',base=settings.SCHEDULE_STAGING_BASE_ID,list=()=>api.airtableList(table);
 const checked=r=>{if(r.failedRows?.length)throw Error('Error log update failed');};
 return {
  async record(record,event){
   const runId=record?.fields?.['run-id']||event.runId;if(!/^[A-Za-z0-9_-]{1,80}$/.test(runId||''))throw Error('Error event missing valid attempt ID');
   const message=safeMessage(event.message,[...secrets,...(event.secrets||[])]),stage=String(event.stage||'collection');
   const started=record?.fields?.started||event.started||null,showId=Object.hasOwn(event,'showId')?event.showId:(record?.fields?.['show-id']??null);
   const key=crypto.createHash('sha256').update(JSON.stringify([runId,stage,event.date||null,showId,message])).digest('hex');
   if(onEvent&&!event.recovered)onEvent({version:1,state:'error',runId,started,stage,date:event.date||null,showId,message});
   return serialize(async()=>{
   const previous=await list(),matches=previous.filter(r=>r.fields['error-id']===key);if(matches.length>1)throw Error('Duplicate error identity');
   const fields={'error-id':key,'run-id':runId,stage,'recorded-at':new Date(clock()).toISOString(),started:started?new Date(started).toISOString():null,date:event.date||null,'show-id':showId,message,'run-log-url':record?.id?`https://airtable.com/${base}/tbluqwuXxOvgDRUBq/${record.id}`:null};
   if(matches.length){for(const name of ['run-id','stage','date','show-id','message'])if(JSON.stringify(matches[0].fields[name]??null)!==JSON.stringify(fields[name]??null))throw Error('Error log identity readback mismatch');return matches[0];}
   if(previous.length>=limit){
    const oldest=[...previous].sort((a,b)=>String(a.fields['recorded-at']).localeCompare(String(b.fields['recorded-at'])))[0];
    const id=oldest.fields['error-id'];if(!/^[a-f0-9]{64}$/.test(id||''))throw Error('Invalid retained error ID');
    fs.mkdirSync(historyRoot,{recursive:true});const file=path.join(historyRoot,id+'.json.gz'),body=Buffer.from(JSON.stringify(oldest));
    if(fs.existsSync(file)){if(!zlib.gunzipSync(fs.readFileSync(file)).equals(body))throw Error('Error archive collision');}else{fs.writeFileSync(file+'.tmp',zlib.gzipSync(body));if(!zlib.gunzipSync(fs.readFileSync(file+'.tmp')).equals(body))throw Error('Error archive verification failed');fs.renameSync(file+'.tmp',file);}
    if(onArchive)await onArchive({at:clock(),kind:'err-logs',outcome:'Created',file,records:1,shows:oldest.fields['show-id']?[oldest.fields['show-id']]:[],dates:oldest.fields.date?[oldest.fields.date]:[],sha256:crypto.createHash('sha256').update(body).digest('hex')});
    checked(await api.airtablePatchRecords(table,[{id:oldest.id,fields}]));
   }else{
    // One POST only. Lost responses are reconciled by the deterministic key.
    try{const response=await request(`https://api.airtable.com/v0/${base}/${table}`,{method:'POST',headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN,'Content-Type':'application/json'},body:JSON.stringify({records:[{fields}]}),signal:AbortSignal.timeout(30000)});if(!response.ok)throw Error('Error log create HTTP '+response.status);}
    catch(error){const found=(await list()).filter(r=>r.fields['error-id']===key);if(found.length!==1)throw Error('Error log creation outcome unresolved: '+safeMessage(error,secrets));}
   }
   const saved=(await list()).filter(r=>r.fields['error-id']===key);if(saved.length!==1)throw Error('Error log readback missing or ambiguous');
   for(const [name,value] of Object.entries(fields))if(JSON.stringify(saved[0].fields[name]??null)!==JSON.stringify(value??null))throw Error('Error log readback mismatch '+name);
   return saved[0];
  });},
  async report(record,report){if(report?.entries?.error)await this.record(record,{stage:'entry-collection',showId:report.entries.show_id,message:report.entries.error});for(const e of report?.entries?.errors||[])await this.record(record,{stage:'entry-collection',date:e.date,showId:report.entries.show_id,message:e.kind+' '+e.id+': '+e.error});for(const day of report?.days||[]){if(!day.ok)await this.record(record,{stage:'collection',date:day.date,showId:day.show_id,message:day.error||'Dated collection failed'});for(const warning of day.warnings||[])if(warning.error)await this.record(record,{stage:'enrichment',date:day.date,showId:day.show_id,message:warning.error});}},
 };
}
module.exports={errorLogs,ERROR_FIELDS,safeMessage};
