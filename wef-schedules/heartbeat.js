"use strict";
const settings=require('./settings'),legacy=require('./collector-core');
const cell=v=>v==null||v===''?null:v;
function heartbeat({api=legacy,request=globalThis.fetch}={}){
 const base=settings.SCHEDULE_STAGING_BASE_ID,table='heartbeat',key='schedules';
 const list=()=>api.airtableList(table,{},base);
 function row(records){if(records.length>1||records.some(r=>r.fields['heartbeat-key']!==key))throw Error('Heartbeat missing key or ambiguous rows; inspect without repair');return records[0];}
 async function verify(fields){const saved=row(await list());if(!saved)throw Error('Heartbeat readback missing');for(const[k,v]of Object.entries(fields))if(JSON.stringify(cell(saved.fields[k]))!==JSON.stringify(cell(v)))throw Error('Heartbeat readback mismatch '+k);return saved;}
 return{async update(run,selectedDate=null){
  if(!run['run-id']||!run.completed||!['Success','Warning','Failed'].includes(run.result))throw Error('Heartbeat requires a finished saved run');
  const fields={'heartbeat-key':key,'checked-at':run.completed,'run-id':run['run-id'],started:run.started,'selected-date':selectedDate,'show-id':run['show-id']??null,result:run.result,rows:run.rows??null,'days-complete':run['days-complete']??0,'days-failed':run['days-failed']??0,'archive-count':run['archive-count']??0,summary:run.summary||'',errors:run.errors||'',...(Object.hasOwn(run,'shows')?{shows:run.shows}:{})};
  const previous=row(await list());
  if(previous){const saved=await api.airtablePatchRecords(table,[{id:previous.id,fields}],base);if(saved.failedRows?.length)throw Error('Heartbeat patch failed');}
  else{
   // No creation retry, including after an uncertain response. Reconcile by key.
   try{const response=await request(`https://api.airtable.com/v0/${base}/${table}`,{method:'POST',headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN,'Content-Type':'application/json'},body:JSON.stringify({records:[{fields}]}),signal:AbortSignal.timeout(30000)});if(!response.ok)throw Error('HTTP '+response.status);}
   catch(error){try{return await verify(fields);}catch(check){throw Error('Heartbeat creation outcome unresolved: '+check.message);}}
  }
  return verify(fields);
 }};
}
module.exports={heartbeat};
