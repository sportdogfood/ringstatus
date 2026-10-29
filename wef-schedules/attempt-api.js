'use strict';
// Logging-only transport: no source calls and no dependency on collector imports.
const settings=require('./settings'),{readAirtable}=require('./airtable-read');
function attemptAPI({request=globalThis.fetch}={}){
 const allowed=new Set(['run-logs','err-logs']),base=settings.SCHEDULE_STAGING_BASE_ID;
 const url=table=>{if(!allowed.has(table))throw Error('Unapproved attempt-log table');return `https://api.airtable.com/v0/${base}/${encodeURIComponent(table)}`;};
 const headers={Authorization:'Bearer '+settings.AIRTABLE_TOKEN,'Content-Type':'application/json'};
 return {
  async airtableList(table){const records=[];let offset;do{const u=new URL(url(table));u.searchParams.set('pageSize','100');if(offset)u.searchParams.set('offset',offset);const data=await readAirtable(u.href,{request,headers});records.push(...data.records);offset=data.offset;}while(offset);return records;},
  async airtablePatchRecords(table,records){for(let i=0;i<records.length;i+=10){const response=await request(url(table),{method:'PATCH',headers,body:JSON.stringify({records:records.slice(i,i+10)}),signal:AbortSignal.timeout(30000)});if(!response.ok)throw Error('Attempt log update HTTP '+response.status);}return{failedRows:[]};},
 };
}
module.exports={attemptAPI};
