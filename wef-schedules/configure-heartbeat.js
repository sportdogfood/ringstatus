"use strict";
// Schema only. Does not invoke the collector or populate business records.
const fs=require('node:fs'),path=require('node:path'),settings=require('./settings');
const fields=[{name:'heartbeat-key',type:'singleLineText'},...['checked-at','started'].map(name=>({name,type:'dateTime',options:{dateFormat:{name:'iso'},timeFormat:{name:'24hour'},timeZone:'utc'}})),{name:'selected-date',type:'date',options:{dateFormat:{name:'iso'}}},...['show-id','rows','days-complete','days-failed','archive-count'].map(name=>({name,type:'number',options:{precision:0}})),...['run-id','result'].map(name=>({name,type:'singleLineText'})),...['summary','errors'].map(name=>({name,type:'multilineText'}))];
async function configure(){
 const dir=path.join(__dirname,'evidence','heartbeat-20261020'),route='meta/bases/'+settings.SCHEDULE_STAGING_BASE_ID+'/tables';
 async function api(method='GET',body){const r=await fetch('https://api.airtable.com/v0/'+route,{method,headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error('Heartbeat schema '+method+' HTTP '+r.status);return r.json();}
 const before=await api();const baseline=path.join(dir,'schema-before.json');if(!fs.existsSync(baseline))fs.writeFileSync(baseline,JSON.stringify(before,null,2));
 if(!before.tables.some(t=>t.name==='heartbeat'))await api('POST',{name:'heartbeat',description:'Latest completed schedules collector status. One row updated by normal scheduled runs. Compare checked-at to the expected five-minute cadence; stale status means no recent confirmed update. Detailed history and archives remain in run-logs.',fields});
 const after=await api(),table=after.tables.find(t=>t.name==='heartbeat');for(const f of fields)if(!table?.fields.some(saved=>saved.name===f.name&&saved.type===f.type))throw Error('Heartbeat schema mismatch '+f.name);
 fs.writeFileSync(path.join(dir,'schema-after.json'),JSON.stringify(after,null,2));console.log(JSON.stringify({ok:true,base:settings.SCHEDULE_STAGING_BASE_ID,table:table.id,business_records_written:0}));
}
if(require.main===module)configure().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={configure};
