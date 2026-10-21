"use strict";
// Approved schema migration only: no collector or record backfill.
const fs=require('node:fs'),path=require('node:path'),settings=require('./settings');
const dir=path.join(__dirname,'evidence','show-index-20261020');
async function configure(){
 const root='meta/bases/'+settings.SCHEDULE_STAGING_BASE_ID+'/tables';
 async function api(route=root,method='GET',body){const r=await fetch('https://api.airtable.com/v0/'+route,{method,headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error('Shows schema '+method+' HTTP '+r.status);return r.json();}
 const before=await api();
 for(const[id,name]of [[settings.SHOWS_TABLE,'shows'],[settings.SCHEDULE_COLLECTIONS_TABLE,'show-collection']]){const table=before.tables.find(t=>t.id===id);if(!table)throw Error('Expected existing table missing '+id);if(table.name!==name)await api(root+'/'+id,'PATCH',{name});}
 const show=before.tables.find(t=>t.id===settings.SHOWS_TABLE);
 const fields=[...['show-status','archive-status'].map(name=>({name,type:'singleLineText'})),...['archive-files','archive-notes'].map(name=>({name,type:'multilineText'})),...['archived-schedule-count','archived-collection-count'].map(name=>({name,type:'number',options:{precision:0}})),{name:'archive-verified-at',type:'dateTime',options:{dateFormat:{name:'iso'},timeFormat:{name:'24hour'},timeZone:'utc'}}];
 for(const field of fields)if(!show.fields.some(f=>f.name===field.name))await api(root+'/'+show.id+'/fields','POST',field);
 for(const id of [settings.SCHEDULE_COLLECTIONS_TABLE,'tbluqwuXxOvgDRUBq','tblj077efYbhlznOJ']){
  const table=before.tables.find(t=>t.id===id),existing=table.fields.find(f=>f.name==='shows');
  if(existing&&existing.options?.linkedTableId!==settings.SHOWS_TABLE)throw Error('Conflicting shows link '+table.name);
  if(!existing)await api(root+'/'+id+'/fields','POST',{name:'shows',type:'multipleRecordLinks',options:{linkedTableId:settings.SHOWS_TABLE}});
 }
 fs.writeFileSync(path.join(dir,'schema-after.json'),JSON.stringify(await api(),null,2));console.log(JSON.stringify({ok:true,renamed_in_place:true,business_records_written:0}));
}
if(require.main===module)configure().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={configure};
