"use strict";
// Metadata only. Business records and their links are written by normal scheduled runs.
async function configure(){
 const settings=require('./settings'),route='https://api.airtable.com/v0/meta/bases/'+settings.BASE_ID+'/tables';
 async function call(url,method='GET',body){await new Promise(r=>setTimeout(r,250));const response=await fetch(url,{method,headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});if(!response.ok)throw Error('Entry consumer schema HTTP '+response.status);return response.json();}
 let schema=await call(route),table=schema.tables.find(t=>t.id===settings.SYNC_ENTRIES_TABLE);if(!table)throw Error('Native CWF entries sync missing');
 const targets={horses:'tblxMpG75KL3Nmt1v',riders:'tblll5l7eG5wc27uP',trainers:'tbljBHTXEJn71RRIl',shows:settings.SYNC_SHOWS_TABLE,'synced-rings':settings.SYNC_RINGS_TABLE,schedules:settings.SYNC_SCHEDULES_TABLE};
 for(const [name,id]of Object.entries(targets)){if(!schema.tables.some(t=>t.id===id))throw Error('Existing helper target missing');const field=table.fields.find(f=>f.name===name);if(field&&(field.type!=='multipleRecordLinks'||field.options.linkedTableId!==id))throw Error('Conflicting entry link '+name);}
 if(table.name!=='wef-entries.entries')await call(route+'/'+table.id,'PATCH',{name:'wef-entries.entries',description:'Read-only CWF team participation synced from wef-entries. Local helper links are maintained by the CWF scheduled task. Private-only follows are excluded.'});
 for(const [name,id]of Object.entries(targets))if(!table.fields.some(f=>f.name===name))await call(route+'/'+table.id+'/fields','POST',{name,type:'multipleRecordLinks',options:{linkedTableId:id}});
 schema=await call(route);table=schema.tables.find(t=>t.id===settings.SYNC_ENTRIES_TABLE);const links=[];
 for(const[name,id]of Object.entries(targets)){const field=table.fields.find(f=>f.name===name),target=schema.tables.find(t=>t.id===id),inverse=target.fields.find(f=>f.id===field?.options?.inverseLinkFieldId);if(field?.options?.linkedTableId!==id||inverse?.options?.linkedTableId!==table.id)throw Error('Reciprocal entry link readback failed');links.push({name,field:field.id,target:id,inverse:inverse.id});}
 return{base:settings.BASE_ID,table:table.id,name:table.name,links,businessRecordsWritten:0};
}
if(require.main===module)configure().then(r=>console.log(JSON.stringify(r))).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={configure};
