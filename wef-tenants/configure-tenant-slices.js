"use strict";
// Metadata only. The normal CWF scheduled runner populates these local fields.
const settings=require('./settings');
const wanted=[{name:'active-team',type:'checkbox',options:{icon:'check',color:'greenBright'},description:'Current trainer participation with an active horse and rider. A display slice only; collection remains inclusive.'},{name:'team-status',type:'singleLineText',description:'Why this entry is shown or excluded from the active team. Contains no private follow ownership.'},{name:'team-checked-at',type:'dateTime',options:{dateFormat:{name:'iso'},timeFormat:{name:'24hour'},timeZone:'utc'},description:'Last normal scheduled evaluation of the tenant slice.'}];
async function configure(){const route='https://api.airtable.com/v0/meta/bases/'+settings.BASE_ID+'/tables';async function call(url,method='GET',body){const r=await fetch(url,{method,headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error('Tenant slice schema '+r.status);return r.json();}
 let schema=await call(route),table=schema.tables.find(t=>t.id===settings.SYNC_ENTRIES_TABLE);if(table?.name!=='wef-entries.entries')throw Error('Expected existing native entries sync');
 for(const f of wanted){const existing=table.fields.find(x=>x.name===f.name);if(existing&&existing.type!==f.type)throw Error('Conflicting slice field '+f.name);}
 for(const f of wanted)if(!table.fields.some(x=>x.name===f.name))await call(route+'/'+table.id+'/fields','POST',f);
 schema=await call(route);table=schema.tables.find(t=>t.id===settings.SYNC_ENTRIES_TABLE);for(const f of wanted)if(!table.fields.some(x=>x.name===f.name&&x.type===f.type))throw Error('Slice schema readback failed');
 return{base:settings.BASE_ID,table:table.id,fields:table.fields.filter(f=>wanted.some(w=>w.name===f.name)).map(f=>({id:f.id,name:f.name,type:f.type})),businessRecordsWritten:0};}
if(require.main===module)configure().then(r=>console.log(JSON.stringify(r))).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={configure};
