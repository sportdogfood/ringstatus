"use strict";
// Explicit configuration only. Never fetches SGL data or runs a collector.
const fs=require('node:fs'),path=require('node:path');
const config=require('./schema.json');
const token=process.env.WEF_SCHEDULES_AIRTABLE_TOKEN;
async function api(route,method='GET',body){
 const r=await fetch('https://api.airtable.com/v0/'+route,{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});
 const j=await r.json();if(!r.ok)throw Error(method+' '+route+': '+r.status+' '+JSON.stringify(j));return j;
}
async function main(){
 let schema=await api('meta/bases/'+config.baseId+'/tables');
 const extra=schema.tables.find(t=>t.name==='Table 1');if(extra)await api('meta/bases/'+config.baseId+'/tables/'+extra.id,'PATCH',{name:'meta',description:'Base setup metadata. Default empty starter rows are not business records.'});
 for(const spec of config.tables){if(!schema.tables.some(t=>t.name===spec.name)){const t=await api('meta/bases/'+config.baseId+'/tables','POST',spec);schema.tables.push(t);}}
 const links=[['rings','show-details','show-details'],['rings','rings-helper','rings-helper'],['entries','show-details','show-details'],['entries','rings','rings'],['entries','horses','horses'],['entries','riders','riders'],['entries','trainers','trainers'],['collections','show-details','show-details'],['collections','trainers','trainers'],['ring-walks','rings-helper','rings-helper'],['horses','trainers','trainers'],['riders','trainers','trainers']];
 for(const [from,name,to]of links){const t=schema.tables.find(t=>t.name===from);if(!t.fields.some(f=>f.name===name)){const f=await api('meta/bases/'+config.baseId+'/tables/'+t.id+'/fields','POST',{name,type:'multipleRecordLinks',options:{linkedTableId:schema.tables.find(t=>t.name===to).id}});t.fields.push(f);}}
 const shared=await api('meta/bases/'+config.sharedBaseId+'/tables');
 const helper=shared.tables.find(t=>t.name==='rings-helper');
 for(const name of ['ring-nickname','ring-lat','ring-lon'])if(!helper.fields.some(f=>f.name===name))await api('meta/bases/'+config.sharedBaseId+'/tables/'+helper.id+'/fields','POST',{name,type:name==='ring-nickname'?'singleLineText':'number',...(name==='ring-nickname'?{}:{options:{precision:6}})});
 const idx=shared.tables.find(t=>t.name==='wef-tenant-index');
 if(!idx.fields.some(f=>f.name==='show-details'))await api('meta/bases/'+config.sharedBaseId+'/tables/'+idx.id+'/fields','POST',{name:'show-details',type:'multipleRecordLinks',options:{linkedTableId:shared.tables.find(t=>t.id==='tblBq3gXC0TYBCmMZ').id}});
 // Seed only owner-approved configuration. Dataset and reference rows are runner-owned.
 async function seed(table,key,fields){const existing=await api(config.baseId+'/'+encodeURIComponent(table));const matches=existing.records.filter(r=>r.fields[key]===fields[key]);if(matches.length>1)throw Error('Ambiguous config '+key);if(!matches.length)await api(config.baseId+'/'+encodeURIComponent(table),'POST',{records:[{fields}]});}
 const tenants=await api(config.sharedBaseId+'/tenants');const cwf=tenants.records.find(r=>r.fields['tenant-key']==='CWF');if(!cwf)throw Error('Existing CWF location missing');
 await seed('tenant-details','tenant-key',{'tenant-key':'CWF','tenant-name':'Castlewood Farm','tenant-id':8778,active:true,...Object.fromEntries(['barn-name','barn-lat','barn-lon','barn-map','notes'].map(k=>[k,cwf.fields[k]??null]))});
 await seed('trainers','trainer-key',{'trainer-key':'8778','trainer-id':8778,'trainer-name':'ALAN KOROTKIN',role:'main',slot:1,active:true});
 await seed('code-reviews','routine',{routine:'Weekly tenant collector review',cadence:'Monday 09:00 America/New_York','next-review':'2026-10-26T09:00:00-04:00','last-result':'Pending initial technical review',notes:'Read-only review. No manual collector runs or record repairs. See repository release evidence.'});
 fs.writeFileSync(path.join(__dirname,'evidence','schema-after.json'),JSON.stringify({tenant:await api('meta/bases/'+config.baseId+'/tables'),shared:await api('meta/bases/'+config.sharedBaseId+'/tables')},null,2));
 console.log(JSON.stringify({ok:true,base:config.baseId,tables:config.tables.map(t=>t.name)}));
}
if(require.main===module)main().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={api};
