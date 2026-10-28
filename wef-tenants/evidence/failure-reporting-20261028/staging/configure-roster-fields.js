"use strict";
// Schema only. Does not populate lists or run business collection.
const fs=require('node:fs'),path=require('node:path'),config=require('./schema.json');
async function main(){
 const token=process.env.WEF_TENANTS_AIRTABLE_TOKEN;
 async function api(route,method='GET',body){const r=await fetch('https://api.airtable.com/v0/'+route,{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});const value=await r.json();if(!r.ok)throw Error('Roster schema '+r.status+' '+JSON.stringify(value));return value;}
 const route='meta/bases/'+config.baseId+'/tables',schema=await api(route);
 const shared=['name','nick-name','aliases','notes','trainer-name','source-base','source-table','source-record','source-link','source-name','helper-refreshed'];
 for(const name of ['horses','riders']){
  const table=schema.tables.find(t=>t.name===name),spec=config.tables.find(t=>t.name===name);
  const fields=[...shared,...(name==='horses'?['show-name']:[])].map(field=>({name:field,type:['aliases','notes'].includes(field)?'multilineText':field==='source-link'?'url':'singleLineText',...(field==='show-name'?{description:'Registered/show display name of this horse or rider. Not the name of a competition and not proof of attendance.'}:{})}));
  fields.push(...['tenant-id','trainer-id'].map(name=>({name,type:'number',options:{precision:0}})),...['source-inactive','source-id-missing'].map(name=>({name,type:'checkbox',options:{icon:'check',color:'greenBright'}})));
  if(name==='riders')fields.push(...['first-name','last-name'].map(name=>({name,type:'singleLineText'})));
  for(const field of fields){if(!table.fields.some(f=>f.name===field.name)){const saved=await api(route+'/'+table.id+'/fields','POST',field);table.fields.push(saved);}if(!spec.fields.some(f=>f.name===field.name))spec.fields.push(field);}
  const search=['name','nick-name','aliases','trainer-ids','tenant-id',...(name==='riders'?['first-name','last-name']:[])];
  const formula='LOWER('+search.map(n=>'{'+n+'}').join('&" | "&')+')';
  if(!table.fields.some(f=>f.name==='lookup-text'))await api(route+'/'+table.id+'/fields','POST',{name:'lookup-text',type:'formula',options:{formula},description:'Searchable helper names and exact identifiers; reflects manual nickname/name edits immediately.'});
 }
 fs.writeFileSync(path.join(__dirname,'schema.json'),JSON.stringify(config,null,2)+'\n');
 fs.writeFileSync(path.join(__dirname,'evidence','standing-roster-20261020','schema-after.json'),JSON.stringify(await api(route),null,2));
 console.log(JSON.stringify({ok:true,changed_tables:['horses','riders'],business_records_written:0}));
}
if(require.main===module)main().catch(e=>{console.error(e.message);process.exitCode=1;});
