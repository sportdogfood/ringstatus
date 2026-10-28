"use strict";
// Metadata only: preserves all record values. Never invokes collection.
const fs=require('node:fs'),path=require('node:path'),settings=require('./settings'),{columns}=require('./identity-policy');
async function configure(){
 const dir=path.join(__dirname,'evidence','identity-policy-20261020');
 async function api(route,method='GET',body){const response=await fetch('https://api.airtable.com/v0/'+route,{method,headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});if(!response.ok)throw Error('Identity metadata '+method+' HTTP '+response.status);return response.json();}
 const route='meta/bases/'+settings.BASE_ID+'/tables',before=await api(route);
 const baseline=path.join(dir,'schema-before.json');if(!fs.existsSync(baseline))fs.writeFileSync(baseline,JSON.stringify(before,null,2));
 for(const name of ['horses','riders']){
  const table=before.tables.find(t=>t.name===name);if(!table)throw Error('Missing '+name);
  if(name==='riders'){
   const old=table.fields.find(f=>f.name==='show-name');
   if(old){if(table.fields.some(f=>f.name==='legacy-display-name'))throw Error('Retired rider field already exists; inspect without merging');await api(route+'/'+table.id+'/fields/'+old.id,'PATCH',{name:'legacy-display-name',description:'Retired 2026-10-20. Preserved historical display values; not used by collection, identity policy or lookups. Riders use rider-name, name, first-name and last-name.'});}
  }
  for(const field of columns[name]){
   const target=table.fields.find(f=>f.name===field);if(!target)throw Error('Missing policy field '+name+'.'+field);
   await api(route+'/'+table.id+'/fields/'+target.id,'PATCH',{description:'Unique populated values within '+name+'. '+(field.endsWith('-key')?'Required stable key; exact case.':field.endsWith('-id')?'Positive integer when known; unknown stays blank.':'Names compare without case or extra whitespace; blanks permitted.')+' Checked before collector writes and helper lookups. Manual Airtable edits are checked on the next scheduled run; conflicts stop roster writes and are logged.'});
  }
  const lookup=table.fields.find(f=>f.name==='lookup-text');if(!lookup)throw Error('Missing helper search formula');
  const search=['name','nick-name',...(name==='horses'?['show-name']:['first-name','last-name']),'aliases',name==='horses'?'horse-name':'rider-name',name==='horses'?'horse-key':'rider-key',name==='horses'?'horse-id':'rider-id','trainer-ids','tenant-id'];
  await api(route+'/'+table.id+'/fields/'+lookup.id,'PATCH',{options:{formula:'LOWER('+search.map(n=>'{'+n+'}').join('&" | "&')+')'}});
 }
 const after=await api(route);fs.writeFileSync(path.join(dir,'schema-after.json'),JSON.stringify(after,null,2));
 const rider=after.tables.find(t=>t.name==='riders');if(rider.fields.some(f=>f.name==='show-name')||rider.fields.find(f=>f.name==='lookup-text').options.formula.includes('legacy-display-name'))throw Error('Rider retirement readback failed');
 console.log(JSON.stringify({ok:true,business_records_written:0,rider_show_name:'retired; historical values preserved',checked_at:new Date().toISOString()}));
}
if(require.main===module)configure().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={configure};
