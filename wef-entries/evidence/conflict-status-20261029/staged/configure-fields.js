"use strict";
const settings=require('./settings');
const fields=[
 ['source-trainer-ids','singleLineText'],['schedule-link-status','singleLineText'],
 ['schedule-link-basis','singleLineText'],['group-link-status','singleLineText'],['link-issues','multilineText']
];
async function configure({token=settings.AIRTABLE_TOKEN,request=globalThis.fetch}={}){
 async function call(suffix='',method='GET',body){const r=await request(`https://api.airtable.com/v0/meta/bases/${settings.BASE_ID}/tables${suffix}`,{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error('Entries schema HTTP '+r.status);return r.json();}
 const schema=await call(),table=schema.tables.find(t=>t.id==='tblqkWmrsYMs2hyQ6'&&t.name==='entries');if(!table)throw Error('Entries target table mismatch');
 for(const[name,type]of fields){const old=table.fields.find(f=>f.name===name);if(old&&old.type!==type)throw Error('Existing field type mismatch: '+name);}
 for(const[name,type]of fields)if(!table.fields.some(f=>f.name===name))await call('/'+table.id+'/fields','POST',{name,type});
 const after=(await call()).tables.find(t=>t.id===table.id);for(const[name,type]of fields)if(!after.fields.some(f=>f.name===name&&f.type===type))throw Error('Field readback failed: '+name);
 return{base:settings.BASE_ID,table:table.id,fields:fields.map(([name])=>after.fields.find(f=>f.name===name))};
}
module.exports={configure,fields};
