"use strict";
// Metadata only. Values are populated exclusively by ordinary scheduled collection.
const settings=require('./settings');
const fields=[{name:'schedule-keys',type:'multilineText',description:'Sorted unique JSON array of exact schedule stage-keys. Singleton agrees with schedule-key; multiple values require blank scalar. No inferred group eligibility.'},{name:'observation-status',type:'singleLineText',description:'successful, failed, unresolved, or dropped. Current eligibility requires successful, active status and fresh last-seen. Retained links otherwise represent history.'}];
async function configure(){
 const root='https://api.airtable.com/v0/meta/bases/'+settings.BASE_ID+'/tables';
 async function call(url,method='GET',body){const r=await fetch(url,{method,headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});if(!r.ok)throw Error('Schedule metadata HTTP '+r.status);return r.json();}
 let table=(await call(root)).tables.find(t=>t.id==='tblqkWmrsYMs2hyQ6'&&t.name==='entries');if(!table)throw Error('Entries table identity mismatch');
 for(const f of fields){const old=table.fields.find(x=>x.name===f.name);if(old&&old.type!==f.type)throw Error('Field type conflict '+f.name);}
 for(const f of fields)if(!table.fields.some(x=>x.name===f.name))await call(root+'/'+table.id+'/fields','POST',f);
 table=(await call(root)).tables.find(t=>t.id===table.id);for(const f of fields)if(!table.fields.some(x=>x.name===f.name&&x.type===f.type))throw Error('Metadata readback mismatch');
 return{base:settings.BASE_ID,table:table.id,fields:table.fields.filter(f=>fields.some(w=>w.name===f.name)),businessRecordsWritten:0};
}
if(require.main===module)configure().then(r=>console.log(JSON.stringify(r))).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={configure};
