"use strict";
// Destination schema only. No business records, source requests or collector execution.
const fs=require('node:fs'),path=require('node:path'),settings=require('./settings');
async function configure(){
 async function schema(base,suffix='',method='GET',body){if(method!=='GET'&&base!==settings.BASE_ID)throw Error('Protected schema');const r=await fetch(`https://api.airtable.com/v0/meta/bases/${base}/tables${suffix}`,{method,headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error('Schema HTTP '+r.status);return r.json();}
 const templates=await schema('appUq5Qz9kARqFija'),target=await schema(settings.BASE_ID);
 const names={shows:['show-key','show_id','customer_id','facility_id','show_name','start_date','end_date','selected_date','available_dates','is_default','last_checked'],'run-logs':['run-id','started','completed','result','show-id','rows','summary','errors','archive-count','archives'],heartbeat:['heartbeat-key','checked-at','show-id','selected-date','result','summary','errors']};
 for(const [name,fields]of Object.entries(names)){
  const template=templates.tables.find(t=>t.name===name);if(!template)throw Error('Missing template '+name);
  const specs=fields.map(n=>{const f=template.fields.find(f=>f.name===n);if(!f)throw Error('Missing prepared field '+n);return{name:n,type:f.type,...(f.options&&['number','date','dateTime','checkbox'].includes(f.type)?{options:f.options}:{})};});
  if(name==='shows')specs.push({name:'default-payload-file',type:'singleLineText'},{name:'default-payload-sha256',type:'singleLineText'});
  const old=target.tables.find(t=>t.name===name);if(old){if(!specs.every(f=>old.fields.some(x=>x.name===f.name&&x.type===f.type)))throw Error('Existing schema conflict '+name);}else await schema(settings.BASE_ID,'','POST',{name,fields:specs});
 }
 const result={at:new Date().toISOString(),businessRecordsWritten:0,schema:await schema(settings.BASE_ID)};fs.writeFileSync(path.join(__dirname,'evidence/schema-configured.json'),JSON.stringify(result,null,2));console.log(JSON.stringify({tables:result.schema.tables.map(t=>({name:t.name,id:t.id})),businessRecordsWritten:0}));
}
if(require.main===module)configure().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={configure};
