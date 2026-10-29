"use strict";
const BASE='appCaSmL819KK7UFJ',RUNS='tblchw5nYX0BhE9DH';
const time={dateFormat:{name:'iso'},timeFormat:{name:'24hour'},timeZone:'utc'};
const text=name=>({name,type:'singleLineText'}),long=name=>({name,type:'multilineText'}),date=name=>({name,type:'dateTime',options:time});
const sourceFields=[...['source-key','name','kind','location','owner'].map(text),long('purpose'),date('last-attempt-at'),date('last-success-at'),text('last-result'),long('last-error')];
const errorFields=[text('error-key'),text('run-id'),date('at'),text('stage'),long('message'),text('source-key')];
async function configureAudit({token,request=fetch}){
 async function call(suffix='',method='GET',body){const r=await request(`https://api.airtable.com/v0/meta/bases/${BASE}/tables${suffix}`,{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error(`Audit schema HTTP ${r.status}: ${(await r.text()).slice(0,1000)}`);return r.json();}
 let schema=await call();const logs=schema.tables.find(t=>t.id===RUNS&&t.name==='run-logs');if(!logs||!logs.fields.some(f=>f.name==='run-id'&&f.type==='singleLineText'))throw Error('Run log schema mismatch');
 // Validate every existing target before any mutation. Never change existing types.
 for(const [name,fields]of [['sources',sourceFields],['err-logs',errorFields]]){const old=schema.tables.find(t=>t.name===name);if(old){for(const f of fields)if(!old.fields.some(x=>x.name===f.name&&x.type===f.type))throw Error('Audit schema conflict '+name+'.'+f.name);if(old.primaryFieldId!==old.fields.find(f=>f.name===fields[0].name).id)throw Error('Audit primary mismatch');}}
 for(const [name,fields]of [['sources',sourceFields],['err-logs',errorFields]])if(!schema.tables.some(t=>t.name===name)){await call('','POST',{name,fields});schema=await call();}
 const sources=schema.tables.find(t=>t.name==='sources'),errors=schema.tables.find(t=>t.name==='err-logs');
 const links=[{from:RUNS,name:'sources',to:sources.id,inverse:'run-logs'},{from:errors.id,name:'run',to:RUNS,inverse:'err-logs'},{from:errors.id,name:'source',to:sources.id,inverse:'err-logs'}];
 for(const l of links){const table=schema.tables.find(t=>t.id===l.from),old=table.fields.find(f=>f.name===l.name);if(old&&(old.type!=='multipleRecordLinks'||old.options?.linkedTableId!==l.to))throw Error('Audit link conflict '+l.name);if(!old){const target=schema.tables.find(t=>t.id===l.to);if(target.fields.some(f=>f.name===l.inverse))throw Error('Audit inverse conflict '+l.inverse);await call(`/${l.from}/fields`,'POST',{name:l.name,type:'multipleRecordLinks',options:{linkedTableId:l.to}});schema=await call();}
  const link=schema.tables.find(t=>t.id===l.from).fields.find(f=>f.name===l.name),target=schema.tables.find(t=>t.id===l.to),inverse=target.fields.find(f=>f.id===link.options?.inverseLinkFieldId);
  if(!inverse||inverse.type!=='multipleRecordLinks'||inverse.options.linkedTableId!==l.from)throw Error('Audit inverse missing');
  if(inverse.name!==l.inverse){if(target.fields.some(f=>f.name===l.inverse))throw Error('Audit inverse name conflict');await call(`/${l.to}/fields/${inverse.id}`,'PATCH',{name:l.inverse});schema=await call();}
 }
 for(const l of links){const link=schema.tables.find(t=>t.id===l.from).fields.find(f=>f.name===l.name),inverse=schema.tables.find(t=>t.id===l.to).fields.find(f=>f.name===l.inverse);if(link.options?.linkedTableId!==l.to||inverse?.id!==link.options.inverseLinkFieldId||inverse.options?.inverseLinkFieldId!==link.id)throw Error('Audit reciprocal readback failed');}
 return schema;
}
module.exports={sourceFields,errorFields,configureAudit};
