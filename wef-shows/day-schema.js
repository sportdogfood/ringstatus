"use strict";
const BASE='appCaSmL819KK7UFJ',SHOWS='tblJA5CUon5s1sWib';
const date={dateFormat:{name:'iso'}},time={...date,timeFormat:{name:'24hour'},timeZone:'utc'};
const fields=[{name:'show-day-key',type:'singleLineText'},
 {name:'show',type:'multipleRecordLinks',options:{linkedTableId:SHOWS}},
 {name:'date',type:'date',options:date},
 ...['unique-entry-count','trip-count'].map(name=>({name,type:'number',options:{precision:0}})),
 ...['count-coverage','count-rules-version'].map(name=>({name,type:'singleLineText'})),
 ...['count-observed-at','count-verified-at','latest-attempt-at'].map(name=>({name,type:'dateTime',options:time})),
 ...['data-evidence-ref','accepted-summary','latest-attempt-reason'].map(name=>({name,type:'multilineText'})),
 {name:'publication-status',type:'singleSelect',options:{choices:['Unknown','Verified','Stale'].map(name=>({name}))}},
 {name:'latest-attempt-status',type:'singleLineText'}];
function inspect(schema){
 const shows=schema.tables.find(t=>t.id===SHOWS&&t.name==='shows');if(!shows)throw Error('Canonical shows table mismatch');
 for(const [name,type]of Object.entries({'show-key':'singleLineText',show_id:'number',customer_id:'number',selected_date:'date',start_date:'date',end_date:'date',available_dates:'multilineText'}))if(!shows.fields.some(f=>f.name===name&&f.type===type))throw Error('Canonical field mismatch '+name);
 const day=schema.tables.find(t=>t.name==='show-days');
 if(day){
  for(const spec of fields){const actual=day.fields.find(f=>f.name===spec.name);if(!actual||actual.type!==spec.type)throw Error('Day schema mismatch '+spec.name);}
  const link=day.fields.find(f=>f.name==='show');if(link.options?.linkedTableId!==SHOWS)throw Error('Day link mismatch');
  if(day.primaryFieldId!==day.fields.find(f=>f.name==='show-day-key').id)throw Error('Day primary mismatch');
  if(!['Unknown','Verified','Stale'].every(n=>day.fields.find(f=>f.name==='publication-status').options.choices.some(c=>c.name===n)))throw Error('Day status choices mismatch');
 }
 return{shows,day};
}
async function configureDays({token,request=fetch}){
 async function call(suffix='',method='GET',body){const response=await request(`https://api.airtable.com/v0/meta/bases/${BASE}/tables${suffix}`,{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});if(!response.ok)throw Error('Day schema HTTP '+response.status);return response.json();}
 let schema=await call(),{day}=inspect(schema);
 if(!day){await call('','POST',{name:'show-days',fields});schema=await call();day=inspect(schema).day;if(!day)throw Error('Day schema creation not visible');}
 let shows=schema.tables.find(t=>t.id===SHOWS),link=day.fields.find(f=>f.name==='show'),reciprocal=shows.fields.find(f=>f.id===link.options.inverseLinkFieldId);
 if(!reciprocal||reciprocal.type!=='multipleRecordLinks'||reciprocal.options.linkedTableId!==day.id)throw Error('Reciprocal link not verified');
 if(reciprocal.name!=='show-days'){
  if(shows.fields.some(f=>f.name==='show-days'))throw Error('Reciprocal name conflict');
  await call(`/${SHOWS}/fields/${reciprocal.id}`,'PATCH',{name:'show-days'});
 }
 schema=await call();({day,shows}=inspect(schema));reciprocal=shows.fields.find(f=>f.name==='show-days');if(reciprocal?.id!==day.fields.find(f=>f.name==='show').options.inverseLinkFieldId)throw Error('Reciprocal readback mismatch');
 return schema;
}
module.exports={fields,inspect,configureDays};
