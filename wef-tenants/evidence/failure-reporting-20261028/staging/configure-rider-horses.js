"use strict";
// Schema configuration only. Never reads/writes business records or runs collection.
async function configure({request=globalThis.fetch,settings=require('./settings')}={}){
 const route='https://api.airtable.com/v0/meta/bases/'+settings.BASE_ID+'/tables';
 async function api(url,method='GET',body){const r=await request(url,{method,headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error('Rider horse schema HTTP '+r.status);return r.json();}
 let schema=await api(route);const riders=schema.tables.find(t=>t.name==='riders'),horses=schema.tables.find(t=>t.name==='horses');if(!riders||!horses)throw Error('Existing rider and horse tables required');
 let field=riders.fields.find(f=>f.name==='horses');
 if(field&&(field.type!=='multipleRecordLinks'||field.options?.linkedTableId!==horses.id))throw Error('Conflicting rider horses field');
 if(!field){
  if(horses.fields.some(f=>f.name==='riders'||f.type==='multipleRecordLinks'&&f.options?.linkedTableId===riders.id)||riders.fields.some(f=>f.type==='multipleRecordLinks'&&f.options?.linkedTableId===horses.id))throw Error('Existing horse-side or rider-side relationship; inspect without creating a duplicate');
  await api(route+'/'+riders.id+'/fields','POST',{name:'horses',type:'multipleRecordLinks',options:{linkedTableId:horses.id},description:'Horses observed in collected entries for this rider. Retained across shows; not ownership. Existing associations are preserved.'});
 }
 schema=await api(route);const savedRiders=schema.tables.find(t=>t.id===riders.id),savedHorses=schema.tables.find(t=>t.id===horses.id);field=savedRiders.fields.find(f=>f.name==='horses');const inverse=savedHorses.fields.find(f=>f.id===field?.options?.inverseLinkFieldId);
 if(field?.type!=='multipleRecordLinks'||field.options.linkedTableId!==horses.id||inverse?.name!=='riders'||inverse.type!=='multipleRecordLinks'||inverse.options?.linkedTableId!==riders.id||inverse.options.inverseLinkFieldId!==field.id)throw Error('Rider horse reciprocal schema readback failed');
 return{base:settings.BASE_ID,riderTable:riders.id,riderField:field.id,horseTable:horses.id,horseField:inverse.id,businessRecordsWritten:0};
}
if(require.main===module)configure().then(result=>console.log(JSON.stringify(result))).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={configure};
