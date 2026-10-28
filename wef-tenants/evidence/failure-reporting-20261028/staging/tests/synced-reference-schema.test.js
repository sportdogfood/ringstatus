"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {configure,links}=require('../configure-synced-references');
function fixture(){
 const schema={tables:['entries','collections','rings-helper','wef-schedules.shows','wef-schedules.rings','wef-schedules.schedules'].map((name,i)=>({id:'table'+i,name,fields:[]}))},calls=[];let serial=0;
 const request=async(url,options)=>{calls.push(options.method);if(options.method==='POST'){const table=schema.tables.find(t=>url.includes('/'+t.id+'/fields')),body=JSON.parse(options.body),target=schema.tables.find(t=>t.id===body.options.linkedTableId),id='field'+(++serial),inverse='inverse'+serial;const field={id,...body,options:{...body.options,inverseLinkFieldId:inverse}};table.fields.push(field);target.fields.push({id:inverse,name:table.name,type:'multipleRecordLinks',options:{linkedTableId:table.id,inverseLinkFieldId:id}});return{ok:true,json:async()=>structuredClone(field)};}return{ok:true,json:async()=>structuredClone(schema)};};
 return{schema,calls,options:{request,pause:async()=>{},settings:{BASE_ID:'testBase',AIRTABLE_TOKEN:'testToken',SYNC_SHOWS_TABLE:'table3',SYNC_RINGS_TABLE:'table4',SYNC_SCHEDULES_TABLE:'table5'}}};
}
test('schema adds only CWF reference links and reuses them without modifying records',async()=>{const f=fixture();const result=await configure(f.options);assert.equal(result.links.length,5);assert.equal(result.businessRecordsWritten,0);await configure(f.options);assert.equal(f.calls.filter(m=>m==='POST').length,links.length);assert.equal(f.calls.filter(m=>m==='PATCH'||m==='DELETE').length,0);});
test('conflicting native reference field is rejected before any schema change',async()=>{const f=fixture();f.schema.tables[0].fields.push({id:'wrong',name:'shows',type:'singleLineText'});await assert.rejects(configure(f.options),/Conflicting/);assert.equal(f.calls.filter(m=>m==='POST').length,0);});

test('recreated synced table with another ID is rejected before schema writes',async()=>{const f=fixture();f.schema.tables[3].id='replacementTable';await assert.rejects(configure(f.options),/identity/);assert.equal(f.calls.filter(m=>m==='POST').length,0);});
