"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {configure}=require('../configure-rider-horses');
function fixture(){
 const schema={tables:[{id:'ridersTable',name:'riders',fields:[]},{id:'horsesTable',name:'horses',fields:[]}]},calls=[];
 const request=async(url,options)=>{calls.push({url,method:options.method});if(options.method==='POST'){const f=JSON.parse(options.body);assert.equal(f.name,'horses');assert.equal(f.options.linkedTableId,'horsesTable');schema.tables[0].fields.push({id:'horseLink',...f,options:{...f.options,inverseLinkFieldId:'riderLink'}});schema.tables[1].fields.push({id:'riderLink',name:'riders',type:'multipleRecordLinks',options:{linkedTableId:'ridersTable',inverseLinkFieldId:'horseLink'}});}return{ok:true,json:async()=>structuredClone(schema)};};
 return{schema,calls,options:{request,settings:{BASE_ID:'testBase',AIRTABLE_TOKEN:'testToken'}}};
}
test('schema configuration creates only the requested relation and reuses it on repeat',async()=>{const f=fixture();const result=await configure(f.options);assert.equal(result.riderField,'horseLink');assert.equal(result.horseField,'riderLink');await configure(f.options);assert.equal(f.calls.filter(c=>c.method==='POST').length,1);assert.ok(f.calls.every(c=>c.url.includes('/meta/bases/testBase/tables')));});
test('existing conflicting field prevents a second relationship from being created',async()=>{const f=fixture();f.schema.tables[0].fields.push({id:'wrong',name:'horses',type:'singleLineText'});await assert.rejects(configure(f.options),/Conflicting/);assert.equal(f.calls.filter(c=>c.method==='POST').length,0);});
test('an existing horse-side rider field prevents duplicate relationship creation',async()=>{const f=fixture();f.schema.tables[1].fields.push({id:'existing',name:'riders',type:'multipleRecordLinks'});await assert.rejects(configure(f.options),/Existing horse-side/);assert.equal(f.calls.filter(c=>c.method==='POST').length,0);});
