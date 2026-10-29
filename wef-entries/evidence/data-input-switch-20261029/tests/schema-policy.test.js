"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {configure,fields}=require('../staged/configure-fields');
const {evaluate}=require('../staged/freshness');
test('schema adapter adds only five approved Entries fields and repeat does not recreate',async()=>{
 const table={id:'tblqkWmrsYMs2hyQ6',name:'entries',fields:[]},calls=[];
 const request=async(url,options)=>{assert(url.startsWith('https://api.airtable.com/v0/meta/bases/appSxF4pYMBaxpTZd/tables'));calls.push(options.method);
  if(options.method==='POST'){assert(url.endsWith('/tblqkWmrsYMs2hyQ6/fields'));const f=JSON.parse(options.body);assert(fields.some(([n,t])=>n===f.name&&t===f.type));table.fields.push({id:'field'+table.fields.length,...f});}
  return{ok:true,json:async()=>({tables:[structuredClone(table)]})};
 };
 await configure({token:'offline',request});await configure({token:'offline',request});assert.equal(table.fields.length,5);assert.equal(calls.filter(x=>x==='POST').length,5);
});
test('schema type conflict stops before any mutation',async()=>{
 let writes=0;await assert.rejects(configure({token:'offline',request:async(url,options)=>{if(options.method!=='GET')writes++;return{ok:true,json:async()=>({tables:[{id:'tblqkWmrsYMs2hyQ6',name:'entries',fields:[{name:'source-trainer-ids',type:'number'}]}]})};}}),/type mismatch/);assert.equal(writes,0);
});
test('producer polling deadlines never become a reader lease or authorize retirement',()=>{
 const item={scopeKey:'scope',observedAt:'2026-01-01T00:00:00Z',producerWork:{'next-due':'2099-01-01T00:00:00Z'}};
 const result=evaluate(item,{attempts:[]},'2026-10-29T00:00:00Z');assert.equal(result.current,false);assert.equal(result.canRetire,false);assert.equal(result.accepted,true);assert.equal(item.observedAt,'2026-01-01T00:00:00Z');
});
test('failed applicable scope is distinguished from known healthy other scopes',()=>{
 const input={attempts:[{reservedClass:{'class-keys':['failed-scope']},status:'Failed'}]};
 assert.equal(evaluate({scopeKey:'healthy-scope'},input).affectedAttempts.length,0);assert.equal(evaluate({scopeKey:'failed-scope'},input).affectedAttempts.length,1);
});
