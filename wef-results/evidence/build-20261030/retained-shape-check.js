"use strict";
const assert=require('node:assert/strict'),{evaluate,project}=require('../../model');
const fixtures=require('./retained-shape-fixtures.json');
const results=[];
for(const row of fixtures.records){
 const [customer,show]=row.fields['class-key'].split('|').map(Number),now=new Date(Date.parse(row.fields['last-success'])+60000).toISOString();
 const e=evaluate(row,{customer,show,now,maxSourceAgeMs:600000});assert.equal(e.status,'ready',row.fields['class-key']+': '+e.reason);
 const a=project(e,undefined,'offline-1',now),b=project(e,a,'offline-2',now);assert.equal(b['revision-number'],1);assert.equal(a['content-hash'],b['content-hash']);
 results.push({class_key:e.key,result_count:e.results.length,source_verified:e.verified,finality:e.finality,repeat_revision:b['revision-number']});
}
assert.equal(results.length,3);console.log(JSON.stringify({scope:'Sanitized retained Airtable source-shape replay only; no API or workflow run',results},null,2));
