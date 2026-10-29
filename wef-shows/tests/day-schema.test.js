const test=require('node:test'),assert=require('node:assert/strict');
const {inspect,configureDays}=require('../day-schema');
const schema={tables:[{id:'tblJA5CUon5s1sWib',name:'shows',fields:Object.entries({'show-key':'singleLineText',show_id:'number',customer_id:'number',selected_date:'date',start_date:'date',end_date:'date',available_dates:'multilineText'}).map(([name,type])=>({name,type}))}]};
test('schema rejects changed canonical field types before writes',async()=>{const bad=structuredClone(schema);bad.tables[0].fields[0].type='number';assert.throws(()=>inspect(bad),/field mismatch/);});
test('schema creates only agreed days and reciprocal, repeat makes no writes',async()=>{
 const saved=structuredClone(schema),writes=[];
 const request=async(url,options)=>{if(options.method==='POST'){writes.push(url);const body=JSON.parse(options.body);assert.equal(body.name,'show-days');assert.deepEqual(body.fields.find(f=>f.name==='show').options,{linkedTableId:'tblJA5CUon5s1sWib'});const table={id:'tblNewDay',name:body.name,fields:body.fields.map((f,i)=>({...f,id:'fld'+i})),primaryFieldId:'fld0'};table.fields[1].options.inverseLinkFieldId='fldReverse';saved.tables.push(table);saved.tables[0].fields.push({id:'fldReverse',name:'show-days',type:'multipleRecordLinks',options:{linkedTableId:table.id}});}else assert.equal(options.method,'GET');return{ok:true,json:async()=>structuredClone(saved)};};
 await configureDays({token:'fixture-only',request});assert.equal(writes.length,1);await configureDays({token:'fixture-only',request});assert.equal(writes.length,1);assert.equal(saved.tables[0].fields.length,8);
});
