"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {airtable}=require('../airtable'),{classEntries}=require('../source');
test('lost create response is read back without a second create',async()=>{
 const rows=[];let creates=0;
 const api=airtable({pause:async()=>{},request:async(url,options)=>{
  if(options.method==='POST'){creates++;for(const r of JSON.parse(options.body).records)rows.push({id:'rec1',fields:r.fields});throw Error('Response lost');}
  return {ok:true,json:async()=>({records:structuredClone(rows)})};
 }});
 assert.equal((await api.upsert('entries','entry-key',[{'entry-key':'key',status:'active'}]))[0].id,'rec1');
 await api.upsert('entries','entry-key',[{'entry-key':'key',status:'active'}]);assert.equal(creates,1);
});
test('unresolved creation is not retried in the same run',async()=>{
 let creates=0;const api=airtable({pause:async()=>{},request:async(url,o)=>{if(o.method==='POST'){creates++;throw Error('Lost');}return{ok:true,json:async()=>({records:[]})};}});
 await assert.rejects(api.upsert('entries','entry-key',[{'entry-key':'key'}]),/unresolved/);
 await assert.rejects(api.upsert('entries','entry-key',[{'entry-key':'key'}]),/unresolved/);assert.equal(creates,1);
});
test('source class identity and explicit empty arrays are validated',()=>{
 assert.deepEqual(classEntries({show_id:75,class_id:123,entries:[]},123,75),[]);
 assert.throws(()=>classEntries({show_id:76,entries:[]},123,75),/show mismatch/);
 assert.throws(()=>classEntries({class_id:124,entries:[]},123,75),/class mismatch/);
 assert.throws(()=>classEntries({},123,75),/explicit entries array/);
});
