const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {report}=require('./candidate/wef-live/error-log');
test('948 repeats become three errors; unchanged runs write none; changed error writes one',async t=>{
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'live-errors-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));const calls=[];
 const api={call:async(base,table,method,body)=>{assert.equal(table,'errs-log');calls.push(body);return{records:body.records};}};
 const rows=Array.from({length:948},(_,i)=>({'error-key':'r1|'+i,'run-id':'r1','show-id':77,date:'2026-11-08',message:'Missing '+['horse','rider','trainer'][i%3],'err-trip':i}));
 assert.deepEqual(await report({api,runtime,lane:'linker',rows}),{logged:3,suppressed:945});assert.equal(calls[0].records.length,3);
 assert.deepEqual(await report({api,runtime,lane:'linker',rows:rows.map(r=>({...r,'run-id':'r2'}))}),{logged:0,suppressed:948});assert.equal(calls.length,1);
 assert.equal((await report({api,runtime,lane:'linker',rows:[...rows,{message:'New failure','show-id':77,date:'2026-11-08'}]})).logged,1);
 await report({api,runtime,lane:'linker',rows:[]});assert.equal((await report({api,runtime,lane:'linker',rows:[rows[0]]})).logged,1);
 assert.equal((await report({api,runtime,lane:'rings',rows:[rows[0]]})).logged,1);
 assert.equal((await report({api,runtime,lane:'linker',rows:[{...rows[0],'show-id':78}]})).logged,1);
});
test('failed response is recorded by caller; uncertain attempted batch is not sent again',async t=>{
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'live-errors-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));let n=0;
 const api={call:async(b,t,m,body)=>{if(++n===2)throw Error('offline failure');return{records:body.records};}};
 const rows=Array.from({length:12},(_,i)=>({message:'Different '+i}));
 await assert.rejects(report({api,runtime,lane:'classes',rows}),/offline failure/);
 const remaining=[];api.call=async(b,t,m,body)=>{remaining.push(...body.records);return{records:body.records};};
 assert.equal((await report({api,runtime,lane:'classes',rows})).logged,0);assert.equal(remaining.length,0);
});
