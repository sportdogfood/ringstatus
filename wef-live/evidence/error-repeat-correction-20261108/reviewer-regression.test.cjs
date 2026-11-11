'use strict';
globalThis.fetch=async()=>{throw Error('Offline network forbidden');};
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {report}=require('./candidate/wef-live/error-log');
const {cleanup}=require('./candidate/wef-live/cleanup');
test('review: successful cleanup clears old condition so its later recurrence logs again',async t=>{
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'review-cleanup-error-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));
 let failing=false,logged=0;
 const api={list:async table=>{if(table==='shows'){if(failing)throw Error('same failure');return[{id:'recShow',fields:{customer_id:15,is_default:true,show_id:77}}];}return[];},call:async(base,table,method,body)=>{assert.equal(table,'errs-log');logged+=body.records.length;return{records:body.records};}};
 await report({api,runtime,lane:'cleanup',rows:[{'show-id':null,message:'Cleanup: same failure'}]});assert.equal(logged,1);
 fs.writeFileSync(path.join(runtime,'cleanup-state.json'),JSON.stringify({show:77,daily_clear_day:'2026-11-07'}));
 const options={api,runtime,clock:()=> '2026-11-08T15:00:00Z'};const success=await cleanup(options);assert.equal(success.result,'Skipped');
 failing=true;const failed=await cleanup(options);assert.equal(failed.result,'Error');assert.equal(logged,2,'a later recurrence must be recorded after successful cleanup');
});
test('review: response lost after accepted error POST must not duplicate unchanged error next run',async t=>{
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'review-error-uncertain-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));const saved=[];let first=true;
 const api={call:async(base,table,method,body)=>{saved.push(...body.records);if(first){first=false;throw Error('Response lost after accepted POST');}return{records:body.records};}};
 const row={'error-key':'run1|0','run-id':'run1','show-id':77,date:'2026-11-08',message:'same condition'};
 await assert.rejects(report({api,runtime,lane:'rings',rows:[row]}),/Response lost/);
 await report({api,runtime,lane:'rings',rows:[{...row,'error-key':'run2|0','run-id':'run2'}]});
 assert.equal(saved.length,1,'uncertain prior POST must be reconciled before another create');
});
