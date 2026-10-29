"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {reader,bridge}=require('../staged/data-input');
const root=path.resolve(__dirname,'../../../..');
const fixturePath='C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/wef-data-day-count-fixture-20261029.json';
function fixture(){const f=JSON.parse(fs.readFileSync(fixturePath));const c=f.context;const show={fields:{'show-key':c['show-key'],customer_id:c['customer-id'],show_id:c['show-id'],start_date:c['start-date'],end_date:c['end-date'],selected_date:c['selected-date']}};return{...f,show};}
test('existing inert bridge projects frozen class evidence and producer policy without source calls',async()=>{
 const f=fixture();const result=await bridge(f,{root,script:path.resolve(__dirname,'../staged/retained-input.py')});
 assert(result.classes.length);assert.equal(result.head,f.head);assert.equal(result.wholeDayComplete,false);assert(result.classes[0].sourceEvidence.collections.length);assert(result.classes[0].producerWork);
});
test('reader chooses completed head over newer Writing and rechecks exact head',async()=>{
 const f=fixture(),temp=fs.mkdtempSync(path.join(os.tmpdir(),'entries-retained-test-'));let heads=0;
 const accepted=Object.values(f.runs).map(fields=>({id:'run-'+fields['run-key'],fields}));
 const latest={...JSON.parse(f.runs[f.head]['state-json']),phase:'Writing',sequence:999};
 const api={list:async(table,base,filter)=>{assert.equal(base,'appdl16HJ5ETm8N69');if(table==='runs'){if(filter.startsWith('{run-key}=')){heads++;return accepted.filter(r=>r.fields['run-key']===f.head);}return [...accepted,{id:'failed',fields:{'run-key':'newer-writing',status:'Failed', 'state-json':JSON.stringify(latest)}}];}assert.equal(table,'collections');return Object.values(f.collections).map(v=>({id:v['_record-id'],fields:v}));}};
 try{const result=await reader(api,{runtime:temp,replay:input=>bridge(input,{root,script:path.resolve(__dirname,'../staged/retained-input.py')})})(f.show);assert.equal(result.head,f.head);assert.equal(heads,1);assert.equal(result.attempts[0].key,'newer-writing');assert(result.classes.length);}finally{fs.rmSync(temp,{recursive:true,force:true});}
});
test('corrupted retained payload fails rather than becoming empty success',async()=>{
 const f=fixture();Object.values(f.collections)[0]['retained-json']+=' ';
 await assert.rejects(bridge(f,{root,script:path.resolve(__dirname,'../staged/retained-input.py')}),/validation failed/);
});
