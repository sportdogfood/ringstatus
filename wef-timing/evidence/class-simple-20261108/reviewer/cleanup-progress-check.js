'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os');
const S=require('../../../class-snapshot'),C=require('../../../class-cleanup');
(async()=>{const dir=fs.mkdtempSync(path.join(os.tmpdir(),'class-review-progress-')),now='2026-11-08T19:28:00Z',store={};let failSummary=true,deletes=0;try{
 store[S.T.shows]=[{id:'rec00000000000001',fields:{is_default:true,show_id:200000077,customer_id:15}}];store[S.T.cleanup]=[{id:'rec00000000000002',fields:{'\uFEFFName':'run-logs',cleanup:'checked'}}];
 store[S.T.runs]=Array.from({length:2000},(_,i)=>({id:'rec'+String(i+100).padStart(14,'0'),createdTime:new Date(Date.parse(now)-86400000+i).toISOString(),fields:{'run-id':'done'+i,result:'Success',completed:'2026-11-07T10:00:00Z','show-id':200000077}}));
 const api={list:async(t,f)=>structuredClone((store[t]||[]).filter(r=>!f||!f.startsWith('OR(')||f.includes(r.id))),create:async()=>{throw Error('Unexpected create');},update:async(t,id,fields)=>{if(failSummary){failSummary=false;throw Error('offline summary write failed');}Object.assign(store[t].find(r=>r.id===id).fields,fields);},remove:async(t,ids)=>{deletes+=ids.length;store[t]=store[t].filter(r=>!ids.includes(r.id));}};
 let firstError;try{await C.run({api,now,runtime:dir});}catch(e){firstError=e.message;}
 const afterFirst=deletes;
 const second=await C.run({api,now:'2026-11-08T19:31:00Z',runtime:dir});
 const result={first_error:firstError,deleted_first:afterFirst,deleted_after_next_cycle:deletes,second};fs.writeFileSync(path.join(__dirname,'cleanup-progress-result.json'),JSON.stringify(result,null,2));console.log(JSON.stringify(result));assert.equal(deletes,500,'A failed summary must not allow another500 within the same three-hour event');
 }finally{fs.rmSync(dir,{recursive:true,force:true});}})().catch(e=>{console.error(e);process.exitCode=1;});
