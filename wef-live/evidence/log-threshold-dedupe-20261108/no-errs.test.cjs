"use strict";
globalThis.fetch=async()=>{throw Error('Offline network forbidden');};
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const runtime=t=>{const p=fs.mkdtempSync(path.join(os.tmpdir(),'no-errs-'));t.after(()=>fs.rmSync(p,{recursive:true,force:true}));return p;};
function api(){const calls=[];return{calls,list:async table=>table==='shows'?[{id:'show',fields:{customer_id:15,show_id:77,is_default:true}}]:table==='show-days'?[{fields:{'show-day-key':'15|77|2026-11-08','day-priority':0,date:'2026-11-08'}}]:[],call:async(base,table,method,body)=>{assert.notEqual(table,'errs-log');calls.push({table,method,body});return{records:(body?.records||[]).map((r,i)=>({id:r.id||'rec'+i,fields:r.fields}))};}};}
test('adapter rejects allerrs-logcreation/update butallowsread fornormalcleanup',async()=>{
 let requests=0;const a=require('./candidate/wef-live/airtable').airtable({token:'offline',pause:async()=>{},request:async()=>{requests++;return{ok:true,json:async()=>({records:[]})};}});for(const method of ['POST','PATCH','DELETE'])await assert.rejects(a.call('appgzosUcVqA4vZdf','errs-log',method,{records:[]}),/disabled by owner/);assert.equal(requests,0);await a.list('errs-log');assert.equal(requests,1);
});
test('failedstatuscheckstilllogsrunandheartbeat,errorreceiptand3minretrywithouterrs-log',async t=>{
 const a=api();const r=await require('./candidate/wef-live/live-cadence').check({api:a,runtime:runtime(t),clock:()=> '2026-11-08T14:00:00Z',getStatus:async()=>{throw Error('HTTP403');}});assert.equal(r.reason,'live-status-unavailable');assert.equal(r.minutes,3);assert.equal(r.issues.length,1);assert(a.calls.some(c=>c.table==='run-logs'&&c.body.records[0].fields.errors.includes('HTTP403')));assert(!a.calls.some(c=>c.table==='errs-log'));
});
test('failedcollectionkeepscollectionerror/runlogs/receiptswithoutcreatingerrs-log',async t=>{
 const a=api(),dir=runtime(t);const r=await require('./candidate/wef-live/lanes').lane('rings',{api:a,runtime:dir,showContext:{show:77,customer:15,date:'2026-11-08'},clock:()=> '2026-11-08T14:00:00Z',getStatus:async()=>true,get:async()=>{throw Error('Source HTTP403');}});assert.equal(r.ok,false);assert(r.stage_errors.some(e=>e.message==='Source HTTP403'));assert(a.calls.some(c=>c.table==='run-logs'&&c.body.records.some(r=>r.fields.errors?.includes('Source HTTP403'))));assert(!a.calls.some(c=>c.table==='errs-log'));assert(fs.existsSync(path.join(dir,'lane-rings-last-run.json')));
});
test('active modules containnoerrs-logwriterandnonewerrorutility',()=>{
 for(const name of ['linker.js','lanes.js','live-cadence.js','cleanup.js']){const s=fs.readFileSync(path.join(__dirname,'candidate/wef-live',name),'utf8');assert(!/log\('errs-log'/.test(s));assert(!/upsert\('errs-log'/.test(s));}assert.equal(fs.existsSync(path.join(__dirname,'candidate/wef-live/error-log.js')),false);
});
