'use strict';
// Compile the logging entry against inert adapters in a temporary runtime.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os'),{spawnSync}=require('node:child_process');
const source=path.resolve(__dirname,'..');
for(const mode of ['success','lock','startup','dated-failure'])test('actual logging entry offline '+mode,t=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'attempt-entry-'));t.after(()=>fs.rmSync(root,{recursive:true,force:true}));fs.mkdirSync(path.join(root,'runtime'));fs.writeFileSync(path.join(root,'runtime','collector.env'),'WEF_SCHEDULES_AIRTABLE_TOKEN=offline-fixture\n');
 const script=`
 const fs=require('fs'),path=require('path'),Module=require('module'),req=Module.createRequire(${JSON.stringify(path.join(source,'attempt-entry.js'))});
 const db={'run-logs':[],'error-log':[]};let next=0,collections=0;
 const api={airtableList:async n=>structuredClone(db[n]),airtablePatchRecords:async(n,rows)=>{for(const r of rows)Object.assign(db[n].find(x=>x.id===r.id).fields,r.fields);return{failedRows:[]};}};
 global.fetch=async(u,o)=>{if(o.method!=='POST')throw Error('Unexpected offline request');const n=decodeURIComponent(new URL(u).pathname.split('/').at(-1));if(!db[n])throw Error('Forbidden business network');for(const r of JSON.parse(o.body).records)db[n].push({id:'rec'+(++next),fields:r.fields});return{ok:true};};
 const file=${JSON.stringify(path.join(root,'attempt-entry.js'))},m=new Module(file);m.filename=file;m.paths=Module._nodeModulePaths(path.dirname(file));
 const realLogger=req('./run-logs');
 m.require=id=>{
 if(id==='./settings')return{AIRTABLE_TOKEN:'offline-fixture'};
 if(id==='./attempt-api')return{attemptAPI:()=>api};
 if(id==='./run-logs')return{runLogs:opts=>realLogger.runLogs({...opts,api})};
 if(id==='./heartbeat'){if(${JSON.stringify(mode)}==='startup')throw Error('fixture startup import');return{heartbeat:()=>null};}
 if(id==='./show-index')return{showIndex:()=>null};
 if(id==='./show-rings')return{showRings:()=>null};
 if(id==='./store')return{runStagedCollector:async({runLog})=>{collections++;if(${JSON.stringify(mode)}==='lock')throw Error('Schedule collector already running');const r=await runLog.begin(),report={ok:${JSON.stringify(mode)}!=='dated-failure',days:[{ok:${JSON.stringify(mode)}!=='dated-failure',show_id:76,date:'2026-10-29',row_count:2,error:'fixture dated failure'}]};await runLog.finish(r,report);return report;}};
 return req(id);};
 m._compile(fs.readFileSync(${JSON.stringify(path.join(source,'attempt-entry.js'))},'utf8'),file);
 m.exports.main().then(r=>console.log(JSON.stringify({db,collections,ok:r.ok}))).catch(e=>console.log(JSON.stringify({db,collections,error:e.message})));
 `;
 const child=spawnSync(process.execPath,['-e',script],{encoding:'utf8',timeout:10000,env:{...process.env,WEF_SCHEDULES_ATTEMPT_ID:'fixture-current',WEF_SCHEDULES_ATTEMPT_STARTED:'2026-10-29T16:00:00.1234567Z'}});assert.equal(child.status,0,child.stderr);
 const result=JSON.parse(child.stdout.trim());assert.equal(result.db['run-logs'].length,1);const fields=result.db['run-logs'][0].fields;assert.equal(fields['run-id'],'fixture-current');assert.equal(fields.started,'2026-10-29T16:00:00.123Z');
 if(mode==='success'){assert.equal(fields.result,'Success');assert.equal(result.db['error-log'].length,0);assert.equal(result.collections,1);}
 else{assert.equal(fields.result,'Failed');assert(result.db['error-log'].length>=1);if(mode==='startup')assert.equal(result.collections,0);}
});
