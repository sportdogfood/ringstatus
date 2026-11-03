'use strict';
// Saved scheduled evidence and Airtable GET only; never invokes collector/provider paths.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const label=process.argv[2];if(!/^[a-z0-9-]+$/.test(label||''))throw Error('Unique label required');
const output=path.join(__dirname,label+'.json');if(fs.existsSync(output))throw Error('Preserve existing evidence');
const root=path.resolve(__dirname,'../..'),runtime=path.join(root,'runtime');
(async()=>{
 const deployed=JSON.parse(fs.readFileSync(path.join(__dirname,'deployed.json'))),cleanup=JSON.parse(fs.readFileSync(path.join(runtime,'cleanup-last-run.json'))),state=JSON.parse(fs.readFileSync(path.join(runtime,'cleanup-state.json')));
 if(Date.parse(cleanup.started)<=Date.parse(deployed.deployed_at))throw Error('New cleanup scheduled evidence not yet available');
 const q=new URLSearchParams({filterByFormula:`{run-id}="${cleanup.run_id}"`});
 const records={};for(const table of ['cleanup-logs','errs-log']){
  const r=await fetch(`https://api.airtable.com/v0/appgzosUcVqA4vZdf/${table}?${q}`,{headers:{Authorization:'Bearer '+process.env.WEF_LIVE_AIRTABLE_TOKEN},signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error(table+' HTTP '+r.status);records[table]=(await r.json()).records;
 }
 const logFile=path.join(runtime,'collector-2026-11-03.log');const log=fs.readFileSync(logFile,'utf8').replace(/\u0000/g,'').split(/\r?\n/).filter(x=>x.startsWith('START ')||x.startsWith('END ')||x.includes('next-check-not-due')).slice(-12);
 const checks={installed_hashes_match:deployed.hashes.every(x=>crypto.createHash('sha256').update(fs.readFileSync(path.join(root,x.file))).digest('hex')===x.sha256),cleanup_completed:cleanup.result==='Success',no_stop:!fs.existsSync(path.join(runtime,'cleanup-stop.json')),readback_actions:records['cleanup-logs'].length===cleanup.actions.length&&records['cleanup-logs'].every(x=>x.fields.result==='Skipped'||x.fields.result==='Success'),no_new_archives:records['cleanup-logs'].every(x=>!x.fields['archive-file']&&x.fields['records-archived']===0),no_cleanup_errors:records['errs-log'].length===0};
 const report={at:new Date().toISOString(),cleanup,state,records,log,checks,result:Object.values(checks).every(Boolean)?'PASS':'FAIL'};fs.writeFileSync(output,JSON.stringify(report,null,2));console.log(JSON.stringify({output,result:report.result,run_id:cleanup.run_id,started:cleanup.started,actions:cleanup.actions.map(x=>({table:x.table,before:x.before,deleted:x.deleted,result:x.result})),checks,log}));if(report.result==='FAIL')process.exitCode=1;
})().catch(e=>{console.error(e.message);process.exitCode=1});
