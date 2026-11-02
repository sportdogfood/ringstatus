'use strict';
// Read-only saved runner evidence and exact Airtable GET readback. No provider/collector calls.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const root=path.resolve(__dirname,'../..'),runtime=path.join(root,'runtime');
const label=process.argv[2];if(!/^[a-z0-9-]+$/.test(label||''))throw Error('Unique evidence label required');
const file=path.join(__dirname,label+'.json');if(fs.existsSync(file))throw Error('Preserve previous evidence');
const load=n=>JSON.parse(fs.readFileSync(path.join(runtime,n),'utf8'));
(async()=>{
 const deployed=JSON.parse(fs.readFileSync(path.join(__dirname,'deployed.json'))),check=load('live-check-last-run.json'),cleanup=load('cleanup-last-run.json');
 if(Date.parse(check.started)<=Date.parse(deployed.deployed_at))throw Error('No postdeployment normal due check yet');
 const report={observed_at:new Date().toISOString(),check,cleanup,heartbeat:[],run_logs:[],errors:[],source:'Saved scheduled receipt + Airtable GET only',manual_run:false};
 const token=process.env.WEF_LIVE_AIRTABLE_TOKEN;if(!token)throw Error('Missing existing read-only credential');
 for(const [table,key,out]of [['heartbeat','run-id','heartbeat'],['run-logs','run-id','run_logs'],['errs-log','run-id','errors']]){
  const q=new URLSearchParams({filterByFormula:`{${key}}="${check.run_id}"`});
  const r=await fetch(`https://api.airtable.com/v0/appgzosUcVqA4vZdf/${table}?${q}`,{headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(30000)});
  if(!r.ok)throw Error('Readback '+table+' HTTP '+r.status);report[out]=(await r.json()).records;
 }
 report.installed_hashes=deployed.hashes.map(({file,sha256})=>({file,match:crypto.createHash('sha256').update(fs.readFileSync(path.join(root,file))).digest('hex')===sha256}));
 report.checks={installed:report.installed_hashes.every(x=>x.match),no_stage_or_log_errors:!check.stage_errors.length&&!check.logging_errors.length,heartbeat:report.heartbeat.length===1&&report.heartbeat[0].fields.result==='Skipped'&&Boolean(report.heartbeat[0].fields.completed),run_log:report.run_logs.length===1&&report.run_logs[0].fields.result==='Skipped'&&Boolean(report.run_logs[0].fields.completed),no_cleanup_action:cleanup.result==='Skipped'&&cleanup.actions.length===0&&!cleanup.new_show&&!cleanup.new_day,no_cleanup_stop:!fs.existsSync(path.join(runtime,'cleanup-stop.json'))};
 report.result=Object.values(report.checks).every(Boolean)?'PASS':'FAIL';
 fs.writeFileSync(file,JSON.stringify(report,null,2));console.log(JSON.stringify({file,result:report.result,run_id:check.run_id,started:check.started,reason:check.reason,next_check_at:check.next_check_at,checks:report.checks,errors:report.errors.map(r=>r.fields.message)}));
 if(report.result!=='PASS')process.exitCode=1;
})().catch(e=>{console.error(e.message);process.exitCode=1;});
