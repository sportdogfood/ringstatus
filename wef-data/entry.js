"use strict";
const fs=require('node:fs'),path=require('node:path'),{spawn}=require('node:child_process');
const {acquireLock,atomicReport}=require('../wef-schedules/collect');
async function scheduled(){
 const runtime=path.join(__dirname,'runtime');fs.mkdirSync(runtime,{recursive:true});
 const attempt=process.env.WEF_LOG_ATTEMPT_ID;
 if(attempt){
  const logging=require('./attempt-log'),root=path.join(runtime,'logging/wef-data');
  const config=JSON.parse(fs.readFileSync(path.join(__dirname,'logging-schema.json'),'utf8')).workflows['wef-data'];
  const row=logging.readAttempt(root,attempt);row['child-pid']=process.pid;logging.atomic(path.join(root,'run-logs',attempt+'.json'),row);
  logging.recoverInterrupted(root,attempt);
  let result;
  try{
   if(fs.existsSync(path.join(runtime,'stopped.json'))){const stop=JSON.parse(fs.readFileSync(path.join(runtime,'stopped.json'),'utf8'));result={status:'Skipped',stage:'StopGuard',reason:'Data stopped; no collection attempted',error:{category:'ExistingStop',operation:'Collect',target:'wef-data',message:'Attempt blocked by retained stop: '+(stop.error||stop.state||'Owner authorization required'),'write-outcome':'NotSent'}};}
   else{
    await collect(runtime);
    const report=JSON.parse(fs.readFileSync(path.join(runtime,'last-run.json'),'utf8'));
    result={status:report.ok?'Succeeded':report.state==='Deferred'?'Deferred':'Failed',stage:report.ok?'Verified':'Collection','run-key':report['run-key'],'show-key':report['show-key'],counts:report.ok?{observations:report.observations}:undefined,error:report.ok?undefined:{category:'Collection',operation:report.operation,target:report.target,message:report.error||'Collection failed','write-outcome':report['write-outcome']||'Unknown'}};
    if(report['identity-conflicts']?.length){
     logging.recordError(root,attempt,{category:'IdentityConflict',operation:'Reconcile',target:'Entry directory',message:'Held conflicting entries: '+report['identity-conflicts'].join(', ')+'; exact prior/source evidence retained in the directory observation.','write-outcome':'NotSent'},'DirectoryReconciliation');
     result.reason='Verified unaffected facts; entry identity reconciliation remains partial';
     if(result.counts)result.counts['identity-conflicts']=report['identity-conflicts'].length;
    }
    if(report['assignment-counts']){const a=report['assignment-counts'];result.counts={...(result.counts||{}),'complete-trip-assignments':a['complete-trips'],'incomplete-trip-assignments':a['incomplete-trips'],'withheld-entry-assignments':a['withheld-entries']};if(a['withheld-entries'])result.reason=[result.reason,'Ambiguous or unsupported parent assignments withheld; verified trip roles retained.'].filter(Boolean).join('; ');}
    if(report.retirement)result.counts={...(result.counts||{}),'retired-records':report.retirement.reduce((n,r)=>n+r['removed-this-wake'],0)};
    if(report['handoff-error'])result.error={category:'Handoff',operation:'Publish',target:'Verified retained cache',message:report['handoff-error'],'write-outcome':'NotApplicable'};
   }
  }catch(error){result={status:'Failed',stage:'Process',error:{category:'Process',message:error.message,'write-outcome':'Unknown'}};process.exitCode=1;}
  if(['Failed','Skipped','Deferred'].includes(result.status)){
   for(const name of ['active.json','pending.json'])if(fs.existsSync(path.join(runtime,name))){const saved=JSON.parse(fs.readFileSync(path.join(runtime,name),'utf8'));result['run-key']=saved['run-key'];if(saved.scope)result.stage=saved.scope;}
  }
  logging.finish(root,attempt,result);
  const delivered=await logging.deliver(root,config);if(!delivered.ok){logging.recordError(root,attempt,{category:'Logging',message:delivered.error,'write-outcome':'Unknown'},'LogDelivery');console.log('Logging retained locally; Airtable delivery pending.');}
  return;
 }
 return collect(runtime);
}
async function collect(runtime){
 if(fs.existsSync(path.join(runtime,'stopped.json'))){console.log('Data stopped; owner authorization required. No collection attempted.');return;}
 const release=acquireLock(path.join(runtime,'collector.lock'));
 try{
  const python=path.join(__dirname,'.venv','Scripts','python.exe');
  const env={...process.env,PYTHONDONTWRITEBYTECODE:'1',GIT_TERMINAL_PROMPT:'0',GCM_INTERACTIVE:'Never'};
  const result=await new Promise((resolve,reject)=>{
   const child=spawn(python,['-B',path.join(__dirname,'scheduled.py')],{cwd:__dirname,env,windowsHide:true,stdio:['ignore','pipe','pipe']});
   let output='',error='';child.stdout.on('data',v=>{output+=v;if(output.length>100000)child.kill();});
   child.stderr.on('data',v=>{error+=v;if(error.length>100000)child.kill();});
   child.on('error',reject);child.on('close',code=>resolve({code,output,error}));
  });
  let report;
  try{report=JSON.parse(result.output.trim());}catch{report={ok:false,state:'Stopped',error:'Scheduled Python process did not return a valid report',exit:result.code};}
  if(result.code!==0||(!report.ok&&report.state!=='Deferred')){
   // This stop is deliberately durable: later wakes cannot retry a failed workflow.
   if(!fs.existsSync(path.join(runtime,'stopped.json')))await atomicReport(path.join(runtime,'stopped.json'),report);
   process.exitCode=1;
  }
  await atomicReport(path.join(runtime,'last-run.json'),report);console.log(JSON.stringify(report));
 }catch(error){
  const report={ok:false,state:'Stopped',error:error.message,at:new Date().toISOString()};
  await atomicReport(path.join(runtime,'stopped.json'),report);await atomicReport(path.join(runtime,'last-run.json'),report);process.exitCode=1;
 }finally{release();}
}
if(require.main===module)scheduled().catch(error=>{console.error(error.message);process.exitCode=1;});
module.exports={scheduled};
