"use strict";
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const runtime=path.join(__dirname,'runtime');fs.mkdirSync(runtime,{recursive:true});
const lock=path.join(runtime,'runner.lock'),runId=crypto.randomUUID(),started=new Date().toISOString();
let handle;
function receipt(report){const file=path.join(runtime,'last-run.json'),text=JSON.stringify(report,null,2);fs.writeFileSync(file+'.tmp',text);fs.renameSync(file+'.tmp',file);console.log(JSON.stringify(report));}
(async()=>{
  try{handle=fs.openSync(lock,'wx');fs.writeFileSync(handle,JSON.stringify({pid:process.pid,run_id:runId,started}));}
  catch(e){receipt({run_id:runId,started,ok:false,errors:['Existing runner lock: inspect before resume']});process.exitCode=1;return;}
  const hardLimit=setTimeout(()=>{receipt({run_id:runId,started,ok:false,errors:['50-second deadline; outcome may be unknown. Reconcile Engine before resume.']});process.exit(1);},50000);
  try{const report=await require('./runner').run({deadline:Date.now()+49000,runId});receipt(report);process.exitCode=report.ok?0:1;}
  catch(e){receipt({run_id:runId,started,ok:false,errors:[e.message]});process.exitCode=1;}
  finally{clearTimeout(hardLimit);fs.closeSync(handle);fs.unlinkSync(lock);}
})();
