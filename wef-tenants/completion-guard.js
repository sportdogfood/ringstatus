"use strict";
const fs=require('node:fs');

// A pending Promise alone does not keep Node alive. Success is allowed only
// after the existing collector, final run-log and heartbeat have all returned.
function runWithCompletion(work) {
 let finished=false,notified=false;
 process.exitCode=1;
 function incomplete(){
  if(finished)return;
  process.exitCode=1;
  if(!notified){
   notified=true;
   fs.writeSync(2,`${new Date().toISOString()} WEF_CWF incomplete_run: collector exited before final run-log and heartbeat completion were confirmed.\n`);
  }
 }
 process.once('beforeExit',incomplete);
 process.once('exit',incomplete);
 return Promise.resolve().then(work).then(report=>{
  if(typeof report?.ok!=='boolean')throw Error('Collector terminal report missing explicit ok result');
  console.log(JSON.stringify(report,null,2));
  console.log(`WEF_CWF_RUN_FINISHED result=${report.ok?'Success':'Failed'}`);
  finished=true;
  process.exitCode=report.ok?0:1;
  process.removeListener('beforeExit',incomplete);
  process.removeListener('exit',incomplete);
  return report;
 }).catch(error=>{
  process.exitCode=1;
  console.error('WEF_CWF_RUN_FAILED: '+String(error?.message||error));
  finished=true;
  process.removeListener('beforeExit',incomplete);
  process.removeListener('exit',incomplete);
 });
}
module.exports={runWithCompletion};
