"use strict";
// Only a separately approved scheduler may enable this entrypoint.
async function main(env=process.env){
  if(env.WEF_RESULTS_RUNNER_ENABLED!=='1')throw Error('Results task activation is not enabled');
  const positive=name=>{const n=Number(env[name]);if(!Number.isFinite(n)||n<=0)throw Error('Missing explicit '+name);return n;};
  const maxSourceAgeMs=positive('WEF_RESULTS_MAX_SOURCE_AGE_MINUTES')*60000,maxAuthorityAgeMs=positive('WEF_RESULTS_MAX_AUTHORITY_AGE_MINUTES')*60000;
  const api=require('./airtable').airtable({token:env.WEF_RESULTS_AIRTABLE_TOKEN});
  const fs=require('node:fs'),path=require('node:path'),os=require('node:os');
  // Transient overlap lock only. No durable business state outside Airtable.
  const lock=path.join(os.tmpdir(),'wef-results-appK9IX59jFwUkB2u.lock');
  let fd;try{fd=fs.openSync(lock,'wx');fs.writeFileSync(fd,String(process.pid));}catch{throw Error('Results overlap lock exists or unavailable; inspect before resume');}
  try{return await require('./runner').run({api,maxSourceAgeMs,maxAuthorityAgeMs});}
  finally{fs.closeSync(fd);fs.unlinkSync(lock);}
}
if(require.main===module){main().then(report=>{console.log(JSON.stringify(report));process.exitCode=report.ok?0:1;}).catch(error=>{console.error(error.message);process.exitCode=1;});}
module.exports={main};
