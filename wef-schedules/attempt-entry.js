'use strict';
// Scheduled logging boundary. Business collection remains in the existing store.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
async function main({envFile=path.join(__dirname,'runtime/collector.env')}={}){
 const runId=process.env.WEF_SCHEDULES_ATTEMPT_ID||crypto.randomUUID(),started=process.env.WEF_SCHEDULES_ATTEMPT_STARTED||new Date().toISOString();
 try{
  process.loadEnvFile(envFile);
  const settings=require('./settings');if(!settings.AIRTABLE_TOKEN)throw Error('WEF_SCHEDULES_AIRTABLE_TOKEN required');
  const {runLogs}=require('./run-logs'),{errorLogs}=require('./error-logs'),{attemptAPI}=require('./attempt-api'),{runAttempt,recoverAttempts,readReceipts,appendReceipt,archiveReceipts}=require('./attempts');
  const receiptRoot=path.join(__dirname,'runtime/attempt-receipts');fs.mkdirSync(receiptRoot,{recursive:true});
  const receiptFile=path.join(receiptRoot,runId+'.jsonl');
  const serialize=require('./logging-lock').loggingLock(path.join(__dirname,'runtime/attempt-logging.lock'));
  const api=attemptAPI(),logger=runLogs({api,serialize}),errors=errorLogs({api,serialize,onEvent:e=>appendReceipt(receiptFile,e)});
  const context={runId,started};
  return await runAttempt({context,logger,errors,recover:async record=>{
   const files=fs.readdirSync(receiptRoot).filter(n=>/^[A-Za-z0-9_-]{1,80}\.jsonl$/.test(n)).sort().map(n=>path.join(receiptRoot,n));
   const receipts=readReceipts(files).filter(r=>r.runId!==runId).sort((a,b)=>a.started.localeCompare(b.started));
   await recoverAttempts({receipts,logger,errors,serialize,acknowledge:(receipt,hash)=>appendReceipt(receipt.sourceFile,{version:1,state:'ack',runId:receipt.runId,started:receipt.started,fingerprint:hash})});
   await archiveReceipts({files,historyRoot:path.join(__dirname,'runtime/history/attempt-receipts'),serialize,onArchive:e=>logger.archive(record,e)});
  },run:async record=>{
   // All product imports happen after attempt creation, including lock checks.
   const operational=runLogs({serialize,heartbeat:require('./heartbeat').heartbeat(),catalog:require('./show-index').showIndex()});
   const scopedErrors=errorLogs({api,serialize,onEvent:e=>appendReceipt(receiptFile,e),onArchive:event=>logger.archive(record,event)});
   const log={begin:async()=>record,archive:(r,e)=>operational.archive(r,e),finish:async(r,report,error)=>{
    r.attemptReport=report;
    await scopedErrors.report(r,report);
    if(error)await scopedErrors.record(r,{stage:'collection',message:error});
    return operational.finish(r,report,error);
   }};
   const result=await require('./store').runStagedCollector({runLog:log,references:require('./show-rings').showRings(),entryCollector:require('./entries-collector').collectEntries});
   return result;
  }});
 }catch(error){
  // A local receipt survives missing environment/imports/connection. Only later
  // normal invocations publish new-attempt evidence; no source work is repeated.
  const {safeMessage}=require('./error-logs');
  const secrets=Object.entries(process.env).filter(([k])=>/WEF_SCHEDULES_.*(TOKEN|COOKIE|AUTHORIZATION|BEARER)/.test(k)).map(([,v])=>v);
  const message=safeMessage(error,secrets);
  console.error('WEF_SCHEDULES_ATTEMPT_FAILURE '+Buffer.from(JSON.stringify({runId,message})).toString('base64'));
  throw Error(message);
 }
}
if(require.main===module)require('./completion-guard').runWithCompletion(()=>main());
module.exports={main};
