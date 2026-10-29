"use strict";
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),zlib=require('node:zlib');
function fingerprint(r){
 const events=[...new Map((r.errors||[]).map(e=>[JSON.stringify([e.stage,e.date||null,e.showId??null,e.message]),e])).keys()].sort();
 return crypto.createHash('sha256').update(JSON.stringify([r.runId,r.started,r.pid??null,r.completed||null,r.exitCode??null,r.error||null,events])).digest('hex');
}
function appendReceipt(file,event){fs.appendFileSync(file,JSON.stringify(event)+'\n',{encoding:'utf8'});}
function readReceipts(files){
 const all=new Map(),acks=new Map();
 for(const file of files){
  if(!fs.existsSync(file))continue;
  const text=fs.readFileSync(file).toString('utf8').replace(/\0/g,'');
  const events=[];
  for(const raw of text.split(/\r?\n/)){
   const line=raw.replace(/^\uFEFF/,'');
   try{if(line.startsWith('{'))events.push(JSON.parse(line));else if(line.startsWith('WEF_SCHEDULES_ATTEMPT '))events.push(JSON.parse(Buffer.from(line.slice(22),'base64').toString('utf8')));}catch{}
  }
  for(const e of events){
   if(e.version!==1||!['started','finished','error','ack'].includes(e.state)||!/^[A-Za-z0-9_-]{1,80}$/.test(e.runId||'')||!Number.isFinite(Date.parse(e.started)))continue;
   const event={...e,started:new Date(e.started).toISOString()};
   if(event.state==='ack'){acks.set(event.runId,event.fingerprint);continue;}
   if(event.state==='finished'&&(!Number.isFinite(Date.parse(event.completed))||Date.parse(event.completed)<Date.parse(event.started)||!Number.isInteger(event.exitCode)))continue;
   const old=all.get(event.runId);if(old&&old.started!==event.started)throw Error('Conflicting wrapper attempt identity');
   if(event.state==='error'){
    const record=old||{runId:event.runId,started:event.started,sourceFile:file};record.errors=[...(record.errors||[]),event];all.set(event.runId,record);
   }else all.set(event.runId,{...old,...event,errors:old?.errors||[],sourceFile:file});
  }
 }
 return [...all.values()].map(r=>({...r,fingerprint:fingerprint(r),ack:acks.get(r.runId)}));
}
async function recoverAttempts({receipts,logger,errors,acknowledge=()=>{},serialize=work=>work()}){return serialize(async()=>{
 const known=new Map((logger.records?await logger.records():[]).map(r=>[r.fields['run-id'],r]));let handled=0;
 for(const receipt of receipts){
  const hash=receipt.fingerprint||fingerprint(receipt);if(receipt.ack===hash)continue;
  if(!receipt.completed&&Number.isInteger(receipt.pid)){try{process.kill(receipt.pid,0);continue;}catch(error){if(error.code!=='ESRCH')continue;}}
  if(handled++>=10)break;
  const prior=known.get(receipt.runId);
  if(prior&&new Date(prior.fields.started).toISOString()!==new Date(receipt.started).toISOString())throw Error('Receipt scope mismatch');
  const record=prior||await logger.begin({runId:receipt.runId,started:receipt.started});
  // Errors replay independently of the business run's terminal state.
  for(const event of receipt.errors||[])await errors.record(record,{...event,recovered:true});
  if(!receipt.completed){
   await errors.record(record,{stage:'wrapper',message:'Wrapper completion receipt missing; outcome unknown. No collection was rerun.',recovered:true});
  }else{
   const message=receipt.error||`Collector terminal Airtable completion was not confirmed; wrapper exit ${receipt.exitCode??'unknown'}.`;
   if(record.fields.result==='Running'){
    await errors.record(record,{stage:'wrapper',message,recovered:true});
    await logger.finish(record,{ok:false,days:[]},Error(message),{completedAt:receipt.completed});
   }else if((receipt.exitCode!==0||record.fields.errors)&&!(receipt.errors||[]).length){
    await errors.record(record,{stage:'wrapper',message:record.fields.errors||message,recovered:true});
   }
  }
  await acknowledge(receipt,hash);
 }
});}
async function runAttempt({context,logger,errors,run,recover}){
 let record;
 try{record=await logger.begin(context);if(record.fields.result!=='Running')return{ok:['Success','Warning'].includes(record.fields.result),attempt_reused:true,days:[]};if(recover)await recover(record);return await run(record);}
 catch(error){
  const stage=/already running|collector lock/i.test(error.message)?'lock':'startup-or-finalization';
  try{await errors.record(record,{runId:context.runId,started:context.started,stage,message:error});}catch(logError){console.error('WEF_SCHEDULES_ERROR_LOG_FAILED: '+logError.message);}
  if(record){try{const latest=await logger.begin(context);if(latest.fields.result!=='Failed')await logger.finish(record,record.attemptReport||{ok:false,days:[]},error);}catch(logError){console.error('WEF_SCHEDULES_ATTEMPT_LOG_FAILED: '+logError.message);}}
  throw error;
 }
}
async function archiveReceipts({files,historyRoot,onArchive,serialize=work=>work(),now=Date.now()}){return serialize(async()=>{
 for(const receipt of readReceipts(files)){
  if(!receipt.completed||receipt.ack!==receipt.fingerprint||now-Date.parse(receipt.completed)<86400000)continue;
  if(Number.isInteger(receipt.pid)){try{process.kill(receipt.pid,0);continue;}catch(error){if(error.code!=='ESRCH')continue;}}
  const body=fs.readFileSync(receipt.sourceFile),hash=crypto.createHash('sha256').update(body).digest('hex');
  fs.mkdirSync(historyRoot,{recursive:true});const file=path.join(historyRoot,receipt.runId+'-'+hash+'.jsonl.gz');let outcome='Reused';
  if(fs.existsSync(file)){if(!zlib.gunzipSync(fs.readFileSync(file)).equals(body))throw Error('Attempt receipt archive collision');}
  else{fs.writeFileSync(file+'.tmp',zlib.gzipSync(body));if(!zlib.gunzipSync(fs.readFileSync(file+'.tmp')).equals(body))throw Error('Attempt receipt archive readback failed');fs.renameSync(file+'.tmp',file);outcome='Created';}
  const event={kind:'attempt-receipts',outcome,file,records:1,shows:[],dates:[],sha256:hash};
  if(onArchive)await onArchive(event);
  if(!fs.readFileSync(receipt.sourceFile).equals(body))throw Error('Attempt receipt changed during archival');
  fs.unlinkSync(receipt.sourceFile);if(onArchive)await onArchive({...event,cleanup:'Complete'});
 }
});}
module.exports={readReceipts,recoverAttempts,runAttempt,appendReceipt,fingerprint,archiveReceipts};
