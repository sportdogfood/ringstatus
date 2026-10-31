'use strict';
const {prepare}=require('./worker');
const {writeAnswers}=require('../wef-timing/data-runner');
const {BASE,T}=require('./transport');
const {POLICY}=require('./calculator');
const {preflight,controls}=require('./operations');
// Shared runner must persist the supplied state before invoking this stage, and
// pass its already verified Data/Live/ring/class rows. No scheduler lives here.
async function stage({api,state,save,markStage=()=>{},runId,now,clock=()=>now,beforePublish=async()=>{},workingDay=require('../wef-timing/purge').localDay(now),...inputs}){
 if(typeof save!=='function'||!state||!runId)throw Error('Entry checkpoint contract missing');
 if(state.pending_write||state.entry_failed)throw Error('Entry stage requires owner review');
 const op=controls({api,state,runId,started:now,clock,context:{...inputs.data.context,'selected-date':workingDay}});
 try{
  markStage('entry_evaluation');state.stage='entry_evaluation';
  const batch=await prepare({...inputs,runId,now});
  state.entry_counts=batch.counts;state.entry_issues=batch.issues;state.stage='entry_publication';markStage(state.stage);
  let saved=[];
  if(batch.writes.length){
   try{await beforePublish();}catch(error){error.entryPrewriteTransient=true;throw error;}
   state.pending_write={base:BASE,table:T['timing-answers'],run_id:runId,policy:POLICY,
    answers:batch.writes.map(f=>({key:f['answer-key'],hash:f['content-hash']}))};
   await save('entry_writing');await op.checkpoint('writing');
   saved=await writeAnswers(api,batch.writes,BASE,T['timing-answers']);
   state.entry_counts.entries_written=saved.length;
   delete state.pending_write;
  }
  markStage('entry_logging');state.stage='entry_logging';
  const result=batch.issues.length||batch.outputs.some(o=>o.fields.availability!=='available')?'partial':batch.writes.length?'complete':'noop';
  await op.finish(result);await op.checkpoint(result);await save(batch.writes.length?'entry_written':'entry_unchanged');
  return{...batch,saved,status:result};
 }catch(error){
  if(error.entryPrewriteTransient===true&&!state.pending_write&&!state.entry_failed){
   try{state.prewrite_failure={run_id:runId,message:error.message,classification:'proven_prewrite_transient'};await op.finish('deferred',error.message);await op.checkpoint('deferred',error.message);await save('deferred',error.message);error.entryDeferred=true;}catch(reportError){state.entry_failed={run_id:runId,message:'Prewrite reporting uncertainty: '+reportError.message};await save('failed',reportError.message);}
   throw error;
  }
  state.entry_failed={run_id:runId,message:error.message};
  // Failure reporting does not retry business writes. Keep the original error
  // and intent even when the reporting service itself is unavailable.
  try{await save('failed',error.message);}catch(reportError){error.sharedReportingError=reportError.message;}
  try{await op.checkpoint('failed',error.message);await op.finish('failed',error.message);}catch(reportError){error.entryReportingError=reportError.message;}
  throw error;
 }
}
module.exports={stage,preflight};
