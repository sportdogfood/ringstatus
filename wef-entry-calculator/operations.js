'use strict';
const {verifyRows}=require('../wef-timing/runner');
const {BASE,T}=require('./transport');
const {POLICY}=require('./calculator');
const KEY='entry-data-runner';
async function write(api,table,key,fields){
 const result=await api.upsert(T[table],key,[fields]);
 if(result.records?.length!==1||!/^rec[A-Za-z0-9]{14}$/.test(result.records[0]?.id||''))throw Error('Entry control write identity mismatch');
 verifyRows(result.records,[fields],key);
 const rows=await api.list(BASE,T[table],`RECORD_ID()='${result.records[0].id}'`);
 if(rows.length!==1||rows[0].id!==result.records[0].id)throw Error('Entry control readback coverage mismatch');
 verifyRows(rows,[fields],key);return rows[0];
}
async function preflight({api,state}){
 if(!state||state.pending_write||state.entry_failed)throw Error('Entry stage requires owner review');
 const rows=await api.list(BASE,T['engine-checkpoints'],`{checkpoint-key}='${KEY}'`);
 if(rows.length>1)throw Error('Duplicate entry checkpoint');
 if(rows.length){
  const f=rows[0].fields;if(f['checkpoint-key']!==KEY||f['policy-version']!==POLICY)throw Error('Entry checkpoint contract mismatch');
  const saved=JSON.parse(f['state-json']);
  if(saved.version!==1||saved.pending_write||saved.entry_failed||f['last-outcome']==='failed')throw Error('Entry checkpoint requires owner review');
 }
 return{checkpoint:rows[0]??null};
}
function controls({api,state,runId,started,clock,context}){
 const counts=()=>state.entry_counts||{};
 async function checkpoint(outcome,error=''){
  const body=JSON.stringify({version:1,pending_write:state.pending_write??null,entry_failed:state.entry_failed??null,
   prewrite_failure:state.prewrite_failure??null,counts:counts(),issues:state.entry_issues||[],stage:state.stage});
  if(body.length>90000)throw Error('Entry checkpoint requires bounded evidence references');
  return write(api,'engine-checkpoints','checkpoint-key',{'checkpoint-key':KEY,'run-id':runId,'started-at':started,
   'completed-at':clock(),'last-outcome':outcome,'duration-ms':Math.max(0,Date.parse(clock())-Date.parse(started)),
   'counts-json':JSON.stringify(counts()),'state-json':body,'error-summary':error,'policy-version':POLICY});
 }
 async function finish(result,error=''){
  const completed=clock(),summary=JSON.stringify({prewrite_failure:state.prewrite_failure??null,counts:counts(),issues:state.entry_issues||[],retention:'archive_required'});
  const common={'run-id':runId,started,completed,result,'show-id':context['show-id'],date:context['selected-date'],summary,errors:JSON.stringify(error?[error]:[])};
  await write(api,'run-logs','run-id',{...common,'entry-count':counts().entries_evaluated||0,'archive-count':0,archives:'[]'});
  await write(api,'heartbeat','heartbeat-key',{...common,'heartbeat-key':KEY,'checked-at':completed});
  if(error)await write(api,'errs-log','error-key',{'error-key':runId+'|entry','run-id':runId,'occurred-at':completed,
   'show-id':context['show-id'],date:context['selected-date'],message:error});
 }
 return{checkpoint,finish};
}
module.exports={preflight,controls};
