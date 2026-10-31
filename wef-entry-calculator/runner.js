'use strict';
const fs=require('node:fs'),path=require('node:path'),{execFile}=require('node:child_process');
const {api:upstream,TIMING,SHOWS,DATA_TABLES,CLASS_TIMING,CLASS_ANSWERS,SYNC_GROUPS,SYNC_RINGS,syncToken}=require('../wef-timing/api');
const {selectShow,pointer}=require('../wef-timing/data-runner');
const {readLiveClasses}=require('../wef-timing/class-live-source');
const {hash}=require('../wef-timing/pace');
const {localDay}=require('../wef-timing/purge');
const {api:entry,BASE,T}=require('./transport');
const {stage,preflight}=require('./stage');
const {controls}=require('./operations');
const transient=error=>error.entryPrewriteTransient===true||error.deferred===true||/HTTP (429|5\d\d)|budget exhausted|day\/source\/budget changed|source context changed|show authority changed|sync source changed|pointer changed during read/.test(error.message);
const ROOT=path.resolve(__dirname,'..');
function readData(context,deadline,workingDay){return new Promise((resolve,reject)=>{
 const remaining=deadline-Date.now()-18000;if(remaining<2000)return reject(Error('Entry source budget exhausted'));
 const child=execFile(path.join(ROOT,'wef-data/.venv/Scripts/python.exe'),['-B',path.join(__dirname,'data-input.py')],
  {windowsHide:true,timeout:remaining,maxBuffer:8*1024*1024,env:{SystemRoot:process.env.SystemRoot,WINDIR:process.env.WINDIR,TEMP:process.env.TEMP,TMP:process.env.TMP,PYTHONDONTWRITEBYTECODE:'1'}},
  (error,out,err)=>{if(error)return reject(Error(error.killed?'Entry source budget exhausted':String(err||error.message).trim()));try{resolve(JSON.parse(out));}catch{reject(Error('Entry projection unreadable'));}});
 child.stdin.end(JSON.stringify({context,working_day:workingDay}));
});}
// These facades expose no upstream write methods, even though the shared API supports them.
function dependencies(deadline){
 const token=process.env.WEF_TIMING_AIRTABLE_TOKEN;
 const ring=upstream({token,deadline,lane:'data_live'}),classes=upstream({token,deadline,lane:'class_live'});
 return{source:{list:(...args)=>ring.list(...args)},classSource:{list:(...args)=>classes.list(...args)},destination:entry({token,deadline})};
}
async function run({deadline=Date.now()+160000,runId,clock=()=>new Date().toISOString(),state,save,
 source,classSource,destination,project=readData,readPointer=pointer,readLive=readLiveClasses}={}){
 if(!runId||!state||typeof save!=='function')throw Error('Entry local journal missing');
 if(!source||!classSource||!destination)({source,classSource,destination}=dependencies(deadline));
 const started=clock();let context,checked=false,stageStarted=false;const workingDay=localDay(started);
 try{
  await preflight({api:destination,state});checked=true;
  delete state.prewrite_failure;
  const selected=selectShow(await source.list(SHOWS,'shows'),clock());context=selected.context;
  if(workingDay<context['start-date']||workingDay>context['end-date'])throw Error('Entry TODAY outside show dates');
  const pinned=readPointer(context),data=await project(context,deadline,workingDay);
  if(hash(readPointer(context))!==hash(pinned)||hash(data.context)!==hash(context))throw Error('Entry source context changed');
  const syncFilter=`AND({show-id}=${context['show-id']},{customer-id}=${context['customer-id']},{date}='${workingDay}')`;
  const groups=await source.list(TIMING,SYNC_GROUPS,syncFilter),rings=await source.list(TIMING,SYNC_RINGS,syncFilter);
  for(const inventory of Object.values(data.inventories))inventory.synced_live={groups,rings};
  const live=await readLive({api:source,customer:context['customer-id'],show:context['show-id'],now:clock()});
  const paceRows=await source.list(TIMING,DATA_TABLES.answers),classRows=await classSource.list(CLASS_TIMING,CLASS_ANSWERS);
  const answers=await destination.list(BASE,T['timing-answers']),keys=new Set();
  for(const r of answers){const key=r.fields['answer-key'];if(!key||keys.has(key))throw Error('Duplicate/missing Entry snapshot');keys.add(key);}
  async function fence(){
   if(Date.now()>deadline-15000||localDay(clock())!==workingDay||hash(readPointer(context))!==hash(pinned))throw Error('Entry day/source/budget changed');
   if(syncToken(groups)!==syncToken(await source.list(TIMING,SYNC_GROUPS,syncFilter))||syncToken(rings)!==syncToken(await source.list(TIMING,SYNC_RINGS,syncFilter)))throw Error('Entry sync source changed');
   const fresh=selectShow(await source.list(SHOWS,'shows'),clock());
   if(fresh.record.id!==selected.record.id||hash(fresh.context)!==hash(context))throw Error('Entry show authority changed');
  }
  await fence();
  const api={list:(...args)=>destination.list(...args),upsert:async(table,key,rows)=>{
   if(table===T['timing-answers'])await fence();return destination.upsert(table,key,rows);
  }};
  stageStarted=true;
  const result=await stage({api,state,save,runId,now:clock(),clock,data,liveClasses:live.classes,classRows,paceRows,answers,workingDay,beforePublish:fence});
  return{run_id:runId,started,completed:clock(),ok:true,status:result.status,counts:result.counts,issues:result.issues,
   working_day:workingDay,source_pointer:pinned,entry_clock_verified:destination.clockVerified===true,retention:'disabled'};
 }catch(error){
  const deferred=checked&&!state.pending_write&&!state.entry_failed&&(error.entryDeferred===true||(!stageStarted&&transient(error)));
  if(deferred){
   state.prewrite_failure={run_id:runId,message:error.message,classification:'proven_prewrite_transient'};
   state.entry_counts={};state.entry_issues=[];
   if(!error.entryDeferred){try{const op=controls({api:destination,state,runId,started,clock,context:{...(context||{}),'selected-date':workingDay}});await op.finish('deferred',error.message);await op.checkpoint('deferred',error.message);}catch(reportError){state.entry_failed={run_id:runId,message:'Prewrite reporting uncertainty: '+reportError.message};error.reporting=reportError.message;}}
  }else state.entry_failed=state.entry_failed||{run_id:runId,message:error.message};
  await save(state.entry_failed?'failed':'deferred',error.message);
  if(!deferred&&checked&&context&&!stageStarted){try{const op=controls({api:destination,state,runId,started,clock,context:{...context,'selected-date':workingDay}});await op.checkpoint('failed',error.message);await op.finish('failed',error.message);}catch(reportError){error.reporting=reportError.message;}}
  return{run_id:runId,started,completed:clock(),ok:false,status:state.entry_failed?'failed':'deferred',errors:[error.message,...(error.reporting?[error.reporting]:[])],counts:state.entry_counts||{},retention:'disabled'};

 }
}
module.exports={run,readData,dependencies};
