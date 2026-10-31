'use strict';
const fs=require('node:fs'),path=require('node:path'),{execFile}=require('node:child_process');
const {api:upstream,TIMING,SHOWS,DATA_TABLES,CLASS_TIMING,CLASS_ANSWERS}=require('../wef-timing/api');
const {selectShow,pointer}=require('../wef-timing/data-runner');
const {readLiveClasses}=require('../wef-timing/class-live-source');
const {hash}=require('../wef-timing/pace');
const {localDay}=require('../wef-timing/purge');
const {api:entry,BASE,T}=require('./transport');
const {stage,preflight}=require('./stage');
const {controls}=require('./operations');
const ROOT=path.resolve(__dirname,'..');
function readData(context,deadline){return new Promise((resolve,reject)=>{
 const remaining=deadline-Date.now()-18000;if(remaining<2000)return reject(Error('Entry source budget exhausted'));
 const child=execFile(path.join(ROOT,'wef-data/.venv/Scripts/python.exe'),['-B',path.join(__dirname,'data-input.py')],
  {windowsHide:true,timeout:remaining,maxBuffer:8*1024*1024,env:{SystemRoot:process.env.SystemRoot,WINDIR:process.env.WINDIR,TEMP:process.env.TEMP,TMP:process.env.TMP,PYTHONDONTWRITEBYTECODE:'1'}},
  (error,out,err)=>{if(error)return reject(Error(error.killed?'Entry source budget exhausted':String(err||error.message).trim()));try{resolve(JSON.parse(out));}catch{reject(Error('Entry projection unreadable'));}});
 child.stdin.end(JSON.stringify(context));
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
 const started=clock();let context,checked=false,stageStarted=false;
 try{
  await preflight({api:destination,state});checked=true;
  const selected=selectShow(await source.list(SHOWS,'shows'),clock());context=selected.context;
  if(context['selected-date']!==localDay(clock()))throw Error('Entry selected day is not today');
  const pinned=readPointer(context),data=await project(context,deadline);
  if(hash(readPointer(context))!==hash(pinned)||hash(data.context)!==hash(context))throw Error('Entry source context changed');
  const live=await readLive({api:source,customer:context['customer-id'],show:context['show-id'],now:clock()});
  const paceRows=await source.list(TIMING,DATA_TABLES.answers),classRows=await classSource.list(CLASS_TIMING,CLASS_ANSWERS);
  const answers=await destination.list(BASE,T['timing-answers']),keys=new Set();
  for(const r of answers){const key=r.fields['answer-key'];if(!key||keys.has(key))throw Error('Duplicate/missing Entry snapshot');keys.add(key);}
  async function fence(){
   if(Date.now()>deadline-15000||localDay(clock())!==context['selected-date']||hash(readPointer(context))!==hash(pinned))throw Error('Entry day/source/budget changed');
   const fresh=selectShow(await source.list(SHOWS,'shows'),clock());
   if(fresh.record.id!==selected.record.id||hash(fresh.context)!==hash(context))throw Error('Entry show authority changed');
  }
  await fence();
  const api={list:(...args)=>destination.list(...args),upsert:async(table,key,rows)=>{
   if(table===T['timing-answers'])await fence();return destination.upsert(table,key,rows);
  }};
  stageStarted=true;
  const result=await stage({api,state,save,runId,now:clock(),clock,data,liveClasses:live.classes,classRows,paceRows,answers});
  return{run_id:runId,started,completed:clock(),ok:true,status:result.status,counts:result.counts,issues:result.issues,
   source_pointer:pinned,entry_clock_verified:destination.clockVerified===true,retention:'disabled'};
 }catch(error){
  state.entry_failed=state.entry_failed||{run_id:runId,message:error.message};await save('failed',error.message);
  if(checked&&context&&!stageStarted){try{const op=controls({api:destination,state,runId,started,clock,context});await op.checkpoint('failed',error.message);await op.finish('failed',error.message);}catch(reportError){error.reporting=reportError.message;}}
  return{run_id:runId,started,completed:clock(),ok:false,status:'failed',errors:[error.message,...(error.reporting?[error.reporting]:[])],counts:state.entry_counts||{},retention:'disabled'};
 }
}
module.exports={run,readData,dependencies};
