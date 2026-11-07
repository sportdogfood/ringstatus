'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const settings=require('./cleanup-settings');
const {LOGS,showOf,completed,protectedRows}=require('./cleanup-fields');
const CLEAR_ON_NEW_SHOW=['horses','riders','trainers','owners','lanes','slots'];
const TABLES=['results','trips','entries','schedules','classes','groups','rings','days','shows','runs',...CLEAR_ON_NEW_SHOW,...LOGS];
function eligible(table,row,trigger,show,runShows={}){
 const sid=showOf(table,row,runShows);
 return TABLES.includes(table)&&completed(table,row)&&
  (trigger==='new-show'||['calendar-day','quick-cleanup'].includes(trigger)&&LOGS.includes(table));
}
function quickRows(table,rows,total=rows.length){
 if(!LOGS.includes(table)||total<1000)return[];
 const available=rows.filter(r=>completed(table,r)&&Number.isFinite(Date.parse(r.createdTime)))
  .sort((a,b)=>Date.parse(a.createdTime)-Date.parse(b.createdTime)||a.id.localeCompare(b.id));
 return available.length>=500?available.slice(0,500):[];
}
function write(file,value){const tmp=file+'.'+process.pid+'.tmp';fs.writeFileSync(tmp,JSON.stringify(value,null,2));fs.renameSync(tmp,file);}
async function canonical(api){
 const rows=(await api.list('w-shows')).filter(r=>r.fields.is_default===true&&Number(r.fields.customer_id)===15);
 if(rows.length!==1||!Number.isSafeInteger(Number(rows[0].fields.show_id))||Number(rows[0].fields.show_id)<=0)throw Error('Cleanup requires one canonical default show');
 return Number(rows[0].fields.show_id);
}
async function cleanup({api=require('./cleanup-airtable').airtable(),runtime=settings.RUNTIME,clock=()=>new Date().toISOString(),
 local=require('./cleanup-local-logs'),protectedRuns=[],alertShow,alertDay,trigger,eventId,alertAt}={}){
 if(!Number.isSafeInteger(alertShow)||alertShow<=0||!/^\d{4}-\d{2}-\d{2}$/.test(alertDay)||!['new-show','calendar-day','log-check'].includes(trigger)||typeof eventId!=='string'||!eventId||(!Number.isFinite(Date.parse(alertAt))||new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(alertAt))!==alertDay))throw Error('Verified Records cleanup alert required');
 fs.mkdirSync(runtime,{recursive:true});
 const stateFile=path.join(runtime,'cleanup-state.json');
 const run={run_id:Date.now(),started:clock(),actions:[],result:'Success'};
 // Live delete-only receipt: counts and action metadata, never deleted record snapshots.
 const savedRun=()=>({...run,selections:undefined,actions:run.actions.map(({rows,...a})=>({...a,target_count:rows.length}))});
 const receipt=()=>write(path.join(runtime,'cleanup-'+run.run_id+'.json'),savedRun());
 const protectedIds=new Set(protectedRuns),deletedIds=new Set();let activeAction;
 const protect=(t,r)=>api.protectedIds?.has(r.id)||protectedIds.has(r.fields['attempt-id'])||(LOGS.includes(t)&&api.loggingSnapshotIds&&!api.loggingSnapshotIds.has(r.id));
 async function publish(action){
  const latest={'\uFEFFName':action.table,started:action.started,result:action.result,'new-show-id':run.show,records:action.before,
   'old-show-id':action.old_show||null,'records-archived':0,'records-deleted':action.deleted||0,
   'archive-file':'',error:action.error||'','run-id':run.run_id};
  latest.completed=action.completed||null;
  // Existing cleanup rows are selected by exact native ID; never create duplicates.
  const row=run.selections.find(r=>r.fields['\uFEFFName']===action.table);
  latest.purpose=String(row.fields.purpose||'').split('\nLatest local cleanup:')[0]+(action.local?'\nLatest local cleanup: '+JSON.stringify(action.local):'');
  const saved=await api.call(settings.BASE_ID,'cleanupV2','PATCH',{records:[{id:row.id,fields:latest}],typecast:true});
  if(saved.records?.length!==1||saved.records[0].id!==row.id)throw Error('Cleanup summary write unconfirmed');
 }
 async function report(action){
  try{await publish(action);}catch(e){action.publication_error=e.message;run.result='Error';run.error='cleanupV2 outcome not saved: '+e.message;}
 }
 async function execute(plans){
  // Live delete-only action logging before deletion.
  for(const a of plans){
   activeAction=a;run.actions.push(a);a.started=clock();a.result=a.rows.length?'Running':'Skipped';
   await report(a);
  }
  for(const a of plans){
   activeAction=a;
   try{if(a.rows.length){
    if(await canonical(api)!==run.show)throw Error('Default show changed during cleanup');
    await api.retireCleanup(a.table,a.rows,{trigger:a.trigger,show:run.show,deletedIds,protectedRuns:protectedIds},
     batch=>{a.deleted=(a.deleted||0)+batch.length;for(const id of batch)deletedIds.add(id);});
    a.result='Success';
   }
   }catch(e){a.result='Error';a.error=e.message;run.result='Error';run.error=e.message;}
   a.completed=clock();await report(a);
  }
  activeAction=null;
 }
 try{
  const previous=fs.existsSync(stateFile)?JSON.parse(fs.readFileSync(stateFile,'utf8')):{};
  run.show=await canonical(api);
  if(run.show!==alertShow)throw Error('Alert does not match canonical default show');
  if(api.prepare)await api.prepare();
  run.selections=(await api.list('cleanupV2')).filter(r=>r.fields.cleanup==='checked'&&TABLES.includes(r.fields['\uFEFFName']));
  const selected=TABLES.filter(t=>run.selections.some(r=>r.fields['\uFEFFName']===t));
  run.new_show=previous.show!==run.show;
  const plan=(table,trigger,rows,before)=>{
   const old=[...new Set(rows.map(r=>showOf(table,r,api.runShows)).filter(n=>Number.isSafeInteger(n)&&n>0&&n!==run.show))];
   return{id:run.run_id+'|'+trigger+'|'+table,table,trigger,rows,before,old_show:old.length===1?old[0]:trigger==='new-show'&&previous.show!==run.show?previous.show:null,deleted:0};
  };
  let handled=false;
  const due=previous.event_id!==eventId&&(trigger==='new-show'&&run.new_show||trigger==='calendar-day'&&previous.show===run.show&&(!previous.calendar_day||alertDay>previous.calendar_day)||trigger==='log-check'&&previous.show===run.show&&(!previous.last_log_check||Date.parse(alertAt)>Date.parse(previous.last_log_check)));
  const localRequest=due?local.request(runtime,{event_id:eventId,run_id:run.run_id},selected):null;
  if(previous.event_id!==eventId&&(trigger==='new-show'||previous.show===run.show)){
   if(trigger==='new-show'&&run.new_show){
    handled=true;
    const plans=[];
    for(const t of selected){const rows=await api.list(t);plans.push(plan(t,'new-show',rows.filter(r=>eligible(t,r,'new-show',run.show,api.runShows)&&!protect(t,r)),rows.length));}
    await execute(plans);
   }
   if(trigger==='calendar-day'&&(!previous.calendar_day||alertDay>previous.calendar_day)){
    handled=true;
    const plans=[];
    for(const t of LOGS.filter(t=>selected.includes(t))){const rows=await api.list(t);plans.push(plan(t,'calendar-day',rows.filter(r=>eligible(t,r,'calendar-day',run.show,api.runShows)&&!protect(t,r)),rows.length));}
    await execute(plans);
   }
   if(trigger==='log-check'&&(!previous.last_log_check||Date.parse(alertAt)>Date.parse(previous.last_log_check))){
    handled=true;
    const quick=[];
    for(const t of LOGS.filter(t=>selected.includes(t))){const rows=await api.list(t);
     const targets=quickRows(t,rows.filter(r=>!protect(t,r)),rows.length);
     if(targets.length)quick.push(plan(t,'quick-cleanup',targets,rows.length));
    }
    await execute(quick);
   }
   if(handled&&localRequest){
    const receipts=await local.wait(localRequest);
    for(const table of ['run-logs','err-logs'].filter(t=>selected.includes(t))){
     let action=run.actions.find(a=>a.table===table);
     if(!action){action=plan(table,trigger,[],0);action.started=run.started;action.result='Skipped';run.actions.push(action);}
     action.local=receipts.map(r=>({workflow:r.workflow,result:r.result,deleted:r.counts?r.counts[table==='run-logs'?'runs':'errors']||0:null,error:r.error||''}));
     if(action.result==='Skipped'&&action.local.some(r=>r.deleted>0))action.result='Success';
     if(receipts.some(r=>r.result==='Error')){action.result='Error';const localError=receipts.filter(r=>r.error).map(r=>r.error).join('; ');action.error=[action.error,localError].filter(Boolean).join('\n');run.result='Error';run.error=[run.error,localError].filter(Boolean).join('\n');}
     action.completed=clock();await report(action);
    }
   }
   if(handled&&run.result==='Success')write(stateFile,{...previous,show:run.show,event_id:eventId,
    calendar_day:['new-show','calendar-day'].includes(trigger)?alertDay:previous.calendar_day,
    last_log_check:!previous.last_log_check||Date.parse(alertAt)>Date.parse(previous.last_log_check)?alertAt:previous.last_log_check,run_id:run.run_id,completed:clock()});
  }
  run.completed=clock();receipt();write(path.join(runtime,'cleanup-last-run.json'),savedRun());
 }catch(e){
  run.result='Error';run.error=e.message;run.completed=clock();
  if(activeAction){activeAction.result='Error';activeAction.error=e.message;activeAction.completed=run.completed;try{await publish(activeAction);}catch(log){run.logging_error=log.message;}}
  receipt();write(path.join(runtime,'cleanup-last-run.json'),savedRun());
 }
 return run;
}
module.exports={cleanup,TABLES,LOGS,completed,eligible,quickRows,canonical};
