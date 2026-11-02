'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const settings=require('./cleanup-settings');
const {LOGS,showOf,completed,protectedRows}=require('./cleanup-fields');
const CLEAR_ON_NEW_SHOW=['horses','riders','trainers','owners','lanes','slots'];
const TABLES=['results','trips','entries','schedules','classes','groups','rings','days','shows','runs',...CLEAR_ON_NEW_SHOW,...LOGS];
function eligible(table,row,trigger,show,runShows={}){
 const sid=showOf(table,row,runShows);
 return TABLES.includes(table)&&completed(table,row)&&
  (trigger==='new-show'?(CLEAR_ON_NEW_SHOW.includes(table)||sid!==null&&sid!==show):trigger==='quick-cleanup'&&LOGS.includes(table));
}
function quickRows(table,rows,total=rows.length){
 if(!LOGS.includes(table)||total<1000)return[];
 const available=rows.filter(r=>completed(table,r)&&Number.isFinite(Date.parse(r.createdTime)))
  .sort((a,b)=>Date.parse(a.createdTime)-Date.parse(b.createdTime)||a.id.localeCompare(b.id));
 return available.length>=500?available.slice(0,500):[];
}
function write(file,value){const tmp=file+'.'+process.pid+'.tmp';fs.writeFileSync(tmp,JSON.stringify(value,null,2));fs.renameSync(tmp,file);}
async function canonical(api){
 const rows=(await api.list('wef-shows.shows')).filter(r=>r.fields.is_default===true&&Number(r.fields.customer_id)===15);
 if(rows.length!==1||!Number.isSafeInteger(Number(rows[0].fields.show_id))||Number(rows[0].fields.show_id)<=0)throw Error('Cleanup requires one canonical default show');
 return Number(rows[0].fields.show_id);
}
async function cleanup({api=require('./cleanup-airtable').airtable(),runtime=settings.RUNTIME,clock=()=>new Date().toISOString(),
 protectedRuns=[],alertShow,alertDay,trigger}={}){
 if(!Number.isSafeInteger(alertShow)||alertShow<=0||!/^\d{4}-\d{2}-\d{2}$/.test(alertDay)||!['new-show','day-change'].includes(trigger))throw Error('Verified show/day alert required');
 fs.mkdirSync(runtime,{recursive:true});
 const stateFile=path.join(runtime,'cleanup-state.json'),stop=path.join(runtime,'cleanup-stop.json');
 const run={run_id:Date.now(),started:clock(),actions:[],result:'Success'};
 // Live delete-only receipt: counts and action metadata, never deleted record snapshots.
 const savedRun=()=>({...run,selections:undefined,actions:run.actions.map(({rows,...a})=>({...a,target_count:rows.length}))});
 const receipt=()=>write(path.join(runtime,'cleanup-'+run.run_id+'.json'),savedRun());
 if(fs.existsSync(stop)){run.result='Blocked';run.error='Cleanup stopped after a prior incomplete/error action; collection continues';run.completed=clock();receipt();return run;}
 const fd=fs.openSync(stop,'wx');try{fs.writeFileSync(fd,JSON.stringify({run_id:run.run_id,started:run.started}));fs.fsyncSync(fd);}finally{fs.closeSync(fd);}
 const protectedIds=new Set(protectedRuns),deletedIds=new Set();let activeAction;
 const protect=(t,r)=>api.protectedIds?.has(r.id)||protectedIds.has(r.fields['attempt-id']);
 async function publish(action){
  const fields={'cleanup-id':action.id,'run-id':run.run_id,table:action.table,trigger:action.trigger,started:action.started,result:action.result,
   'new-show-id':run.show,'records-archived':0,'records-deleted':action.deleted||0};
  if(action.old_show)fields['old-show-id']=action.old_show;
  if(action.completed)fields.completed=action.completed;
  if(action.error)fields.error=action.error;
  await api.upsert('cleanup-logs','cleanup-id',[fields]);
  const latest={'\uFEFFName':action.table,started:action.started,result:action.result,'new-show-id':run.show,records:action.before,
   'old-show-id':action.old_show||null,'records-archived':0,'records-deleted':action.deleted||0,
   'archive-file':'',error:action.error||'','run-id':run.run_id};
  latest.completed=action.completed||null;
  // Existing cleanup rows are selected by exact native ID; never create duplicates.
  const row=run.selections.find(r=>r.fields['\uFEFFName']===action.table);
  const saved=await api.call(settings.BASE_ID,'cleanup','PATCH',{records:[{id:row.id,fields:latest}]});
  if(saved.records?.length!==1||saved.records[0].id!==row.id)throw Error('Cleanup summary write unconfirmed');
 }
 async function execute(plans){
  // Live delete-only action logging before deletion.
  for(const a of plans){
   activeAction=a;run.actions.push(a);a.started=clock();a.result=a.rows.length?'Running':'Skipped';
   await publish(a);
  }
  for(const a of plans){
   activeAction=a;
   if(a.rows.length){
    if(await canonical(api)!==run.show)throw Error('Default show changed during cleanup');
    await api.retireCleanup(a.table,a.rows,{trigger:a.trigger,show:run.show,deletedIds,protectedRuns:protectedIds},
     batch=>{a.deleted=(a.deleted||0)+batch.length;for(const id of batch)deletedIds.add(id);});
    a.result='Success';
   }
   a.completed=clock();await publish(a);
  }
  activeAction=null;
 }
 try{
  const previous=fs.existsSync(stateFile)?JSON.parse(fs.readFileSync(stateFile,'utf8')):{};
  run.show=await canonical(api);
  if(run.show!==alertShow)throw Error('Alert does not match canonical default show');
  if(api.prepare)await api.prepare();
  run.selections=(await api.list('cleanup')).filter(r=>r.fields.cleanup==='checked'&&TABLES.includes(r.fields['\uFEFFName']));
  const selected=TABLES.filter(t=>run.selections.some(r=>r.fields['\uFEFFName']===t));
  run.new_show=previous.show!==run.show;
  const plan=(table,trigger,rows,before)=>{
   const old=[...new Set(rows.map(r=>showOf(table,r,api.runShows)).filter(n=>Number.isSafeInteger(n)&&n>0&&n!==run.show))];
   return{id:run.run_id+'|'+trigger+'|'+table,table,trigger,rows,before,old_show:old.length===1?old[0]:trigger==='new-show'&&previous.show!==run.show?previous.show:null,deleted:0};
  };
  if(trigger==='new-show'&&run.new_show){
   const plans=[];
   for(const t of selected){const rows=await api.list(t);plans.push(plan(t,'new-show',rows.filter(r=>eligible(t,r,'new-show',run.show,api.runShows)&&!protect(t,r)),rows.length));}
   await execute(plans);
  }
  if(trigger==='day-change'&&(previous.show!==run.show||previous.day!==alertDay)){
   const quick=[];
   for(const t of LOGS.filter(t=>selected.includes(t))){const rows=await api.list(t);
    const targets=quickRows(t,rows.filter(r=>!protect(t,r)),rows.length);
    if(targets.length)quick.push(plan(t,'quick-cleanup',targets,rows.length));
   }
   await execute(quick);
  }
  write(stateFile,{show:run.show,day:alertDay,run_id:run.run_id,completed:clock()});
  run.completed=clock();receipt();write(path.join(runtime,'cleanup-last-run.json'),savedRun());fs.unlinkSync(stop);
 }catch(e){
  run.result='Error';run.error=e.message;run.completed=clock();
  if(activeAction){activeAction.result='Error';activeAction.error=e.message;activeAction.completed=run.completed;try{await publish(activeAction);}catch(log){run.logging_error=log.message;}}
  try{await api.upsert('err-logs','error-id',[{'error-id':'cleanup-'+run.run_id,'occurred-at':run.completed,category:'Cleanup',operation:'Cleanup',target:activeAction?.table||'cleanup',message:'Cleanup: '+e.message,'write-outcome':'Unknown'}]);}catch(log){run.error_log_error=log.message;}
  write(stop,{run_id:run.run_id,at:run.completed,error:e.message});receipt();write(path.join(runtime,'cleanup-last-run.json'),savedRun());
 }
 return run;
}
module.exports={cleanup,TABLES,LOGS,completed,eligible,quickRows,canonical};
