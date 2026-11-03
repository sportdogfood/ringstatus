'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const settings=require('./settings');
const LOGS=['heartbeat','run-logs','errs-log'];
// Fixed owned-table boundary: an accidentally checked reference/catalog never authorizes deletion.
const TABLES=['live-trip-results','live-trip-messages','live-trip-comments','live-trips','live-entries','live-class-appearances','live-classes','live-groups','live-rings',...LOGS];
const terminal=new Set(['Success','Partial','Failed','Skipped','Error']);
const localDay=value=>new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(value));
function completed(table,row){
 const f=row.fields||{};
 if(table==='errs-log')return Number.isFinite(Date.parse(f['occurred-at']));
 return !LOGS.includes(table)||(terminal.has(f.result)&&Number.isFinite(Date.parse(f.completed)));
}
function eligible(table,row,trigger,show){
 return TABLES.includes(table)&&completed(table,row)&&
  (trigger==='new-show'||(['calendar-day','quick-cleanup'].includes(trigger)&&LOGS.includes(table)));
}
function protectedRun(row,runs){
 const id=row.fields?.['run-id'];
 return typeof id==='string'&&[...runs].some(run=>id===run||id.startsWith(run+'|'));
}
function quickRows(table,rows,total=rows.length){
 if(!LOGS.includes(table)||total<1000)return[];
 const available=rows.filter(r=>completed(table,r)&&Number.isFinite(Date.parse(r.createdTime)))
  .sort((a,b)=>Date.parse(a.createdTime)-Date.parse(b.createdTime)||a.id.localeCompare(b.id));
 return available.length>=500?available.slice(0,500):[];
}
function write(file,value){const tmp=file+'.'+process.pid+'.tmp';fs.writeFileSync(tmp,JSON.stringify(value,null,2));fs.renameSync(tmp,file);}
async function canonical(api){
 const rows=(await api.list('shows',settings.SHOWS_BASE)).filter(r=>r.fields.is_default===true&&Number(r.fields.customer_id)===15);
 if(rows.length!==1||!Number.isSafeInteger(Number(rows[0].fields.show_id))||Number(rows[0].fields.show_id)<=0)throw Error('Cleanup requires one canonical default show');
 return Number(rows[0].fields.show_id);
}
async function cleanup({api=require('./airtable').airtable(),runtime=settings.RUNTIME,clock=()=>new Date().toISOString(),
 protectedRuns=[],checkShow=true}={}){
 fs.mkdirSync(runtime,{recursive:true});
 const stateFile=path.join(runtime,'cleanup-state.json'),stop=path.join(runtime,'cleanup-stop.json');
 const run={run_id:crypto.randomUUID(),started:clock(),actions:[],result:'Success'};
 // Persist action/count logs only, never a backup of deleted record fields.
 const savedRun=()=>({...run,selections:undefined,actions:run.actions.map(({rows,...a})=>({...a,target_count:rows.length}))});
 const receipt=()=>write(path.join(runtime,'cleanup-'+run.run_id+'.json'),savedRun());
 if(fs.existsSync(stop)){run.result='Blocked';run.error='Cleanup stopped after a prior incomplete/error action; collection continues';run.completed=clock();receipt();return run;}
 const protectedIds=new Set(protectedRuns),deletedIds=new Set();let activeAction;
 const protect=(t,r)=>LOGS.includes(t)&&protectedRun(r,protectedIds);
 async function publish(action){
  const fields={'cleanup-id':action.id,'run-id':run.run_id,table:action.table,trigger:action.trigger,started:action.started,result:action.result,
   'new-show-id':run.show,'records-archived':0,'records-deleted':action.deleted||0};
  if(action.old_show)fields['old-show-id']=action.old_show;
  if(action.completed)fields.completed=action.completed;
  if(action.error)fields.error=action.error;
  await api.upsert('cleanup-logs','cleanup-id',[fields]);
  const latest={Name:action.table,started:action.started,result:action.result,'new-show-id':run.show,records:action.before,
   'old-show-id':action.old_show||null,'records-archived':0,'records-deleted':action.deleted||0,
   'archive-file':'',error:action.error||'','run-id':run.run_id};
  latest.completed=action.completed||null;
  // Existing cleanup rows are selected by exact native ID; never create duplicates.
  const row=run.selections.find(r=>r.fields.Name===action.table);
  const saved=await api.call(settings.BASE_ID,'cleanup','PATCH',{records:[{id:row.id,fields:latest}]});
  if(saved.records?.length!==1||saved.records[0].id!==row.id)throw Error('Cleanup summary write unconfirmed');
 }
 async function execute(plans){
  // Log the planned exact targets before deletion; cleanup is delete-only.
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
  run.day=localDay(run.started);
  // Existing checkpoints have no day: derive it from their original completion, without renewing it.
  const previousDay=previous.day||(previous.completed?localDay(previous.completed):run.day);
  run.new_day=previousDay!==run.day;
  const lastQuick=Date.parse(previous.quick_checked_at||previous.completed);
  run.quick_due=!Number.isFinite(lastQuick)||Date.parse(run.started)-lastQuick>=3*60*60*1000;
  // Quiet dispatcher wakes use only local state unless calendar/three-hour work is due.
  if(!checkShow&&!run.new_day&&!run.quick_due)return{...run,result:'Skipped',reason:'Cleanup not due',completed:clock()};
  run.show=await canonical(api);
  run.new_show=previous.show!==run.show;
  if(!run.new_show&&!run.new_day&&!run.quick_due){
   if(!previous.day)write(stateFile,{...previous,day:previousDay});
   run.result='Skipped';run.reason='No show/day change; three-hour log check not due';run.completed=clock();
   receipt();write(path.join(runtime,'cleanup-last-run.json'),savedRun());return run;
  }
  const fd=fs.openSync(stop,'wx');try{fs.writeFileSync(fd,JSON.stringify({run_id:run.run_id,started:run.started}));fs.fsyncSync(fd);}finally{fs.closeSync(fd);}
  run.selections=(await api.list('cleanup')).filter(r=>r.fields.cleanup===true&&TABLES.includes(r.fields.Name));
  if(new Set(run.selections.map(r=>r.fields.Name)).size!==run.selections.length)throw Error('Duplicate checked cleanup table');
  const selected=TABLES.filter(t=>run.selections.some(r=>r.fields.Name===t));
  const plan=(table,trigger,rows,before)=>{
   const old=[...new Set(rows.map(r=>Number(r.fields['show-id'])).filter(n=>Number.isSafeInteger(n)&&n>0&&n!==run.show))];
   return{id:run.run_id+'|'+trigger+'|'+table,table,trigger,rows,before,old_show:old.length===1?old[0]:trigger==='new-show'&&previous.show!==run.show?previous.show:null,deleted:0};
  };
  if(run.new_show){
   const plans=[];
   for(const t of selected){const rows=await api.list(t);plans.push(plan(t,'new-show',rows.filter(r=>eligible(t,r,'new-show',run.show)&&!protect(t,r)),rows.length));}
   await execute(plans);
  }
  const quick=[];
  for(const t of LOGS.filter(t=>!run.new_show&&(run.new_day||run.quick_due)&&selected.includes(t))){const rows=await api.list(t);
   // Threshold uses full table count; selection excludes active/current-invocation records.
   const available=rows.filter(r=>completed(t,r)&&!protect(t,r));
   const targets=run.new_day?available:quickRows(t,available,rows.length);
   quick.push(plan(t,run.new_day?'calendar-day':'quick-cleanup',targets,rows.length));
  }
  await execute(quick);
  write(stateFile,{show:run.show,day:run.day,quick_checked_at:run.started,run_id:run.run_id,completed:clock()});
  run.completed=clock();receipt();write(path.join(runtime,'cleanup-last-run.json'),savedRun());fs.unlinkSync(stop);
 }catch(e){
  run.result='Error';run.error=e.message;run.completed=clock();
  if(activeAction){activeAction.result='Error';activeAction.error=e.message;activeAction.completed=run.completed;try{await publish(activeAction);}catch(log){run.logging_error=log.message;}}
  try{await api.upsert('errs-log','error-key',[{'error-key':run.run_id+'|cleanup','run-id':run.run_id,'occurred-at':run.completed,'show-id':run.show||null,message:'Cleanup: '+e.message}]);}catch(log){run.error_log_error=log.message;}
  write(stop,{run_id:run.run_id,at:run.completed,error:e.message});receipt();write(path.join(runtime,'cleanup-last-run.json'),savedRun());
 }
 return run;
}
module.exports={cleanup,TABLES,LOGS,completed,eligible,quickRows,canonical,protectedRun};
