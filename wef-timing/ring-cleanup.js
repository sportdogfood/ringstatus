'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const policy=require('./ring-cleanup-plan'),handoff=require('./cleanup-request');
const {api:makeApi,TIMING,SHOWS,CLASS_TIMING,CLASS_ANSWERS,CLEANUP_TABLES:T}=require('./api');
const {decode}=require('./checkpoint-state'),{hash}=require('./pace'),{selectShow,sameAuthority}=require('./data-source');
const tableId=name=>name==='timing-answers'?T.answers:T[name];
const idsFilter=ids=>'OR('+ids.map(id=>`RECORD_ID()='${id}'`).join(',')+')';
function write(file,value){
 const tmp=file+'.'+crypto.randomUUID()+'.tmp',body=JSON.stringify(value,null,2);let fd;
 try{fd=fs.openSync(tmp,'wx');fs.writeFileSync(fd,body);fs.fsyncSync(fd);fs.closeSync(fd);fd=undefined;fs.renameSync(tmp,file);if(fs.readFileSync(file,'utf8')!==body)throw Error('Cleanup state readback mismatch');}
 finally{if(fd!==undefined)fs.closeSync(fd);if(fs.existsSync(tmp))fs.unlinkSync(tmp);}
}
function strings(value,out=new Set()){
 if(typeof value==='string'){out.add(value);if(value.startsWith('{')||value.startsWith('[')){try{strings(JSON.parse(value),out);}catch{}}}
 else if(Array.isArray(value))for(const item of value)strings(item,out);
 else if(value&&typeof value==='object')for(const item of Object.values(value))strings(item,out);
 return out;
}
function protections(checkpoints,answers,logs,context,now,classAnswers=[],referencesVerified=false){
 const roots=new Set(),runs=new Set(),dependencies=new Map(),safe=new Set();
 for(const row of checkpoints){const state=decode(row.fields['state-json']);for(const value of strings(state)){roots.add(value);for(const match of value.matchAll(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi))runs.add(match[0]);}}
 for(const row of logs['errs-log']||[])if(row.fields['run-id'])runs.add(row.fields['run-id']);
 for(const table of ['heartbeat','run-logs'])for(const row of logs[table]||[]){const f=row.fields;if(!Number.isFinite(Date.parse(f.completed))||f.errors||!['Success','Skipped','complete','deferred'].includes(f.result))if(f['run-id'])runs.add(f['run-id']);}
 const byKey=new Map();
 for(const row of answers){const key=row.fields['answer-key'];if(!key||byKey.has(key))throw Error('Ambiguous Ring answer identity during cleanup');byKey.set(key,row);}
 const byId=new Map(answers.map(row=>[row.id,row.fields['answer-key']]));
 if(referencesVerified){
  for(const row of classAnswers){
   const payload=JSON.parse(row.fields['payload-json']),refs=JSON.parse(row.fields['input-refs-json']);
   if(!payload||typeof payload!=='object'||Array.isArray(payload)||!Array.isArray(refs))throw Error('Unreadable retained Class references');
   for(const value of strings([payload,refs])){if(byKey.has(value))roots.add(value);if(byId.has(value))roots.add(byId.get(value));}
  }
 }else for(const key of byKey.keys())roots.add(key);
 for(const row of answers){
  const f=row.fields,key=f['answer-key'];let p;
  try{p=JSON.parse(f['payload-json']);}catch{throw Error('Unreadable answer references during cleanup');}
  const refs=strings(p);strings(f['input-refs-json']||'',refs);dependencies.set(key,new Set([...refs].filter(x=>x!==key&&byKey.has(x))));
  let identity=p.identity;
  if(!identity){const encoded=p.current?.key||p.history?.observation?.key;try{const i=JSON.parse(encoded);identity={customer:i[0],show:i[1],date:i[2]};}catch{}}
  const payloadHash=f['answer-kind']==='ring-pace'&&p.policy==='live-ring-pace-v2'?hash({policy:p.policy,payload:p}):hash(p);
  const valid=identity?.customer===context.customer&&identity.show===f['show-id']&&identity.date===f.date&&payloadHash===f['content-hash'];
  if(valid&&identity.show!==context.show&&policy.eligible('timing-answers',row,{now,referencesVerified:true}))safe.add(key);else roots.add(key);
 }
 const queue=[...roots];for(let i=0;i<queue.length;i++)for(const dependency of dependencies.get(queue[i])||[])if(!roots.has(dependency)){roots.add(dependency);queue.push(dependency);}
 return{protectedRuns:[...runs],referencedAnswerKeys:[...roots],referencesVerified,parentRunIds:(logs.heartbeat||[]).map(r=>r.fields['run-id']).filter(Boolean),dependencies,safe};
}
function orderedAnswers(rows,dependencies){
 const byKey=new Map(rows.map(r=>[r.fields['answer-key'],r])),incoming=new Map([...byKey.keys()].map(k=>[k,0]));
 for(const key of byKey.keys())for(const target of dependencies.get(key)||[])if(incoming.has(target))incoming.set(target,incoming.get(target)+1);
 const queue=[...incoming].filter(([,n])=>n===0).map(([k])=>k),result=[];
 for(let i=0;i<queue.length;i++){const key=queue[i];result.push(byKey.get(key));for(const target of dependencies.get(key)||[])if(incoming.has(target)){incoming.set(target,incoming.get(target)-1);if(incoming.get(target)===0)queue.push(target);}}
 // Cycles remain protected; never remove a referenced record to force progress.
 return result;
}
async function cleanup({api,runtime=path.join(__dirname,'runtime/data'),clock=()=>new Date().toISOString(),deadline=Date.now()+120000,budgetNow=Date.now}={}){
 api||=makeApi({token:process.env.WEF_TIMING_AIRTABLE_TOKEN,lane:'ring_cleanup',deadline});
 fs.mkdirSync(runtime,{recursive:true});
 const stateFile=path.join(runtime,'cleanup-state.json'),stopFile=path.join(runtime,'cleanup-stop.json');
 const report={run_id:crypto.randomUUID(),started:clock(),policy:'delete-only',result:'Skipped',actions:[],deleted:0};
 let state={version:1,completed_events:[],work:[],cursor:0},active;
 const save=()=>write(stateFile,state);
 const receipt=()=>{report.completed=clock();write(path.join(runtime,'cleanup-last-run.json'),report);write(path.join(runtime,'cleanup-'+report.run_id+'.json'),report);};
 try{
  if(fs.existsSync(stateFile))state=JSON.parse(fs.readFileSync(stateFile,'utf8'));
  if(state.version!==1||!Array.isArray(state.work)||!Array.isArray(state.completed_events))throw Error('Unsupported cleanup state');
  if(fs.existsSync(stopFile)||state.pending){report.result='Blocked';report.error='Unfinished cleanup operation requires owner review';receipt();return report;}
  if(budgetNow()>deadline-30000){report.reason='Shared runner budget used; cleanup deferred';receipt();return report;}
  const selected=selectShow(await api.list(SHOWS,'shows'),clock()),context={customer:selected.context['customer-id'],show:selected.context['show-id']};
  if(context.customer!==15)throw Error('Ring cleanup customer identity unsupported');
  const alert=handoff.read({runtime,now:clock()});
  if(alert&&state.completed_events.some(e=>e.event_id===alert.event_id&&e.request_hash!==alert.request_hash))throw Error('Coordinator event identity changed');
  if(alert&&state.completed_events.some(e=>e.request_hash===alert.request_hash)){
   // Completion is durable before consumption; an interrupted unlink is a no-op.
   fs.unlinkSync(path.join(runtime,'cleanup-request.json'));
  }
  const pendingAlert=alert&&!state.completed_events.some(e=>e.request_hash===alert.request_hash)?alert:null;
  const selections=await api.list(TIMING,T.cleanup),checked=new Map();
  for(const row of selections){const f=row.fields,name=f.Name??f['\uFEFFName'];if(f.cleanup!=='checked'||!policy.OWNED.includes(name))continue;if(checked.has(name))throw Error('Duplicate checked cleanup table');checked.set(name,row.id);}
  if(!checked.size&&!state.work.length){report.reason='No checked Ring-owned tables';receipt();return report;}
  const checkpoints=await api.list(TIMING,T.checkpoint),checkpointToken=hash(checkpoints);
  const data={};for(const name of policy.LOGS)data[name]=await api.list(TIMING,tableId(name));
  const answers=checked.has('timing-answers')||state.work.some(w=>w.table==='timing-answers')?await api.list(TIMING,T.answers):[];
  let referenceCoverage=true;
  async function loadProtections(){
   let classAnswers=[];referenceCoverage=true;
   if((pendingAlert&&checked.has('timing-answers'))||state.work.some(w=>w.table==='timing-answers')){
    try{
     classAnswers=await api.list(CLASS_TIMING,CLASS_ANSWERS);
     // Validate coverage before allowing any Ring answer deletion.
     for(const row of classAnswers){const p=JSON.parse(row.fields['payload-json']),r=JSON.parse(row.fields['input-refs-json']);if(!p||typeof p!=='object'||Array.isArray(p)||!Array.isArray(r))throw Error('Unreadable retained Class references');}
    }catch(e){referenceCoverage=false;report.withheld_answers='Class reference coverage unavailable: '+e.message;}
   }
   return protections(checkpoints,answers,data,context,clock(),classAnswers,referenceCoverage);
  }
  let protection=await loadProtections();
  function add(table,trigger,rows,before){
   if(!rows.length)return;
   state.work.push({id:crypto.randomUUID(),table,trigger,customer:context.customer,show:context.show,request_hash:trigger==='coordinator-new-show'?pendingAlert.request_hash:null,started:clock(),summary_id:checked.get(table),before,deleted:0,targets:rows.map(r=>({id:r.id,hash:hash(r.fields),key:r.fields['answer-key']||null}))});
  }
  if(!state.work.length){
   if(pendingAlert){
    for(const name of policy.OWNED.filter(n=>checked.has(n))){const rows=name==='timing-answers'?answers:data[name];let targets=policy.plan({table:name,rows,checked:true,trigger:'coordinator-new-show',alert:pendingAlert,context,now:clock(),protection});if(name==='timing-answers')targets=orderedAnswers(targets,protection.dependencies);add(name,'coordinator-new-show',targets,rows.length);}
    state.active_event={...pendingAlert};
   }else{
    // Heartbeats precede parent run logs. Unresolved errors protect their run.
    for(const name of ['heartbeat','run-logs','errs-log'].filter(n=>checked.has(n)))add(name,'quick-cleanup',policy.plan({table:name,rows:data[name],checked:true,trigger:'quick-cleanup',context,now:clock(),protection}),data[name].length);
   }
   save();
  }
  async function summary(work,done,error=''){
   await api.updateCleanup(work.summary_id,{records:work.before,started:work.started,completed:done?clock():null,result:done&&!error?'Success':null,'new-show-id':work.show,'old-show-id':state.active_event?.old_show??null,'records-archived':0,'records-deleted':work.deleted,'archive-file':'',error});
  }
  const retained=()=>({protectedRuns:protection.protectedRuns,referencedAnswerKeys:protection.referencedAnswerKeys,parentRunIds:protection.parentRunIds,referencesVerified:protection.referencesVerified});
  const heldActions=new Set();
  while(state.work.length&&budgetNow()<=deadline-30000){
   const index=(state.cursor||0)%state.work.length,work=state.work[index];active=work;
   if(work.show!==context.show||work.customer!==context.customer||!checked.has(work.table)||checked.get(work.table)!==work.summary_id)throw Error('Cleanup scope/selection changed');
   if(!work.targets.length){await summary(work,true);state.work.splice(index,1);state.cursor=index;save();active=null;continue;}
   if(work.table==='timing-answers'){
    protection=await loadProtections();
    if(!referenceCoverage){heldActions.add(work.id);state.cursor=index+1;active=null;if(heldActions.size>=state.work.length)break;continue;}
    work.targets=work.targets.filter(t=>!protection.referencedAnswerKeys.includes(t.key));save();
    if(!work.targets.length)continue;
   }
   const current=selectShow(await api.list(SHOWS,'shows'),clock());if(!sameAuthority(selected,current)||hash(await api.list(TIMING,T.checkpoint))!==checkpointToken)throw Error('Cleanup authority/checkpoint changed');
   const blockedKeys=new Set();
   if(work.table==='timing-answers'){
    const remaining=new Set(work.targets.map(t=>t.key));
    for(const target of work.targets)for(const dependency of protection.dependencies.get(target.key)||[])if(remaining.has(dependency))blockedKeys.add(dependency);
   }
   // Only independent rows share a DELETE. Referrers must have confirmed absence
   // before their dependencies become eligible for a later DELETE request.
   const targets=work.targets.filter(t=>!blockedKeys.has(t.key)).slice(0,10);
   if(!targets.length)throw Error('Unfinished Ring reference dependency');
   const rows=await api.list(TIMING,tableId(work.table),idsFilter(targets.map(r=>r.id)));
   if(rows.length!==targets.length||new Set(rows.map(r=>r.id)).size!==rows.length||targets.some(t=>!rows.some(r=>r.id===t.id&&hash(r.fields)===t.hash))||rows.some(r=>!policy.eligible(work.table,r,{...retained(),now:clock()})))throw Error('Cleanup targets changed or protected');
   // Retain IDs/hashes only; no deleted-field backup or cleanup archive.
   state.pending={action:work.id,table:work.table,targets,at:clock()};save();
   await summary(work,false);
   await api.removeCleanupRows(tableId(work.table),targets.map(t=>t.id));
   work.deleted+=targets.length;report.deleted+=targets.length;const removed=new Set(targets.map(t=>t.id));work.targets=work.targets.filter(t=>!removed.has(t.id));delete state.pending;save();
   const done=work.targets.length===0;await summary(work,done);
   report.actions.push({action:work.id,table:work.table,trigger:work.trigger,deleted:targets.length,complete:done});
   if(done){state.work.splice(index,1);state.cursor=index;}else state.cursor=index+1;
   save();active=null;
  }
  if(!state.work.length&&state.active_event&&(!checked.has('timing-answers')||referenceCoverage)){
   const event=state.active_event;state.completed_events.push({event_id:event.event_id,request_hash:event.request_hash,completed:clock()});delete state.active_event;save();
   const saved=handoff.read({runtime,now:clock()});if(saved&&saved.request_hash!==event.request_hash)throw Error('Cleanup request changed before consumption');if(saved)fs.unlinkSync(path.join(runtime,'cleanup-request.json'));
  }
  report.result=state.work.length||state.active_event?'Deferred':report.deleted?'Success':'Skipped';report.remaining=state.work.reduce((n,w)=>n+w.targets.length,0);receipt();return report;
 }catch(error){
  report.result='Error';report.error=error.message;
  write(stopFile,{run_id:report.run_id,at:clock(),error:error.message,pending:state.pending??null});
  if(active){try{await api.updateCleanup(active.summary_id,{result:null,completed:clock(),error:error.message,'records-deleted':active.deleted});}catch(e){report.reporting_error=e.message;}}
  receipt();return report;
 }
}
module.exports={cleanup,protections,orderedAnswers};
