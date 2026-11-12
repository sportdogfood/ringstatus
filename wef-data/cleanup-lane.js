'use strict';
// Independent, delete-only lane. The existing Windows task also starts daily at 19:00 Eastern.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const BASE='appdl16HJ5ETm8N69',CONTROL='tblIhyIiYgvHHeChy';
const SHOW=new Set(['schedules','rings','groups','classes','trips','entries','results','horses','riders','trainers','owners']);
const LOGS=new Set(['runs','run-logs','err-logs','heartbeat']);
const read=(file,fallback)=>fs.existsSync(file)?JSON.parse(fs.readFileSync(file,'utf8')):fallback;
const digest=value=>crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const alive=pid=>{try{if(!pid)return false;process.kill(pid,0);return true;}catch(e){return e.code==='EPERM';}};
function eastern(at){
 return {day:new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(at),hour:Number(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',hour:'2-digit',hourCycle:'h23'}).format(at))};
}
function logSlot(at){
 const {day,hour}=eastern(new Date(at));
 const slotDay=hour===0?new Date(Date.parse(day+'T00:00:00Z')-86400000).toISOString().slice(0,10):day;
 return slotDay+'|'+Math.floor(((hour+23)%24)/3);
}
function alertFor(state,show,now=new Date()){
 const at=new Date(now),{day,hour}=eastern(at);
 let trigger,event_id;
 if(show!==state.show){trigger='new-show';event_id='new-show|'+state.show+'|'+show+'|'+day;}
 else if(hour>=19&&state.daily_clear_date!==day){trigger='calendar-day';event_id='daily-19|'+day;}
 else if(logSlot(at)!==logSlot(state.last_log_check||state.completed||0)){trigger='log-check';event_id='log-check|'+logSlot(at);}
 else return null;
 return {event_id,trigger,show_id:show,calendar_day:day,issued_at:at.toISOString()};
}
function completed(row){return Boolean(row.fields.completed)&&!['Started','Running'].includes(row.fields.status);}
function choose(rows,eligible,trigger){
 if(trigger==='log-check'&&rows.length<1000)return [];
 const selected=rows.filter(eligible).sort((a,b)=>Date.parse(a.createdTime)-Date.parse(b.createdTime));
 return trigger==='log-check'?selected.slice(0,rows.length-500):selected;
}
function localLogs(runtime){
 const families=[],pending=new Set();
 const roots=['wef-data','wef-data-linking'].map(workflow=>path.join(runtime,'logging',workflow));
 const fast=path.join(runtime,'fast');
 const notices=read(path.join(fast,'owner-alerts.json'),{submitted:{}});
 if(!notices?.submitted||typeof notices.submitted!=='object'||Array.isArray(notices.submitted)||Object.values(notices.submitted).some(v=>!v||!Number.isFinite(Date.parse(v.at))||typeof v.message!=='string'||!v.message.trim()))throw Error('Invalid Fast owner notification receipt; failure evidence retained');
 if(fs.existsSync(fast))for(const entry of fs.readdirSync(fast,{withFileTypes:true})){
  if(entry.isDirectory()&&/^(day\d+|fast-save|fast-linker)$/.test(entry.name))roots.push(path.join(fast,entry.name,'logging'));
 }
 for(const root of roots){
  const workflow=path.relative(runtime,root);
  const files=dir=>fs.existsSync(path.join(root,dir))?fs.readdirSync(path.join(root,dir)).filter(n=>n.endsWith('.json')).map(n=>({file:path.join(root,dir,n),data:read(path.join(root,dir,n))})):[];
  const runs=files('run-logs'),errors=files('err-logs');
  const newest=runs.slice().sort((a,b)=>Date.parse(b.data.started)-Date.parse(a.data.started))[0];
  for(const run of runs){
   const a=run.data,fields={'attempt-id':a.id,workflow:a.workflow,started:a.started,status:a.status,stage:a.stage};
   for(const n of ['completed','elapsed-seconds','run-key','source-revision','show-key','counts-json','reason'])if(a[n]!==undefined)fields[n]=a[n];
   const ackFile=path.join(root,'acks','run-logs-'+a.id+'.json'),ack=read(ackFile,{});
   const children=errors.filter(e=>e.data.attempt===a.id).map(e=>{
    const v=e.data,f={'error-id':v.id,run:[ack.id],'occurred-at':v.at,stage:v.stage,category:v.category,operation:v.operation,target:v.target,message:v.message,'write-outcome':v['write-outcome']};
    const af=path.join(root,'acks','err-logs-'+v.id+'.json');return {...e,ackFile:af,delivered:read(af,{}).hash===digest(f)};
   });
   const needsNotice=root.startsWith(fast+path.sep)&&(children.some(e=>!notices.submitted?.[e.data.id])||(['Partial','Failed','Interrupted'].includes(a.status)&&!notices.submitted?.['run|'+a.id]));
   const delivered=ack.hash===digest(fields)&&children.every(e=>e.delivered)&&!needsNotice;
   const current=run===newest||alive(a['owner-pid'])||alive(a['child-pid']);
   if(!delivered||current){pending.add(a.id);if(ack.id)pending.add(ack.id);}
   families.push({run,ackFile,children,workflow,delivered,current,runCount:runs.length,errorCount:errors.length});
  }
 }
 return {families,pending};
}
function pruneLocal(families,trigger){
 const out={runs:0,errors:0,acks:0};
 for(const workflow of new Set(families.map(f=>f.workflow))){
  const list=families.filter(f=>f.workflow===workflow).sort((a,b)=>Date.parse(a.run.data.started)-Date.parse(b.run.data.started));
  let nr=0,ne=0;
  for(const f of list){
   if(!f.delivered||f.current||!f.run.data.completed||['Started','Running'].includes(f.run.data.status))continue;
   const removeRun=trigger!=='log-check'||(f.runCount>=1000&&nr<f.runCount-500);
   for(const e of f.children){
    if(trigger==='log-check'&&(f.errorCount<1000||ne>=f.errorCount-500))continue;
    fs.unlinkSync(e.file);out.errors++;ne++;
   }
   if(removeRun&&f.children.every(e=>!fs.existsSync(e.file))){
    fs.unlinkSync(f.run.file);out.runs++;nr++;
   }
  }
 }
 return out;
}
function airtable(token){
 let last=0;
 async function call(method,route,body){
  const delay=250-(Date.now()-last);if(delay>0)await new Promise(r=>setTimeout(r,delay));last=Date.now();
  const res=await fetch('https://api.airtable.com/v0/'+route,{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:body?JSON.stringify(body):undefined});
  const text=await res.text();if(!res.ok)throw Error(method+' '+route.split('?')[0]+' HTTP '+res.status+': '+text.split(token).join('[redacted]'));
  return JSON.parse(text);
 }
 return {
  schema:async()=> (await call('GET','meta/bases/'+BASE+'/tables')).tables,
  list:async table=>{const rows=[];let offset;do{const j=await call('GET',BASE+'/'+table+'?pageSize=100'+(offset?'&offset='+encodeURIComponent(offset):''));rows.push(...j.records);offset=j.offset;}while(offset);return rows;},
  remove:async(table,ids)=>call('DELETE',BASE+'/'+table+'?'+ids.map(id=>'records[]='+encodeURIComponent(id)).join('&')),
  summary:async(id,fields)=>call('PATCH',BASE+'/'+CONTROL,{records:[{id,fields}],typecast:true})
 };
}
async function cleanup(event,{api=airtable(process.env.AIRTABLE_TOKEN),runtime=path.join(__dirname,'runtime')}={}){
 if(!['new-show','calendar-day','log-check'].includes(event?.trigger))return {result:'Skipped',actions:[]};
 const stateDir=path.join(runtime,'cleanup'),stateFile=path.join(stateDir,'cleanup-state.json');
 fs.mkdirSync(stateDir,{recursive:true});
 const state=read(stateFile,{});
 if(event.event_id&&event.event_id===state.event_id)return {result:'Success',duplicate:true,actions:[]};
 const run={run_id:Date.now(),event_id:event.event_id,trigger:event.trigger,started:new Date().toISOString(),actions:[],errors:[]};
 const tables=await api.schema(),byName=new Map(tables.map(t=>[t.name,t]));
 const controls=await api.list(CONTROL),cache=new Map();
 const rows=async name=>{if(!cache.has(name))cache.set(name,await api.list(byName.get(name).id));return cache.get(name);};
 let local={families:[],pending:new Set()},parents=[],heartbeats=[],loggingReady=true;
 try{
  local=localLogs(runtime);parents=await rows('run-logs');heartbeats=await rows('heartbeat');
  await rows('err-logs'); // Capture parent links before deleting completed run logs.
 }catch(e){loggingReady=false;run.errors.push({table:'logging',error:e.message});}
 const protectedIds=new Set([...local.pending,...heartbeats.flatMap(r=>r.fields['last-run']||[])]);
 for(const r of parents)if(!completed(r)||local.pending.has(r.fields['attempt-id']))protectedIds.add(r.id);
 const finishedParents=new Set(parents.filter(completed).map(r=>r.id));
 const currentRunKeys=new Set(parents.filter(r=>protectedIds.has(r.id)).map(r=>r.fields['run-key']).filter(Boolean));
 for(const control of controls){
  const name=control.fields['\uFEFFName'];
  if(control.fields.cleanup!=='checked'||!byName.has(name)||(!LOGS.has(name)&&!(event.trigger==='new-show'&&SHOW.has(name))))continue;
  const action={table:name,deleted:0,started:new Date().toISOString()};run.actions.push(action);
  try{
   const all=await rows(name);
   const eligible=r=>{
    if(SHOW.has(name))return true;
    if(!loggingReady)return false;
    if(name==='heartbeat')return completed(r); // Current heartbeat rows have no completion field.
    if(name==='err-logs')return (r.fields.run||[]).length>0&&r.fields.run.every(id=>finishedParents.has(id)&&!protectedIds.has(id));
    return completed(r)&&!protectedIds.has(r.id)&&!currentRunKeys.has(r.fields['run-key']);
   };
   const selected=choose(all,eligible,event.trigger);
   for(let i=0;i<selected.length;i+=10){
    const ids=selected.slice(i,i+10).map(r=>r.id);
    try{
     const response=await api.remove(byName.get(name).id,ids),deleted=new Set((response.records||[]).filter(r=>r.deleted).map(r=>r.id));
     action.deleted+=ids.filter(id=>deleted.has(id)).length;
     const missing=ids.filter(id=>!deleted.has(id));if(missing.length)run.errors.push({table:name,record_ids:missing,error:'Airtable did not confirm deletion'});
    }
    catch(e){run.errors.push({table:name,record_ids:ids,error:e.message});}
   }
  }catch(e){run.errors.push({table:name,error:e.message});}
  const errors=run.errors.filter(e=>e.table===name||(LOGS.has(name)&&e.table==='logging'));
  action.completed=new Date().toISOString();action.result=errors.length?'Error':'Success';
  try{await api.summary(control.id,{started:action.started,completed:action.completed,result:action.result,'records-deleted':action.deleted,'run-id':run.run_id,error:errors.map(e=>JSON.stringify(e)).join('\n')});}
  catch(e){run.errors.push({table:name,operation:'cleanupV2 update',error:e.message});}
 }
 // Local cleanup runs here, never in collection/linking and never waits for them.
 try{
  const selected=new Set(controls.filter(r=>r.fields.cleanup==='checked').map(r=>r.fields['\uFEFFName']));
  if(loggingReady&&selected.has('run-logs')&&selected.has('err-logs'))run.local=pruneLocal(localLogs(runtime).families,event.trigger);
 }catch(e){run.errors.push({table:'local logs',error:e.message});}
 run.completed=new Date().toISOString();run.result=run.errors.length?'Error':'Success';
 fs.appendFileSync(path.join(stateDir,'history.jsonl'),JSON.stringify(run)+'\n');
 fs.writeFileSync(path.join(stateDir,'simple-last-run.json'),JSON.stringify(run,null,2));
 if(run.result==='Success')fs.writeFileSync(stateFile,JSON.stringify({...state,event_id:event.event_id,show:event.show_id,calendar_day:event.calendar_day,completed:run.completed,run_id:run.run_id,last_log_check:event.issued_at,...(event.trigger==='calendar-day'?{daily_clear_date:event.calendar_day}:{})}));
 return run;
}
async function main(){
 const records=read(path.join(__dirname,'..','.git','ringstatus-control','wef-records-shows-transition-alert-state.json'),{});
 const state=read(path.join(__dirname,'runtime','cleanup','cleanup-state.json'),{});
 const daily=alertFor(state,state.show);
 const alert=records.cleanup_alert;
 const pending=Date.parse(alert?.issued_at)>Date.parse(state.last_log_check||state.completed||0)?alert:undefined;
 // Preserve a pending new-show alert; it already clears completed logs as well.
 const event=pending?.trigger==='new-show'&&pending.show_id!==state.show
  ?pending:daily?.trigger==='calendar-day'?daily:pending;
 const result=await cleanup(event);console.log(JSON.stringify(result));if(result.result==='Error')process.exitCode=1;
}
if(require.main===module)main().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={cleanup,choose,localLogs,pruneLocal,completed,main,alertFor};
