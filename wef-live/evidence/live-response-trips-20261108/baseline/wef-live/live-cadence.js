'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const settings=require('./settings'),model=require('./model'),activity=require('./activity');
const easternValue=value=>typeof value==='string'&&/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(value)?value:null;
function inWindow(now,eastern){
 const time=easternValue(eastern)?.slice(11,16)||new Intl.DateTimeFormat('en-GB',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(new Date(now));
 return time>='05:00'&&time<='19:00';
}
async function context(api){
 const defaults=(await api.list('shows',settings.SHOWS_BASE)).filter(r=>Number(r.fields.customer_id)===15&&r.fields.is_default===true);
 if(defaults.length!==1)throw Error('Expected one default wef-shows record');
 const f=defaults[0].fields,show=model.integer(f.show_id,'show ID');
 const days=await api.list('show-days',settings.SHOWS_BASE);
 const matches=days.filter(r=>r.fields['show-day-key']?.startsWith(`15|${show}|`)&&r.fields['day-priority']!==null&&r.fields['day-priority']!==undefined&&r.fields['day-priority']!==''&&Number(r.fields['day-priority'])===0);
 if(matches.length!==1)throw Error('Expected one canonical day-priority 0');
 const date=model.day(matches[0].fields.date)||model.day(matches[0].fields['show-day-key'].split('|')[2]);
 if(!date||matches[0].fields['show-day-key']!==`15|${show}|${date}`)throw Error('Invalid canonical day-0 identity');
 const published=matches[0].fields['eastern-time (from show-times)'];
 const eastern_time=easternValue(Array.isArray(published)?published[0]:published);
 return{show,customer:15,date,authority:defaults[0].id,checked_at:f.last_checked,active:true,...(eastern_time?{eastern_time}:{})};
}
function decision(status,scope,now){
 const today=easternValue(scope?.eastern_time)?.slice(0,10)||model.localDay(now);
 if(status===false)return{collect:false,minutes:60,reason:'live-false',today};
 if(status!==true)return{collect:false,minutes:3,reason:'live-status-unavailable',today};
 if(!scope)return{collect:false,minutes:60,reason:'show-context-unavailable',today};
 if(scope.date!==today)return{collect:false,minutes:60,reason:'priority-date-mismatch',today};
 if(!inWindow(now,scope?.eastern_time))return{collect:false,minutes:60,reason:'outside-collection-window',today};
 return{collect:true,minutes:3,reason:'live-today',today};
}
function write(file,value){const tmp=file+'.'+process.pid+'.tmp';fs.writeFileSync(tmp,JSON.stringify(value,null,2));fs.renameSync(tmp,file);}
async function check({api=require('./airtable').airtable(),getStatus=activity.readStatus,clock=()=>new Date().toISOString(),runtime=settings.RUNTIME}={}){
 const now=clock(),ms=Date.parse(now);if(!Number.isFinite(ms))throw Error('Invalid Live check time');
 const stateFile=path.join(runtime,'live-cadence-state.json');let previous,issues=[];
 try{previous=JSON.parse(fs.readFileSync(stateFile,'utf8'));}catch(e){if(e.code!=='ENOENT')issues.push({message:'Live cadence state: '+e.message});}
 const last=Date.parse(previous?.checked_at);let due=Date.parse(previous?.next_check_at);
 // Failed status checks use the existing three-minute task, including older hourly checkpoints.
 if(previous?.reason==='live-status-unavailable')due=Math.floor(last/60000)*60000+3*60000;
 // Known eligible activity can start at05:00; after19:00 wait an hour, not another three minutes.
 const entersWindow=previous?.reason==='outside-collection-window'&&previous.live_status===true&&previous.date===model.localDay(now)&&inWindow(now);
 if(previous?.reason==='live-today'&&!inWindow(now))due=Math.floor(last/60000)*60000+3600000;
 if(!entersWindow&&Number.isFinite(last)&&last<=ms&&Number.isFinite(due)&&due>ms&&due<=last+3600000)return{due:false,next_check_at:new Date(due).toISOString()};
 const r={due:true,run_id:crypto.randomUUID(),lane:'live-check',started:now,ok:true,issues,logging_errors:[],stage_errors:[]};
 try{r.live_status=activity.statusValue(await getStatus({customer:15}));}catch(e){issues.push({message:'Live-status check: '+e.message});}
 try{r.scope=await context(api);}catch(e){issues.push({message:'Shows context: '+e.message});}
 Object.assign(r,decision(r.live_status,r.scope,now));
 if(r.reason==='priority-date-mismatch')issues.push({message:`Warning: live status is true but today ${r.today} differs from Shows priority-0 date ${r.scope.date} for show ${r.scope.show}`});
 // Match scheduled minutes; launch jitter must not turn a three-minute interval into six.
 r.next_check_at=new Date(Math.floor(ms/60000)*60000+r.minutes*60000).toISOString();r.completed=clock();
 try{fs.mkdirSync(runtime,{recursive:true});write(stateFile,{checked_at:now,next_check_at:r.next_check_at,live_status:r.live_status??null,show:r.scope?.show,date:r.scope?.date,reason:r.reason});}
 catch(e){issues.push({message:'Live cadence state save: '+e.message});}
 async function log(table,key,fields){
  for(const f of fields){try{
   const d=await api.call(settings.BASE_ID,table,'POST',{records:[{fields:f}]});
   if(d.records?.length!==1||d.records[0].fields?.[key]!==f[key])throw Error('Incomplete log response');
  }catch(e){r.logging_errors.push(table+': '+e.message);}}
 }
 if(!r.collect){
  const summary=JSON.stringify({lane:'live-check',live_status:r.live_status??null,today:r.today,priority_0_date:r.scope?.date,reason:r.reason,next_check_at:r.next_check_at,collection:'Skipped'});
  const common={'run-id':r.run_id,started:r.started,completed:r.completed,result:'Skipped',summary,errors:[...new Set(issues.map(e=>e.message))].join('\n').slice(0,60000),...(r.scope?{'show-id':r.scope.show,date:r.scope.date}:{})};
  await log('heartbeat','heartbeat-key',[{...common,'heartbeat-key':r.run_id,'checked-at':r.completed}]);
  await log('run-logs','run-id',[common]);
 }
 const errors=[...issues,...r.logging_errors.map(message=>({message:'Live check logging: '+message}))];
 try{await require('./error-log').report({api,runtime,lane:'live-check',rows:errors.map((e,i)=>({'error-key':r.run_id+'|'+i,'run-id':r.run_id,'occurred-at':r.completed,...(r.scope?{'show-id':r.scope.show,date:r.scope.date}:{}),message:e.message}))});}catch(e){r.logging_errors.push('errs-log: '+e.message);}
 try{write(path.join(runtime,'live-check-'+r.run_id+'.json'),r);write(path.join(runtime,'live-check-last-run.json'),r);}catch(e){r.logging_errors.push('Local check receipt: '+e.message);}
 return r;
}
module.exports={check,decision,context,inWindow};
