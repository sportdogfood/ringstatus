"use strict";
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const settings=require('./settings');
// Preserve the proven discovery helper and its two-request validation contract.
const {discoverFocus}=require('../wef-schedules/collect');
const {archiveFile}=require('../wef-tenants/archive');
async function fetchSource(url){
 const u=new URL(url);
 if(u.origin!=='https://wef-schedules-decoded.gombcg.workers.dev'||!['/ring','/schedule'].includes(u.pathname))throw Error('Unapproved show source');
 const {fetchTextWithConfiguredTransport}=require('../wef-schedules/lib/sgl_fetch_adapter');
 const result=await fetchTextWithConfiguredTransport(url,target=>require('./source-fetch').fetchSourceText(target),{env:settings});
 if(!result.response.ok)throw Error('Show source HTTP '+result.response.status);
 if(result.response.headers?.get('x-rs-decoded')!=='1')throw Error('Show source decoding not confirmed');
 const payload=JSON.parse(result.text);if(!payload||typeof payload!=='object'||Array.isArray(payload)||payload._t)throw Error('Invalid show source payload');return payload;
}
async function runShows({api=require('./airtable').airtable(),fetchJson=fetchSource,clock=()=>new Date().toISOString(),archive=(kind,value,scope)=>archiveFile(path.join(settings.RUNTIME,'history'),kind,value,scope),statePath=path.join(settings.RUNTIME,'focus.json'),policyPath=path.join(path.dirname(statePath),'discovery-policy.json'),archiveRoot=path.join(settings.RUNTIME,'history'),logLimit=1000,publishDays=require('./show-days').publishDays,readSummaries,dayStopPath=path.join(settings.RUNTIME,'show-days-stopped.json')}={}){
 const report={ok:false,run_id:crypto.randomUUID(),started:clock(),rows:0,errors:[],archives:[],telemetry_errors:[]};
 const audit=require('./audit').createAudit({api,runId:report.run_id,clock});let runRecord=null,errorLogSaved=false;
 let log={'run-id':report.run_id,started:report.started,result:'Running',summary:'Scheduled Shows run started',errors:'',archives:'','archive-count':0},ready=false;
 async function keep(kind,value,scope={}){audit.stage('archive',kind==='default-ring'?'default-ring':kind==='default-schedule'?'default-schedule':null);try{const e=await archive(kind,value,scope);report.archives.push(e);return e;}catch(e){report.archives.push({kind,outcome:'Failed',error:e.message});throw e;}}
 async function save(){log['archive-count']=report.archives.length;log.archives=report.archives.map(e=>`${e.at||clock()}; ${e.kind}; ${e.outcome}; ${e.records??0} records; ${e.file||'no file'}; SHA256 ${e.sha256||'unverified'}${e.error?'; '+e.error:''}`).join('\n');const records=await api.upsert('run-logs','run-id',[log]);runRecord=records[0].id;}
 try{
  const logs=await api.list('run-logs');if(logs.length>=logLimit){const old=logs.filter(r=>r.fields.result!=='Running'&&!(r.fields['err-logs']||[]).length).sort((a,b)=>String(a.fields.started).localeCompare(String(b.fields.started)))[0];if(old){await keep('run-logs',[old]);await api.call(settings.BASE_ID,'run-logs','PATCH',{records:[{id:old.id,fields:{...log,completed:null,rows:null,'show-id':null,sources:[]}}]});}else report.log_retention='All log slots protected; appended a new run record';}
  await save();ready=true;
  const policy=require('./discovery-policy');
  audit.stage('show-authority');
  const previous=await api.list('shows');
  let savedShow=policy.canonicalShow(previous);
  const before=savedShow.fields,instant=clock();
  const checkpoint=fs.existsSync(policyPath)?JSON.parse(fs.readFileSync(policyPath,'utf8')):null;
  const plan=policy.discoveryPlan(instant,checkpoint,before.show_id);
  const atomic=require('../wef-schedules/collect').atomicReport;
  report.discovery={timezone:policy.timezone,show:plan.due?'Due':'Skipped',show_day:'Retained'};
  let wanted={...before},payload;
  if(plan.due){
   // Save the attempted slot first so a crash cannot turn five-minute wakes into source polling.
   plan.state={...plan.state,next:new Date(Date.parse(instant)+3*3600000).toISOString(),failed:true};
   await atomic(policyPath,plan.state);
   let ring;
   const observe=async url=>{const key=new URL(url).pathname==='/ring'?'default-ring':'default-schedule';const p=await audit.observe(key,()=>fetchJson(url));if(key==='default-ring'){ring=p;await keep('default-ring',p);audit.stage('source-validation','default-ring');}return p;};
   const found=await discoverFocus({statePath,now:instant,io:{fetchJson:observe}});
   audit.stage('show-validation','default-schedule');
   const f=found.show.fields,p=found.payload,meta=p.show||{},sid=Number(f.show_id),selected=f.sgl_default_day;
   if(!selected||!Number.isSafeInteger(Number(meta.facility_id))||Number(meta.facility_id)<=0||!f.show_name||Number(ring?.show?.show_id)!==sid)throw Error('Incomplete canonical show metadata');
   const payloadArchive=await keep('default-schedule',p,{shows:[sid],dates:[selected]});
   const same=sid===before.show_id;
   wanted={'show-key':`15|${sid}`,show_id:sid,customer_id:15,facility_id:Number(meta.facility_id),show_name:f.show_name,start_date:f.start_date,end_date:f.end_date,selected_date:same?before.selected_date:selected,available_dates:require('../wef-schedules/collect').selectedDays(f).sort().join(', '),is_default:true,last_checked:clock(),'default-payload-file':payloadArchive.file,'default-payload-sha256':payloadArchive.sha256,source_selected_date:selected,selection_basis:same?(before.selection_basis||'source'):'source',selection_at:same?(before.selection_at||before.last_checked):clock()};
   // A retained source-based selection follows the refreshed source. Calendar selections stay independent.
   if(wanted.selection_basis==='source'){wanted.selected_date=selected;if(!same||selected!==before.selected_date)wanted.selection_at=clock();}
   payload=p;report.discovery.show='Checked';
   plan.state.failed=false;
   if(sid!==plan.state.baseline){plan.state.complete=true;plan.state.cycle=policy.sunday(instant);plan.state.next=policy.nextSunday(instant);}
  }else{
   if(plan.state.failed)throw Error('Previous new-show check failed; awaiting next three-hour slot');
   payload=require('../wef-schedules/show-source').verifiedPayload(before,archiveRoot);
   wanted.source_selected_date=before.source_selected_date||require('../wef-schedules/collect').isoDate(payload.show_date??payload.showDate);
   wanted.selection_basis=before.selection_basis||'source';wanted.selection_at=before.selection_at||before.last_checked;
  }
  const selected=policy.calendarSelection(wanted,instant);
  if(selected!==wanted.selected_date){wanted.selected_date=selected;wanted.selection_basis='calendar';wanted.selection_at=instant;report.discovery.show_day='Advanced';}
  const meta=payload.show||{};
  if(Number(meta.show_id)!==wanted.show_id||Number(meta.facility_id)!==wanted.facility_id||require('../wef-schedules/collect').isoDate(meta.start_date)!==wanted.start_date||require('../wef-schedules/collect').isoDate(meta.end_date)!==wanted.end_date||meta.show_name!==wanted.show_name)throw Error('Canonical metadata/payload mismatch');
  if(wanted.selected_date<wanted.start_date||wanted.selected_date>wanted.end_date||!policy.selectionMatches(wanted,payload))throw Error('Canonical selection evidence invalid');
  wanted.authority_checked_at=clock();
  // Never send read-only reciprocal link fields back as updates.
  const allowed=['show-key','show_id','customer_id','facility_id','show_name','start_date','end_date','selected_date','available_dates','is_default','last_checked','default-payload-file','default-payload-sha256','source_selected_date','selection_basis','selection_at','authority_checked_at'];
  wanted=Object.fromEntries(allowed.map(k=>[k,wanted[k]]));
  audit.stage('show-publication');
  if(plan.due||report.discovery.show_day==='Advanced')await keep('shows-before',previous);
  savedShow=(await api.upsert('shows','show-key',[wanted]))[0];
  const stale=previous.filter(r=>r.fields.is_default&&r.fields['show-key']!==wanted['show-key']&&Number(r.fields.customer_id)===15).map(r=>({'show-key':r.fields['show-key'],is_default:false}));if(stale.length)await api.upsert('shows','show-key',stale);
  await atomic(policyPath,plan.state);
  report.ok=true;report.discovery_ok=true;report.rows=1;report.show_id=wanted.show_id;report.selected_date=wanted.selected_date;
  report.source_selected_date=wanted.source_selected_date;report.next_show_check=plan.state.next;
  report.summary=`Show ${wanted.show_id}; selected ${wanted.selected_date} (${wanted.selection_basis}); source date ${wanted.source_selected_date}, last checked ${wanted.last_checked}. New-show check ${report.discovery.show}; next ${plan.state.next}. Calendar day ${report.discovery.show_day}. No downstream jobs triggered.`;
  try{
   audit.stage('day-priority');
   report.day_priority=await require('./day-priority').updatePriority({api,show:savedShow,now:clock(),discovered:report.discovery.show==='Checked',statePath:path.join(path.dirname(statePath),'day-priority.json')});
   report.summary+=' Day-priority: '+report.day_priority.status+'.';
  }catch(error){audit.capture(error);report.ok=false;report.errors.push('Day-priority: '+error.message);report.summary+=' Day-priority failed: '+error.message;}
  try{
   audit.stage('day-publication');
   if(fs.existsSync(dayStopPath))throw Error('Day publication stopped after prior failure; owner authorization required');
   report.show_days=await publishDays({api,show:savedShow,clock,readSummaries:async show=>{const value=await audit.observe('season-counts',()=>(readSummaries||require('./day-source').reader(api))(show));audit.stage('day-publication');return value;}});
   if(!report.show_days.ok)throw Error(report.show_days.errors.join('; '));
   report.summary+=' Show-days: '+report.show_days.days.map(d=>d.date+' '+d.status).join(', ')+'.';
  }catch(error){
   audit.capture(error);report.ok=false;report.errors.push('Show-days: '+error.message);report.summary+=' Show authority retained; show-days publication failed: '+error.message;
   if(!fs.existsSync(dayStopPath))await require('../wef-schedules/collect').atomicReport(dayStopPath,{at:clock(),run_id:report.run_id,error:error.message});
  }

 }catch(e){audit.capture(e);report.ok=false;report.errors.push(e.message);report.summary='Discovery failed; last verified show retained. '+e.message;}
 // Telemetry writes occur after business work; a telemetry failure does not
 // erase discovery or count publication already completed.
 try{audit.stage('source-log');log.sources=await audit.saveSources();}catch(e){audit.capture(e);report.telemetry_errors.push('Source logging: '+e.message);}
 try{report.error_records=await audit.saveErrors(runRecord);errorLogSaved=true;}catch(e){report.telemetry_errors.push('Error logging: '+e.message);}
 report.observability={...audit.snapshot(),ok:report.telemetry_errors.length===0,logging_errors:[...report.telemetry_errors]};
 report.completed=clock();
 if(ready)try{audit.stage('completion-log');log={...log,completed:report.completed,result:report.ok?'Success':report.discovery_ok?'Partial':'Failed','show-id':report.show_id??null,rows:report.rows,summary:report.summary+(report.telemetry_errors.length?' Telemetry incomplete: '+report.telemetry_errors.join('; '):''),errors:[...report.errors,...report.telemetry_errors].join('\n')};await save();await api.upsert('heartbeat','heartbeat-key',[{'heartbeat-key':'WEF-default','checked-at':report.completed,'show-id':report.show_id??null,'selected-date':report.selected_date??null,result:report.ok?'Success':report.discovery_ok?'Partial':'Failed',summary:report.summary,errors:report.errors.join('\n')}]);}catch(e){audit.capture(e);report.ok=false;report.errors.push('Completion evidence: '+e.message);report.telemetry_errors.push('Completion logging: '+e.message);if(errorLogSaved)try{report.error_records=await audit.saveErrors(runRecord);}catch(logError){report.errors.push('Error logging: '+logError.message);}report.observability={...audit.snapshot(),ok:report.telemetry_errors.length===0,logging_errors:[...report.telemetry_errors]};}
 return report;
}
module.exports={runShows,fetchSource};
