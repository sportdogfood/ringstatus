"use strict";
const path=require('node:path'),crypto=require('node:crypto');
const settings=require('./settings'),model=require('./model'),{archiveFile}=require('./runtime-utils');
const hash=v=>crypto.createHash('sha256').update(JSON.stringify(v)).digest('hex');
const fields=['entry-key','show-id','date','entry-id','class-id','horse-id','horse-name','rider-id','rider-name','class-name','ring-number','status','observation-status','last-seen','schedule-key','schedule-keys','schedules','schedule-link-status','schedule-link-basis','group-link-status'];
const project=row=>Object.fromEntries(fields.filter(k=>row[k]!=null).map(k=>[k,row[k]]));
async function runEntries({api=require('./airtable').airtable(),readInput,clock=()=>new Date().toISOString(),assess=require('./freshness').evaluate,archive=(kind,value,scope)=>archiveFile(path.join(settings.RUNTIME,'history'),kind,value,scope),logLimit=1000}={}){
 const report={ok:false,run_id:crypto.randomUUID(),started:clock(),rows:0,accepted_rows:0,collections:[],errors:[],link_issues:[],archives:[],source_fetches:0,tenant_reads:0};
 let show,log={'run-id':report.run_id,started:report.started,result:'Running',summary:'Linking accepted Data records.',errors:'','archive-count':0,archives:''},logReady=false;
 async function keep(kind,value,scope={}){try{const event=await archive(kind,value,scope);report.archives.push(event);return event;}catch(e){report.archives.push({kind,outcome:'Failed',error:e.message});throw e;}}
 async function saveLog(){log={...log,'archive-count':report.archives.length,archives:report.archives.map(a=>`${a.at||clock()}: ${a.kind} ${a.outcome}; ${(a.dates||[]).join(',')}; ${a.records??0} records; ${a.file||'no file'}; SHA256 ${a.sha256||'unverified'}${a.error?'; '+a.error:''}`).join('\n')};await api.upsert('run-logs','run-id',[log]);}
 try{
  const logs=await api.list('run-logs');
  if(logs.length>=logLimit){const oldest=logs.filter(r=>r.fields.result!=='Running').sort((a,b)=>String(a.fields.started).localeCompare(String(b.fields.started)))[0];if(!oldest)throw Error('No finished run-log slot available');await keep('run-logs',[oldest]);await api.call(settings.BASE_ID,'run-logs','PATCH',{records:[{id:oldest.id,fields:{...log,completed:null,'show-id':null,rows:null}}]});}
  await saveLog();logReady=true;
  const shows=await api.list(settings.SHARED_SHOWS_TABLE,settings.SHOWS_BASE_ID);
  const authority=model.scope(shows,clock());show=authority.show;
  report.show_id=Number(show.show_id);report.show_source={base:settings.SHOWS_BASE_ID,record:authority.showRecord,checked_at:show.last_checked};log['show-id']=report.show_id;
  const input=await (readInput||require('./data-input').reader(api))(shows.find(r=>r.id===authority.showRecord));
  if(!input.head||!Array.isArray(input.classes))throw Error('Accepted Data input unavailable');
  if(!input.classes.length)throw Error('Accepted Data revision contains no retained class observations; availability unknown, prior records preserved');
  report.data_revision=input.head;report.producer_attempts=input.attempts||[];
  let schedules=[],health=[];
  try{
   const [source,local,dated]=await Promise.all([api.list('schedules',settings.SHARED_BASE_ID),api.list(settings.SYNC_TABLE),api.list('show-collection',settings.SHARED_BASE_ID)]);
   schedules=model.schedulesFor(show,source,local);health=dated;
  }catch(error){report.link_issues.push({kind:'schedule-reference',reason:error.message});}
  const [oldEntries,oldCollections]=await Promise.all([api.list('entries'),api.list('collections')]);
  const before=new Map(oldEntries.map(r=>[r.fields['entry-key'],r]));if(before.size!==oldEntries.length)throw Error('Duplicate existing participation key');
  const plans=[],wanted=new Map();
  for(const item of input.classes){
   const date=String(item.payload?.class_related_data?.date||'').slice(0,10),ring=item.payload?.class_related_data?.ring,cid=item.payload?.class?.class_id;
   const id=['data',show.customer_id,show.show_id,date,ring,cid].join('|');
   if(plans.some(p=>p.id===id))throw Error('Duplicate retained class scope');
   const plan={id,item,date,rows:[],current:false,pending:new Map()};plans.push(plan);
   try{
    const freshness=assess(item,input,clock());plan.current=false;plan.canRetire=freshness.canRetire===true;plan.reason=freshness.reason;plan.affectedAttempts=freshness.affectedAttempts||[];
    const dated=health.filter(r=>Number(r.fields['show-id'])===report.show_id&&r.fields.date===date);
    const age=dated.length===1?Date.parse(clock())-Date.parse(dated[0].fields.completed):NaN;
    const validHealth=dated.length===1&&dated[0].fields.status==='complete'&&Number.isFinite(age)&&age>=-60000&&age<=1800000;
    const available=validHealth?schedules:[];
    const linked=model.linkClass(item.payload,show,available,item.observedAt,{inventory:freshness.affectedAttempts?.length?undefined:item.inventory,sourceEvidence:item.sourceEvidence});
    for(const row of linked){
     const old=before.get(row['entry-key']);
     if(old&&Date.parse(old.fields['last-seen'])>Date.parse(item.observedAt)){report.link_issues.push({'entry-key':row['entry-key'],reason:'Newer existing observation retained'});plan.rows.push(old.fields);continue;}
     const issues=['group','schedule'].filter(k=>['unresolved','Conflict'].includes(row[k+'-link-status'])).map(k=>`${k}: ${row[k+'-link-reason']}`);
     if(freshness.affectedAttempts?.length)issues.push('source: '+plan.reason);
     if(!validHealth)issues.push('schedule: dated health unavailable or stale');
     const value={...project(row),'source-trainer-ids':row['source-trainer-ids'].join(','),'link-issues':issues.join('\n')};
     // Success describes accepted observation, not freshness.
     if(row['schedule-link-status']!=='linked')value['schedule-link-basis']='';
     let prior;try{prior=JSON.parse(old?.fields['source-json']||'null');}catch{prior={unparsed:true};}
     value['source-json']=JSON.stringify({format:'wef-entry-link-v1',revision:input.head,scope:item.scopeKey,sourceAsOf:item.observedAt,sourceEvidence:item.sourceEvidence,currentness:'not-asserted',latestApplicableAttempts:freshness.affectedAttempts,inventoryEvidence:item.inventory?{revision:item.inventory.revision,observedAt:item.inventory.observedAt,collections:item.inventory.collections}:null,trips:row['source-trips'],groupId:row['source-group-id']??null,groupEvidence:row['group-link-evidence']??null,seasonReference:'unavailable',priorLink:row['schedule-link-status']==='linked'?undefined:(prior?.priorLink||{keys:old?.fields['schedule-keys'],scalar:old?.fields['schedule-key'],observedAt:old?.fields['last-seen']})});
     if(wanted.has(value['entry-key'])||plan.pending.has(value['entry-key']))throw Error('Repeated participation across class scopes');
     plan.pending.set(value['entry-key'],value);plan.rows.push(value);
     if(issues.length)report.link_issues.push({'entry-key':row['entry-key'],reasons:issues});
    }
    // Retire only membership previously established by this Data scope, never
    // old feed-only rows or all entries absent from the current projection.
    if(plan.canRetire){const oldScope=oldCollections.filter(r=>r.fields['collection-id']===id);if(oldScope.length>1)throw Error('Duplicate previous class scope');
     for(const ref of oldScope[0]?.fields.entries||[]){const old=oldEntries.find(r=>r.id===ref);if(!old)throw Error('Missing previous scope member');
      if(old.fields.date!==date||Number(old.fields['class-id'])!==Number(cid)||Number(old.fields['show-id'])!==report.show_id)throw Error('Previous scope identity mismatch');
      if(linked.some(r=>r['entry-key']===old.fields['entry-key'])||Date.parse(old.fields['last-seen'])>=Date.parse(item.observedAt))continue;
      plan.pending.set(old.fields['entry-key'],{'entry-key':old.fields['entry-key'],status:'dropped','observation-status':'dropped','schedule-link-status':'unresolved','schedule-link-basis':'','group-link-status':'unresolved','link-issues':'Absent from a later complete accepted class observation; prior links retained as history.'});
     }
    }
    plan.status='complete';for(const [key,value]of plan.pending)wanted.set(key,value);report.rows+=plan.rows.length;report.accepted_rows+=plan.rows.length;
   }catch(error){plan.rows=[];plan.status='failed';plan.reason=error.message;report.errors.push(id+': '+error.message);}
   report.collections.push({scope:id,status:plan.status,rows:plan.rows.length,source_as_of:item.observedAt,reason:plan.reason});
  }
  await keep('entries-before',oldEntries,{shows:[report.show_id],dates:authority.dates});
  const saved=wanted.size?await api.upsert('entries','entry-key',[...wanted.values()]):[];
  const updates=[];
  for(const plan of plans){const common={'collection-id':plan.id,'source-kind':'retained-class','source-id':plan.item.scopeKey,'show-id':report.show_id,date:plan.date,started:report.started,completed:clock(),status:plan.status,message:JSON.stringify({reason:plan.reason,revision:input.head,sourceAsOf:plan.item.observedAt,sourceEvidence:plan.item.sourceEvidence,currentness:'not-asserted',latestApplicableAttempts:plan.affectedAttempts||[]})};
   if(plan.status!=='failed'){const event=await keep('retained-class-dataset',plan.rows,{shows:[report.show_id],dates:[plan.date]});Object.assign(common,{'row-count':plan.rows.length,'dataset-id':input.head,fingerprint:hash(plan.rows),'archive-file':event.file,entries:plan.rows.map(r=>{const s=saved.find(x=>x.fields['entry-key']===r['entry-key'])||before.get(r['entry-key']);if(!s)throw Error('Saved class membership missing');return s.id;})});}
   updates.push(common);
  }
  if(updates.length)await api.upsert('collections','collection-id',updates);
  report.ok=report.errors.length===0;report.summary=`Entries linking: ${report.rows} observed rows; ${report.accepted_rows} accepted source rows (currentness not asserted); ${report.link_issues.length} link issues; Data ${input.head}. No source acquisition or tenant selection.`;
 }catch(error){report.errors.push(error.message);report.summary='Linking stopped; prior unverified scopes preserved. '+error.message;report.ok=false;}
 report.completed=clock();report.result=report.ok?(report.link_issues.length||report.collections.some(c=>c.status==='historical')?'Partial':'Success'):'Failed';
 if(logReady){log={...log,completed:report.completed,result:report.result,rows:report.rows,summary:report.summary,errors:report.errors.join('\n')};try{await saveLog();await api.upsert('heartbeat','heartbeat-key',[{'heartbeat-key':'wef-entries','checked-at':report.completed,'selected-date':show?.selected_date||null,'show-id':show?Number(show.show_id):null,result:report.result,summary:report.summary,errors:report.errors.join('\n')}]);}catch(error){report.ok=false;report.result='Failed';report.errors.push('Completion evidence failed: '+error.message);}}
 return report;
}
module.exports={runEntries};
