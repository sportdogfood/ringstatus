"use strict";
const fs=require('node:fs'),path=require('node:path'),{execFile}=require('node:child_process');
const {api:makeApi,TIMING,SHOWS,LIVE,DATA_TABLES:T,CLASS_TIMING,CLASS_ANSWERS}=require('./api');
const {POLICY,hash,ageReason,evaluate}=require('./pace');
const {archive,verifyRows,collectionToken}=require('./runner');
const {prepare:prepareClasses}=require('./class-worker');
const {readLiveClasses}=require('./class-live-source');
const purge=require('./purge'),ROOT=path.resolve(__dirname,'..');
function defer(message){const error=Error(message);error.deferred=true;return error;}
function selectShow(rows,now){
  const defaults=rows.filter(r=>r.fields.is_default===true);if(defaults.length!==1)throw defer('Canonical default show unresolved');
  const s=defaults[0],f=s.fields,customer=Number(f.customer_id),show=Number(f.show_id);
  if(!Number.isSafeInteger(customer)||customer<=0||!Number.isSafeInteger(show)||show<=0||f['show-key']!==`${customer}|${show}`||ageReason(f.last_checked,now,1800000))throw defer('Canonical show identity/freshness invalid');
  if(![f.start_date,f.end_date,f.selected_date].every(purge.validDay)||f.selected_date<f.start_date||f.selected_date>f.end_date)throw defer('Canonical dates invalid');
  return{record:s,context:{'show-key':f['show-key'],'show-id':show,'customer-id':customer,'start-date':f.start_date,'end-date':f.end_date,'selected-date':f.selected_date}};
}
function sameAuthority(left,right){return left.record.id===right.record.id&&hash(left.context)===hash(right.context);}
async function writeAnswers(api,writes,base=TIMING,table=T.answers){
  if(!writes.length)return [];
  const ids=new Map();
  for(let i=0;i<writes.length;i+=10){
    const batch=writes.slice(i,i+10),result=await api.upsert(table,'answer-key',batch);
    if(!Array.isArray(result?.records)||result.records.length!==batch.length)throw Error('Timing write response coverage mismatch');
    verifyRows(result.records,batch,'answer-key');
    for(const row of result.records){
      if(!/^rec[A-Za-z0-9]{14}$/.test(row.id)||ids.has(row.id))throw Error('Timing write response identity mismatch');
      ids.set(row.id,row.fields['answer-key']);
    }
  }
  // Independent GET readback; batch only the query, never the evaluated scope.
  // list() follows every offset, including an unexpected duplicate response page.
  const saved=[],recordIds=[...ids.keys()];
  for(let i=0;i<recordIds.length;i+=100){
    const batch=recordIds.slice(i,i+100);
    saved.push(...await api.list(base,table,'OR('+batch.map(id=>`RECORD_ID()='${id}'`).join(',')+')'));
  }
  if(saved.some(row=>!ids.has(row.id)||ids.get(row.id)!==row.fields['answer-key'])||new Set(saved.map(row=>row.id)).size!==saved.length)throw Error('Timing readback identity mismatch');
  verifyRows(saved,writes,'answer-key');
  if(saved.length!==writes.length)throw Error('Timing readback coverage mismatch');
  return saved;
}
function pointer(context){
  let p;try{p=JSON.parse(fs.readFileSync(path.join(ROOT,'wef-data/runtime/handoff','head-'+context['show-key'].replace('|','-')+'.json'),'utf8'));}
  catch(error){if(error.code==='ENOENT')throw defer('Current-show Data handoff unavailable');throw error;}
  const fence=['show-key','start-date','end-date','selected-date'].map(k=>context[k]);
  if(hash(p.authority)!==hash(fence)||p['show-key']!==context['show-key'])throw defer('Waiting for current Data authority');
  return p;
}
function loadData(context,deadline){
  return new Promise((resolve,reject)=>{
    const remaining=deadline-Date.now()-18000;if(remaining<2000){const e=Error('Data input budget deferred');e.deferred=true;reject(e);return;}
    const child=execFile(path.join(ROOT,'wef-data/.venv/Scripts/python.exe'),['-B',path.join(__dirname,'data-input.py')],{
      windowsHide:true,timeout:remaining,maxBuffer:8*1024*1024,env:{SystemRoot:process.env.SystemRoot,WINDIR:process.env.WINDIR,TEMP:process.env.TEMP,TMP:process.env.TMP,PYTHONDONTWRITEBYTECODE:'1'}
    },(error,stdout,stderr)=>{
      if(error){const e=Error(error.killed?'Data input budget deferred':String(stderr||'Data retained input unavailable').trim());e.deferred=Boolean(error.killed)||e.message.includes('pointer changed during read');reject(e);return;}
      try{resolve(JSON.parse(stdout));}catch{reject(Error('Data input projection unreadable'));}
    });child.stdin.end(JSON.stringify(context));
  });
}
async function run({api,classApi,clock=()=>new Date().toISOString(),deadline=Date.now()+160000,runId=require('node:crypto').randomUUID(),readData=loadData,readPointer=pointer,readArchive=archive,readLive=readLiveClasses,prepareClassBatch=prepareClasses,resumeRunId=process.env.WEF_TIMING_RESUME_RUN_ID}={}){
  api=api||makeApi({token:process.env.WEF_TIMING_AIRTABLE_TOKEN,deadline,lane:'data_live'});
  classApi=classApi||makeApi({token:process.env.WEF_TIMING_AIRTABLE_TOKEN,deadline,lane:'class_live'});
  const startMs=Date.now(),started=clock(),report={run_id:runId,started,ok:false,errors:[],source_lane:'data_live',destination:TIMING,
    counts:{evaluated:0,written:0,unchanged:0,deferred:0,available:0,partial:0,unavailable:0,purged:0,purge_deferred:0}};
  let state={version:1,baselines:{},cursor:''},checkpointLoaded=false,resumeBlocked=false,stage='checkpoint_read';
  report.issues=[];report.counts.groups_skipped=0;report.counts.classes_skipped=0;
  const issue=(kind,identity,message)=>{report.issues.push({stage,kind,identity,message});report.errors.push(kind+': '+message);report.counts[kind==='class'?'classes_skipped':'groups_skipped']++;};
  async function save(outcome,error=''){
    const body=JSON.stringify(state);
    const f={'checkpoint-key':'ring-pace-data-runner','run-id':runId,'started-at':started,'completed-at':clock(),'last-outcome':outcome,'duration-ms':Date.now()-startMs,
      'counts-json':JSON.stringify(report.counts),'state-json':body,'error-summary':error,'policy-version':POLICY};
    await api.upsert(T.checkpoint,'checkpoint-key',[f]);verifyRows(await api.list(TIMING,T.checkpoint),[f],'checkpoint-key');
  }
  async function publish(transport,writes,base,table){
    if(!writes.length)return [];
    stage=base===TIMING?'ring_publication':'class_publication';
    state.pending_write={base,table,run_id:runId,answers:writes.map(f=>({key:f['answer-key'],hash:f['content-hash']}))};
    await save('writing');
    const rows=await writeAnswers(transport,writes,base,table);
    delete state.pending_write;await save('written');return rows;
  }
  try{
    const checkpoints=await api.list(TIMING,T.checkpoint),old=checkpoints.filter(r=>r.fields['checkpoint-key']==='ring-pace-data-runner');
    if(old.length>1)throw Error('Duplicate Data pace checkpoint');
    if(old.length){state=JSON.parse(old[0].fields['state-json']);if(state.version!==1)throw Error('Data pace checkpoint version unsupported');
      if(state.pending_write||state.pending_purge||state.class_retention?.pending_purge){resumeBlocked=true;throw Error('Uncertain write or retention intent requires owner review');}
      if(old[0].fields['last-outcome']==='failed'&&!state.retry_safe&&old[0].fields['run-id']!==resumeRunId){resumeBlocked=true;throw Error('Data pace checkpoint requires owner review');}}
    checkpointLoaded=true;stage='source_read';
    const selected=selectShow(await api.list(SHOWS,'shows'),clock()),{record:show,context}=selected,sid=context['show-id'],customer=context['customer-id'];
    if(context['selected-date']!==purge.localDay(clock()))throw defer('Selected show date is not today');
    const pinned=readPointer(context);let data;
    try{data=await readData(context,deadline);}catch(error){if(!error.deferred)throw error;delete state.purge_authority;state.input_status='budget_deferred';await save('input_deferred');report.status='deferred';report.deferred_reason=state.input_status;report.completed=clock();return report;}
    if(!data.inventories?.[context['selected-date']])throw Error('Today Data inventory missing');
    const liveBefore=await api.list(LIVE,'collections',`AND({show-id}=${sid},{source-kind}='groups')`);
    const again=selectShow(await api.list(SHOWS,'shows'),clock());
    const showChanged=!sameAuthority(selected,again),afterPointer=showChanged?null:readPointer(context);
    const headChanged=afterPointer!==null&&hash(afterPointer)!==hash(pinned),proofMismatch=data.proof['run-key']!==pinned.head;
    if(showChanged||headChanged||proofMismatch){
      delete state.purge_authority;state.input_status=showChanged?'show_authority_changed':headChanged?'data_head_changed':'data_proof_mismatch';
      state.fence_diagnostic={reason:state.input_status,show_before:{id:show.id,context,last_checked:show.fields.last_checked},show_after:{id:again.record.id,context:again.context,last_checked:again.record.fields.last_checked},data_head_before:pinned.head,data_head_after:afterPointer?.head??null,pointer_hash_before:hash(pinned),pointer_hash_after:afterPointer?hash(afterPointer):null,proof_head:data.proof['run-key']};
      report.fence_diagnostic=state.fence_diagnostic;
      report.completed=clock();await save('input_deferred');report.status='deferred';report.deferred_reason=state.input_status;report.completed=clock();return report;
    }
    delete state.fence_diagnostic;
    state.input_status='verified';state.source_head=data.proof;state.source_context=context;state.source_pointer=pinned;const scopeProblems=new Map(),groups=new Map();
    stage='group_validation';
    const excludedGroups=new Set();
    for(const [day,inventory]of Object.entries(data.inventories)){
      if(!purge.validDay(day)||!inventory.inventory_complete)throw Error('Data inventory incomplete');
      const reason=ageReason(inventory.observed_at,clock(),1800000);scopeProblems.set(day,reason);
      // Validate/count identities across the full inventory before filtering rows.
      const identityCounts=new Map();
      for(const group of inventory.groups){
        const i=group?.identity;if(!i||i.customer!==customer||i.show!==sid||i.date!==day||!Number.isSafeInteger(i.ring)||i.ring<=0||!Number.isSafeInteger(i.group)||i.group<=0)throw Error('Data projected identity mismatch');
        const key=[sid,day,i.ring,i.group].join('|');identityCounts.set(key,(identityCounts.get(key)||0)+1);
      }
      for(const group of inventory.groups){
        const i=group.identity,key=[sid,day,i.ring,i.group].join('|');
        if(identityCounts.get(key)>1){excludedGroups.add(group);issue('group',i,'Data group identity repeated');continue;}
        if(!Array.isArray(group.rows)||group.rows.some(r=>!r||typeof r!=='object')||!Array.isArray(group.class_keys)){excludedGroups.add(group);issue('group',i,'Malformed group rows or class membership');continue;}
        groups.set(key,{...group,original:group,observedAt:inventory.observed_at,refs:inventory.refs,reason});
      }
    }
    const liveSamples=new Map(),blocked=new Set();state.live_input_issues=[];
    for(const c of liveBefore){
      if(c.fields.status!=='complete'||c.fields.date!==purge.localDay(clock())||ageReason(c.fields.completed,clock(),600000))continue;
      try{
        const values=readArchive(c.fields['archive-file'],path.join(ROOT,'wef-live/runtime/history'),c.fields.fingerprint),candidate=new Map();
        if(!Array.isArray(values)||values.length!==c.fields['row-count'])throw Error('Live archive coverage mismatch');
        for(const g of values){const raw=JSON.parse(g['source-json']),rn=Number(raw.ring_number),gid=g['class-group-id'];
          if(g['show-id']!==sid||g['customer-id']!==customer||g.date!==c.fields.date||g['scope-status']!=='current'||!Number.isSafeInteger(rn)||!Number.isSafeInteger(gid)||!Array.isArray(g._classes)||!g._classes.length||g._classes.some(k=>typeof k!=='string')||new Set(g._classes).size!==g._classes.length)throw Error('Live identity/membership invalid');
          if(ageReason(g['last-seen'],clock(),600000)||Date.parse(g['last-seen'])>Date.parse(c.fields.completed))throw Error('Live observation invalid');
          const key=[sid,g.date,rn,gid].join('|'),classes=[...g._classes].sort();
          if(candidate.has(key)||liveSamples.has(key)||blocked.has(key)){blocked.add(key);liveSamples.delete(key);throw Error('Duplicate Live group');}
          candidate.set(key,{at:g['last-seen'],total:g.total,gone:g.gone,classes,membership:hash({members:classes,ring:rn,gid,status:g.status}),
            ref:{base:LIVE,table:'collections',record:c.id,fingerprint:c.fields.fingerprint,observed_at:g['last-seen']}});
        }
        for(const [k,v]of candidate)liveSamples.set(k,v);
      }catch{state.live_input_issues.push('live_archive_unusable');}
    }
    if(collectionToken(liveBefore)!==collectionToken(await api.list(LIVE,'collections',`AND({show-id}=${sid},{source-kind}='groups')`))){delete state.purge_authority;state.input_status='live_changed';await save('input_deferred');report.status='deferred';report.deferred_reason=state.input_status;report.completed=clock();return report;}
    let existing=await api.list(TIMING,T.answers);const seen=new Set();for(const r of existing){if(seen.has(r.fields['answer-key']))throw Error('Duplicate Data snapshot');seen.add(r.fields['answer-key']);}
    if(state.pending_purge){
      for(const p of state.pending_purge.records){const actual=existing.find(r=>r.id===p.id);if(actual)throw Error('Pending Data purge requires owner resolution');}
      state.last_purge={...state.pending_purge,reconciled_at:clock()};delete state.pending_purge;
    }
    for(const [day,reason] of scopeProblems)scopeProblems.set(day,reason||ageReason(data.inventories[day]?.observed_at,clock(),1800000));
    const verified=report.issues.length?{reason:'malformed_data_retention_blocked'}:purge.authority(show,scopeProblems,clock());
    if(!verified.reason){verified.context.source_lane='data_live';const retired=new Set(state.retired_shows||[]);if(state.current_show&&state.current_show!==sid)retired.add(state.current_show);retired.delete(sid);verified.context.retired_shows=[...retired];}
    const plan=purge.plan(existing,verified,state,runId);state.purge_status=plan.reason||'confirmed';if(plan.next)state.purge_authority=plan.next;else delete state.purge_authority;
    if(plan.confirmed){state.retired_shows=verified.context.retired_shows;state.retirement_cutoffs=state.retirement_cutoffs||{};const k=`${customer}|${sid}`;state.retirement_cutoffs[k]=[state.retirement_cutoffs[k]||'',verified.context.day].sort().at(-1);state.current_show=sid;}
    const latest=new Map();for(const r of existing){const k=r.fields['scope-key'];if(!latest.has(k)||r.fields['evaluated-at']>latest.get(k).fields['evaluated-at'])latest.set(k,r);}
    const paceDay=purge.localDay(clock()),keys=[...groups.keys()].filter(k=>context['selected-date']===paceDay&&groups.get(k).identity.date===paceDay).sort();
    const split=keys.findIndex(k=>k>state.cursor),ordered=split<0?keys:[...keys.slice(split),...keys.slice(0,split)];
    const writes=[],baselines={},processed=[];
    for(const key of ordered){if(Date.now()>deadline-15000)throw Error('Time budget exhausted before all groups were evaluated');
      const g=groups.get(key);let live=liveSamples.get(key);if(live&&hash(live.classes)!==hash(g.class_keys)){live=null;state.live_input_issues.push('live_data_membership_conflict');}
      const refs=[{base:SHOWS,table:'shows',record:show.id,observed_at:show.fields.last_checked},
        ...g.refs.map(ref=>({...ref,observed_at:g.observedAt})),...(live?[live.ref]:[])];
      const result=evaluate({identity:g.identity,rows:g.rows,observedAt:g.observedAt,inputRefs:refs,sourceReason:g.reason||ageReason(g.observedAt,clock(),1800000),live,previous:state.baselines[key],now:clock(),sourceLane:'data_live'});
      result.fields['answer-key']=result.fields['scope-key']+'|'+runId+'|'+result.fields['content-hash'].slice(0,16);
      report.counts.evaluated++;report.counts[result.fields.availability]++;
      if(latest.get(result.fields['scope-key'])?.fields['content-hash']!==result.fields['content-hash'])writes.push(result.fields);else report.counts.unchanged++;
      baselines[key]=result.baseline;processed.push(key);
    }
    if(purge.localDay(clock())!==paceDay){state.input_status='day_changed';await save('input_deferred');report.status='deferred';report.deferred_reason=state.input_status;report.completed=clock();return report;}
    const savedPace=await publish(api,writes,TIMING,T.answers),savedKeys=new Set(savedPace.map(r=>r.fields['answer-key']));
    const afterPace=[...existing.filter(r=>!savedKeys.has(r.fields['answer-key'])),...savedPace];
    report.counts.written=writes.length;
    stage='class_source_read';
    let liveClass;
    try{liveClass=await readLive({api,customer,show:sid,now:clock(),readArchive});}
    catch(error){if(error.code!=='SOURCE_CHANGED')throw error;state.input_status='live_class_source_changed';report.completed=clock();await save('input_deferred');report.status='deferred';report.deferred_reason=state.input_status;report.completed=clock();return report;}
    const classDay=purge.localDay(clock());
    const classRows=await classApi.list(CLASS_TIMING,CLASS_ANSWERS),classKeys=new Set();
    for(const row of classRows){if(classKeys.has(row.fields['answer-key']))throw Error('Duplicate class snapshot');classKeys.add(row.fields['answer-key']);}
    const classData={...data,inventories:Object.fromEntries(Object.entries(data.inventories).filter(([day])=>day===classDay).map(([day,inv])=>[day,{...inv,groups:inv.groups.filter(g=>!excludedGroups.has(g))}]))};
    const batch=context['selected-date']===classDay?prepareClassBatch({data:classData,liveClasses:liveClass.classes,answers:[...afterPace.filter(r=>r.fields['answer-kind']==='ring-pace'),...classRows],now:clock(),runId,onIssue:(identity,message)=>issue('class',identity,message)}):{outputs:[],writes:[],unchanged:0};
    report.counts.classes_evaluated=batch.outputs.length;report.counts.classes_unchanged=batch.unchanged;
    report.counts.classes_unavailable=batch.outputs.filter(x=>x.fields.availability==='unavailable').length;
    if(Date.now()>deadline-15000)throw Error('Time budget exhausted before class publication');
    if(purge.localDay(clock())!==classDay){state.input_status='day_changed';await save('input_deferred');report.status='deferred';report.deferred_reason=state.input_status;report.completed=clock();return report;}
    await publish(classApi,batch.writes,CLASS_TIMING,CLASS_ANSWERS);
    report.counts.classes_written=batch.writes.length;state.class_live_issues=liveClass.issues;state.class_scope_count=batch.outputs.length;state.evaluation_cutoff=classDay;
    for(const [k,v]of Object.entries(baselines)){if(v)state.baselines[k]=v;else delete state.baselines[k];}
    for(const k of Object.keys(state.baselines))if(!keys.includes(k))delete state.baselines[k];
    state.cursor=processed.at(-1)||state.cursor;state.scope_count=keys.length;state.live_input_issues=[...new Set(state.live_input_issues)];
    if(report.issues.length){delete state.purge_authority;state.purge_status='malformed_data_retention_blocked';report.purge_status=state.purge_status;report.completed=clock();state.retry_safe=true;state.last_issues=report.issues;stage='partial_checkpoint';report.status='partial';await save('partial',report.errors.join('; '));return report;}
    // No configuration override: Timing archive/index verification is not implemented.
    // Keep the proven intent/readback path behind this fail-closed prerequisite.
    const archiveReady=false,deleted=new Set();
    const purgeRows=plan.rows.filter(r=>r.fields.date<verified.context.day);
    for(let offset=0;archiveReady&&offset<purgeRows.length;offset+=10){
      if(Date.now()>deadline-22000)break;
      const fence=selectShow(await api.list(SHOWS,'shows'),clock());
      if(!sameAuthority(selected,fence)){
        delete state.purge_authority;state.purge_status='show_authority_changed_before_delete';
        state.fence_diagnostic={reason:state.purge_status,show_before:{id:show.id,context},show_after:{id:fence.record.id,context:fence.context}};report.fence_diagnostic=state.fence_diagnostic;break;
      }
      const purgePointer=readPointer(context);
      if(hash(purgePointer)!==hash(pinned)){
        delete state.purge_authority;state.purge_status='data_head_changed_before_delete';
        state.fence_diagnostic={reason:state.purge_status,data_head_before:pinned.head,data_head_after:purgePointer.head,pointer_hash_before:hash(pinned),pointer_hash_after:hash(purgePointer)};report.fence_diagnostic=state.fence_diagnostic;break;
      }
      const refreshedScopes=new Map([...scopeProblems].map(([day,reason])=>[day,reason||ageReason(data.inventories[day]?.observed_at,clock(),1800000)]));
      const refreshedAuthority=purge.authority(fence.record,refreshedScopes,clock());
      if(refreshedAuthority.reason||refreshedAuthority.token!==verified.token){delete state.purge_authority;state.purge_status=refreshedAuthority.reason||'purge_authority_changed';break;}
      const batch=purgeRows.slice(offset,offset+10);state.pending_purge={context:verified.context,run_id:runId,records:batch.map(r=>({id:r.id,key:r.fields['answer-key'],hash:r.fields['content-hash']}))};
      await save('purging');await api.removeSnapshots(batch.map(r=>r.id));for(const r of batch)deleted.add(r.id);
      report.counts.purged+=batch.length;state.last_purge={...state.pending_purge,verified_at:clock()};delete state.pending_purge;
    }
    if(!archiveReady)state.purge_status='archive_required';report.purge_status=state.purge_status;
    report.counts.purge_deferred=purgeRows.length-deleted.size;
    stage='completion_checkpoint';state.retry_safe=true;delete state.last_issues;report.completed=clock();report.clock_verified=api.clockVerified;await save(report.counts.purge_deferred?'purging_deferred':report.counts.deferred?'partial':(writes.length||batch.writes.length||report.counts.purged)?'complete':'noop');report.status='complete';report.ok=true;return report;
  }catch(error){
    if(error.deferred){delete state.purge_authority;state.input_status=error.message;await save('input_unavailable');report.completed=clock();report.status='deferred';report.deferred_reason=state.input_status;report.completed=clock();return report;}
    report.status=resumeBlocked?'blocked':'failed';report.errors.push(error.message);report.issues.push({stage,message:error.message,run_id:runId});report.completed=clock();
    if(checkpointLoaded&&!resumeBlocked){state.retry_safe=!state.pending_write&&!state.pending_purge&&!state.class_retention?.pending_purge;state.last_issues=report.issues;delete state.purge_authority;try{await save('failed',stage+': '+error.message);}catch(e){report.errors.push('Failure reporting: '+e.message);}}
    return report;
  }
}
module.exports={run,selectShow,loadData,pointer,writeAnswers};
