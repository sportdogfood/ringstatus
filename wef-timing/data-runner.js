"use strict";
const fs=require('node:fs'),path=require('node:path'),{execFile}=require('node:child_process');
const {api:makeApi,TIMING,SHOWS,LIVE,DATA_TABLES:T}=require('./api');
const {POLICY,hash,ageReason,evaluate}=require('./pace');
const {archive,verifyRows,collectionToken}=require('./runner');
const purge=require('./purge'),ROOT=path.resolve(__dirname,'..');
function defer(message){const error=Error(message);error.deferred=true;return error;}
function selectShow(rows,now){
  const defaults=rows.filter(r=>r.fields.is_default===true);if(defaults.length!==1)throw defer('Canonical default show unresolved');
  const s=defaults[0],f=s.fields,customer=Number(f.customer_id),show=Number(f.show_id);
  if(!Number.isSafeInteger(customer)||customer<=0||!Number.isSafeInteger(show)||show<=0||f['show-key']!==`${customer}|${show}`||ageReason(f.last_checked,now,1800000))throw defer('Canonical show identity/freshness invalid');
  if(![f.start_date,f.end_date,f.selected_date].every(purge.validDay)||f.selected_date<f.start_date||f.selected_date>f.end_date)throw defer('Canonical dates invalid');
  return{record:s,context:{'show-key':f['show-key'],'show-id':show,'customer-id':customer,'start-date':f.start_date,'end-date':f.end_date,'selected-date':f.selected_date}};
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
    const remaining=Math.min(24000,deadline-Date.now()-18000);if(remaining<2000){const e=Error('Data input budget deferred');e.deferred=true;reject(e);return;}
    const child=execFile(path.join(ROOT,'wef-data/.venv/Scripts/python.exe'),['-B',path.join(__dirname,'data-input.py')],{
      windowsHide:true,timeout:remaining,maxBuffer:8*1024*1024,env:{SystemRoot:process.env.SystemRoot,WINDIR:process.env.WINDIR,TEMP:process.env.TEMP,TMP:process.env.TMP,PYTHONDONTWRITEBYTECODE:'1'}
    },(error,stdout,stderr)=>{
      if(error){const e=Error(error.killed?'Data input budget deferred':String(stderr||'Data retained input unavailable').trim());e.deferred=Boolean(error.killed)||e.message.includes('pointer changed during read');reject(e);return;}
      try{resolve(JSON.parse(stdout));}catch{reject(Error('Data input projection unreadable'));}
    });child.stdin.end(JSON.stringify(context));
  });
}
async function run({api,clock=()=>new Date().toISOString(),deadline=Date.now()+160000,runId=require('node:crypto').randomUUID(),readData=loadData,readPointer=pointer,readArchive=archive}={}){
  api=api||makeApi({token:process.env.WEF_TIMING_AIRTABLE_TOKEN,deadline,lane:'data_live'});
  const startMs=Date.now(),started=clock(),report={run_id:runId,started,ok:false,errors:[],source_lane:'data_live',destination:TIMING,
    counts:{evaluated:0,written:0,unchanged:0,deferred:0,available:0,partial:0,unavailable:0,purged:0,purge_deferred:0}};
  let state={version:1,baselines:{},cursor:''};
  async function save(outcome,error=''){
    const body=JSON.stringify(state);if(body.length>90000)throw Error('Data pace checkpoint capacity guard');
    const f={'checkpoint-key':'ring-pace-data-runner','run-id':runId,'started-at':started,'completed-at':clock(),'last-outcome':outcome,'duration-ms':Date.now()-startMs,
      'counts-json':JSON.stringify(report.counts),'state-json':body,'error-summary':error,'policy-version':POLICY};
    await api.upsert(T.checkpoint,'checkpoint-key',[f]);verifyRows(await api.list(TIMING,T.checkpoint),[f],'checkpoint-key');
  }
  try{
    const checkpoints=await api.list(TIMING,T.checkpoint),old=checkpoints.filter(r=>r.fields['checkpoint-key']==='ring-pace-data-runner');
    if(old.length>1)throw Error('Duplicate Data pace checkpoint');
    if(old.length){state=JSON.parse(old[0].fields['state-json']);if(state.version!==1||old[0].fields['last-outcome']==='failed')throw Error('Data pace checkpoint requires owner review');}
    const selected=selectShow(await api.list(SHOWS,'shows'),clock()),{record:show,context}=selected,sid=context['show-id'],customer=context['customer-id'];
    const pinned=readPointer(context);let data;
    try{data=await readData(context,deadline);}catch(error){if(!error.deferred)throw error;delete state.purge_authority;state.input_status='budget_deferred';await save('input_deferred');report.ok=true;return report;}
    const liveBefore=await api.list(LIVE,'collections',`AND({show-id}=${sid},{source-kind}='groups')`);
    const again=selectShow(await api.list(SHOWS,'shows'),clock());
    if(hash(again.record)!==hash(show)||hash(readPointer(context))!==hash(pinned)||data.proof['run-key']!==pinned.head){
      delete state.purge_authority;state.input_status='source_changed';await save('input_deferred');report.ok=true;return report;
    }
    state.input_status='verified';state.source_head=data.proof;const scopeProblems=new Map(),groups=new Map();
    for(const [day,inventory]of Object.entries(data.inventories)){
      if(!purge.validDay(day)||!inventory.inventory_complete)throw Error('Data inventory incomplete');
      const reason=ageReason(inventory.observed_at,clock(),1800000);scopeProblems.set(day,reason);
      for(const group of inventory.groups){
        const i=group.identity;if(i.customer!==customer||i.show!==sid||i.date!==day)throw Error('Data projected identity mismatch');
        const key=[sid,day,i.ring,i.group].join('|');if(groups.has(key))throw Error('Data group identity repeated');
        groups.set(key,{...group,observedAt:inventory.observed_at,refs:inventory.refs,reason});
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
    if(collectionToken(liveBefore)!==collectionToken(await api.list(LIVE,'collections',`AND({show-id}=${sid},{source-kind}='groups')`))){delete state.purge_authority;state.input_status='live_changed';await save('input_deferred');report.ok=true;return report;}
    let existing=await api.list(TIMING,T.answers);const seen=new Set();for(const r of existing){if(seen.has(r.fields['answer-key']))throw Error('Duplicate Data snapshot');seen.add(r.fields['answer-key']);}
    if(state.pending_purge){
      for(const p of state.pending_purge.records){const actual=existing.find(r=>r.id===p.id);if(actual)throw Error('Pending Data purge requires owner resolution');}
      state.last_purge={...state.pending_purge,reconciled_at:clock()};delete state.pending_purge;
    }
    const verified=purge.authority(show,scopeProblems,clock());
    if(!verified.reason){verified.context.source_lane='data_live';const retired=new Set(state.retired_shows||[]);if(state.current_show&&state.current_show!==sid)retired.add(state.current_show);retired.delete(sid);verified.context.retired_shows=[...retired];}
    const plan=purge.plan(existing,verified,state,runId);state.purge_status=plan.reason||'confirmed';if(plan.next)state.purge_authority=plan.next;else delete state.purge_authority;
    if(plan.confirmed){state.retired_shows=verified.context.retired_shows;state.retirement_cutoffs=state.retirement_cutoffs||{};const k=`${customer}|${sid}`;state.retirement_cutoffs[k]=[state.retirement_cutoffs[k]||'',verified.context.day].sort().at(-1);state.current_show=sid;}
    const deleted=new Set();for(let offset=0;offset<Math.min(40,plan.rows.length);offset+=10){
      if(Date.now()>deadline-22000)break;
      const fence=selectShow(await api.list(SHOWS,'shows'),clock());if(hash(fence.record)!==hash(show)||hash(readPointer(context))!==hash(pinned)){delete state.purge_authority;state.purge_status='source_changed_before_delete';break;}
      const batch=plan.rows.slice(offset,offset+10);state.pending_purge={context:verified.context,run_id:runId,records:batch.map(r=>({id:r.id,key:r.fields['answer-key'],hash:r.fields['content-hash']}))};
      await save('purging');await api.removeSnapshots(batch.map(r=>r.id));for(const r of batch)deleted.add(r.id);
      report.counts.purged+=batch.length;state.last_purge={...state.pending_purge,verified_at:clock()};delete state.pending_purge;
    }
    existing=existing.filter(r=>!deleted.has(r.id));report.counts.purge_deferred=plan.rows.length-deleted.size;
    if(report.counts.purge_deferred){await save('purging_deferred');report.ok=true;return report;}
    const latest=new Map();for(const r of existing){const k=r.fields['scope-key'];if(!latest.has(k)||r.fields['evaluated-at']>latest.get(k).fields['evaluated-at'])latest.set(k,r);}
    const cutoff=state.retirement_cutoffs?.[`${customer}|${sid}`],keys=[...groups.keys()].filter(k=>!cutoff||groups.get(k).identity.date>=cutoff).sort();
    const split=keys.findIndex(k=>k>state.cursor),ordered=split<0?keys:[...keys.slice(split),...keys.slice(0,split)];
    const writes=[],baselines={},processed=[];
    for(const key of ordered){if(Date.now()>deadline-15000)throw Error('Time budget exhausted before all groups were evaluated');
      const g=groups.get(key);let live=liveSamples.get(key);if(live&&hash(live.classes)!==hash(g.class_keys)){live=null;state.live_input_issues.push('live_data_membership_conflict');}
      const refs=[{base:SHOWS,table:'shows',record:show.id,observed_at:show.fields.last_checked},
        ...g.refs.map(ref=>({...ref,observed_at:g.observedAt})),...(live?[live.ref]:[])];
      const result=evaluate({identity:g.identity,rows:g.rows,observedAt:g.observedAt,inputRefs:refs,sourceReason:g.reason,live,previous:state.baselines[key],now:clock(),sourceLane:'data_live'});
      result.fields['answer-key']=result.fields['scope-key']+'|'+runId+'|'+result.fields['content-hash'].slice(0,16);
      report.counts.evaluated++;report.counts[result.fields.availability]++;
      if(latest.get(result.fields['scope-key'])?.fields['content-hash']!==result.fields['content-hash'])writes.push(result.fields);else report.counts.unchanged++;
      baselines[key]=result.baseline;processed.push(key);
    }
    for(let i=0;i<writes.length;i+=10)await api.upsert(T.answers,'answer-key',writes.slice(i,i+10));
    if(writes.length)verifyRows(await api.list(TIMING,T.answers),writes,'answer-key');report.counts.written=writes.length;
    for(const [k,v]of Object.entries(baselines)){if(v)state.baselines[k]=v;else delete state.baselines[k];}
    for(const k of Object.keys(state.baselines))if(!keys.includes(k))delete state.baselines[k];
    state.cursor=processed.at(-1)||state.cursor;state.scope_count=keys.length;state.live_input_issues=[...new Set(state.live_input_issues)];
    report.completed=clock();report.clock_verified=api.clockVerified;await save(report.counts.deferred?'partial':writes.length?'complete':'noop');report.ok=true;return report;
  }catch(error){
    if(error.deferred){delete state.purge_authority;state.input_status=error.message;await save('input_unavailable');report.completed=clock();report.ok=true;return report;}
    report.errors.push(error.message);report.completed=clock();try{await save('failed',error.message);}catch(e){report.errors.push('Failure reporting: '+e.message);}return report;
  }
}
module.exports={run,selectShow,loadData,pointer};
