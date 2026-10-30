"use strict";
const fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib');
const {api:makeApi,ENGINE,SCHEDULES,LIVE,TABLES}=require('./api');
const {POLICY,hash,digest,ageReason,evaluate}=require('./pace');
const purge=require('./purge');
const ROOT=path.resolve(__dirname,'..');
const SOURCE_FIELDS=new Set(('schedule_key schedule_short full_nesting_key schedule_date sql_date live_groups show_id show_date ring_number day_number class_group_sequence group_name class_number class_name class_type schedule_break schedule_sequencetype class_groupxclasses_id class_group_id class_id schedule_show_datev2 schedule_show_display_datev2 schedule_show_display_date_dayv2 scheduled_date sgl_token_raw sgl_token_prefix sgl_token_length sgl_token_hash estimated_start_time estimated_end_time total_trips completed_trips status latest_status latest_estimated_start_time ___latest_estimated_start_time app_show_idv2 app_sql_datev2 app_dow_rawv2 customer_id shows').split(' '));
function scheduleFingerprint(rows) {
  const picked=rows.map(row=>{
    const fields=Object.fromEntries(Object.entries(row.fields).filter(([k])=>SOURCE_FIELDS.has(k.trim())&&!['shows','live_groups'].includes(k)));
    for(const k of ['manual_time_override','manual_time_overide'])if(row.fields[k]===true)fields[k]=true;
    return{key:row.key,fields,...(row.inactive===true?{inactive:true,dropped_at:row.dropped_at}:{})};
  }).sort((a,b)=>String(a.key).localeCompare(String(b.key)));
  return hash(picked);
}
function archive(file,root,expected) {
  const target=path.resolve(file),base=path.resolve(root)+path.sep;
  if(!target.toLowerCase().startsWith(base.toLowerCase())||!target.endsWith('.json.gz'))throw Error('Archive outside producer history');
  const body=zlib.gunzipSync(fs.readFileSync(target));
  if(expected&&digest(body)!==expected)throw Error('Producer archive hash mismatch');
  return JSON.parse(body);
}
function unique(rows,key) {
  const found=new Set();for(const r of rows){const k=r.fields[key];if(!k||found.has(k))throw Error('Duplicate/missing '+key);found.add(k);}
}
function normalizedEqual(a,b) {return hash(a??null)===hash(b??null);}
function verifyRows(saved,wanted,key) {
  unique(saved,key);
  for(const f of wanted){const r=saved.find(r=>r.fields[key]===f[key]);if(!r)throw Error('Engine readback missing '+key);
    for(const [k,v]of Object.entries(f)){const actual=r.fields[k];if((v===''||v==null)&&(actual==null||actual===''))continue;if(!normalizedEqual(actual,v))throw Error('Engine readback mismatch '+k);}}
}
const collectionToken=rows=>hash(rows.map(r=>({id:r.id,fields:r.fields})).sort((a,b)=>a.id.localeCompare(b.id)));
async function run({api,clock=()=>new Date().toISOString(),deadline=Date.now()+50000,runId=require('node:crypto').randomUUID(),readArchive=archive}={}) {
  const started=clock(),began=Date.now();api=api||makeApi({token:process.env.WEF_TIMING_AIRTABLE_TOKEN,deadline});
  let state={version:1,baselines:{},cursor:''},checkpointRows=[];
  const report={run_id:runId,started,ok:false,counts:{evaluated:0,written:0,unchanged:0,deferred:0,available:0,partial:0,unavailable:0,purged:0,purge_deferred:0},errors:[]};
  async function checkpoint(outcome,error='') {
    const json=JSON.stringify(state);if(json.length>90000)throw Error('Checkpoint capacity exceeds 90000 characters');
    const fields={'checkpoint-key':'ring-pace-runner','run-id':runId,'started-at':started,'completed-at':clock(),'last-outcome':outcome,
      'duration-ms':Date.now()-began,'counts-json':JSON.stringify(report.counts),'state-json':json,'error-summary':error,'policy-version':POLICY};
    await api.upsert(TABLES.checkpoint,'checkpoint-key',[fields]);
    const saved=await api.list(ENGINE,TABLES.checkpoint);verifyRows(saved,[fields],'checkpoint-key');
  }
  try {
    checkpointRows=await api.list(ENGINE,TABLES.checkpoint);unique(checkpointRows,'checkpoint-key');
    const old=checkpointRows.find(r=>r.fields['checkpoint-key']==='ring-pace-runner');
    if(old){state=JSON.parse(old.fields['state-json']);if(state.version!==1)throw Error('Checkpoint version unsupported');if(old.fields['last-outcome']==='failed')throw Error('Previous failure requires owner-authorized review');}
    const shows=await api.list(SCHEDULES,'shows'),defaults=shows.filter(r=>r.fields.is_default===true);
    if(defaults.length!==1)throw Error('Exactly one lane-local default show required');
    const show=defaults[0],sid=Number(show.fields.show_id),customer=Number(show.fields.customer_id);
    if(!Number.isSafeInteger(sid)||sid<=0||!Number.isSafeInteger(customer)||customer<=0)throw Error('Invalid lane show identity');
    const before=await api.list(SCHEDULES,'show-collection',`{show-id}=${sid}`);
    const liveBefore=await api.list(LIVE,'collections',`AND({show-id}=${sid},{source-kind}='groups')`);
    const schedules=await api.list(SCHEDULES,'schedules',`{show-id}=${sid}`);unique(schedules,'stage-key');
    const rings=await api.list(SCHEDULES,'rings',`{show_id}=${sid}`);
    const after=await api.list(SCHEDULES,'show-collection',`{show-id}=${sid}`);
    const liveAfter=await api.list(LIVE,'collections',`AND({show-id}=${sid},{source-kind}='groups')`);
    const showAfter=(await api.list(SCHEDULES,'shows')).filter(r=>r.fields.is_default===true);
    if(showAfter.length!==1||!normalizedEqual(showAfter[0],show)||collectionToken(before)!==collectionToken(after)||collectionToken(liveBefore)!==collectionToken(liveAfter)) {
      delete state.purge_authority;
      report.counts.deferred=schedules.length;await checkpoint('deferred_source_changed');report.ok=true;return report;
    }
    const scopeProblems=new Map(),evidence=new Map();
    const dates=[...new Set([...schedules.map(r=>r.fields.date),...before.map(r=>r.fields.date)])];
    for(const date of dates){
      const cs=before.filter(r=>r.fields.date===date),rs=schedules.filter(r=>r.fields.date===date);
      let reason=ageReason(show.fields.last_checked,clock(),1800000),sourceRows;
      if(cs.length!==1||cs[0].fields.status!=='complete')reason=reason||'schedule_collection_not_complete';
      if(!reason)reason=ageReason(cs[0].fields.completed,clock(),1800000);
      if(!reason){
        const c=cs[0].fields;
        try{
          if(rs.length!==c['row-count']||rs.some(r=>r.fields['dataset-id']!==c['dataset-id']))throw Error('dataset_membership');
          sourceRows=rs.map(r=>JSON.parse(r.fields['row-json']));
          if(scheduleFingerprint(sourceRows)!==c.fingerprint)throw Error('fingerprint');
          const saved=readArchive(path.join(ROOT,'wef-schedules/runtime/history/datasets',c['dataset-id']+'.json.gz'),path.join(ROOT,'wef-schedules/runtime/history/datasets'));
          if(scheduleFingerprint(saved)!==c.fingerprint)throw Error('archive_fingerprint');
          // Structured Airtable fields must agree with their verified frozen source projection.
          const mapping={'show-id':'show_id',date:'show_date',ring:'ring_number','group-id':'class_group_id','class-id':'class_id','start-time':'estimated_start_time','end-time':'estimated_end_time','total-trips':'total_trips','type':'class_type'};
          for(let i=0;i<rs.length;i++){
            const f=rs[i].fields,raw=sourceRows[i];
            if(f['stage-key']!==`${sid}|${date}|${raw.key}`)throw Error('key');
            if(f.status!==(raw.inactive===true?'dropped':'active'))throw Error('retention_status');
            for(const [dest,src]of Object.entries(mapping))if(!normalizedEqual(f[dest],raw.fields[src]))throw Error('projection_'+dest);
          }
        }catch{reason='schedule_consistency_unproven';}
      }
      scopeProblems.set(date,reason);
      evidence.set(date,{base:SCHEDULES,table:'show-collection',record:cs[0]?.id??null,dataset:cs[0]?.fields['dataset-id']??null,fingerprint:cs[0]?.fields.fingerprint??null,observed_at:cs[0]?.fields.completed??null});
    }
    const liveSamples=new Map(),liveReasons=[],invalidLiveKeys=new Set();
    for(const c of liveBefore){
      if(c.fields.status!=='complete'||ageReason(c.fields.completed,clock(),600000)){liveReasons.push('live_collection_unusable');continue;}
      try{
        const rows=readArchive(c.fields['archive-file'],path.join(ROOT,'wef-live/runtime/history'),c.fields.fingerprint);
        if(!Array.isArray(rows)||rows.length!==c.fields['row-count'])throw Error('Live membership mismatch');
        const candidate=new Map();
        for(const g of rows){
          const raw=JSON.parse(g['source-json']),ring=Number(raw.ring_number),gid=Number(g['class-group-id']);
          if(Number(g['show-id'])!==sid||Number(g['customer-id'])!==customer||g.date!==c.fields.date||g['scope-status']!=='current'||!Number.isSafeInteger(ring)||!Number.isSafeInteger(gid))throw Error('Live group identity conflict');
          if(!Array.isArray(g._classes)||!g._classes.length||g._classes.some(x=>typeof x!=='string')||new Set(g._classes).size!==g._classes.length)throw Error('Live class membership unavailable');
          if(ageReason(g['last-seen'],clock(),600000)||Date.parse(g['last-seen'])>Date.parse(c.fields.completed))throw Error('Invalid live observation time');
          const members=[...g._classes].sort(),key=[sid,g.date,ring,gid].join('|');
          if(candidate.has(key)||liveSamples.has(key)||invalidLiveKeys.has(key)){invalidLiveKeys.add(key);liveSamples.delete(key);throw Error('Duplicate live group');}
          candidate.set(key,{at:g['last-seen'],total:g.total,gone:g.gone,classes:members,
            membership:hash({members,ring,gid,status:g.status}),ref:{base:LIVE,table:'collections',record:c.id,fingerprint:c.fields.fingerprint,observed_at:g['last-seen']}});
        }
        for(const [key,sample]of candidate)liveSamples.set(key,sample);
      }catch{liveReasons.push('live_archive_unusable');}
    }
    const groups=new Map();
    const verifiedAuthority=purge.authority(show,scopeProblems,clock());
    const retiredBefore=state.retirement_cutoffs?.[`${customer}|${sid}`];
    for(const r of schedules){const f=r.fields;if(f.status!=='active')continue;const gid=Number(f['group-id']),ring=Number(f.ring);
      if(!Number.isSafeInteger(gid)||gid<=0||!Number.isSafeInteger(ring)||ring<0)throw Error('Schedule group/ring identity missing');
      if((retiredBefore&&f.date<retiredBefore)||(!verifiedAuthority.reason&&f.date<verifiedAuthority.context.day))continue;
      const key=[sid,f.date,ring,gid].join('|');if(!groups.has(key))groups.set(key,[]);groups.get(key).push(f);}
    let existing=await api.list(ENGINE,TABLES.answers);unique(existing,'answer-key');
    // Resolve an interrupted deletion before constructing or replacing any intent.
    if(state.pending_purge){
      const remaining=state.pending_purge.records.filter(saved=>{
        const actual=existing.find(r=>r.id===saved.id);
        if(actual&&(actual.fields['answer-key']!==saved.key||actual.fields['content-hash']!==saved.hash))throw Error('Pending purge identity changed; owner resolution required');
        return Boolean(actual);
      });
      if(remaining.length)throw Error('Pending purge unresolved; owner resolution required');
      state.last_purge={...state.pending_purge,reconciled_at:clock()};delete state.pending_purge;
    }
    if(!Number.isSafeInteger(state.purge_active_show)&&Number.isSafeInteger(state.current_show))state.purge_active_show=state.current_show;
    if(!verifiedAuthority.reason){
      const retired=new Set(state.retired_shows||[]);
      if(Number.isSafeInteger(state.purge_active_show)&&state.purge_active_show!==sid)retired.add(state.purge_active_show);
      retired.delete(sid);verifiedAuthority.context.retired_shows=[...retired].sort((a,b)=>a-b);
    }
    const purgePlan=purge.plan(existing,verifiedAuthority,state,runId);
    state.purge_status=purgePlan.reason||'confirmed';
    if(purgePlan.next)state.purge_authority=purgePlan.next;else delete state.purge_authority;
    if(purgePlan.confirmed){
      state.retired_shows=verifiedAuthority.context.retired_shows;state.purge_active_show=sid;
      state.retirement_cutoffs=state.retirement_cutoffs||{};
      const cutoffKey=`${customer}|${sid}`,priorCutoff=state.retirement_cutoffs[cutoffKey];
      if(!priorCutoff||priorCutoff<verifiedAuthority.context.day)state.retirement_cutoffs[cutoffKey]=verifiedAuthority.context.day;
    }
    // Every batch is scoped, recorded before deletion, and independently read back.
    // An unknown response fails/stops this runner; no deletion is blindly retried.
    const victims=purgePlan.rows.slice(0,40),deleted=new Set();
    for(let i=0;i<victims.length;i+=10){
      if(Date.now()>deadline-22000)break;
      const fenceShows=(await api.list(SCHEDULES,'shows')).filter(r=>r.fields.is_default===true);
      const fenceScopes=await api.list(SCHEDULES,'show-collection',`AND({show-id}=${sid},{date}='${verifiedAuthority.context.day}')`);
      if(fenceShows.length!==1||!normalizedEqual(fenceShows[0],show)||collectionToken(fenceScopes)!==collectionToken(before.filter(r=>r.fields.date===verifiedAuthority.context.day))){
        state.purge_status='source_changed_before_delete';delete state.purge_authority;break;
      }
      const batch=victims.slice(i,i+10);
      if(batch.some(r=>!purge.eligible(r,verifiedAuthority.context)))throw Error('Purge scope changed');
      state.pending_purge={context:verifiedAuthority.context,run_id:runId,records:batch.map(r=>({id:r.id,key:r.fields['answer-key'],hash:r.fields['content-hash']}))};
      await checkpoint('purging');
      await api.removeSnapshots(batch.map(r=>r.id));
      for(const r of batch)deleted.add(r.id);
      report.counts.purged+=batch.length;
      state.last_purge={...state.pending_purge,verified_at:clock()};delete state.pending_purge;
    }
    report.counts.purge_deferred=purgePlan.rows.length-deleted.size;
    existing=existing.filter(r=>!deleted.has(r.id));
    if(purgePlan.confirmed){
      for(const key of Object.keys(state.baselines)){
        const parts=key.split('|');if(Number(parts[0])!==sid||parts[1]<verifiedAuthority.context.day)delete state.baselines[key];
      }
    }
    if(report.counts.purge_deferred){
      report.completed=clock();report.clock_verified=api.clockVerified;
      await checkpoint('purging_deferred');report.ok=true;return report;
    }
    if(existing.length>=4000)throw Error('Snapshot capacity guard: 4000 records; retention decision required');
    const latest=new Map();for(const r of existing){const k=r.fields['scope-key'];if(!latest.has(k)||r.fields['evaluated-at']>latest.get(k).fields['evaluated-at'])latest.set(k,r);}
    const keys=[...groups.keys()].sort(),split=keys.findIndex(k=>k>state.cursor),ordered=split<0?keys:[...keys.slice(split),...keys.slice(0,split)];
    const writes=[],pendingBaselines={},processed=[];
    for(const key of ordered){
      if(Date.now()>deadline-15000||processed.length>=80){report.counts.deferred=ordered.length-processed.length;break;}
      const rows=groups.get(key).sort((a,b)=>a['stage-key'].localeCompare(b['stage-key'])),f=rows[0],date=f.date;
      const matches=rings.filter(r=>Number(r.fields.show_id)===sid&&r.fields.ring_date===date&&Number(r.fields.ring_number)===Number(f.ring));
      const reason=scopeProblems.get(date)||(matches.length===1?null:'ring_reference_unresolved');
      let live=liveSamples.get(key);
      const classKeys=[...new Set(rows.filter(r=>Number.isSafeInteger(r['class-id'])).map(r=>[sid,date,r['class-id']].join('|')))].sort();
      if(live&&!normalizedEqual(live.classes,classKeys)){live=null;liveReasons.push('live_schedule_membership_conflict');}
      const refs=[{base:SCHEDULES,table:'shows',record:show.id,observed_at:show.fields.last_checked},evidence.get(date),
        ...rows.map(r=>({base:SCHEDULES,table:'schedules',key:r['stage-key'],dataset:r['dataset-id']})),...(live?[live.ref]:[])];
      const result=evaluate({identity:{customer,show:sid,date,ring:Number(f.ring),group:Number(f['group-id'])},rows,observedAt:evidence.get(date)?.observed_at,inputRefs:refs,
        sourceReason:reason,live,previous:state.baselines[key],now:clock()});
      result.fields['answer-key']=result.fields['scope-key']+'|'+runId+'|'+result.fields['content-hash'].slice(0,16);
      report.counts.evaluated++;report.counts[result.fields.availability]++;
      if(latest.get(result.fields['scope-key'])?.fields['content-hash']!==result.fields['content-hash'])writes.push(result.fields);else report.counts.unchanged++;
      pendingBaselines[key]=result.baseline;processed.push(key);
    }
    if(existing.length+writes.length>4000)throw Error('Snapshot capacity guard');
    for(let i=0;i<writes.length;i+=10)await api.upsert(TABLES.answers,'answer-key',writes.slice(i,i+10));
    if(writes.length){const saved=await api.list(ENGINE,TABLES.answers);verifyRows(saved,writes,'answer-key');report.counts.written=writes.length;}
    // Baselines advance only after exact snapshot readback. Unknown writes stop this invocation.
    for(const [key,value]of Object.entries(pendingBaselines)){if(value)state.baselines[key]=value;else delete state.baselines[key];}
    for(const key of Object.keys(state.baselines))if(!groups.has(key))delete state.baselines[key];
    state.cursor=processed.at(-1)||state.cursor;state.last_source_observations=Object.fromEntries([...evidence].map(([date,e])=>[date,e.observed_at]));
    state.live_input_issues=[...new Set(liveReasons)];state.current_show=sid;state.scope_count=groups.size;
    report.completed=clock();report.clock_verified=api.clockVerified;report.pending=report.counts.deferred;
    await checkpoint(report.counts.deferred||report.counts.purge_deferred?'partial':writes.length||report.counts.purged?'complete':'noop');report.ok=true;return report;
  }catch(error){report.ok=false;report.errors.push(error.message);report.completed=clock();try{await checkpoint('failed',error.message);}catch(e){report.errors.push('Failure status not delivered: '+e.message);}return report;}
}
module.exports={run,scheduleFingerprint,archive,verifyRows,collectionToken};
