"use strict";
const crypto = require('node:crypto');
const POLICY = 'ring-pace-v1';
const canonical = x => Array.isArray(x) ? x.map(canonical) : x && typeof x === 'object' ? Object.fromEntries(Object.keys(x).sort().map(k => [k, canonical(x[k])])) : x;
const hash = x => crypto.createHash('sha256').update(JSON.stringify(canonical(x))).digest('hex');
const digest = s => crypto.createHash('sha256').update(s).digest('hex');
const integer = n => Number.isSafeInteger(n) && n >= 0;
function ageReason(at, now, limit) {
  const t = Date.parse(at), n = Date.parse(now);
  if (!Number.isFinite(t) || !Number.isFinite(n)) return 'observation_time_missing';
  if (t > n) return 'future_observation';
  return n - t >= limit ? 'stale_observation' : null;
}
function minutes(text) {
  const m = /^(\d{2}):(\d{2}):(\d{2})$/.exec(text || '');
  if (!m || +m[1] > 23 || +m[2] > 59 || +m[3] > 59) return null;
  return +m[1] * 60 + +m[2] + +m[3] / 60;
}
function schedulePace(rows) {
  if (!rows.length) return {value:null,reason:'schedule_missing'};
  if (rows.some(r => ['schooling','break'].includes(String(r.type).toLowerCase()))) return {value:null,reason:'non_class_block'};
  if (rows.length !== 1) return {value:null,reason:'group_aggregation_unproven'};
  const r = rows[0], a = minutes(r['start-time']), b = minutes(r['end-time']), n = r['total-trips'];
  if (!integer(n) || n <= 0) return {value:null,reason:'positive_trip_count_missing'};
  if (a === null || b === null || b <= a) return {value:null,reason:'schedule_window_unusable'};
  return {value:(b-a)/n,window:b-a,total:n,reason:null};
}
function completed(sample,now) {
  return String(sample?.status||'').trim().toLowerCase()==='completed' && !['future_observation','observation_time_missing'].includes(ageReason(sample?.at,now,600000));
}
function finalTransition(live,previous,identity,now){
 const status=String(previous?.status||'').toLowerCase();
 if(!completed(live,now)||!['underway','completed'].includes(status)||!Array.isArray(live.classes)||!Array.isArray(previous.classes)
  ||hash(live.classes)!==hash(previous.classes)||live.total!==previous.total||!integer(live.total)||live.total<=0)return null;
 const prior=status==='underway'?previous:previous.final_transition?.before;
 const at=Date.parse(live.at),before=Date.parse(previous.at),m=previous.measurement;
 if(!prior||!Number.isFinite(before)||before>at||(at===before&&live.gone!==previous.gone)||!m||!Array.isArray(m.endpoints)||m.endpoints.length!==2)return null;
 if(String(prior.status).toLowerCase()!=='underway'||hash(prior.classes)!==hash(live.classes)||prior.total!==live.total||!integer(prior.gone)||prior.gone>prior.total
  ||previous.membership!==hash({members:previous.classes,ring:identity.ring,gid:identity.group,status:previous.status}))return null;
 const [a,b]=m.endpoints,dt=Date.parse(b.at)-Date.parse(a.at),dn=b.gone-a.gone;
 if(dt<=0||dt>=600000||dn<=0||m.value!==dt/60000/dn||m.from!==a.at||m.to!==b.at||m.completed!==dn||Date.parse(b.at)>Date.parse(prior.at)
  ||a.total!==prior.total||b.total!==prior.total||!integer(a.gone)||!integer(b.gone)||b.gone>prior.gone||a.gone>a.total
  ||a.membership!==prior.membership||b.membership!==prior.membership
  ||prior.membership!==hash({members:prior.classes,ring:identity.ring,gid:identity.group,status:prior.status}))return null;
 return {before:{at:prior.at,total:prior.total,gone:prior.gone,status:prior.status,classes:prior.classes,membership:prior.membership,ref:prior.ref??null},completed_at:live.at};
}
function intervalPace(sample, previous, now) {
  if (!sample) return {value:null,reason:'live_observation_missing',baseline:null};
  if (sample.source_issue) return {value:null,reason:sample.source_issue,baseline:null};
  const bad = ageReason(sample.at,now,600000);
  if (bad && !(bad==='stale_observation' && completed(sample,now))) return {value:null,reason:bad,baseline:null};
  if (!integer(sample.total) || !integer(sample.gone) || sample.gone > sample.total || sample.total <= 0)
    return {value:null,reason:'live_counts_invalid',baseline:null};
  const same = previous && sample.membership === previous.membership && sample.total === previous.total;
  if (!same || sample.gone < previous.gone) return {value:null,reason:'interval_baseline_required',baseline:sample};
  const dt = Date.parse(sample.at)-Date.parse(previous.at), dn = sample.gone-previous.gone;
  if (dt < 0) return {value:null,reason:'observation_regressed',baseline:null};
  if (dt === 0) {
    if (dn !== 0) return {value:null,reason:'same_time_count_conflict',baseline:null};
    return {value:null,reason:'repeated_observation',baseline:previous};
  }
  if (dt >= 600000) return {value:null,reason:'interval_gap',baseline:sample};
  if (!dn) return {value:null,reason:'no_new_completions',baseline:previous};
  return {value:dt/60000/dn,reason:null,baseline:sample,from:previous.at,to:sample.at,completed:dn,
    endpoints:[previous,sample].map(s=>({at:s.at,total:s.total,gone:s.gone,membership:s.membership,ref:s.ref??null}))};
}
function evaluate({identity,rows,observedAt,inputRefs,sourceReason,live,previous,now,sourceLane='schedules_live',liveIssue=null}) {
  if(!['schedules_live','data_live'].includes(sourceLane))throw Error('Unsupported pace source lane');
  const final = completed(live,now);
  const planned = sourceReason && !(final && sourceReason==='stale_observation') ? {value:null,reason:sourceReason} : schedulePace(rows);
  const sourceAccepted=!sourceReason||sourceReason==='stale_observation';
  const transition=sourceAccepted&&!liveIssue?finalTransition(live,previous,identity,now):null;
  const observed = sourceReason && sourceReason !== 'stale_observation' ? {value:null,reason:sourceReason,baseline:null} : liveIssue ? {value:null,reason:liveIssue,baseline:null} : intervalPace(live,previous,now);
  if(transition){Object.assign(observed,previous.measurement,{reason:null,baseline:{...live,measurement:previous.measurement,final_transition:transition}});}
  const compatible = previous && (!live || (live.membership === previous.membership && live.total === previous.total && live.gone >= previous.gone));
  const retainable = compatible && previous.measurement && ['repeated_observation','no_new_completions','stale_observation','live_observation_missing','interval_gap'].includes(observed.reason);
  if (retainable) observed.baseline = {...(observed.baseline || previous),measurement:previous.measurement};
  // Re-reading an identical sample retains its prior measured interval, never changes its clock.
  if (retainable && ['repeated_observation','no_new_completions'].includes(observed.reason) && (!ageReason(previous.measurement.to,now,600000) || (final && ageReason(previous.measurement.to,now,600000)==='stale_observation'))) {
    Object.assign(observed,previous.measurement,{reason:observed.reason === 'no_new_completions' ? observed.reason : null});
  }
  if (observed.value !== null && observed.baseline) observed.baseline = {...observed.baseline,measurement:{value:observed.value,from:observed.from,to:observed.to,completed:observed.completed,endpoints:observed.endpoints}};
  const value = observed.value ?? planned.value, method = observed.value !== null ? 'observed_interval' : planned.value !== null ? 'schedule_window' : 'unavailable';
  const finalization=transition||observed.baseline?.final_transition||null;
  const proven = observed.value !== null ? observed.baseline?.measurement : retainable ? previous.measurement : null;
  const countValid = live && integer(live.total) && integer(live.gone) && live.gone <= live.total && !['future_observation','observation_time_missing'].includes(ageReason(live.at,now,600000));
  const remaining = countValid ? live.total-live.gone : null;
  const countIssue = final && (!countValid || remaining!==0) ? 'completed_count_conflict' : null;
  const clockValid = live && !['future_observation','observation_time_missing'].includes(ageReason(live.at,now,600000));
  const remainingMinutes = final ? 0 : remaining !== null && !ageReason(live.at,now,600000) && value !== null ? remaining*value : null;
  const payload = {schema_version:1,identity,phase:'base',schedule_pace_minutes_per_trip:planned.value,observed_interval_pace_minutes_per_trip:observed.value,
    selected_pace_minutes_per_trip:value,pace_method:method,schedule_window_minutes:planned.window??null,total_trips:planned.total??null,
    group_progress:clockValid&&(countValid||final)?{total:live.total,completed:live.gone,remaining,status:live.status??null,observed_at:live.at,quality:final?'final':ageReason(live.at,now,600000)||'current',age_quality:ageReason(live.at,now,600000)||'current',lifecycle:final?'completed':'active_or_unknown',count_issue:countIssue,remaining_work_trips:final?0:remaining,ref:live.ref??null}:null,
    final_transition:finalization,remaining_minutes:remainingMinutes,remaining_time_as_of:remainingMinutes===null?null:live.at,
    last_proven_interval:proven?{value:proven.value,from:proven.from,to:proven.to,completed:proven.completed,endpoints:proven.endpoints,quality:final?'final':ageReason(proven.to,now,600000)||'current'}:null,
    interval:observed.value===null?null:{from:observed.from,to:observed.to,new_completions:observed.completed,endpoints:observed.endpoints},
    schedule_members:rows.map(r=>({'stage-key':r['stage-key'],'class-id':r['class-id']??null,'start-time':r['start-time']??null,'end-time':r['end-time']??null,'total-trips':r['total-trips']??null,'type':r.type??null})),
    reasons:[planned.reason,observed.reason,countIssue,live?.source_issue].filter(Boolean),labels:['schedule_window_is_estimated','interval_includes_pauses','snapshot_is_as_of_not_serving_lease']};
  const contentHash=hash({policy:POLICY,payload,inputRefs,observedAt}),scopeKey=[sourceLane,identity.show,identity.date,identity.ring,identity.group,'base'].join('|');
  const fields={'answer-key':scopeKey+'|'+contentHash,'scope-key':scopeKey,'answer-kind':'ring-pace','source-lane':sourceLane,
    'show-id':identity.show,date:identity.date,'ring-number':identity.ring,'group-id':identity.group,phase:'base',
    availability:value===null?'unavailable':method==='observed_interval'?'available':'partial','pace-method':method,
    ...(value===null?{}:{'pace-minutes-per-trip':Math.round(value*10000)/10000}),'evaluated-at':now,'observed-at':observed.value!==null?observed.to:observedAt||'',
    'reason-codes-json':JSON.stringify(payload.reasons),'input-refs-json':JSON.stringify(inputRefs),'payload-json':JSON.stringify(payload),'content-hash':contentHash};
  return {fields,baseline:observed.baseline};
}
module.exports={completed,POLICY,canonical,hash,digest,ageReason,minutes,schedulePace,intervalPace,evaluate};
