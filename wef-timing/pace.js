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
function intervalPace(sample, previous, now) {
  if (!sample) return {value:null,reason:'live_observation_missing',baseline:null};
  const bad = ageReason(sample.at,now,600000);
  if (bad) return {value:null,reason:bad,baseline:null};
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
function evaluate({identity,rows,observedAt,inputRefs,sourceReason,live,previous,now,sourceLane='schedules_live'}) {
  if(!['schedules_live','data_live'].includes(sourceLane))throw Error('Unsupported pace source lane');
  const planned = sourceReason ? {value:null,reason:sourceReason} : schedulePace(rows);
  const observed = sourceReason ? {value:null,reason:sourceReason,baseline:null} : intervalPace(live,previous,now);
  // Re-reading an identical sample retains its prior measured interval, never changes its clock.
  if (observed.reason === 'repeated_observation' && previous?.measurement && !ageReason(previous.measurement.to,now,600000)) {
    Object.assign(observed,previous.measurement,{reason:null});
  }
  if (observed.value !== null && observed.baseline) observed.baseline = {...observed.baseline,measurement:{value:observed.value,from:observed.from,to:observed.to,completed:observed.completed,endpoints:observed.endpoints}};
  const value = observed.value ?? planned.value, method = observed.value !== null ? 'observed_interval' : planned.value !== null ? 'schedule_window' : 'unavailable';
  const payload = {schema_version:1,identity,phase:'base',schedule_pace_minutes_per_trip:planned.value,observed_interval_pace_minutes_per_trip:observed.value,
    selected_pace_minutes_per_trip:value,pace_method:method,schedule_window_minutes:planned.window??null,total_trips:planned.total??null,
    interval:observed.value===null?null:{from:observed.from,to:observed.to,new_completions:observed.completed,endpoints:observed.endpoints},
    schedule_members:rows.map(r=>({'stage-key':r['stage-key'],'class-id':r['class-id']??null,'start-time':r['start-time']??null,'end-time':r['end-time']??null,'total-trips':r['total-trips']??null,'type':r.type??null})),
    reasons:[planned.reason,observed.reason].filter(Boolean),labels:['schedule_window_is_estimated','interval_includes_pauses','snapshot_is_as_of_not_serving_lease']};
  const contentHash=hash({policy:POLICY,payload,inputRefs,observedAt}),scopeKey=[sourceLane,identity.show,identity.date,identity.ring,identity.group,'base'].join('|');
  const fields={'answer-key':scopeKey+'|'+contentHash,'scope-key':scopeKey,'answer-kind':'ring-pace','source-lane':sourceLane,
    'show-id':identity.show,date:identity.date,'ring-number':identity.ring,'group-id':identity.group,phase:'base',
    availability:value===null?'unavailable':method==='observed_interval'?'available':'partial','pace-method':method,
    ...(value===null?{}:{'pace-minutes-per-trip':Math.round(value*10000)/10000}),'evaluated-at':now,'observed-at':observed.value!==null?observed.to:observedAt||'',
    'reason-codes-json':JSON.stringify(payload.reasons),'input-refs-json':JSON.stringify(inputRefs),'payload-json':JSON.stringify(payload),'content-hash':contentHash};
  return {fields,baseline:observed.baseline};
}
module.exports={POLICY,canonical,hash,digest,ageReason,minutes,schedulePace,intervalPace,evaluate};
