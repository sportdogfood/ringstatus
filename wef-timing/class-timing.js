'use strict';

const {createHash} = require('node:crypto');
const POLICY = 'class-timing-v1';
const canonical = value => Array.isArray(value) ? value.map(canonical)
  : value && typeof value === 'object'
    ? Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])])) : value;
const hash = value => createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
const text = value => typeof value === 'string' && value.trim().length > 0;
const positive = value => Number.isSafeInteger(value) && value > 0;
const count = value => Number.isSafeInteger(value) && value >= 0;
const identityKeys = ['customer', 'show', 'date', 'ring', 'group', 'class', 'occurrence', 'phase'];

// Reject offsetless times and calendar rollover; all arithmetic uses full instants.
function instant(value) {
  if (typeof value !== 'string') return null;
  const m = /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (!m || +m[2] > 23 || +m[3] > 59 || +m[4] > 59) return null;
  if (m[6] !== 'Z' && (+m[6].slice(1, 3) > 23 || +m[6].slice(4) > 59)) return null;
  const calendar = Date.parse(m[1] + 'T00:00:00Z');
  if (!Number.isFinite(calendar) || new Date(calendar).toISOString().slice(0, 10) !== m[1]) return null;
  const result = Date.parse(value);
  return Number.isFinite(result) ? result : null;
}

function localDate(at, timezone) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: timezone, year: 'numeric', month: '2-digit', day: '2-digit',
  }).formatToParts(at);
  const get = key => parts.find(p => p.type === key).value;
  return `${get('year')}-${get('month')}-${get('day')}`;
}

/**
 * Pure adapter boundary: no reads, writes, clock, runners, or delivery actions.
 * evaluateClassTiming({identity, sourceLane:'data_live', timezone, now, data?, live?, ringPace?})
 * identity: {customer,show,date,ring,group,class,occurrence,phase}; phase is 'base'.
 * Each evidence object supplies matching identity, source_lane, observed_at,
 * valid_until, ref:{source_key,revision}. The caller verifies source content and
 * chooses valid_until under its approved freshness policy; this module checks it.
 * data/live may contain start:{at,kind:'estimated'|'actual'},
 * counts:{scope:'class',total,completed?,completion_scope:'whole_class'|'phase'|'unknown'},
 * completion:{state:'completed',at,all_phases_complete:true} (Live only).
 * ringPace supplies group identity (customer/show/date/ring/group/phase),
 * member_class_ids, pace_minutes_per_trip, method:'observed_interval'|'schedule_window',
 * answer_key and the same evidence envelope. Counts never come from group pace.
 * No missing field is interpreted as zero or as completion. Caller-owned explicit
 * manual overrides should already be resolved into the chosen start evidence.
 */
function evaluateClassTiming(input) {
  const {identity, sourceLane, timezone, now, data, live, ringPace} = input;
  if (sourceLane !== 'data_live') throw Error('Invalid source lane');
  if (input.schedule != null || input.schedules != null) throw Error('Schedules evidence is not supported');
  if (!identity || !['customer', 'show', 'ring', 'group', 'class'].every(k => positive(identity[k]))
      || !text(identity.occurrence) || identity.phase !== 'base'
      || instant(identity.date + 'T00:00:00Z') === null) throw Error('Invalid class identity');
  const evaluated = instant(now);
  if (evaluated === null) throw Error('Invalid evaluation timestamp');
  if (!text(timezone)) throw Error('Missing timezone');
  try { localDate(evaluated, timezone); } catch { throw Error('Invalid timezone'); }
  const cleanIdentity = Object.fromEntries(identityKeys.map(k => [k, identity[k]]));
  const reasons = [...(input.issues || [])], refs = [];
  function evidence(value, name, group = false) {
    if (!value) { reasons.push(name + '_missing'); return null; }
    const keys = group ? ['customer', 'show', 'date', 'ring', 'group', 'phase'] : identityKeys;
    if (value.source_lane !== sourceLane || !keys.every(k => value.identity?.[k] === identity[k])) {
      reasons.push(name + '_identity_mismatch'); return null;
    }
    const observed = instant(value.observed_at), expires = instant(value.valid_until);
    const maxAge = name === 'live' || (group && value.method === 'observed_interval') ? 600000 : 1800000;
    const finalPace = name==='ring_pace' && group && value.lifecycle==='final' && value.final_verified===true && identity.date===localDate(evaluated,timezone);
    if (observed === null || expires === null || observed > evaluated || expires <= observed
        || (!finalPace && (expires <= evaluated || evaluated - observed >= maxAge)) || expires - observed > maxAge) {
      reasons.push(name + '_observation_unusable'); return null;
    }
    if (!text(value.ref?.source_key) || !text(value.ref?.revision)) {
      reasons.push(name + '_provenance_missing'); return null;
    }
    return value;
  }
  function remember(value, role) {
    refs.push({...structuredClone(value.ref), role, source_key:value.ref.source_key, revision:value.ref.revision,
      observed_at:value.observed_at, valid_until:value.valid_until});
  }
  const sources = {live:evidence(live, 'live'), data:evidence(data, 'data')};
  for (const [name, source] of Object.entries(sources)) if (source) remember(source, name + '_evidence');
  let start = null, counts = null;
  for (const [name, source] of Object.entries(sources)) {
    if (!source) continue;
    if (!start && source.start) {
      const at = instant(source.start.at);
      if (at !== null && ['actual', 'estimated'].includes(source.start.kind)
          && (source.start.kind !== 'actual' || (name === 'live' && at <= instant(source.observed_at)))
          && localDate(at, timezone) === identity.date) {
        start = {at:new Date(at).toISOString(), kind:source.start.kind, source:name};
        remember(source, 'start');
      } else reasons.push(name + '_start_unusable');
    }
    if (!counts && source.counts) {
      const c = source.counts;
      if (c.scope === 'class' && count(c.total)
          && (c.completed == null || (count(c.completed) && c.completed <= c.total))) {
        counts = {total:c.total, completed:c.completed ?? null, source:name,
          completion_scope:['whole_class', 'phase'].includes(c.completion_scope) ? c.completion_scope : 'unknown'};
        remember(source, 'counts');
      } else reasons.push(name + '_counts_unusable');
    }
  }
  let pace = 2.5746, paceMethod = 'default_2_5746_minutes', paceAnswer = null;
  const p = evidence(ringPace, 'ring_pace', true);
  if (p) {
    if (Number.isFinite(p.pace_minutes_per_trip) && p.pace_minutes_per_trip > 0
        && ['observed_interval', 'schedule_window'].includes(p.method) && text(p.answer_key)
        && Array.isArray(p.member_class_ids) && p.member_class_ids.every(positive)
        && new Set(p.member_class_ids).size === p.member_class_ids.length && p.member_class_ids.includes(identity.class)) {
      pace = p.pace_minutes_per_trip; paceMethod = p.method; paceAnswer = p.answer_key;
      remember(p, 'pace');
    } else reasons.push('ring_pace_value_or_membership_unusable');
  }
  if (!start) reasons.push('class_start_missing');
  if (!counts) reasons.push('class_counts_missing');
  let duration = counts ? counts.total * pace : null;
  if (duration !== null && !Number.isFinite(duration)) { duration = null; reasons.push('duration_overflow'); }
  const startMs = start ? instant(start.at) : null;
  const endMs = startMs !== null && duration !== null ? startMs + duration * 60000 : null;
  const endValid = endMs !== null && Number.isFinite(endMs) && Math.abs(endMs) <= 8640000000000000;
  if (endMs !== null && !endValid) reasons.push('end_overflow');
  const end = endValid ? new Date(endMs).toISOString() : null;
  const endReached = endValid && evaluated >= endMs;
  // Owner's end-plus-equal-count rule is advisory even when round coverage is
  // unknown. Explicit phase-only/multi-round evidence must suppress the candidate.
  const countsComplete = counts !== null && counts.total > 0 && counts.completed === counts.total
    && counts.completion_scope !== 'phase';
  const completed = sources.live?.completion;
  const completionConflict = completed && (completed.all_phases_complete !== true || completed.state !== 'completed');
  const candidate = Boolean(endReached && countsComplete && !completionConflict);
  let actualCompletion = null;
  if (completed) {
    const at = instant(completed.at);
    if (completed.state === 'completed' && completed.all_phases_complete === true && at !== null
        && at <= instant(sources.live.observed_at) && localDate(at, timezone) >= identity.date && (startMs === null || at >= startMs)) {
      actualCompletion = new Date(at).toISOString(); remember(sources.live, 'completion');
    } else reasons.push('actual_completion_unusable');
  }
  const payload = {schema_version:1, policy:POLICY, identity:cleanIdentity, source_lane:sourceLane, timezone,
    evaluated_at:new Date(evaluated).toISOString(), start_at:start?.at ?? null, start_kind:start?.kind ?? 'unknown',
    start_source:start?.source ?? null, total_trips:counts?.total ?? null, completed_trips:counts?.completed ?? null,
    count_source:counts?.source ?? null, count_completion_scope:counts?.completion_scope ?? 'unknown',
    pace_minutes_per_trip:pace, pace_method:paceMethod, pace_answer_key:paceAnswer,
    duration_minutes:duration, estimated_end_at:end, end_kind:end ? 'estimated' : 'unknown',
    minutes_until_start:startMs === null ? null : Math.max(0, (startMs - evaluated) / 60000),
    minutes_since_start:startMs === null ? null : Math.max(0, (evaluated - startMs) / 60000),
    actual_completed_at:actualCompletion, completion_state:actualCompletion ? 'actual' : candidate ? 'estimated' : 'unknown',
    completion_candidate:candidate, reasons:[...new Set(reasons)].sort(), input_refs:refs};
  // Countdown/evaluation time and the validation head are receipt context. An
  // unrelated Data head must not revise a class whose consumed immutable parts
  // are unchanged. Keep the full proof in the receipt payload for reproduction;
  // hash exact consumed parts, their original observations and calculated state.
  // Completion transitions remain material. The worker owns A -> B -> A revisions.
  const {evaluated_at, minutes_until_start, minutes_since_start, ...material} = payload;
  material.input_refs = payload.input_refs.map(({producer_proof, ...ref}) => ref);
  const contentHash = hash(material);
  // JSON tuple encoding preserves exact occurrence keys, including delimiter characters.
  const scopeKey = JSON.stringify([sourceLane, 'class-timing', ...identityKeys.map(k => identity[k])]);
  const fields = {'answer-key':scopeKey + '|' + contentHash, 'scope-key':scopeKey, 'answer-kind':'class-timing',
    'source-lane':sourceLane, 'show-id':identity.show, date:identity.date, 'ring-number':identity.ring,
    'group-id':identity.group, phase:identity.phase, availability:end ? 'partial' : 'unavailable',
    'pace-method':paceMethod, 'pace-minutes-per-trip':Math.round(pace*10000)/10000, 'evaluated-at':payload.evaluated_at,
    'observed-at':refs.length ? refs.map(r => r.observed_at).sort((a,b) => instant(a)-instant(b))[0] : '',
    'reason-codes-json':JSON.stringify(payload.reasons), 'input-refs-json':JSON.stringify(refs),
    'payload-json':JSON.stringify(payload), 'content-hash':contentHash};
  return {payload, fields, flags:{completion_candidate:candidate, actual_completion:actualCompletion !== null,
    data_fallback:start?.source === 'data' || counts?.source === 'data', default_pace:paceMethod === 'default_2_5746_minutes'}};
}

module.exports = {POLICY, evaluateClassTiming, instant};
