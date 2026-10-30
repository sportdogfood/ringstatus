"use strict";
const path = require('node:path');
const {SCHEDULES} = require('./api');
const {archive, scheduleFingerprint, collectionToken} = require('./runner');
const {ageReason, hash} = require('./pace');
const {validDay} = require('./purge');
const HISTORY = path.resolve(__dirname, '../wef-schedules/runtime/history/datasets');
const LIMIT = 1800000;
const positive = value => Number.isSafeInteger(value) && value > 0;
const count = value => Number.isSafeInteger(value) && value >= 0;
const equal = (a, b) => hash(a ?? null) === hash(b ?? null);
const COLUMNS = {
  'show-id':'show_id', date:'show_date', ring:'ring_number', 'group-id':'class_group_id',
  group:'group_name', sequence:'class_group_sequence', 'class-id':'class_id',
  'class-number':'class_number', 'class-name':'class_name', type:'class_type',
  'sequence-type':'schedule_sequencetype', 'start-time':'estimated_start_time',
  'end-time':'estimated_end_time', 'total-trips':'total_trips', 'completed-trips':'completed_trips'
};
const NUMERIC = new Set(['show-id','ring','group-id','sequence','class-id','class-number','total-trips','completed-trips']);
function projected(value, field) {
  if (value === undefined || value === null || value === '') return null;
  return NUMERIC.has(field) ? Number(value) : String(value);
}
function unique(rows, field) {
  const seen = new Set();
  for (const row of rows) {
    const key = row.fields[field];
    if (typeof key !== 'string' || !key || seen.has(key)) throw Error('Duplicate/missing ' + field);
    seen.add(key);
  }
}
function selected(rows, now) {
  const found = rows.filter(r => r.fields.is_default === true);
  if (found.length !== 1) throw Error('Exactly one Schedules default show required');
  const show = found[0], f = show.fields;
  if (!positive(f.show_id) || !positive(f.customer_id) || f['show-key'] !== `${f.customer_id}|${f.show_id}`)
    throw Error('Schedules show identity unproven');
  if (![f.start_date,f.end_date,f.selected_date].every(validDay) || f.start_date > f.end_date || f.selected_date < f.start_date || f.selected_date > f.end_date)
    throw Error('Schedules show dates invalid');
  const reason = ageReason(f.last_checked, now, LIMIT);
  if (reason) throw Error('Schedules show ' + reason);
  return show;
}

// Read-only adapter: callers supply a paginated list API. No default transport or writes.
// Invalid dates expose reasons, never rows usable for arithmetic. Blocks are retained
// explicitly; source status is evidence only, never a class-completion declaration.
async function loadSchedulesSource({api, clock = () => new Date().toISOString(), readArchive = archive} = {}) {
  if (!api || typeof api.list !== 'function') throw Error('Injected read API required');
  const show = selected(await api.list(SCHEDULES, 'shows'), clock()), s = show.fields;
  const sid = s.show_id, customer = s.customer_id, filter = `{show-id}=${sid}`;
  const before = await api.list(SCHEDULES, 'show-collection', filter);
  const schedules = await api.list(SCHEDULES, 'schedules', filter);
  const rings = await api.list(SCHEDULES, 'rings', `{show_id}=${sid}`);
  unique(schedules, 'stage-key');
  if (before.some(r => r.fields['show-id'] !== sid) || schedules.some(r => r.fields['show-id'] !== sid))
    throw Error('Schedules response contains another show');
  const dates = {}, rows = [];
  const days = [...new Set([...before,...schedules].map(r => r.fields.date))].sort();
  for (const day of days) {
    const collections = before.filter(r => r.fields.date === day), records = schedules.filter(r => r.fields.date === day);
    const c = collections[0]?.fields;
    const evidence = {base:SCHEDULES,table:'show-collection',record:collections[0]?.id ?? null,
      dataset:c?.['dataset-id'] ?? null,fingerprint:c?.fingerprint ?? null,observed_at:c?.completed ?? null};
    const result = {observedAt:evidence.observed_at,validUntil:null,reason:null,sourceRefs:[evidence],rows:[]};
    dates[day] = result;
    if (!validDay(day) || day < s.start_date || day > s.end_date) { result.reason='schedule_date_invalid'; continue; }
    if (collections.length !== 1 || c.status !== 'complete') { result.reason='schedule_collection_not_complete'; continue; }
    result.reason = ageReason(c.completed, clock(), LIMIT);
    if (result.reason) continue;
    try {
      if (!Number.isSafeInteger(c['row-count']) || c['row-count'] < 0 || records.length !== c['row-count'] ||
          typeof c['dataset-id'] !== 'string' || !/^[A-Za-z0-9_-]+$/.test(c['dataset-id']) ||
          records.some(r => r.fields['dataset-id'] !== c['dataset-id'])) throw Error('dataset_membership');
      const raw = records.map(r => JSON.parse(r.fields['row-json']));
      if (scheduleFingerprint(raw) !== c.fingerprint) throw Error('source_fingerprint');
      const frozen = await readArchive(path.join(HISTORY,c['dataset-id']+'.json.gz'),HISTORY);
      if (!Array.isArray(frozen) || frozen.length !== records.length || scheduleFingerprint(frozen) !== c.fingerprint) throw Error('archive_fingerprint');
      for (let n=0;n<records.length;n++) {
        const f=records[n].fields, r=raw[n];
        if (f['stage-key'] !== `${sid}|${day}|${r.key}` || f.status !== (r.inactive ? 'dropped' : 'active')) throw Error('stage_retention');
        if (r.fields.customer_id != null && Number(r.fields.customer_id) !== customer) throw Error('customer');
        for (const [dest,src] of Object.entries(COLUMNS)) {
          if (!equal(f[dest],projected(r.fields[src],dest))) throw Error('projection_'+dest);
        }
      }
      const active=records.map((r,n)=>({record:r,raw:raw[n]})).filter(x=>!x.raw.inactive);
      for (const {record,raw:r} of active) {
        const f=record.fields, ring=f.ring, group=f['group-id'], classId=f['class-id'];
        if (!positive(group) || !count(ring)) throw Error('group_ring_identity');
        const matches=rings.filter(x=>x.fields.show_id===sid && x.fields.ring_date===day && x.fields.ring_number===ring);
        if (matches.length!==1) throw Error('ring_reference_unresolved');
        const block=Boolean(r.fields.schedule_break) || ['break','schooling'].includes(String(f.type).toLowerCase()) || !positive(classId);
        const members=active.filter(x=>x.record.fields.ring===ring && x.record.fields['group-id']===group);
        const singleton=members.length===1 && !block;
        const validCounts=count(f['total-trips']) && count(f['completed-trips']) && f['completed-trips']<=f['total-trips'];
        result.rows.push({
          identity:{customer,show:sid,date:day,ring,group,class:positive(classId)?classId:null},
          stageKey:f['stage-key'],classOccurrenceId:r.fields.class_groupxclasses_id ?? null,
          groupKey:[sid,day,ring,group].join('|'),classKey:positive(classId)?[sid,day,classId].join('|'):null,
          classNumber:f['class-number'] ?? null,className:f['class-name'] ?? null,groupName:f.group ?? null,
          sequence:f.sequence ?? null,sequenceType:f['sequence-type'] ?? null,type:f.type ?? null,isBlock:block,
          startTime:f['start-time'] ?? null,endTime:f['end-time'] ?? null,
          totalTrips:singleton && count(f['total-trips'])?f['total-trips']:null,
          completedTrips:singleton && validCounts?f['completed-trips']:null,
          countScope:singleton?'singleton-class-group':'unresolved',
          sourceTotalTrips:f['total-trips'] ?? null,sourceCompletedTrips:f['completed-trips'] ?? null,
          sourceStatus:r.fields.status ?? null,classStatus:null,retentionStatus:'active',
          manualTimeOverride:r.fields.manual_time_override===true || r.fields.manual_time_overide===true,
          observedAt:c.completed,sourceRefs:[evidence,{base:SCHEDULES,table:'schedules',record:record.id,key:f['stage-key'],dataset:c['dataset-id']},
            {base:SCHEDULES,table:'rings',record:matches[0].id}],
          reasons:block?['non_class_block']:!singleton?['class_count_semantics_unproven']:validCounts?[]:['class_completed_count_unusable']
        });
      }
    } catch (error) {
      result.reason='schedule_consistency_unproven';result.detail=error.message;result.rows=[];
    }
  }
  // Fence after archive validation as well as table reads; refresh no source clocks.
  const after=await api.list(SCHEDULES,'show-collection',filter);
  const ringsAfter=await api.list(SCHEDULES,'rings',`{show_id}=${sid}`);
  const showAfter=selected(await api.list(SCHEDULES,'shows'),clock());
  if (collectionToken(before)!==collectionToken(after) || collectionToken(rings)!==collectionToken(ringsAfter) || !equal(show,showAfter)) {
    const error=Error('Schedules source changed during read');error.code='SOURCE_CHANGED';throw error;
  }
  const now=clock();
  for (const result of Object.values(dates)) {
    if (!result.reason) result.reason=ageReason(result.observedAt,now,LIMIT);
    if (result.reason) result.rows=[];
    else {
      result.validUntil=new Date(Math.min(Date.parse(result.observedAt),Date.parse(s.last_checked))+LIMIT).toISOString();
      rows.push(...result.rows);
    }
  }
  return {sourceLane:'schedules_live',show,context:{customer,show:sid,selectedDate:s.selected_date,startDate:s.start_date,endDate:s.end_date},
    evaluatedAt:now,sourceRefs:[{base:SCHEDULES,table:'shows',record:show.id,observed_at:s.last_checked}],dates,rows};
}
module.exports={loadSchedulesSource};
