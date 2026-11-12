import { createHash } from 'node:crypto';
import { APP_BASE, equals, prefix } from './shared-base.mjs';
import { sharedScope, buildSharedDay, isSharedDataConflict } from './shared-projection.mjs';
import { overlayDay0 } from './shared-live.mjs';
import { fingerprint } from '../../../public/wef-schedule-v2/data-diff.js';
import { validDataset } from '../../../public/wef-schedule-v2/device-data.js';
import { makeDay0Diff } from '../../../public/wef-schedule-v2/day0-diff.js';
import { encodePayload, decodePayload } from './shared-payload.mjs';

const hash = value => createHash('sha256').update(fingerprint(JSON.parse(JSON.stringify(value)))).digest('hex');
const version = data => hash([data.metadata.showId,data.days,data.ringDetails,data.records]);
function parseFeed(scope, day, row) {
  if (!row || row['show-day-key'] !== day['show-day-key'] || row['show-key'] !== scope.show['show-key'] ||
      row.date !== day.date) throw Error(`schedule_feed_missing:${day.date}`);
  const {day0Diff, pending, ...data} = decodePayload(row['payload-json']);
  if (pending && (pending.date !== day.date || typeof pending.reason !== 'string' ||
      !Number.isSafeInteger(pending.sourceRowCount) || pending.sourceRowCount < 0)) throw Error('invalid_pending_schedule_day');
  if (pending && !data.records) {
    if (row['row-count'] !== 0) throw Error('invalid_pending_schedule_day');
    return {data:null,pending};
  }
  if (!validDataset(data,scope.show.id,'day0') || data.metadata.baseId !== APP_BASE ||
      data.metadata.source !== 'wef-schedule-app' || data.days[0].id !== day.id ||
      data.days[0].date !== day.date || data.records.length !== row['row-count'] || data.records.some(r => r.live)) {
    throw Error('invalid_saved_schedule_feed');
  }
  data.metadata.dataVersion = version(data);
  return {data,day0Diff,pending};
}

export async function scheduleScope(api) {
  const {show,day0} = await sharedScope(api);
  return {showId:show.id,show:show.show_name,defaultDate:day0.date,timezone:'America/New_York'};
}

export async function readScheduleFeed(api, showId, lane) {
  const scope = await sharedScope(api);
  if (showId !== scope.show.id) throw Error('schedule_show_changed');
  if (lane === 'day0') {
    const matches = await api.read('feed',equals('show-day-key',scope.day0['show-day-key']));
    if (matches.length !== 1) throw Error('day0_feed_unavailable');
    const {data,day0Diff,pending} = parseFeed(scope,scope.day0,matches[0]);
    if (!data) throw Error('day0_feed_unavailable');
    if (day0Diff?.baseVersion === data.metadata.dataVersion && day0Diff.showId === showId &&
        day0Diff.date === scope.day0.date) return day0Diff;
    return {...makeDay0Diff(data,data),checkedAt:data.metadata.capturedAt};
  }
  if (lane !== 'full') throw Error('invalid_feed_lane');
  const saved = await api.read('feed',equals('show-key',scope.show['show-key']));
  const all = scope.days.map(day => {
    const matches = saved.filter(r => r['show-day-key'] === day['show-day-key']);
    if (matches.length > 1) throw Error('duplicate_schedule_feed');
    return {...parseFeed(scope,day,matches[0]),day,saved:matches[0]};
  });
  const available = all.filter(d => d.data).map(d => d.data);
  if (!available.length) throw Error('day0_feed_unavailable');
  const checkedAt = all.map(d => d.saved['generated-at']).sort().at(-1);
  const dayVersions = Object.fromEntries(available.map(d => [d.days[0].date,d.metadata.dataVersion]));
  const pendingDays = all.filter(d=>d.pending).map(d=>({...d.pending,hasSavedData:Boolean(d.data)}));
  return {
    metadata:{showId,show:scope.show.show_name,baseId:APP_BASE,source:'wef-schedule-app',timezone:'America/New_York',
      capturedAt:checkedAt,refreshedAt:checkedAt,defaultDate:scope.day0.date,dayVersions,pendingDays,
      package:{lane:'full',complete:true,startedAt:checkedAt,dates:scope.days.map(d => d.date)}},
    days:all.map(({day,data,pending}) => ({...(data?.days[0] ?? {id:day.id,date:day.date,rings:[]}),
      ringDetails:data?.ringDetails ?? {},...(pending ? {pending:true} : {})})),
    records:available.flatMap(d => d.records),ringDetails:Object.assign({},...available.map(d => d.ringDetails))
  };
}

function serialized(data) {
  return encodePayload(data);
}
function fieldsFor(scope, day, data, day0Diff) {
  return {'show-day-key':day['show-day-key'],'show-key':scope.show['show-key'],date:day.date,
    'day-priority':day['day-priority'],'generated-at':data.metadata.capturedAt,
    ...(data.metadata.sourceUpdatedAt ? {'source-updated-at':data.metadata.sourceUpdatedAt} : {}),
    'row-count':data.records.length,'payload-json':serialized({...data,...(day0Diff ? {day0Diff} : {})})};
}

export async function refreshScheduleFeed(api, lane, now, progress = []) {
  const scope = await sharedScope(api);
  const saved = await api.read('feed',equals('show-key',scope.show['show-key']));
  const savedByKey = new Map();
  for (const row of saved) {
    if (savedByKey.has(row['show-day-key'])) throw Error('duplicate_schedule_feed');
    savedByKey.set(row['show-day-key'],row);
  }
  const checkScope = async () => {
    const current = await sharedScope(api);
    const keys = value => value.days.map(d => [d.id,d['day-priority']]);
    if (current.show.id !== scope.show.id || fingerprint(keys(current)) !== fingerprint(keys(scope))) throw Error('schedule_scope_changed');
  };
  if (lane === 'full') {
    const schedules = await api.read('schedules',prefix('schedule-key',scope.show['show-key'] + '|'));
    const byDay = new Map(scope.days.map(day => [day.date,schedules.filter(r =>
      r['schedule-key']?.startsWith(day['show-day-key'] + '|') &&
      !['dropped','absent'].includes(String(r['record-status']).toLowerCase())
    )]));
    const changes = [], pendingDays = [];
    for (const day of scope.days) {
      const previous = savedByKey.get(day['show-day-key']);
      const old = previous && parseFeed(scope,day,previous);
      let dataset;
      try {({dataset} = await buildSharedDay(api,scope,day,now,byDay.get(day.date),{snapshots:true}));}
      catch(error) {
        const incomplete = /^(invalid_shared_class|missing_shared_class|unconfirmed_empty_day:)/.test(error.message);
        if (api.onConflict && isSharedDataConflict(error)) {
          api.onConflict({reason:error.message,date:day.date,recordKey:error.details?.recordKey??day['show-day-key'],
            details:{source:'Shared Data day projection',...error.details,scheduleKeys:byDay.get(day.date).map(r=>r['schedule-key']),
              handling:old?.data?'Retain confirmed day package; continue other dates.':'Retain source records; await admin review; continue other dates.'}});
        } else if (day.date === scope.day0.date || !incomplete) throw error;
        const pending = {date:day.date,reason:error.message,sourceRowCount:byDay.get(day.date).length};
        pendingDays.push(pending);
        if (fingerprint(old?.pending ?? null) === fingerprint(pending) && previous['day-priority'] === day['day-priority']) continue;
        const retained = old?.data ?? {metadata:{defaultDate:day.date}};
        changes.push({'show-day-key':day['show-day-key'],'show-key':scope.show['show-key'],date:day.date,
          'day-priority':day['day-priority'],'generated-at':now,'row-count':old?.data?.records.length ?? 0,
          'payload-json':serialized({...retained,pending})});
        continue;
      }
      dataset.metadata.dataVersion = version(dataset);
      const matching = old?.day0Diff?.baseVersion === dataset.metadata.dataVersion ? old.day0Diff : null;
      if (old?.data && Date.parse(old.data.metadata.sourceUpdatedAt) > Date.parse(dataset.metadata.sourceUpdatedAt)) {
        throw Error(`older_shared_dataset:${day.date}`);
      }
      if (!old?.pending && old?.data?.metadata.dataVersion === dataset.metadata.dataVersion &&
          old.data.metadata.sourceUpdatedAt === dataset.metadata.sourceUpdatedAt &&
          previous['day-priority'] === day['day-priority'] &&
          (day.date === scope.day0.date || !old.day0Diff)) continue;
      changes.push(fieldsFor(scope,day,dataset,day.date === scope.day0.date ? matching : null));
    }
    await checkScope();
    // Validate every day before the first write. A failed upsert cannot clear a date.
    for (const fields of changes) {
      await api.upsert('feed',fields);
      progress.push(fields['show-day-key']);
    }
    return {showId:scope.show.show_id,date:scope.day0.date,upserts:progress,pendingDays};
  }
  if (lane !== 'day0') throw Error('invalid_feed_lane');
  const day = scope.day0, previous = savedByKey.get(day['show-day-key']);
  const base = parseFeed(scope,day,previous);
  if (!base.data) throw Error('day0_feed_unavailable');
  const built = await buildSharedDay(api,scope,day,now);
  const effective = await overlayDay0(api,scope,built,now);
  const diff = makeDay0Diff(base.data,effective);
  const {checkedAt:ignored,...oldDiff} = base.day0Diff ?? {};
  const before = base.day0Diff ? oldDiff : makeDay0Diff(base.data,base.data);
  const changed = fingerprint(before) !== fingerprint(diff);
  if (changed) {
    await checkScope();
    const [current] = await api.read('feed',equals('show-day-key',day['show-day-key']));
    if (current?.id !== previous.id || current?.['payload-json'] !== previous['payload-json']) throw Error('day0_base_changed');
    await api.upsert('feed',{'show-day-key':day['show-day-key'],'payload-json':serialized({...base.data,day0Diff:{...diff,checkedAt:now}})});
    progress.push(day['show-day-key']);
  }
  return {showId:scope.show.show_id,date:day.date,changed,
    added:diff.rows.upsert.filter(r => !base.data.records.some(old => old.key === r.key)).map(r => r.key),
    updated:diff.rows.upsert.filter(r => base.data.records.some(old => old.key === r.key)).map(r => r.key),
    removed:diff.rows.remove,tripChanges:diff.entries.map(e => ({row:e.row,upsert:e.upsert.map(t => t.key),removed:e.remove}))};
}
