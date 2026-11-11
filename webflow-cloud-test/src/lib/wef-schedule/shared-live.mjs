import { equals } from './shared-base.mjs';

const positiveOrder = value => Number.isInteger(value) && value > 0 && value < 100000;
const dateInShowZone = value => new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(value));
const idOf = value => typeof value === 'string' ? value : value?.id;
function pointsTo(value,row,key) {
  if (Array.isArray(value)) return value.length === 1 && idOf(value[0]) === row.id;
  return value === row.id || (Boolean(row[key]) && value === row[key]);
}
function one(rows) {return rows.length === 1 ? rows[0] : null;}
function distinctLinks(value,groups) {
  if (!Array.isArray(value)) return null;
  const result = value.map(id => groups.find(g => g.id === idOf(id))?.['class-group-id']);
  return result.every(Number.isSafeInteger) && new Set(result).size === result.length ? result : null;
}

// Use the established Live publication age window and original, zoned observation time.
export async function overlayDay0(api, scope, built, now) {
  const {show,day0} = scope, {dataset,trips} = built;
  if (day0.date !== dataset.metadata.defaultDate || day0.date !== dateInShowZone(now)) return dataset;
  const live = {};
  for (const name of ['liveRings','liveGroups','liveClasses','liveEntries']) {
    live[name] = await api.read(name, equals('date',day0.date));
  }
  const current = row => {
    const value = row['source-updated-at'];
    const age = Date.parse(now) - Date.parse(value);
    return row.date === day0.date && row['customer-id'] === show.customer_id &&
      row['scope-status'] !== 'historical' && typeof value === 'string' &&
      /T.*(?:Z|[+-]\d{2}:\d{2})$/.test(value) && age >= -60000 && age <= 360000;
  };
  const sameShow = row => current(row) && row['show-id'] === show.show_id;
  const data = structuredClone(dataset);
  for (const row of data.records) {
    const ring = one(live.liveRings.filter(r => sameShow(r) && r['ring-id'] === row.ring &&
      r['ring-number'] === data.ringDetails[row.ring].number));
    if (!ring) continue;
    const groupId = Number(row.groupId?.split('|').at(-1));
    const groups = live.liveGroups.filter(g => current(g) && pointsTo(g.ring,ring,'ring-key'));
    const group = one(groups.filter(g => g['class-group-id'] === groupId));
    const classId = Number(row.classId?.split('|').at(-1));
    const uniqueOccurrence = row.classId && data.records.filter(r => r.classId === row.classId).length === 1;
    const liveClass = uniqueOccurrence && one(live.liveClasses.filter(c => sameShow(c) && c['class-id'] === classId &&
      c['detail-available'] === true && pointsTo(c.ring,ring,'ring-key')));
    const state = {};
    for (const [source,target] of [['current-group','currentGroups'],['next-group','nextGroups'],['completed-groups','completedGroups']]) {
      const keys = distinctLinks(ring[source],groups);
      if (keys !== null) state[target] = keys;
    }
    if (group) {
      if (typeof group.status === 'string' && group.status.trim()) state.groupStatus = group.status;
      if (typeof group['is-live'] === 'boolean') state.groupIsLive = group['is-live'];
      if (/^(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?$/.test(group['estimated-start-time'] ?? '') &&
          !['00:00','00:00:00'].includes(group['estimated-start-time'])) state.groupEstimatedStart = group['estimated-start-time'];
    }
    if (liveClass) {
      for (const entry of row.entries) {
        const source = trips.get(entry.key)?.raw;
        if (!source || !Number.isSafeInteger(Number(source.trip_id)) || Number(source.trip_id) <= 0) continue;
        const matched = one(live.liveEntries.filter(e => sameShow(e) &&
          e['participation-link-status'] === 'current' &&
          Number.isFinite(Date.parse(e['participation-verified-at'])) &&
          Date.parse(e['participation-verified-at']) <= Date.parse(now) &&
          pointsTo(e.class,liveClass,'class-key') && e['trip-id'] === Number(source.trip_id) &&
          e['entry-id'] === Number(source.entry_id) && e['horse-id'] === Number(source.horse_id) &&
          ['rider','trainer'].every(k => !Number(source[k+'_id']) || e[k+'-id'] === Number(source[k+'_id']))));
        if (!matched) continue;
        if (positiveOrder(matched['order-of-go'])) entry.order = matched['order-of-go'];
        if (positiveOrder(matched['actual-order'])) entry.actualOrder = matched['actual-order'];
        if (typeof matched.scratch === 'boolean') entry.status = matched.scratch ? 'scratch' : '';
        if (liveClass['current-trip-id'] === matched['trip-id'] && entry.status !== 'scratch') state.currentTripKey = entry.key;
      }
      row.entries.sort((a,b) => (a.order ?? Infinity) - (b.order ?? Infinity) ||
        String(a.entryNo).localeCompare(String(b.entryNo),undefined,{numeric:true}) || a.key.localeCompare(b.key));
    }
    if (Object.keys(state).length) row.live = state;
  }
  return data;
}
