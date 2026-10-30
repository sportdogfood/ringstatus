"use strict";

// Pure tenant-independent linking. Callers supply validated retained observations
// and source/local schedule parity; this module performs no IO or record repairs.
function positive(value, label) {
  if (!['number', 'string'].includes(typeof value) || String(value).trim() === '') throw Error('Invalid ' + label);
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n <= 0) throw Error('Invalid ' + label);
  return n;
}
function day(value) {
  const date = String(value ?? '').slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !Number.isFinite(Date.parse(date)) || new Date(date).toISOString().slice(0, 10) !== date) throw Error('Invalid source date');
  return date;
}
const sameId = (a, b) => {
  try { return positive(a, 'identity') === positive(b, 'identity'); }
  catch { return false; }
};
const roster = ids => [...new Set(ids.map(id => positive(id, 'inventory class ID')))].sort((a, b) => a - b);
const equalRoster = (a, b) => JSON.stringify(roster(a)) === JSON.stringify(roster(b));
const unresolved = reason => ({status: 'unresolved', reason});
const duplicateKey = (schedules,record) => schedules.filter(r=>r.fields.status==='active'&&r.fields['stage-key']===record.fields['stage-key']).length>1;

function scheduleSource(record) {
  const f = record.fields;
  const raw = f['row-json'] ? JSON.parse(f['row-json']).fields : {};
  for (const [field, source] of [['show-id','show_id'],['class-id','class_id'],['ring','ring_number'],['group-id','class_group_id']]) {
    if (raw[source] != null && f[field] != null && !sameId(raw[source], f[field])) throw Error('Conflicting schedule ' + field);
  }
  if (raw.show_date && day(raw.show_date) !== day(f.date)) throw Error('Conflicting schedule date');
  if (typeof f['stage-key'] !== 'string' || !f['stage-key'] || !record.id) throw Error('Missing operational schedule identity');
  return {record, groupId: f['group-id'] ?? raw.class_group_id, occurrenceId: raw.class_groupxclasses_id};
}

// This is the existing normalizeEntries show/class/date selector, separated from
// its trainer-feed normalization. A class association does not prove group eligibility.
function classSchedule(schedules, showId, classId, date, ring) {
  const matches = schedules.filter(r => sameId(r.fields['show-id'], showId) && sameId(r.fields['class-id'], classId) && r.fields.date === date && r.fields.status === 'active');
  if (matches.some(r=>duplicateKey(schedules,r))) return {status:'Conflict',reason:'Duplicate shared schedule key'};
  if (matches.length > 1) return {status: 'Conflict', reason: 'Ambiguous class/date schedule'};
  if (!matches.length) return unresolved('No active class/date schedule');
  if (!sameId(matches[0].fields.ring, ring)) return unresolved('Source/schedule ring-number conflict');
  try { return {status: 'linked', basis: 'class-date', ...scheduleSource(matches[0])}; }
  catch (error) { return unresolved(error.message); }
}

// inventory is a normalized COMPLETE group inventory from the same accepted
// revision as sourceEvidence. Each group carries date, ringNumber, ringId,
// groupId, classIds and optional occurrences [{classId, occurrenceId}].
function groupSchedule({payload, trips, schedules, show, inventory, sourceEvidence}) {
  try {
    if (!sourceEvidence?.revision || inventory?.complete !== true || inventory.revision !== sourceEvidence.revision) return unresolved('Complete same-revision group inventory unavailable');
    if (!sameId(inventory.showId, show.show_id) || !sameId(inventory.customerId, show.customer_id)) return unresolved('Inventory source/customer/show conflict');
    const c = payload.class, related = payload.class_related_data, date = day(related.date);
    const ring = positive(related.ring, 'ring number'), groupId = positive(related.class_group_id, 'parent group ID');
    const ringId = positive(c.schedule_ring_id, 'ring ID');
    const groups = inventory.groups.filter(g => g.date === date && sameId(g.ringNumber, ring));
    const parents = groups.filter(g => sameId(g.groupId, groupId));
    if (parents.length !== 1) return unresolved('Explicit parent group missing or duplicated');
    const parent = parents[0];
    if (!sameId(parent.ringId, ringId)) return unresolved('Source/inventory ring-ID conflict');
    const oog = payload.class_group_order_of_go?.entries;
    if (!Array.isArray(oog)) return unresolved('Group order membership unavailable');
    if (!equalRoster(oog.map(r => r.class_id), parent.classIds)) return unresolved('Group order roster incomplete or contradicts parent inventory');
    if (groups.filter(g => equalRoster(g.classIds, parent.classIds)).length !== 1) return unresolved('Indistinguishable group rosters');
    for (const trip of trips) {
      const uuid = trip.entryxclasses_uuid;
      if (typeof uuid !== 'string' || !uuid.trim() || uuid !== uuid.trim()) return unresolved('Explicit trip UUID unavailable');
      const tripId = positive(trip.trip_id, 'trip ID');
      const matches = oog.filter(r => sameId(r.class_id, c.class_id) && sameId(r.trip_id, tripId) && r.entryxclasses_uuid === uuid);
      if (matches.length !== 1 || ['entry_id','horse_id','rider_id'].some(k => !sameId(matches[0]?.[k], trip[k]))) return unresolved('Own-class group trip identity missing, conflicting or duplicated');
      if (Number(trip.trainer_id) > 0 && Number(matches[0].trainer_id) > 0 && !sameId(matches[0].trainer_id, trip.trainer_id)) return unresolved('Group trip trainer identity conflict');
    }
    const candidates = schedules.filter(r => sameId(r.fields['show-id'], show.show_id) && sameId(r.fields['class-id'], c.class_id) && r.fields.date === date && sameId(r.fields.ring, ring) && r.fields.status === 'active').map(scheduleSource);
    if(candidates.some(r=>duplicateKey(schedules,r.record)))return unresolved('Duplicate shared schedule key');
    // The shared schedule may omit group metadata. A unique class/date/ring
    // still establishes the ordinary schedule link; group membership above is
    // independently proven by retained source roster and exact trip identities.
    if(candidates.length===1 && candidates[0].groupId==null)
      return {...candidates[0],status:'linked',basis:'class-date',groupId,revision:sourceEvidence.revision};
    const matches = candidates.filter(r => sameId(r.groupId, groupId));
    if (matches.length !== 1) return unresolved('Operational group occurrence missing or ambiguous');
    const occurrences = (parent.occurrences || []).filter(o => sameId(o.classId, c.class_id));
    if (occurrences.length > 1) return unresolved('Repeated source group occurrence');
    if (occurrences.length === 1 && !sameId(occurrences[0].occurrenceId, matches[0].occurrenceId)) return unresolved('Source/operational occurrence conflict');
    return {status: 'linked', basis: 'observed-group', groupId, revision: sourceEvidence.revision, ...matches[0]};
  } catch (error) { return unresolved(error.message); }
}

function linkClass(payload, show, schedules, observedAt, {inventory, sourceEvidence} = {}) {
  const sid = positive(show.show_id, 'show ID'), cid = positive(payload.class?.class_id, 'class ID');
  if (!sameId(payload.show_id, sid) || !sameId(payload.class.show_id, sid) || !sameId(payload.class.customer_id, show.customer_id)) throw Error('Class source/customer/show mismatch');
  const date = day(payload.class_related_data?.date), ring = positive(payload.class_related_data?.ring, 'ring number');
  if (date < day(show.start_date) || date > day(show.end_date)) throw Error('Class date outside canonical show');
  if (!Number.isFinite(Date.parse(observedAt))) throw Error('Missing source observation time');
  if (!Array.isArray(payload.trips) || payload.total_entry_trips !== payload.trips.length) throw Error('Incomplete class membership');
  const rows = new Map(), seenTrips = new Set();
  for (const trip of payload.trips) {
    if (trip.class_id != null && !sameId(trip.class_id, cid)) throw Error('Trip class conflict');
    const entry = positive(trip.entry_id, 'entry ID'), horse = positive(trip.horse_id, 'horse ID'), rider = positive(trip.rider_id, 'rider ID');
    const tripKey = trip.entryxclasses_uuid || (trip.trip_id != null ? 'id:' + positive(trip.trip_id, 'trip ID') : null);
    if (tripKey && seenTrips.has(tripKey)) throw Error('Duplicate trip identity');
    if (tripKey) seenTrips.add(tripKey);
    const key = `${sid}|${date}|${entry}|${cid}`, prior = rows.get(key);
    if (prior && (prior['horse-id'] !== horse || prior['rider-id'] !== rider)) throw Error('Conflicting participation identity');
    const trainer = trip.trainer_id == null || trip.trainer_id === '' || Number(trip.trainer_id) === 0 ? null : positive(trip.trainer_id, 'trainer ID');
    const row = prior || {'entry-key': key, 'show-id': sid, date, 'entry-id': entry, 'class-id': cid,
      'horse-id': horse, 'horse-name': String(trip.horse || ''), 'rider-id': rider, 'rider-name': String(trip.rider_name || trip.rider || ''),
      'source-trainer-ids': [], 'ring-number': ring, 'class-name': String(payload.class.name || ''), status: 'active',
      'observation-status': 'successful', 'last-seen': observedAt, 'source-trips': []};
    if (trainer) row['source-trainer-ids'] = [...new Set([...row['source-trainer-ids'], trainer])].sort((a,b) => a-b);
    row['source-trips'].push(trip); rows.set(key, row);
  }
  const ordinary = classSchedule(schedules, sid, cid, date, ring);
  for (const row of rows.values()) {
    const group = groupSchedule({payload, trips: row['source-trips'], schedules, show, inventory, sourceEvidence});
    const selected = group.status === 'linked' ? group : ordinary;
    row['group-link-status'] = group.status;
    if (group.status === 'linked') { row['source-group-id'] = group.groupId; row['group-link-evidence'] = group.revision; }
    else row['group-link-reason'] = group.reason;
    row['schedule-link-status'] = selected.status;
    if (selected.status === 'linked') {
      row['schedule-link-basis'] = selected.basis;
      row['schedule-key'] = selected.record.fields['stage-key'];
      row['schedule-keys'] = JSON.stringify([row['schedule-key']]);
      row.schedules = [selected.record.id];
    } else row['schedule-link-reason'] = selected.reason;
    // Unresolved links are omitted, not cleared. Persistence must preserve prior
    // verified links as history and never publish them as fresh group eligibility.
  }
  return [...rows.values()].sort((a,b) => a['entry-key'].localeCompare(b['entry-key']));
}

module.exports = {linkClass, classSchedule, groupSchedule};
