'use strict';

const { instant } = require('../wef-timing/class-timing');
const { hash } = require('../wef-timing/pace');
const { localDay } = require('../wef-timing/purge');
const { orderValue, providerTime } = require('../wef-entry-calculator/calculator');
const { base: BASE, links, unique, url, fillReferences } = require('./linker');

const PACE = 2.5746;
const positive = value => Number.isFinite(value) && value > 0;
const count = value => Number.isSafeInteger(value) && value >= 0 ? value : null;
const round = value => Math.round(value * 10000) / 10000;
const iso = ms => Number.isFinite(ms) ? new Date(Math.round(ms)).toISOString() : null;
const active = row => String(row.fields['record-status']).toLowerCase() === 'active';
const json = value => { try { return JSON.parse(value); } catch { return null; } };
const flag = value => [1, '1', true].includes(value) ? true : [0, '0', false].includes(value) ? false : null;
const before = (value, now) => instant(value) !== null && instant(value) <= now;

function counts(fields) {
  const total = count(fields?.total), gone = count(fields?.gone);
  return { total, gone, left: total !== null && gone !== null && gone <= total ? total - gone : null };
}

function latest(rows, field) {
  return rows.slice().sort((a, b) => instant(b.fields[field]) - instant(a.fields[field]))[0] || null;
}

function snapshots({ input, customer, show, day, at, runId, previous = [] }) {
  const now = instant(at);
  if (!Number.isSafeInteger(customer) || customer <= 0 || !Number.isSafeInteger(show) || show <= 0 || now === null || localDay(at) !== day || !runId) throw Error('Verified show, Eastern day and cycle required');
  const prefix = `${customer}|${show}|${day}|`, nativePrefix = `${show}|${day}|`;
  const tripRows = input.trips.filter(row => active(row) && row.fields.date === day && row.fields['trip-key']?.startsWith(prefix));
  const keyCounts = new Map();
  for (const row of tripRows) keyCounts.set(row.fields['trip-key'], (keyCounts.get(row.fields['trip-key']) || 0) + 1);
  const issues = [];
  const contexts = [];
  for (const trip of tripRows) {
    const f = trip.fields, key = f['trip-key'];
    if (keyCounts.get(key) !== 1) { issues.push({ key, reason: 'duplicate_trip_key' }); continue; }
    const ring = unique(input.rings.filter(row => links(f.ring).includes(row.id) && row.fields['ring-key'] === nativePrefix + f['ring-id']));
    const cls = unique(input.classes.filter(row => links(f.class).includes(row.id) && row.fields['class-key'] === f['class-key'] && f['class-key'] === nativePrefix + f['class-id'] && links(row.fields.ring).includes(ring?.id)));
    const group = unique(input.groups.filter(row => links(f.groups).includes(row.id) && row.fields['group-key']?.startsWith(nativePrefix) && links(row.fields.ring).includes(ring?.id) && links(cls?.fields.groups).includes(row.id)));
    const ringNumber = ring?.fields['ring-number'] ?? null;
    const classKey = `${prefix}${ringNumber}|${f['class-id']}`;
    const groupId = group ? Number(group.fields['group-key'].split('|').at(-1)) : null;
    const schedule = unique(input.schedules.filter(row => row.fields.kind === 'class' && row.fields['schedule-key']?.startsWith(prefix) && row.fields.class === classKey && !String(row.fields['record-status']).toLowerCase().startsWith('dropped')));
    const occurrence = schedule?.fields.group === `${prefix}${ringNumber}|${groupId}` ? schedule : null;
    const scratch = f.scratch === true, gone = flag(f.gone) ?? flag(f['gone-in']);
    const current = group && group.fields.status === 'Underway' && links(ring?.fields['current-group']).includes(group.id);
    const next = group && group.fields.status === 'Upcoming' && links(ring?.fields['next-group']).includes(group.id);
    const role = current ? 'Underway' : next ? 'Upcoming' : null;
    const reported = role === 'Underway' && !scratch && gone === false && before(cls?.fields['last-seen'], now) && Number(cls.fields['current-trip-id']) === Number(f['trip-id']);
    const classStatus = occurrence && role ? latest(input.classStatus.filter(row => {
      const c = row.fields;
      return c['class-key'] === classKey && c['show-id'] === show && c.date === day && c.phase === 'base' && c.status === role && before(c['snapshot-at'], now) && before(c['observed-at'], now);
    }), 'snapshot-at') : null;
    contexts.push({ trip, f, key, ring, cls, group, groupId, ringNumber, classKey, schedule: occurrence, classStatus, scratch, gone, role, reported, order: orderValue(f['order-of-go']) });
  }

  const queues = new Map();
  for (const item of contexts) {
    const id = item.group?.id;
    if (!id) continue;
    if (!queues.has(id)) queues.set(id, []);
    queues.get(id).push(item);
  }
  for (const [id, roster] of queues) {
    const group = roster[0].group.fields, raw = json(group['source-json']);
    const pending = roster.filter(row => !row.scratch && row.gone === false);
    const current = pending.filter(row => row.reported);
    const complete = raw && raw.JOUnderway !== true && raw.R2Underway !== true && counts(group).left === pending.length &&
      roster.every(row => (row.scratch || row.gone !== null) && before(row.f['last-seen'], now)) &&
      pending.every(row => row.order !== null) && new Set(pending.map(row => row.order)).size === pending.length && current.length <= 1;
    pending.sort((a, b) => Number(b.reported) - Number(a.reported) || a.order - b.order);
    queues.set(id, complete ? pending : null);
  }

  const output = [];
  for (const item of contexts) {
    const { trip, f, key, ring, cls, group, groupId, ringNumber, schedule, classStatus, scratch, gone, role, reported, order } = item;
    const reasons = [];
    const observed = before(f['last-seen'], now) ? f['last-seen'] : null;
    const status = scratch ? 'Scratched' : gone === true ? 'Gone' : reported ? 'Reported current' : gone === false ? 'Pending' : 'Unknown';
    const queue = queues.get(group?.id), rank = queue?.indexOf(item) ?? -1, ahead = rank < 0 ? null : rank;
    const nextGroup = unique(input.groups.filter(row => links(ring?.fields['next-group']).includes(row.id) && row.fields['group-key']?.startsWith(nativePrefix)));
    const nextTrip = rank < 0 ? null : queue[rank + 1] || (role === 'Underway' ? queues.get(nextGroup?.id)?.[0] : null);
    if (status === 'Pending' && ahead === null) reasons.push('pending_sequence_unavailable');

    const ringStatus = ring ? latest(input.ringStatus.filter(row => {
      const r = row.fields, p = json(r['payload-json']);
      return r['scope-key'] === `ring-current-group|${prefix}${ring.fields['ring-id']}` && r['show-id'] === show && r.date === day && r['ring-number'] === ringNumber && before(r['evaluated-at'], now) && before(r['observed-at'], now) && p?.policy === 'current-group-snapshot-v1' && p.as_of === r['observed-at'] && hash(p) === r['content-hash'];
    }), 'evaluated-at') : null;
    const cf = classStatus?.fields || {}, rf = ringStatus?.fields || {};
    const currentGroup = unique(input.groups.filter(row => links(ring?.fields['current-group']).includes(row.id) && row.fields['group-key']?.startsWith(nativePrefix)));
    const compatibleRing = currentGroup && rf['group-id'] === Number(currentGroup.fields['group-key'].split('|').at(-1));
    const classPace = cf['average-pace'], ringPace = compatibleRing ? rf['pace-minutes-per-trip'] : null;
    const paceFor = peer => {
      const measured = peer.classStatus?.fields['average-pace'];
      return positive(measured) ? measured : positive(ringPace) ? ringPace : PACE;
    };
    const pace = paceFor(item);
    const method = positive(classPace) ? 'measured-class' : positive(ringPace) ? 'ring' : 'default';
    let estimate = null, finish = null, basis = 'unavailable';
    if (role && observed && rank >= 0) {
      const workAhead = queue.slice(0, rank).reduce((sum, peer) => sum + paceFor(peer), 0);
      if (role === 'Upcoming') {
        const anchor = instant(cf['estimated-start']);
        if (anchor !== null && queue.every(peer => instant(peer.classStatus?.fields['estimated-start']) === anchor)) {
          estimate = anchor + workAhead * 60000;
          basis = 'class-estimated-start';
        } else reasons.push('class_start_or_shared_group_anchor_unavailable');
      } else if (role === 'Underway' && before(group.fields['last-seen'], now)) {
        const asOf = instant(group.fields['last-seen']);
        const currentTrip = queue[0]?.reported ? queue[0] : null;
        const started = instant(json(currentTrip?.f['source-json'])?.live?.r1_start);
        const elapsed = started !== null && started <= asOf && localDay(iso(started)) === day ? (asOf - started) / 60000 : null;
        const currentPace = currentTrip ? paceFor(currentTrip) : pace;
        const remaining = elapsed !== null && elapsed < currentPace ? currentPace - elapsed : currentPace;
        if (reported) finish = asOf + remaining * 60000;
        else estimate = asOf + (workAhead + (currentTrip ? remaining - currentPace : 0)) * 60000;
        basis = elapsed !== null && elapsed < currentPace ? 'observed-progress-with-current-remainder' : 'observed-progress-with-full-trip-pace';
      }
    }
    if (estimate !== null && (estimate < now || localDay(iso(estimate)) !== day)) {
      estimate = null;
      reasons.push('estimate_elapsed_or_outside_day');
    }
    if (estimate !== null) finish = estimate + pace * 60000;
    if (finish !== null && (finish < now || localDay(iso(finish)) !== day)) finish = null;
    if (estimate === null && status === 'Pending') reasons.push('estimated_go_unavailable');
    const startIn = estimate === null ? reported ? 0 : null : round((estimate - now) / 60000);
    const endIn = finish === null ? null : round((finish - now) / 60000);
    const plannedStart = providerTime(schedule?.fields['start-time'], day);
    const planned = queues.has(group?.id) ? contexts.filter(row => row.group?.id === group.id && !row.scratch) : [];
    const plannedRank = planned.length && planned.every(row => row.order !== null) && new Set(planned.map(row => row.order)).size === planned.length ? planned.filter(row => row.order < order).length : null;
    const plannedGo = plannedStart && plannedRank !== null && order !== null ? instant(plannedStart) + plannedRank * pace * 60000 : null;
    const late = estimate !== null && plannedGo !== null ? round((estimate - plannedGo) / 60000) : null;
    const upstreamLate = positive(cf['schedule-offset-minutes']) || (role && compatibleRing && positive(json(rf['payload-json'])?.offset_minutes));
    const context = { class: { ...counts(cls?.fields), estimatedStart: cf['estimated-start'] ?? null,
      estimatedEnd: cf['estimated-end'] ?? null, observedAt: cf['observed-at'] ?? null }, group: counts(group?.fields),
      nextTrip: nextTrip ? { key: nextTrip.key, trip: url('trips', nextTrip.trip), horse: nextTrip.f['horse-name'] ?? null, rider: nextTrip.f['rider-name'] ?? null } : null,
      horse: f['horse-name'] ?? null, rider: f['rider-name'] ?? null, role, runningOrder: rank < 0 ? null : rank + 1,
      startIn, endIn, plannedGo: iso(plannedGo), lateMinutes: late, lateWarning: positive(late) || upstreamLate ? 'Running late' : null };
    const snapshotKey = `${key}|${runId}`;
    const existing = unique(previous.filter(row => row.fields['snapshot-key'] === snapshotKey));
    const prior = latest(previous.filter(row => row.fields['trip-key'] === key && row.fields['snapshot-key'] !== snapshotKey && instant(row.fields['snapshot-at']) < now), 'snapshot-at');
    const snapshot = fillReferences({
      'snapshot-key': snapshotKey, 'trip-key': key, 'trip-id': String(f['trip-id']), date: day, 'show-id': show,
      'ring-number': ringNumber, 'class-id': f['class-id'] ?? null, 'group-id': groupId, 'snapshot-at': at, 'observed-at': observed,
      source: 'live', status, 'order-of-go': order, 'actual-order': orderValue(f['actual-order']), 'trips-ahead': ahead,
      'pace-used': pace, 'pace-method': method, 'class-start': cf['estimated-start'] ?? null, 'preceding-class-end': null,
      'estimated-go-time': iso(estimate), 'estimate-basis': estimate !== null || finish !== null ? basis : 'unavailable',
      'provider-go-time': providerTime(f['estimated-go-time'], day), 'provider-observed-at': observed,
      'previous-snapshot': prior?.fields['snapshot-key'] ?? null, 'trip-context': JSON.stringify(context),
      ...Object.fromEntries(['trip','class','schedule','ring-status','class-status'].filter(name => existing?.fields[name]).map(name => [name, existing.fields[name]])),
    }, { trips: trip, classes: cls, schedules: schedule, ringStatus, classStatus }, reason => reasons.push(reason));
    snapshot['unavailable-reasons'] = [...new Set(reasons)].join('\n');
    const ignored = new Set(['snapshot-key','snapshot-at','previous-snapshot']);
    snapshot.changes = JSON.stringify(Object.entries(snapshot).filter(([name,value]) => !ignored.has(name) && JSON.stringify(value ?? null) !== JSON.stringify(prior?.fields[name] ?? null)).map(([field,current]) => ({ field, previous: prior?.fields[field] ?? null, current })));
    output.push({ trip, snapshot, calculated: {
      'calculated-order-of-go': context.runningOrder, 'calculated-estimated-go-time': iso(estimate),
      'calculated-at-time': at, 'go-time-start-in': startIn, 'go-time-end-in': endIn,
    } });
  }
  return { output, issues };
}

module.exports = { BASE, PACE, snapshots };
