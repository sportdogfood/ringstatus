import { projectData } from './data.mjs';
import { APP_BASE, equals, prefix } from './shared-base.mjs';

const dateText = value => /^\d{4}-\d{2}-\d{2}$/.test(value ?? '') && Number.isFinite(Date.parse(value));
const positive = value => Number.isSafeInteger(Number(value)) && Number(value) > 0;
const json = value => typeof value === 'string' ? JSON.parse(value) : value;
const removed = row => ['absent','dropped'].includes(String(row['record-status']).toLowerCase());
const linkedId = value => typeof value === 'string' ? value : value?.id;
function put(map, key, value, kind) {
  if (map.has(key) && JSON.stringify(map.get(key)) !== JSON.stringify(value)) throw Error(`conflicting_shared_${kind}_identity`);
  map.set(key, value);
  return value;
}

export async function sharedScope(api) {
  const shows = await api.read('shows', '{is_default}=1');
  if (shows.length !== 1) throw Error('invalid_shared_default_show');
  const show = shows[0];
  if (!show.is_default || !positive(show.customer_id) || !positive(show.show_id) || !positive(show.facility_id) ||
      show['show-key'] !== `${show.customer_id}|${show.show_id}` || !show.show_name) throw Error('invalid_shared_show');
  const days = await api.read('days', equals('show', show['show-key']));
  if (days.some(d => d.show !== show['show-key'] || !dateText(d.date) ||
    d['show-day-key'] !== `${d.show}|${d.date}` || !Number.isSafeInteger(d['day-priority']))
    || new Set(days.map(d => d.date)).size !== days.length) throw Error('invalid_shared_days');
  const day0 = days.filter(d => d['day-priority'] === 0);
  if (day0.length !== 1) throw Error('invalid_shared_day0');
  return {show, days:days.sort((a,b) => a.date.localeCompare(b.date)), day0:day0[0]};
}

export async function buildSharedDay(api, scope, day, now, scopedSchedules, {snapshots=false} = {}) {
  const {show} = scope, showId = show.id, dayId = day.id, key = day['show-day-key'];
  const ringPrefix = `${show.customer_id}|${show.facility_id}|`;
  const schedules = (scopedSchedules ?? await api.read('schedules', prefix('schedule-key', key + '|'))).filter(r => !removed(r));
  if (!schedules.length) throw Error(`unconfirmed_empty_day:${day.date}`);
  const classes = (await api.read('classes', prefix('class-key', key + '|'))).filter(r => !removed(r));
  const trips = await api.read('trips', prefix('trip-key', key + '|'));
  const masters = (await api.read('rings', prefix('ring-key', ringPrefix))).filter(r => r.active && r['ring-key'] === ringPrefix + 'id:' + r['ring-id']);
  const needed = new Set(schedules.filter(r => r.kind === 'class').map(r => r.class));
  const classMap = new Map(), rings = new Map(), groups = new Map(), entries = new Map();
  const inScope = row => row['w-shows'] === show['show-key'] && row['w-days'] === key;
  function ring(keyRef, number, id, name) {
    if (keyRef !== key + '|' + number || !positive(number) || !positive(id)) throw Error('invalid_shared_ring');
    return put(rings, keyRef, {id:keyRef,show:[showId],day:[dayId],'ring-id':id,'ring-number':number,'ring-name':name},'ring');
  }
  const scheduleJson = new Map(schedules.map(r => [r.id,json(r['source-json'])]));
  for (const row of schedules.filter(r => r.kind !== 'class')) {
    const raw = scheduleJson.get(row.id);
    if (!raw || Number(raw.show_id) !== show.show_id || Number(raw.customer_id) !== show.customer_id ||
      String(raw.day).slice(0,10) !== day.date) throw Error('invalid_shared_break');
    ring(row.ring, Number(raw.ring), Number(raw.ring_id), raw.ring_name);
  }
  for (const row of classes.filter(r => needed.has(r['class-key']))) {
    const raw = json(row['source-json']);
    const classKey = row['class-key'], number = Number(row.ring?.split('|').at(-1));
    const ringId = Number(raw?.class?.schedule_ring_id);
    if (!inScope(row) || Number(raw?.show_id) !== show.show_id || Number(raw?.class?.class_id) !== row['class-id'] ||
      classKey !== `${row.ring}|${row['class-id']}` || !row.ring?.startsWith(key + '|') ||
      !Number.isSafeInteger(row['entry-count']) || row['entry-count'] < 0) throw Error('invalid_shared_class');
    if (!rings.has(row.ring)) {
      const match = masters.filter(r => r['ring-id'] === ringId && r['ring-number'] === number);
      if (match.length > 1) throw Error('ambiguous_ring_master');
      ring(row.ring, number, ringId, match[0]?.name);
    }
    if (rings.get(row.ring)['ring-id'] !== ringId || classMap.has(classKey)) throw Error('shared_class_ring_conflict');
    classMap.set(classKey,{row,raw});
  }
  if ([...needed].some(k => !classMap.has(k))) throw Error('missing_shared_class');
  const projectedSchedules = schedules.map(row => {
    const raw = scheduleJson.get(row.id);
    if (!raw || !inScope(row) || !rings.has(row.ring) || !row['schedule-key'].startsWith(row.ring + '|') ||
      row.group !== `${row.ring}|${raw.class_group_id}` || !Number.isSafeInteger(row['source-position']) ||
      (row.kind === 'class' && (row.class !== `${row.ring}|${raw.class_id}` ||
        row['schedule-key'] !== `${row.ring}|${raw.class_groupxclasses_id}`))) throw Error('invalid_shared_schedule');
    put(groups,row.group,{id:row.group,'group-name':raw.group_name},'group');
    return {...row, show:[showId], day:[dayId], ring:[row.ring], group:[row.group],
      class:row.kind === 'class' ? [row.class] : [], 'source-json':raw};
  });
  const scopedTrips = new Map();
  // Full Data uses the trip arrays saved with each validated shared class.
  // A normalized row is optional here and supplies only its existing record ID.
  const sourceTrips = snapshots ? [...classMap].flatMap(([classKey,{row,raw}]) => {
    if (!Array.isArray(raw.trips)) throw Error('incomplete_shared_class_trips');
    return raw.trips.map(trip => {
      const tripKey=`${classKey}|${trip.entry_id}|${trip.entryxclasses_uuid}`;
      const ids=trips.filter(t=>t['trip-key']===tripKey);
      if (ids.length>1) throw Error('invalid_shared_trip_identity');
      return {id:ids[0]?.id ?? tripKey,'trip-key':tripKey,'w-shows':show['show-key'],'w-days':key,
        class:classKey,'source-uuid':trip.entryxclasses_uuid,'trip-id':trip.trip_id,'record-status':'active',
        'order-of-go':trip.order_of_go,'actual-order':trip.actual_order,'estimated-go-time':trip.estimated_go_time,
        scratch:trip.scratch_trip,'gone-in':trip.gone_in,'last-success':row['last-success'],'source-json':trip};
    });
  }) : trips;
  for (const row of sourceTrips) {
    const [customer, showNumber, date, ringNumber, classNumber, entryNumber, uuid, ...extra] = row['trip-key']?.split('|') ?? [];
    const classKey = [customer,showNumber,date,ringNumber,classNumber].join('|');
    const group = classMap.get(classKey);
    if (!group) continue;
    if (!positive(entryNumber) || !uuid || extra.length || (row['w-days'] && row['w-days'] !== key) ||
      (row['w-shows'] && row['w-shows'] !== show['show-key']) || (row.class && row.class !== classKey) ||
      scopedTrips.has(row['trip-key'])) throw Error('invalid_shared_trip_identity');
    const raw = json(row['source-json']);
    if (!raw || String(raw.entry_id) !== entryNumber || raw.entryxclasses_uuid !== uuid ||
      row['source-uuid'] !== uuid || (row['trip-id'] && Number(row['trip-id']) !== Number(raw.trip_id))) throw Error('invalid_shared_trip_source');
    if (!removed(row) && !snapshots) {
      if (row['record-status'] !== 'active') throw Error('unknown_shared_trip_status');
      const links = group.row.trips ?? [];
      if (row.class && !links.some(link => linkedId(link) === row.id)) throw Error('shared_trip_link_conflict');
      if (!row.class) {
        const evidence = group.raw.trips?.filter(t => t.entryxclasses_uuid === uuid &&
          ['entry_id','horse_id','rider_id','trainer_id','number'].every(k => t[k] === raw[k]));
        if (evidence?.length !== 1) throw Error('unverified_shared_trip_membership');
      }
    }
    scopedTrips.set(row['trip-key'], {row,raw,classKey});
  }
  for (const [classKey,group] of classMap) {
    const list = group.raw.trips;
    const found = [...scopedTrips.values()].filter(t => t.classKey === classKey && !removed(t.row));
    if (!Array.isArray(list) || list.some(t => !scopedTrips.has(`${classKey}|${t.entry_id}|${t.entryxclasses_uuid}`)) ||
      (group.row['entry-count'] > 0 && !found.length)) throw Error('incomplete_shared_class_trips');
  }
  const activeTrips = [...scopedTrips.values()].filter(t => !removed(t.row));
  const kinds = ['horse','rider','trainer'];
  const refs = Object.fromEntries(kinds.map(kind => [kind,new Map()]));
  const entities = Object.fromEntries(kinds.map(kind => [kind,new Map()]));
  for (const kind of kinds) {
    const ids = [...new Set(activeTrips.map(t => Number(t.raw[kind+'_id'])).filter(positive))];
    for (let i=0; i<ids.length; i+=30) {
      const batch = ids.slice(i,i+30);
      const records = await api.read(kind+'s', `OR(${batch.map(id => equals(kind+'-id',id)).join(',')})`);
      for (const row of records) {
        const id = row[kind+'-id'];
        if (!batch.includes(id) || refs[kind].has(id)) throw Error('ambiguous_shared_participant');
        refs[kind].set(id,row);
      }
    }
  }
  function participant(kind,row,raw) {
    const id = Number(raw[kind+'_id']);
    if (!positive(id)) {
      if (kind === 'horse' || positive(row[kind])) throw Error('missing_shared_participant');
      return [];
    }
    if (row[kind] && Number(row[kind]) !== id) throw Error('shared_participant_conflict');
    const ref = refs[kind].get(id);
    const name = ref?.[kind+'-name'] || raw[kind+'_name'] || raw[kind];
    if (typeof name !== 'string' || !name.trim()) throw Error('missing_shared_participant_name');
    const entityId = ref?.id ?? `${kind}:${id}`;
    put(entities[kind],entityId,{id:entityId,[kind+'-id']:id,[kind+'-name']:name,
      [`${kind}-nickname (from w-${kind}s)`]:ref?.[kind+'-nickname'],
      [`${kind}-fallback (from w-${kind}s)`]:ref?.[kind+'-fallback']},kind);
    return [entityId];
  }
  const projectedTrips = activeTrips.map(({row,raw,classKey}) => {
    const number = Number(raw.number), entry = `${show.show_id}|${raw.entry_id}`;
    if (!positive(number) || (row.entry && row.entry !== entry)) throw Error('invalid_shared_entry');
    const horse = participant('horse',row,raw), rider = participant('rider',row,raw), trainer = participant('trainer',row,raw);
    put(entries,entry,{id:entry,show:[showId],horse,'entry-number':number},'entry');
    return {...row,class:[classKey],entry:[entry],horse,rider,trainer,'source-json':raw,
      'order-of-go':row['order-of-go'] ?? raw.order_of_go, scratch:row.scratch ?? raw.scratch_trip,
      'gone-in':row['gone-in'] ?? raw.gone_in};
  });
  const dayRings = [...rings.values()];
  const dataset = projectData({
    baseId:APP_BASE,capturedAt:now,
    shows:[{id:showId,'show-name':show.show_name,'customer-id':show.customer_id,'facility-id':show.facility_id,'selected-date':day.date}],
    days:[{id:dayId,show:[showId],date:day.date}], rings:dayRings, groups:[...groups.values()],
    schedules:projectedSchedules, classes:[...classMap].map(([id,{row,raw}]) => ({...row,id,day:[dayId],ring:[row.ring],'source-json':raw})),
    entries:[...entries.values()], trips:projectedTrips, horses:[...entities.horse.values()],riders:[...entities.rider.values()],trainers:[...entities.trainer.values()],
    ringHelper:masters.filter(m => dayRings.some(r => r['ring-id'] === m['ring-id'] && r['ring-number'] === m['ring-number']))
      .map(m => ({id:m.id,key:m['ring-key'],ringId:m['ring-id'],nickname:m['ring-nickname'],active:true}))
  });
  const versions = [...schedules,...classes.filter(r => needed.has(r['class-key'])),...activeTrips.map(t => t.row)]
    .map(r => r['last-success']).filter(t => Number.isFinite(Date.parse(t))).sort();
  Object.assign(dataset.metadata,{source:'wef-schedule-app',sourceDayPriority:day['day-priority'],showKey:show['show-key'],
    sourceUpdatedAt:versions.at(-1) ?? null,liveIntegration:'data-only',package:{lane:'day0',complete:true,startedAt:now,dates:[day.date]}});
  return {dataset, trips:scopedTrips};
}
