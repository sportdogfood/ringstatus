import { diffRecords, fingerprint } from './data-diff.js';

function edits(before, after) {
  const diff = diffRecords(before,after,'key');
  const touched = new Set([...diff.added,...diff.updated]);
  const changes = {upsert:after.filter(row => touched.has(row.key)),remove:diff.removed};
  if (diff.reordered) changes.order = after.map(row => row.key);
  return changes;
}
const rowHeader = ({entries,...row}) => row;
const hasEdits = change => Boolean(change.upsert.length || change.remove.length || change.order);

export function makeDay0Diff(base, effective) {
  const day = effective.days[0];
  if (base.metadata.showId !== effective.metadata.showId || base.days[0].date !== day.date) throw Error('day0_base_scope');
  const prior = new Map(base.records.map(row => [row.key,row]));
  const rows = edits(base.records.map(rowHeader),effective.records.map(rowHeader));
  const entries = effective.records.flatMap(row => {
    const changes = edits(prior.get(row.key)?.entries ?? [],row.entries);
    return hasEdits(changes) ? [{row:row.key,...changes}] : [];
  });
  const pairs = details => Object.entries(details).map(([key,value]) => ({key,value}));
  const rings = edits(pairs(base.ringDetails),pairs(effective.ringDetails));
  delete rings.order;
  return {
    lane:'day0', showId:base.metadata.showId, date:day.date, baseVersion:base.metadata.dataVersion,
    rows, entries, rings,
    ...(fingerprint(base.days[0]) === fingerprint(day) ? {} : {day})
  };
}

function applyEdits(before, change) {
  if (!Array.isArray(change?.upsert) || !Array.isArray(change.remove)) throw Error('invalid_day0_edits');
  const values = new Map(before.map(row => [row.key,row]));
  const seen = new Set();
  for (const key of change.remove) {
    if (!values.delete(key)) throw Error('unknown_day0_removal');
    seen.add(key);
  }
  for (const row of change.upsert) {
    if (typeof row?.key !== 'string' || !row.key || seen.has(row.key)) throw Error('invalid_day0_key');
    seen.add(row.key);
    values.set(row.key,row);
  }
  if (!change.order) return [...values.values()];
  if (!Array.isArray(change.order) || change.order.length !== values.size ||
      new Set(change.order).size !== values.size || !change.order.every(key => values.has(key))) throw Error('invalid_day0_order');
  return change.order.map(key => values.get(key));
}

export function day0Matches(full, diff) {
  return Boolean(full?.metadata && diff?.lane === 'day0' && diff.showId === full.metadata.showId &&
    diff.date === full.metadata.defaultDate && typeof diff.baseVersion === 'string' &&
    diff.baseVersion === full.metadata.dayVersions?.[diff.date]);
}

export function applyDay0Diff(full, diff) {
  if (!day0Matches(full,diff)) throw Error('day0_version_mismatch');
  const original = full.days.find(day => day.date === diff.date);
  if (!original || !Array.isArray(diff.entries) || !Number.isFinite(Date.parse(diff.checkedAt))) throw Error('invalid_day0_diff');
  const {ringDetails:dayRingDetails,...dayBase} = original;
  const day = diff.day ?? dayBase;
  if (day.date !== original.date || day.id !== original.id || !Array.isArray(day.rings)) throw Error('invalid_day0_day');
  const baseRows = full.records.filter(row => row.date === diff.date);
  const prior = new Map(baseRows.map(row => [row.key,row]));
  const changes = new Map(diff.entries.map(change => [change.row,change]));
  if (changes.size !== diff.entries.length) throw Error('duplicate_day0_entries');
  const records = applyEdits(baseRows.map(rowHeader),diff.rows).map(row => {
    const entries = prior.get(row.key)?.entries ?? [];
    const change = changes.get(row.key);
    changes.delete(row.key);
    return {...row,entries:change ? applyEdits(entries,change) : entries};
  });
  const ringDetails = Object.fromEntries(applyEdits(
    Object.entries(dayRingDetails ?? full.ringDetails).map(([key,value]) => ({key,value})),diff.rings
  ).map(({key,value}) => [key,value]));
  if (changes.size || records.some(row => row.date !== diff.date || !ringDetails[row.ring]) ||
      day.rings.some(ring => !ringDetails[ring])) throw Error('invalid_day0_scope');
  return {
    metadata:{...full.metadata,refreshedAt:diff.checkedAt,package:{lane:'day0',complete:true,startedAt:diff.checkedAt,dates:[diff.date]},
      appliedBaseVersion:diff.baseVersion},
    days:[{...day,ringDetails}], records,ringDetails
  };
}
