"use strict";

async function backfillX(base, {date, now = new Date().toISOString()}) {
  const C = typeof resultsCore === 'function' ? resultsCore(base) : require('./event-core').resultsCore(base);
  const read = async (name, fields) => (await base.getTable(name).selectRecordsAsync({fields})).records;
  const day = (await read('w-days', ['show-day-key', 'date', 'day-priority']))
    .filter(r => C.text(r, 'date') === date && C.cell(r, 'day-priority') === 0);
  if (day.length !== 1) throw Error('Backfill requires exactly one priority-zero show day for ' + date);
  const dayKey = C.text(day[0], 'show-day-key');
  const showKey = dayKey.split('|').slice(0, 2).join('|');
  const [trips, results, snapshots, shows, horses, riders] = await Promise.all([
    read('wef-data.trips', ['trip-key', 'class', 'entry', 'horse', 'rider', 'source-json', 'last-success', 'class_related_data.results_verified (from class)']),
    read('wef-data.results', ['result-key', 'trip', 'class', 'entry', 'last-success', ...C.xFields]),
    read('snapshot', ['snapshot-key']), read('w-shows', ['show-key']), read('w-horses', ['horse-id']), read('w-riders', ['rider-id'])
  ]);
  function index(rows, field) {
    const map = new Map();
    for (const row of rows) {
      const key = C.text(row, field);
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(row);
    }
    return map;
  }
  const tripIndex = index(trips, 'trip-key'), resultIndex = index(results, 'result-key');
  const existing = index(snapshots, 'snapshot-key');
  const horseIndex = index(horses, 'horse-id'), riderIndex = index(riders, 'rider-id');
  const show = shows.filter(r => C.text(r, 'show-key') === showKey);
  const report = {date, showId: Number(showKey.split('|')[1]), created: 0, existing: 0, conflicts: []};
  const pending = [];
  for (const [key, sourceTrips] of tripIndex) {
    if (C.dayKey(key) !== dayKey) continue;
    const verified = sourceTrips.find(t => C.text(t, 'class_related_data.results_verified (from class)') === '1');
    if (!verified) continue;
    const matches = resultIndex.get(key) || [];
    const instances = C.instances(matches, C.xFields);
    if (!instances.length) continue;
    const actualKey = 'X|' + key, parts = key.split('|');
    let raw;
    try { raw = C.parse(C.cell(verified, 'source-json')); } catch { raw = {}; }
    const entry = C.text(verified, 'entry'), horse = C.text(verified, 'horse'), rider = C.text(verified, 'rider');
    const issues = [];
    if (sourceTrips.length !== 1) issues.push('Duplicate source trip-key');
    if (parts.length !== 7 || entry !== parts[1] + '|' + parts[5] || String(raw?.entry_id) !== parts[5]) issues.push('Entry identity conflict');
    if (!horse || !rider || String(raw?.horse_id) !== horse || String(raw?.rider_id) !== rider) issues.push('Horse/rider identity conflict');
    if (matches.some(r => C.text(r, 'class') !== C.text(verified, 'class') || C.text(r, 'entry') !== entry ||
      (C.cell(r, 'trip') || []).length !== 1 || C.cell(r, 'trip')[0].id !== verified.id)) issues.push('Result identity conflict');
    if ((existing.get(actualKey) || []).length > 1) issues.push('Duplicate snapshot-key');
    if (issues.length) {report.conflicts.push({tripKey: key, issues}); continue;}
    if (existing.has(actualKey)) {report.existing++; continue;}
    const identity = {tripKey: key, tripRecord: verified.id, entry: {key: entry, id: parts[5]},
      horse: {id: horse, name: raw.horse || ''}, rider: {id: rider, name: raw.rider_name || ''}};
    const links = Object.fromEntries([
      ['w-shows', show], ['w-days', day], ['w-horses', horseIndex.get(horse) || []], ['w-riders', riderIndex.get(rider) || []]
    ].filter(([, rows]) => rows.length === 1).map(([field, rows]) => [field, [{id: rows[0].id}]]));
    pending.push({fields: {'snapshot-key': actualKey, ...C.scope(key), finality: 'X', 'check-status': 'Complete',
      'check-reason': 'Verified nonzero results captured by one-time backfill', 'accepted-at': now,
      'source-observed-at': C.text(verified, 'last-success'), 'results-verified': true, 'result-count': instances.length,
      'results-json': JSON.stringify({identity, instances}), 'phase-json': JSON.stringify({recordType: 'result', trigger: 'X'}),
      'source-evidence-json': JSON.stringify({source: 'wef-data.trips', recordId: verified.id, resultRecords: matches.map(r => r.id), backfill: true}),
      'wef-data.results': matches.map(r => ({id: r.id})), ...links}});
  }
  const table = base.getTable('snapshot'), created = [];
  for (let offset = 0; offset < pending.length; offset += 50) {
    const batch = pending.slice(offset, offset + 50);
    const ids = await table.createRecordsAsync(batch);
    if (ids.length !== batch.length) throw Error('Backfill create count mismatch');
    created.push(...ids.map((id, i) => ({id, fields: batch[i].fields})));
  }
  if (created.length) {
    const fields = [...new Set(pending.flatMap(row => Object.keys(row.fields)))];
    const saved = new Map((await read('snapshot', fields)).map(row => [row.id, row]));
    for (const {id, fields: expected} of created) {
      const row = saved.get(id);
      const matches = row && Object.entries(expected).every(([field, value]) => {
        const cell = C.cell(row, field);
        return Array.isArray(value) ? C.equal((cell || []).map(link => link.id).sort(), value.map(link => link.id).sort()) : C.equal(cell === '' ? null : cell, value === '' ? null : value);
      });
      if (!matches) throw Error('Backfill snapshot readback mismatch: ' + id);
    }
  }
  report.created = created.length;
  return report;
}

if (typeof module !== 'undefined') module.exports = {backfillX};
