"use strict";

// Shared by disabled native Airtable automations. No external calls or source writes.
async function resultsWorkflow(base, {mode, source = '', recordId = '', now = new Date().toISOString()} = {}) {
  if (base.id !== 'appK9IX59jFwUkB2u') throw Error('Wrong Results base');
  if (!['event', 'reconcile'].includes(mode)) throw Error('Unknown Results operation');
  const table = name => base.getTable(name);
  const read = async name => (await table(name).selectRecordsAsync()).records;
  const val = (r, name) => r.getCellValue(name);
  const text = (r, name) => String(val(r, name) ?? '').trim();
  const nonzero = value => {
    if (value === null || value === undefined || typeof value === 'boolean' || String(value).trim() === '') return false;
    return Number.isFinite(Number(value)) && Number(value) !== 0;
  };
  const json = value => typeof value === 'string' && value.trim() ? JSON.parse(value) : {};
  const [trips, results, days, horses, riders, snapshots] = await Promise.all(
    ['wef-data.trips', 'wef-data.results', 'wef-shows.show-days', 'w-horses', 'w-riders', 'snapshot'].map(read));
  const dest = table('snapshot');
  const report = {mode, created: 0, updated: 0, expected: 0, complete: 0, conflicts: 0};
  const rows = new Map();
  for (const r of snapshots) {
    const key = text(r, 'snapshot-key');
    if (!rows.has(key)) rows.set(key, []);
    rows.get(key).push(r);
  }
  // Read back every write; never retry an ambiguous mutation.
  async function save(key, fields) {
    const old = rows.get(key) || [];
    if (old.length > 1) return false;
    fields = {'snapshot-key': key, ...fields};
    const normalized = v => v === '' || v === false || v === undefined ? null : v;
    const same = (a, b) => JSON.stringify(normalized(a)) === JSON.stringify(normalized(b));
    const delta = Object.fromEntries(Object.entries(fields).filter(([name, value]) => !old.length || !same(val(old[0], name), value)));
    if (!Object.keys(delta).length) return true;
    const id = old.length ? old[0].id : await dest.createRecordAsync(fields);
    if (old.length) await dest.updateRecordAsync(id, delta);
    const saved = await dest.selectRecordAsync(id);
    if (!saved || Object.entries(fields).some(([name, value]) => !same(val(saved, name), value))) throw Error('Results write readback mismatch');
    rows.set(key, [saved]);
    if (old.length) report.updated++; else report.created++;
    return true;
  }
  async function flag(records, reason) {
    for (const r of records) {
      const fields = {'check-status': 'Conflict', 'check-reason': reason};
      if (text(r, 'check-status') === 'Conflict' && text(r, 'check-reason') === reason) continue;
      await dest.updateRecordAsync(r.id, fields);
      const saved = await dest.selectRecordAsync(r.id);
      if (!saved || text(saved, 'check-status') !== 'Conflict' || text(saved, 'check-reason') !== reason) throw Error('Conflict readback mismatch');
      report.updated++;
    }
  }
  let selected = trips;
  if (mode === 'event') {
    if (!['wef-data.trips', 'wef-data.results'].includes(source) || !recordId) throw Error('Missing source event');
    const changed = (source === 'wef-data.trips' ? trips : results).find(r => r.id === recordId);
    if (!changed) throw Error('Source event record missing');
    const key = text(changed, source === 'wef-data.trips' ? 'trip-key' : 'result-key');
    selected = trips.filter(r => text(r, 'trip-key') === key);
    if (!selected.length) throw Error('Result event has no matching local trip');
  }
  const yNames = ['score1', 'time-one', 'faults-one', 'time-fault-one'];
  const xNames = ['score1', 'score2', 'score3', 'score4', 'score5', 'score6',
    'time-one', 'time-two', 'time-three', 'faults-one', 'faults-two', 'faults-three',
    'time-fault-one', 'time-fault-two', 'time-fault-three', 'placing', 'position', 'points-earned', 'prize-money'];
  const pick = (names, getter) => Object.fromEntries(names.map(n => [n, getter(n)]).filter(([, v]) => nonzero(v)));
  // Receipt clocks remain in saved evidence, but are not changes to result content.
  const businessContent = payload => JSON.stringify({...payload,
    ...(payload.instances ? {instances: payload.instances.map(({observedAt, ...instance}) => instance).sort((a, b) => a.record.localeCompare(b.record))} : {})});
  const handled = new Set();
  for (const trip of selected) {
    const key = text(trip, 'trip-key');
    if (handled.has(key)) continue;
    handled.add(key);
    const parts = key.split('|');
    if (parts.length !== 7) throw Error('Unrecognized source trip-key');
    const scope = parts.slice(0, 3).join('|');
    const eligibleDays = days.filter(d => text(d, 'show-day-key') === scope && val(d, 'day-priority') === 0);
    if (!eligibleDays.length) continue;
    const issues = [];
    if (eligibleDays.length > 1) issues.push('Duplicate priority-zero show-day');
    if (trips.filter(t => text(t, 'trip-key') === key).length > 1) issues.push('Duplicate source trip-key');
    let raw = {};
    try { raw = json(val(trip, 'source-json')); } catch { issues.push('Invalid trip source-json'); }
    const horseId = text(trip, 'horse') || String(raw.horse_id ?? '');
    const riderId = text(trip, 'rider') || String(raw.rider_id ?? '');
    const entry = text(trip, 'entry');
    if (entry !== parts[1] + '|' + parts[5]) issues.push('Entry identity disagrees with trip-key');
    for (const [id, rawId, label] of [[horseId, raw.horse_id, 'Horse'], [riderId, raw.rider_id, 'Rider']]) {
      if (!id || (rawId !== undefined && String(rawId) !== id)) issues.push(label + ' identity missing or inconsistent');
    }
    const hs = horses.filter(h => text(h, 'horse-id') === horseId);
    const rs = riders.filter(r => text(r, 'rider-id') === riderId);
    if (hs.length !== 1) issues.push('Horse local match missing or duplicate');
    if (rs.length !== 1) issues.push('Rider local match missing or duplicate');
    const identity = {tripKey: key, tripRecord: trip.id, tripId: text(trip, 'trip-id'),
      entry: {key: entry, id: parts[5]},
      horse: {id: horseId, record: hs.length === 1 ? hs[0].id : null, name: hs.length === 1 ? text(hs[0], 'horse-name') : raw.horse || ''},
      rider: {id: riderId, record: rs.length === 1 ? rs[0].id : null, name: rs.length === 1 ? text(rs[0], 'rider-name') : raw.rider_name || ''}};
    const matching = results.filter(r => text(r, 'result-key') === key);
    const xIssues = [];
    const instances = matching.map(r => {
      const links = val(r, 'trip') || [];
      if (links.length !== 1 || links[0].id !== trip.id || text(r, 'class') !== text(trip, 'class') || text(r, 'entry') !== entry) xIssues.push('Result/trip identity conflict');
      return {record: r.id, values: pick(xNames, n => val(r, n)), observedAt: val(r, 'last-success') || null};
    }).filter(r => Object.keys(r.values).length);
    if (matching.length > 1) xIssues.push('Duplicate source result-key');
    const yValues = pick(yNames, n => raw[n.replaceAll('-', '_')]);
    const choices = [
      {kind: 'X', qualifies: text(trip, 'class_related_data.results_verified (from class)') === '1' && instances.length > 0, payload: {identity, instances}},
      {kind: 'Y', qualifies: text(trip, 'gone-in') === '1' && Object.keys(yValues).length > 0, payload: {identity, values: yValues}}
    ];
    for (const {kind, qualifies, payload} of choices) {
      const resultKey = kind + '|' + key, expectedKey = 'expected|' + resultKey;
      let actual = rows.get(resultKey) || [];
      const problems = [...issues, ...(kind === 'X' ? xIssues : [])];
      if (actual.length > 1) problems.push('Duplicate triggered result record');
      if ((rows.get(expectedKey) || []).length > 1) problems.push('Duplicate expectation record');
      // Existing event output is historical evidence; source changes are flagged, not overwritten.
      if (actual.length === 1) {
        let matches = false;
        try { matches = businessContent(json(val(actual[0], 'results-json'))) === businessContent(payload); } catch { /* Invalid saved evidence is a conflict. */ }
        if (!qualifies || !matches) problems.push('Triggered result differs from current source');
      }
      if (actual.length === 1 && text(actual[0], 'check-status') === 'Conflict' && !problems.length) problems.push(text(actual[0], 'check-reason'));
      const common = {'customer-id': Number(parts[0]), 'show-id': Number(parts[1]), 'date': parts[2],
        'ring-number': Number(parts[3]), 'class-id': Number(parts[4]), 'finality': kind};
      if (mode === 'event' && qualifies && !actual.length) {
        await save(resultKey, {...common, 'check-status': problems.length ? 'Conflict' : 'Complete',
          'check-reason': problems.join('; ') || 'Created by source alert', 'accepted-at': now,
          'source-observed-at': text(trip, 'last-success'), 'results-verified': kind === 'X',
          'results-json': JSON.stringify(payload), 'result-count': kind === 'X' ? instances.length : 1,
          'source-evidence-json': JSON.stringify({source, recordId, trip: trip.id, results: matching.map(r => r.id)}),
          'phase-json': JSON.stringify({recordType: 'result', trigger: kind === 'X' ? 'results_verified=1' : 'gone-in=1'})});
        actual = rows.get(resultKey) || [];
      }
      if (problems.length) {
        await flag([...(rows.get(expectedKey) || []), ...actual], [...new Set(problems)].join('; '));
        report.conflicts++;
      }
      const status = problems.length ? 'Conflict' : actual.length === 1 ? 'Complete' : 'Expected';
      const reason = problems.length ? [...new Set(problems)].join('; ') : actual.length ? 'Triggered record created: ' + actual[0].id : qualifies ? 'Qualifies; awaiting source alert record' : 'Awaiting qualifying source state';
      await save(expectedKey, {...common, 'check-status': status, 'check-reason': reason,
        'phase-json': JSON.stringify({recordType: 'expectation', resultKey, resultRecord: actual.length === 1 ? actual[0].id : null}),
        'source-evidence-json': JSON.stringify(identity)});
      if (status === 'Expected') report.expected++;
      if (status === 'Complete') report.complete++;
    }
  }
  if (mode === 'reconcile') {
    // Inspect saved workflow rows too: a vanished source must not hide duplicates.
    const sourceKeys = new Set(trips.map(t => text(t, 'trip-key')));
    for (const [key, saved] of rows) {
      const parts = key.split('|');
      const prefix = parts[0] === 'expected' ? 2 : 1;
      const kind = parts[prefix - 1];
      const tripParts = parts.slice(prefix);
      if (!['X', 'Y'].includes(kind) || tripParts.length !== 7 || (prefix === 2 && parts[0] !== 'expected')) continue;
      if (sourceKeys.has(tripParts.join('|'))) continue;
      const scope = tripParts.slice(0, 3).join('|');
      if (!days.some(d => text(d, 'show-day-key') === scope && val(d, 'day-priority') === 0)) continue;
      const reason = 'Source trip missing' + (saved.length > 1 ? '; Duplicate saved workflow key' : '');
      await flag(saved, reason);
      report.conflicts++;
    }
  }
  return report;
}

async function runResultsWorkflow(base, config) {
  if (base.id !== 'appK9IX59jFwUkB2u') throw Error('Wrong Results base');
  const started = new Date().toISOString();
  const runId = 'results-' + started + '-' + Math.random().toString(36).slice(2);
  const runs = base.getTable('run-logs'), errors = base.getTable('errs-log'), beats = base.getTable('heartbeat');
  async function checkedCreate(t, fields) {
    const id = await t.createRecordAsync(fields);
    const row = await t.selectRecordAsync(id);
    if (!row || Object.entries(fields).some(([k, v]) => row.getCellValue(k) !== v)) throw Error('Results log readback mismatch');
    return id;
  }
  let report, failure;
  try { report = await resultsWorkflow(base, {...config, now: started}); }
  catch (e) { failure = e; }
  const completed = new Date().toISOString();
  const result = failure ? 'Failed' : report.conflicts ? 'Conflict' : 'Complete';
  const summary = JSON.stringify(report || {mode: config.mode, error: String(failure.message)});
  await checkedCreate(runs, {'run-id': runId, started, completed, result, summary});
  if (failure) await checkedCreate(errors, {'error-key': runId, 'run-id': runId, 'occurred-at': completed, message: String(failure.message)});
  // One heartbeat per operation avoids conflicting alert/reconciliation writers.
  const heartbeatKey = 'workflow|' + config.mode;
  const found = (await beats.selectRecordsAsync()).records.filter(r => r.getCellValue('heartbeat-key') === heartbeatKey);
  if (found.length > 1) throw Error('Duplicate Results heartbeat; no repair');
  const fields = {'heartbeat-key': heartbeatKey, 'run-id': runId, started, completed, 'checked-at': completed, result, summary};
  if (!failure) fields['last-success'] = completed;
  if (!found.length) await checkedCreate(beats, fields);
  else {
    await beats.updateRecordAsync(found[0].id, fields);
    const saved = await beats.selectRecordAsync(found[0].id);
    if (!saved || Object.entries(fields).some(([k, v]) => saved.getCellValue(k) !== v)) throw Error('Results heartbeat readback mismatch');
  }
  if (failure) throw failure;
  return report;
}

// Node export supports offline checks; native automations append their invocation.
if (typeof module !== 'undefined') module.exports = {resultsWorkflow, runResultsWorkflow};
