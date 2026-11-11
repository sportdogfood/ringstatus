"use strict";

async function resultsEvent(base, {source, recordId, now = new Date().toISOString(), reconcile = false} = {}) {
  const C = typeof resultsCore === 'function' ? resultsCore(base) : require('./event-core').resultsCore(base);
  const [trips, results, days, rows] = await Promise.all(['wef-data.trips', 'wef-data.results', 'w-days', 'snapshot'].map(C.read));
  let selected = trips;
  if (!reconcile) {
    if (!['wef-data.trips', 'wef-data.results'].includes(source)) throw Error('Unknown Results event source');
    const changed = (source === 'wef-data.trips' ? trips : results).find(r => r.id === recordId);
    if (!changed) throw Error('Results event source record missing');
    const key = C.text(changed, source === 'wef-data.trips' ? 'trip-key' : 'result-key');
    selected = trips.filter(t => C.text(t, 'trip-key') === key);
    if (!selected.length) throw Error('Matching trip not yet available');
  }
  const report = {created: 0, expected: 0, complete: 0, conflicts: 0};
  const handled = new Set();
  for (const trip of selected) {
    const key = C.text(trip, 'trip-key');
    if (handled.has(key) || !C.eligible(key, days)) continue;
    handled.add(key);
    const common = C.scope(key), parts = key.split('|');
    report.showId = common['show-id'];
    const raw = C.parse(C.cell(trip, 'source-json'));
    const identity = {tripKey: key, tripRecord: trip.id, entry: {key: C.text(trip, 'entry'), id: parts[5]},
      horse: {id: C.text(trip, 'horse'), name: raw.horse || ''}, rider: {id: C.text(trip, 'rider'), name: raw.rider_name || ''}};
    const matching = results.filter(r => C.text(r, 'result-key') === key);
    const issues = [];
    if (trips.filter(t => C.text(t, 'trip-key') === key).length > 1) issues.push('Duplicate source trip-key');
    if (days.filter(d => C.text(d, 'show-day-key') === C.dayKey(key) && C.cell(d, 'day-priority') === 0).length > 1) issues.push('Duplicate show-day');
    if (identity.entry.key !== parts[1] + '|' + parts[5] || String(raw.entry_id) !== parts[5]) issues.push('Entry identity conflict');
    if (!identity.horse.id || !identity.rider.id || String(raw.horse_id) !== identity.horse.id || String(raw.rider_id) !== identity.rider.id) issues.push('Horse/rider identity conflict');
    if (matching.some(r => C.text(r, 'class') !== C.text(trip, 'class') || C.text(r, 'entry') !== identity.entry.key ||
      (C.cell(r, 'trip') || []).length !== 1 || C.cell(r, 'trip')[0].id !== trip.id)) issues.push('Result identity conflict');
    // Multiple matching result instances are retained, not collapsed into one value.
    const x = C.instances(matching, C.xFields), y = C.instances(matching, C.yFields);
    for (const kind of ['Y', 'X']) {
      const actualKey = kind + '|' + key, expectedKey = 'expected|' + actualKey;
      const actual = rows.filter(r => C.text(r, 'snapshot-key') === actualKey);
      const expectations = rows.filter(r => C.text(r, 'snapshot-key') === expectedKey);
      const problems = [...issues];
      if (actual.length > 1 || expectations.length > 1) problems.push('Duplicate Results record');
      const prior = expectations[0] ? C.parse(C.cell(expectations[0], 'phase-json')) : {};
      const state = {...prior, recordType: 'expectation', trigger: kind};
      const verified = C.text(trip, 'class_related_data.results_verified (from class)') === '1';
      let qualifies = kind === 'X' && verified && x.length > 0;
      if (kind === 'Y') {
        if (!reconcile && C.text(trip, 'gone-in') === '1' && !state.listening && !actual.length) {
          state.listening = true; state.armedAt = now; state.baseline = C.content(y);
        } else if (!reconcile && source === 'wef-data.results' && state.listening && C.text(trip, 'gone-in') === '1') {
          qualifies = y.length > 0 && C.content(y) !== state.baseline;
        }
      }
      const payload = {identity, instances: kind === 'X' ? x : y};
      if (actual.length === 1) {
        const saved = C.parse(C.cell(actual[0], 'results-json'));
        // Y is the first-change receipt. Later scores are expected and never replace it.
        if (kind === 'X' && (!verified || !C.equal(saved.identity, identity) || C.content(saved.instances || []) !== C.content(x))) problems.push('Verified result changed');
        if (C.text(actual[0], 'check-status') === 'Conflict') problems.push(C.text(actual[0], 'check-reason'));
      }
      if (!reconcile && qualifies && !actual.length) {
        const saved = await C.save(rows, actualKey, {...common, finality: kind, 'check-status': problems.length ? 'Conflict' : 'Complete',
          'check-reason': problems.join('; ') || (kind === 'Y' ? 'First result change after gone-in' : 'Verified nonzero results'),
          'accepted-at': now, 'source-observed-at': C.text(trip, 'last-success'), 'results-verified': kind === 'X',
          'result-count': payload.instances.length, 'results-json': JSON.stringify(payload),
          'phase-json': JSON.stringify({recordType: 'result', trigger: kind}),
          'source-evidence-json': JSON.stringify({source, recordId, resultRecords: matching.map(r => r.id)})});
        actual.push(saved); report.created++;
      }
      if (problems.length) for (const r of actual) await C.write('snapshot', r.id, {'check-status': 'Conflict', 'check-reason': [...new Set(problems)].join('; ')});
      const status = problems.length ? 'Conflict' : actual.length === 1 ? 'Complete' : 'Expected';
      await C.save(rows, expectedKey, {...common, finality: kind, 'check-status': status,
        'check-reason': problems.length ? [...new Set(problems)].join('; ') : actual.length ? 'Result record read back: ' + actual[0].id : kind === 'Y' && state.listening ? 'Listening for first nonzero result change' : 'Awaiting source event',
        'results-json': JSON.stringify({identity}), 'phase-json': JSON.stringify(state)});
      report[status === 'Expected' ? 'expected' : status === 'Complete' ? 'complete' : 'conflicts']++;
    }
  }
  if (reconcile) for (const row of rows) {
    const key = C.text(row, 'snapshot-key').replace(/^expected\|/, '').replace(/^[XY]\|/, '');
    if (key.split('|').length === 7 && C.eligible(key, days) && !trips.some(t => C.text(t, 'trip-key') === key)) {
      await C.write('snapshot', row.id, {'check-status': 'Conflict', 'check-reason': 'Source trip missing'}); report.conflicts++;
    }
  }
  return report;
}

if (typeof module !== 'undefined') module.exports = {resultsEvent};
