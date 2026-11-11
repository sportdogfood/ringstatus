"use strict";

function resultsCore(base) {
  if (base.id !== 'appK9IX59jFwUkB2u') throw Error('Wrong Results base');
  const cell = (r, field) => r.getCellValue(field);
  const text = (r, field) => String(cell(r, field) ?? '').trim();
  const parse = value => value ? JSON.parse(value) : {};
  const read = async name => (await base.getTable(name).selectRecordsAsync()).records;
  const equal = (a, b) => JSON.stringify(a ?? null) === JSON.stringify(b ?? null);
  const nonzero = v => v !== null && v !== undefined && typeof v !== 'boolean' && String(v).trim() !== '' && Number.isFinite(Number(v)) && Number(v) !== 0;
  const yFields = ['score1', 'time-one', 'faults-one', 'time-fault-one'];
  const xFields = [...Array.from({length: 6}, (_, i) => 'score' + (i + 1)),
    ...['time', 'faults', 'time-fault'].flatMap(n => ['one', 'two', 'three'].map(p => n + '-' + p)),
    'placing', 'position', 'points-earned', 'prize-money'];
  const instances = (records, fields) => records.map(r => ({record: r.id, key: text(r, 'result-key'),
    values: Object.fromEntries(fields.filter(f => nonzero(cell(r, f))).map(f => [f, cell(r, f)])),
    observedAt: cell(r, 'last-success') || null})).filter(r => Object.keys(r.values).length).sort((a, b) => a.record.localeCompare(b.record));
  const content = instances => JSON.stringify(instances.map(({observedAt, ...r}) => r));
  async function write(name, id, fields) {
    const t = base.getTable(name);
    if (id) await t.updateRecordAsync(id, fields); else id = await t.createRecordAsync(fields);
    const saved = await t.selectRecordAsync(id);
    const normalize = v => v === '' || v === false || v === undefined ? null : v;
    const comparable = v => Array.isArray(v) ? v.map(x => x && typeof x === 'object' && 'id' in x ? {id: x.id} : x) : v && typeof v === 'object' && 'name' in v ? {name: v.name} : normalize(v);
    if (!saved || Object.entries(fields).some(([k, v]) => !equal(comparable(cell(saved, k)), comparable(v)))) throw Error('Results write readback mismatch: ' + name);
    return saved;
  }
  async function save(rows, key, fields) {
    const found = rows.filter(r => text(r, 'snapshot-key') === key);
    if (found.length > 1) {
      for (const r of found) await write('snapshot', r.id, {'check-status': 'Conflict', 'check-reason': 'Duplicate snapshot-key'});
      throw Error('Duplicate snapshot-key: ' + key);
    }
    const old = found[0];
    const delta = Object.fromEntries(Object.entries(fields).filter(([f, v]) => !old || !equal(cell(old, f) ?? null, v === '' || v === false ? null : v)));
    if (old && !Object.keys(delta).length) return old;
    const saved = await write('snapshot', old?.id, old ? delta : {'snapshot-key': key, ...fields});
    if (old) rows[rows.indexOf(old)] = saved; else rows.push(saved);
    return saved;
  }
  const dayKey = tripKey => tripKey.split('|').slice(0, 3).join('|');
  const eligible = (key, days) => days.some(d => text(d, 'show-day-key') === dayKey(key) && cell(d, 'day-priority') === 0);
  const scope = key => {const p = key.split('|'); if (p.length !== 7 || !p.slice(0, 2).every(s => /^\d+$/.test(s))) throw Error('Invalid trip-key'); return {'customer-id': Number(p[0]), 'show-id': Number(p[1]), date: p[2], 'ring-number': Number(p[3]), 'class-id': Number(p[4])};};
  return {cell, text, parse, read, equal, nonzero, yFields, xFields, instances, content, write, save, dayKey, eligible, scope};
}

async function resultsLogged(base, operation, action) {
  const C = typeof resultsCore === 'function' ? resultsCore(base) : require('./event-core').resultsCore(base);
  const started = new Date().toISOString(), runId = 'events|' + started + '|' + Math.random().toString(36).slice(2);
  // Persist the running state before work so failed logging cannot look like completion.
  const run = await C.write('run-logs', null, {'run-id': runId, started, result: 'Running'});
  let report, failure;
  try { report = await action(); } catch (e) { failure = e; }
  const completed = new Date().toISOString(), result = failure ? 'Failed' : 'Complete';
  const summary = JSON.stringify({operation, ...(report || {}), ...(failure ? {error: String(failure.message)} : {})});
  if (failure) await C.write('errs-log', null, {'error-key': runId, 'run-id': runId, 'occurred-at': completed, message: String(failure.message)});
  await C.write('run-logs', run.id, {completed, result, summary, ...(report?.showId ? {'show-id': report.showId} : {})});
  const beats = (await C.read('heartbeat')).filter(r => C.text(r, 'heartbeat-key') === 'events|' + operation);
  if (beats.length > 1) throw Error('Duplicate Results heartbeat');
  await C.write('heartbeat', beats[0]?.id, {'heartbeat-key': 'events|' + operation, 'run-id': runId, started, completed, 'checked-at': completed,
    result, summary, ...(!failure ? {'last-success': completed} : {})});
  if (failure) throw failure;
  return report;
}

if (typeof module !== 'undefined') module.exports = {resultsCore, resultsLogged};
