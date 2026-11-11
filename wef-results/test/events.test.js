"use strict";
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const {resultsCore, resultsLogged} = require('../event-core'), {resultsEvent} = require('../events'), {linkResults} = require('../linker'), {cleanResults} = require('../cleaner'), {buildEvents} = require('../build-events');
const schema = require('../evidence/events-20261107/schema-before.json');
const key = '27|444|2030-02-12|3|555|666|UUID';
const now = '2030-02-12T18:00:00Z';
function mock() {
  const data = Object.fromEntries(schema.tables.map(t => [t.name, []])); data.cleanup = [];
  const calls = [], fields = Object.fromEntries(schema.tables.map(t => [t.name, new Set(t.fields.map(f => f.name))]));
  fields.snapshot = new Set([...fields.snapshot, 'w-shows', 'w-days', 'wef-data.results', 'w-horses', 'w-riders']);
  fields.cleanup = new Set(['Name', 'records', 'purpose', 'cleanup', 'started', 'completed', 'result', 'old-show-id', 'new-show-id', 'records-archived', 'records-deleted', 'archive-file', 'error', 'run-id']);
  let seq = 0;
  const record = (name, id, values) => ({id, values, getCellValue(field) {if (!fields[name].has(field)) throw Error('Unknown field ' + name + '.' + field); return this.values[field] ?? null;}});
  const add = (name, values, id = 'recTest' + ++seq) => {const r = record(name, id, structuredClone(values)); data[name].push(r); return r;};
  const base = {id: 'appK9IX59jFwUkB2u', data, calls, add, getTable(name) {
    if (!data[name]) throw Error('Unknown table ' + name);
    const normalize = values => Object.fromEntries(Object.entries(values).map(([f, v]) => {
      if (!fields[name].has(f)) throw Error('Unknown write field ' + name + '.' + f);
      if (name === 'cleanup' && f === 'result' && v && v.name !== 'Success') throw Error('Unknown cleanup result choice');
      return [f, Array.isArray(v) ? v.map(x => ({...x, name: 'Display name'})) : v && typeof v === 'object' && v.name ? {...v, id: 'selChoice', color: 'blue'} : v];
    }));
    return {
      selectRecordsAsync: async () => ({records: [...data[name]]}),
      selectRecordAsync: async id => data[name].find(r => r.id === id) || null,
      createRecordAsync: async values => {calls.push({type: 'create', name, values}); const r = add(name, normalize(values)); return r.id;},
      createRecordsAsync: async rows => {calls.push({type: 'batch-create', name, count: rows.length}); return rows.map(({fields}) => add(name, normalize(fields)).id);},
      updateRecordAsync: async (id, values) => {calls.push({type: 'update', name, id, values}); const r = data[name].find(r => r.id === id); if (!r) throw Error('Missing update row'); Object.assign(r.values, normalize(values));},
      deleteRecordsAsync: async ids => {calls.push({type: 'delete', name, ids}); if (base.deleteFailure) throw Error('Delete outcome unknown'); data[name] = data[name].filter(r => !ids.includes(r.id));}
    };
  }};
  base.trip = add('wef-data.trips', {'trip-key': key, class: key.split('|').slice(0, 5).join('|'), entry: '444|666', horse: '777', rider: '888', 'gone-in': '0', 'class_related_data.results_verified (from class)': '0',
    'source-json': JSON.stringify({entry_id: 666, horse_id: 777, rider_id: 888, horse: 'Horse', rider_name: 'Rider'}), 'last-success': '2030-02-12T17:59:00Z'});
  base.result = add('wef-data.results', {'result-key': key, trip: [{id: base.trip.id}], class: base.trip.values.class, entry: '444|666', score1: '0', 'time-one': '0', 'faults-one': '0', 'time-fault-one': '0', 'last-success': '2030-02-12T17:59:00Z'});
  add('w-days', {'show-day-key': '27|444|2030-02-12', date: '2030-02-12', 'day-priority': 0});
  add('w-shows', {'show-key': '27|444', show_id: 444, customer_id: 27, is_default: true});
  add('w-horses', {'horse-id': 777, 'horse-name': 'Horse'}); add('w-riders', {'rider-id': 888, 'rider-name': 'Rider'});
  for (const name of ['snapshot', 'run-logs', 'errs-log', 'heartbeat', 'w-days']) add('cleanup', {Name: name, cleanup: {name: 'checked'}});
  return base;
}
const event = (b, source = 'wef-data.trips') => resultsEvent(b, {source, recordId: source.endsWith('trips') ? b.trip.id : b.result.id, now});
const saved = (b, prefix) => b.data.snapshot.find(r => r.values['snapshot-key'] === prefix + '|' + key);
test('one-time backfill creates only verified X rows, skips conflicts and preserves existing snapshots', async () => {
  const {backfillX} = require('../backfill-x');
  const b = mock();
  b.trip.values['class_related_data.results_verified (from class)'] = '1';
  b.result.values['time-one'] = '68.5';
  b.result.values['score2'] = '74';
  const badKey = key.replace('666|UUID', '667|OTHER');
  const bad = b.add('wef-data.trips', {...b.trip.values, 'trip-key': badKey, entry: '444|667', rider: '',
    'source-json': JSON.stringify({entry_id: 667, horse_id: 777, rider_id: 888})});
  b.add('wef-data.results', {...b.result.values, 'result-key': badKey, entry: '444|667', trip: [{id: bad.id}]});
  const report = await backfillX(b, {date: '2030-02-12', now});
  assert.equal(report.created, 1); assert.equal(report.conflicts.length, 1);
  assert.equal(b.data.snapshot.length, 1); assert.ok(saved(b, 'X').values['results-verified']);
  assert.deepEqual(JSON.parse(saved(b, 'X').values['results-json']).instances[0].values, {score2: '74', 'time-one': '68.5'});
  const repeat = await backfillX(b, {date: '2030-02-12', now});
  assert.equal(repeat.created, 0); assert.equal(repeat.existing, 1);
  await assert.rejects(backfillX(b, {date: '2030-02-13', now}), /priority-zero show day/);
});
test('gone-in arms listening without requiring existing nonzero results', async () => {
  const b = mock(); b.trip.values['gone-in'] = '1'; await event(b);
  assert.equal(saved(b, 'Y'), undefined); assert.equal(saved(b, 'expected|Y').values['check-status'], 'Expected');
  assert.equal(JSON.parse(saved(b, 'expected|Y').values['phase-json']).listening, true);
});
test('first subsequent nonzero result change creates Y once with identities and source time', async () => {
  const b = mock(); b.trip.values['gone-in'] = '1'; await event(b); b.result.values['time-one'] = '68.5';
  await event(b, 'wef-data.results'); const y = saved(b, 'Y'), payload = JSON.parse(y.values['results-json']);
  assert.deepEqual(payload.instances[0].values, {'time-one': '68.5'}); assert.equal(payload.identity.entry.key, '444|666');
  assert.equal(y.values['source-observed-at'], '2030-02-12T17:59:00Z'); assert.equal(saved(b, 'expected|Y').values['check-status'], 'Complete');
  const original = y.values['results-json']; b.result.values.score1 = '80'; await event(b, 'wef-data.results'); assert.equal(saved(b, 'Y').values['results-json'], original);
  assert.equal(b.data.snapshot.filter(r => r.values['snapshot-key'] === 'Y|' + key).length, 1);
});
test('old nonzero values at gone-in are only a baseline; receipt clocks never trigger Y', async () => {
  const b = mock(); b.trip.values['gone-in'] = '1'; b.result.values.score1 = '70'; await event(b);
  b.result.values['last-success'] = '2030-02-12T18:01:00Z'; await event(b, 'wef-data.results'); assert.equal(saved(b, 'Y'), undefined);
  b.result.values.score1 = '71'; await event(b, 'wef-data.results'); assert.equal(JSON.parse(saved(b, 'Y').values['results-json']).instances[0].values.score1, '71');
});
test('result alerts before gone-in cannot arm or create Y', async () => {
  const b = mock(); b.result.values.score1 = '70'; await event(b, 'wef-data.results'); assert.equal(saved(b, 'Y'), undefined);
  assert.equal(JSON.parse(saved(b, 'expected|Y').values['phase-json']).listening, undefined);
});
test('X captures every nonzero field and result instance with one nonzero sufficient', async () => {
  const b = mock(); b.trip.values['class_related_data.results_verified (from class)'] = '1'; b.result.values['faults-one'] = '4';
  b.add('wef-data.results', {...b.result.values, score6: '82', 'faults-one': '0', 'time-two': '44.2'});
  await event(b); const x = saved(b, 'X'); assert.equal(x.values['result-count'], 2);
  const p = JSON.parse(x.values['results-json']); assert.deepEqual(p.instances.find(r => r.record === b.result.id).values, {'faults-one': '4'}); assert.ok(p.instances.some(r => r.values.score6 === '82'));
  assert.equal(x.values['results-verified'], true);
});
test('X waits for nonzero result arrival and later source alert, never a poll', async () => {
  const b = mock(); b.trip.values['class_related_data.results_verified (from class)'] = '1'; await event(b); assert.equal(saved(b, 'X'), undefined);
  b.result.values.score1 = '85'; await resultsEvent(b, {reconcile: true, now}); assert.equal(saved(b, 'X'), undefined);
  await event(b, 'wef-data.results'); assert.ok(saved(b, 'X'));
});
test('polling never arms Y or creates X/Y even when both source flags qualify', async () => {
  const b = mock(); b.trip.values['gone-in'] = '1'; b.trip.values['class_related_data.results_verified (from class)'] = '1'; b.result.values.score1 = '85';
  await resultsEvent(b, {reconcile: true, now}); assert.equal(saved(b, 'X'), undefined); assert.equal(saved(b, 'Y'), undefined);
  assert.equal(JSON.parse(saved(b, 'expected|Y').values['phase-json']).listening, undefined);
});
test('only exact dynamic priority-zero scopes qualify; null is not zero', async () => {
  const b = mock(); b.data['w-days'][0].values['day-priority'] = null; await event(b); assert.equal(b.data.snapshot.length, 0);
  b.data['w-days'][0].values['day-priority'] = 0; await event(b); assert.equal(b.data.snapshot.length, 2);
});
test('nonzero ignores blanks null booleans and nonnumeric text, retains numeric nonzero', () => {
  const C = resultsCore(mock()); for (const v of [null, undefined, '', ' ', false, true, '0', 0, 'NaN', 'error']) assert.equal(C.nonzero(v), false);
  for (const v of [4, '0.5', '-1']) assert.equal(C.nonzero(v), true);
});
test('changed verified X becomes Conflict and preserves original result values', async () => {
  const b = mock(); b.trip.values['class_related_data.results_verified (from class)'] = '1'; b.result.values.score1 = '80'; await event(b);
  const original = saved(b, 'X').values['results-json']; b.result.values.score1 = '81'; await resultsEvent(b, {reconcile: true, now});
  assert.equal(saved(b, 'X').values['results-json'], original); assert.equal(saved(b, 'X').values['check-status'], 'Conflict');
});
test('missing source trip and duplicate saved keys are visible conflicts', async () => {
  const b = mock(); await event(b); b.data['wef-data.trips'] = []; await resultsEvent(b, {reconcile: true, now});
  assert.equal(saved(b, 'expected|X').values['check-status'], 'Conflict');
  const c = mock(); await event(c); c.add('snapshot', {...saved(c, 'expected|X').values}); await assert.rejects(event(c), /Duplicate snapshot-key/);
});
test('linker fills all five empty links, preserves nonempty links and repeats quietly', async () => {
  const b = mock(); await event(b); await linkResults(b);
  const row = saved(b, 'expected|Y'); for (const f of ['w-shows', 'w-days', 'wef-data.results', 'w-horses', 'w-riders']) assert.ok(row.values[f]?.length);
  row.values['w-horses'] = [{id: 'recOwnerLink'}]; const before = b.calls.length; await linkResults(b);
  assert.deepEqual(row.values['w-horses'], [{id: 'recOwnerLink'}]); assert.equal(b.calls.length, before);
});
test('missing reference links do not gate result creation; linker fills them when available', async () => {
  const b = mock(); b.data['w-horses'] = []; b.trip.values['class_related_data.results_verified (from class)'] = '1'; b.result.values.score1 = '80';
  await event(b); await linkResults(b); assert.equal(saved(b, 'X').values['check-status'], 'Complete'); assert.equal(saved(b, 'X').values['w-horses'], undefined);
  const h = b.add('w-horses', {'horse-id': 777, 'horse-name': 'Different label'}); await linkResults(b); assert.equal(saved(b, 'X').values['w-horses'][0].id, h.id);
});
function log(b, n, values = {}) {return b.add('run-logs', {'run-id': 'run' + n, started: '2030-02-10T00:00:00Z', completed: '2030-02-10T00:' + String(n % 60).padStart(2, '0') + ':00Z', result: 'Complete', 'show-id': 333, ...values});}
const cutoff = '2030-02-13T00:00:00Z';
test('first normal run at 7 Eastern clears eligible rows in all three checked log tables once', async () => {
  const b = mock();
  const old = log(b, 1, {result: 'Success'}), active = log(b, 2, {result: 'Running', completed: ''});
  const current = log(b, 3, {completed: now}), unknown = log(b, 4, {result: 'Partial'});
  b.add('errs-log', {'error-key': 'err', 'run-id': old.values['run-id'], 'occurred-at': '2030-02-10T00:00:00Z'});
  b.add('heartbeat', {'heartbeat-key': 'normal', completed: '2030-02-10T00:00:00Z', result: 'Complete'});
  const latest = b.add('heartbeat', {'heartbeat-key': 'normal', completed: '2030-02-10T01:00:00Z', result: 'Success'});
  assert.equal((await cleanResults(b, {now})).daily, false);
  assert.equal(b.data['run-logs'].length, 4);
  assert.equal((await cleanResults(b, {now: cutoff})).daily, true);
  assert.deepEqual(b.data['run-logs'].map(r => r.id).sort(), [active.id, current.id, unknown.id].sort());
  assert.equal(b.data['errs-log'].length, 0);
  assert.deepEqual(b.data.heartbeat.map(r => r.id), [latest.id]);
  const late = log(b, 5);
  assert.equal((await cleanResults(b, {now: '2030-02-13T01:00:00Z'})).daily, false);
  assert.ok(b.data['run-logs'].includes(late));
  assert.ok(b.data['w-days'].length);
  assert.ok(b.data['wef-data.trips'].length);
});
test('cap at exactly 1,000 deletes oldest eligible rows to 500', async () => {
  const b = mock();
  for (let i = 0; i < 1000; i++) log(b, i, {completed: new Date(Date.parse('2029-12-01T00:00:00Z') + i * 1000).toISOString()});
  const retainedFirst = b.data['run-logs'][500].id;
  assert.equal((await cleanResults(b, {now})).deleted, 500);
  assert.equal(b.data['run-logs'].length, 500);
  assert.equal(b.data['run-logs'][0].id, retainedFirst);
  assert.equal((await cleanResults(b, {now})).deleted, 0);
  assert.ok(b.calls.filter(c => c.type === 'delete').every(c => c.ids.length <= 50));
});
test('a cap drain continues below 1,000 when protected rows become eligible', async () => {
  const b = mock();
  for (let i = 0; i < 999; i++) log(b, i, {result: i < 300 ? 'Complete' : 'Running'});
  assert.equal((await cleanResults(b, {now})).deleted, 0);
  log(b, 999, {result: 'Running'});
  assert.deepEqual((await cleanResults(b, {now})).draining, ['run-logs']);
  assert.equal(b.data['run-logs'].length, 700);
  for (const row of b.data['run-logs'].slice(0, 250)) row.values.result = 'Success';
  assert.deepEqual((await cleanResults(b, {now})).draining, []);
  assert.equal(b.data['run-logs'].length, 500);
});
test('cap drain uses readback count when another run adds a log during deletion', async () => {
  const b = mock();
  for (let i = 0; i < 1000; i++) log(b, i);
  const getTable = b.getTable.bind(b);
  let inserted = false;
  b.getTable = name => {
    const table = getTable(name);
    if (name !== 'run-logs') return table;
    const remove = table.deleteRecordsAsync;
    table.deleteRecordsAsync = async ids => {
      await remove(ids);
      if (!inserted) {log(b, 1001); inserted = true;}
    };
    return table;
  };
  assert.deepEqual((await cleanResults(b, {now})).draining, ['run-logs']);
  assert.equal(b.data['run-logs'].length, 501);
  assert.equal((await cleanResults(b, {now})).deleted, 1);
  assert.equal(b.data['run-logs'].length, 500);
});
test('new default show clears the full checked snapshot once, including new-show rows', async () => {
  const b = mock();
  const first = b.data['w-shows'][0];
  const next = b.add('w-shows', {'show-key': '27|333', show_id: 333, customer_id: 27});
  const old = log(b, 1);
  const active = log(b, 2, {result: 'Running'});
  for (const [show, status] of [[333, 'Complete'], [444, 'Complete'], [333, 'Expected'], [333, 'Conflict']])
    b.add('snapshot', {'snapshot-key': 's' + b.data.snapshot.length, 'show-id': show, 'check-status': status});
  assert.equal((await cleanResults(b, {now})).newShow, false);
  first.values.selected_date = '2030-02-13';
  assert.equal((await cleanResults(b, {now})).newShow, false);
  assert.equal(b.data.snapshot.length, 4);
  next.values.is_default = true;
  assert.equal((await cleanResults(b, {now})).newShow, false);
  assert.equal(b.data.snapshot.length, 4);
  first.values.is_default = false;
  assert.equal((await cleanResults(b, {now})).newShow, true);
  assert.equal(b.data.snapshot.length, 0);
  assert.ok(!b.data['run-logs'].includes(old) && b.data['run-logs'].includes(active));
  b.add('snapshot', {'snapshot-key': 'after'});
  assert.equal((await cleanResults(b, {now})).newShow, false);
  assert.equal(b.data.snapshot.length, 1);
});
test('cleanup uses Eastern DST and protects priority-zero and unchecked logs', async () => {
  const b = mock();
  log(b, 1, {completed: '2026-11-01T03:59:59Z'});
  const sameDay = log(b, 2, {completed: '2026-11-01T04:00:00Z'});
  const protectedRun = log(b, 3, {completed: '2026-10-30T04:00:00Z', 'show-id': 444});
  const linkedRun = log(b, 4, {completed: '2026-10-30T04:00:00Z'});
  b.add('errs-log', {'error-key': 'unchecked', 'run-id': linkedRun.values['run-id'], 'occurred-at': '2026-10-30T04:00:00Z'});
  b.data.cleanup.find(r => r.values.Name === 'errs-log').values.cleanup = null;
  assert.equal((await cleanResults(b, {now: '2026-11-01T23:59:59Z'})).daily, false);
  assert.equal((await cleanResults(b, {now: '2026-11-02T00:00:00Z'})).daily, true);
  assert.deepEqual(b.data['run-logs'].map(r => r.id), [sameDay.id, protectedRun.id, linkedRun.id]);
  assert.equal(b.data['errs-log'].length, 1);
});
test('unknown cleanup delete outcome stops without retry and records error', async () => {
  const b = mock(); log(b, 1); b.deleteFailure = true;
  await assert.rejects(cleanResults(b, {now: cutoff}), /Delete outcome unknown/);
  assert.equal(b.calls.filter(c => c.type === 'delete').length, 1);
  const c = b.data.cleanup.find(r => r.values.Name === 'run-logs');
  assert.equal(c.values.result, null);
  assert.match(c.values.error, /unknown/);
  b.deleteFailure = false;
  await assert.rejects(cleanResults(b, {now: cutoff}), /Previous cleanup deletion requires review/);
  assert.equal(b.calls.filter(c => c.type === 'delete').length, 1);
});
test('runner wrapper logs actual failure and leaves source tables untouched', async () => {
  const b = mock(); await assert.rejects(resultsLogged(b, 'event', async () => {throw Error('Missing source event');}), /Missing source event/);
  assert.equal(b.data['run-logs'][0].values.result, 'Failed'); assert.equal(b.data['errs-log'].length, 1); assert.equal(b.data.heartbeat[0].values['last-success'], undefined);
  assert.ok(b.calls.every(c => !c.name.startsWith('wef-data.')));
});
test('wrong base rejects before any writes', async () => {const b = mock(); b.id = 'wrong'; await assert.rejects(event(b), /Wrong Results base/); assert.equal(b.calls.length, 0);});
test('native packaging keeps one separate cleanup schedule and no cleanup in events', async () => {
  const {script, definitions} = buildEvents(schema, '2026-11-08T03:00:00Z');
  assert.equal(definitions.length, 6);
  assert.ok(!script.includes('fetch(') && !script.includes('cleanResults'));
  const schedules = definitions.filter(d => d.trigger.type === 'cron');
  assert.equal(schedules.length, 2);
  assert.equal(schedules[0].trigger.inputs.config.width, 10);
  assert.equal(schedules[1].trigger.inputs.config.width, 10);
  const cleanup = schedules[1].nodes[0].inputs.script;
  assert.ok(cleanup.includes('cleanResults') && !cleanup.includes('resultsEvent') && !cleanup.includes('fetch('));
  const b = mock();
  const context = vm.createContext({base: b, input: {config: () => ({operation: 'event', source: 'wef-data.trips', recordId: b.trip.id})}, Date, Intl, console});
  await new vm.Script('(async () => {' + script + '})()').runInContext(context);
  assert.equal(b.data['run-logs'].length, 1);
  assert.ok(saved(b, 'expected|Y'));
  await new vm.Script('(async () => {' + cleanup + '})()').runInContext(context);
  assert.equal(b.data['run-logs'].length, 2);
  assert.equal(b.calls.filter(c => c.type === 'delete').length, 0);
});
