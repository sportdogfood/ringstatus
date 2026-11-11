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
test('daily cleaner protects today/active/missing clocks and all source/reference tables', async () => {
  const b = mock(); const old = log(b, 1), active = log(b, 2, {result: 'Running', completed: ''}), current = log(b, 3, {completed: now}), unknown = log(b, 4, {completed: ''});
  b.add('errs-log', {'error-key': 'err', 'run-id': old.values['run-id'], 'occurred-at': '2030-02-10T00:00:00Z'});
  await cleanResults(b, {reason: 'daily', now}); assert.deepEqual(b.data['run-logs'].map(r => r.id).sort(), [active.id, current.id, unknown.id].sort());
  assert.equal(b.data['errs-log'].length, 0); assert.ok(b.data['w-days'].length); assert.ok(b.data['wef-data.trips'].length);
  assert.ok(b.calls.filter(c => c.type === 'delete').every(c => ['run-logs', 'errs-log'].includes(c.name)));
});
test('cap only above 1000 deletes oldest 500 eligible; repeat at 501 deletes none', async () => {
  const b = mock(); for (let i = 0; i < 1001; i++) log(b, i, {completed: new Date(Date.parse('2029-12-01T00:00:00Z') + i * 1000).toISOString()});
  const retainedFirst = b.data['run-logs'][500].id; await cleanResults(b, {reason: 'cap', now}); assert.equal(b.data['run-logs'].length, 501);
  assert.equal(b.data['run-logs'][0].id, retainedFirst); await cleanResults(b, {reason: 'cap', now}); assert.equal(b.data['run-logs'].length, 501);
  assert.ok(b.calls.filter(c => c.type === 'delete').every(c => c.ids.length <= 50));
});
test('cap keeps exactly 1000 and protects current records even above threshold', async () => {
  const b = mock(); for (let i = 0; i < 1000; i++) log(b, i); await cleanResults(b, {reason: 'cap', now}); assert.equal(b.data['run-logs'].length, 1000);
  log(b, 1001, {completed: now}); b.data['run-logs'].forEach(r => r.values.completed = now); await cleanResults(b, {reason: 'cap', now}); assert.equal(b.data['run-logs'].length, 1001);
});
test('new default cleanup removes only completed old-show records and retains expectations/conflicts/current', async () => {
  const b = mock(); b.add('w-shows', {'show-key': '27|333', show_id: 333, customer_id: 27});
  for (const [show, date, status] of [[333, '2030-02-10', 'Complete'], [444, '2030-02-10', 'Complete'], [333, '2030-02-10', 'Expected'], [333, '2030-02-10', 'Conflict'], [333, '2030-02-12', 'Complete']]) b.add('snapshot', {'snapshot-key': 'old' + b.data.snapshot.length, 'customer-id': 27, 'show-id': show, date, 'check-status': status});
  await cleanResults(b, {reason: 'new-show', now}); assert.equal(b.data.snapshot.length, 4);
  const count = b.calls.length; await cleanResults(b, {reason: 'new-show', now}); assert.equal(b.data.snapshot.length, 4);
  assert.equal(b.calls.slice(count).filter(c => c.type === 'delete').length, 0);
});
test('cleanup follows Eastern date across DST and preserves ambiguous default shows', async () => {
  const b = mock(); log(b, 1, {completed: '2026-11-01T03:59:59Z'}); const sameEasternDay = log(b, 2, {completed: '2026-11-01T04:00:00Z'});
  await cleanResults(b, {reason: 'daily', now: '2026-11-02T00:00:00Z'}); assert.deepEqual(b.data['run-logs'].map(r => r.id), [sameEasternDay.id]);
  b.add('w-shows', {'show-key': '27|333', show_id: 333, customer_id: 27, is_default: true}); b.add('snapshot', {'show-id': 222, date: '2020-01-01', 'check-status': 'Complete'});
  await cleanResults(b, {reason: 'new-show', now}); assert.equal(b.data.snapshot.length, 1);
});
test('unknown cleanup delete outcome stops without retry and records error without a fake success', async () => {
  const b = mock(); log(b, 1); b.deleteFailure = true; await assert.rejects(cleanResults(b, {reason: 'daily', now}), /Delete outcome unknown/);
  assert.equal(b.calls.filter(c => c.type === 'delete').length, 1); const c = b.data.cleanup.find(r => r.values.Name === 'run-logs'); assert.equal(c.values.result, null); assert.match(c.values.error, /unknown/);
});
test('runner wrapper logs actual failure and leaves source tables untouched', async () => {
  const b = mock(); await assert.rejects(resultsLogged(b, 'event', async () => {throw Error('Missing source event');}), /Missing source event/);
  assert.equal(b.data['run-logs'][0].values.result, 'Failed'); assert.equal(b.data['errs-log'].length, 1); assert.equal(b.data.heartbeat[0].values['last-success'], undefined);
  assert.ok(b.calls.every(c => !c.name.startsWith('wef-data.')));
});
test('wrong base rejects before any writes', async () => {const b = mock(); b.id = 'wrong'; await assert.rejects(event(b), /Wrong Results base/); assert.equal(b.calls.length, 0);});
test('native packaging uses current schemas, exact schedules, nine inactive candidates and no external calls', async () => {
  const current = structuredClone(schema); current.tables.push({name: 'cleanup', id: 'tblCleanup', fields: []});
  const {script, definitions} = buildEvents(current, '2026-11-08T03:00:00Z'); assert.equal(definitions.length, 9); assert.ok(!script.includes('fetch('));
  const schedules = definitions.filter(d => d.trigger.type === 'cron'); assert.equal(schedules[0].trigger.inputs.config.width, 10); assert.equal(schedules[1].trigger.inputs.config.width, 3);
  assert.equal(schedules[2].trigger.inputs.config.tz, 'America/New_York'); assert.equal(schedules[2].trigger.inputs.config.localTriggerTimes[0].hour, 19);
  const b = mock(); const context = vm.createContext({base: b, input: {config: () => ({operation: 'event', source: 'wef-data.trips', recordId: b.trip.id})}, Date, Intl, console});
  await new vm.Script('(async () => {' + script + '})()').runInContext(context); assert.equal(b.data['run-logs'].length, 1); assert.ok(saved(b, 'expected|Y'));
});

test('all cleaner reasons protect priority-zero show logs and matching errors', async () => {
  for (const reason of ['daily', 'cap', 'new-show']) {
    const b = mock(), count = reason === 'cap' ? 1001 : 1;
    b.add('w-shows', {'show-key': '27|333', show_id: 333, customer_id: 27});
    b.add('w-days', {'show-day-key': '27|333|2030-02-10', 'day-priority': 0});
    for (let i = 0; i < count; i++) {
      const run = log(b, i);
      b.add('errs-log', {'error-key': 'protected' + i, 'run-id': run.values['run-id'], 'occurred-at': '2030-02-10T00:00:00Z'});
    }
    await cleanResults(b, {reason, now});
    assert.equal(b.data['run-logs'].length, count, reason); assert.equal(b.data['errs-log'].length, count, reason);
    assert.equal(b.calls.filter(c => c.type === 'delete').length, 0, reason);
  }
});

test('all cleaner reasons accept proven completed Success and retain Partial/Running/unknown', async () => {
  for (const reason of ['daily', 'cap', 'new-show']) {
    const b = mock(), count = reason === 'cap' ? 1001 : 1;
    for (let i = 0; i < count; i++) {
      const run = log(b, i, {result: 'Success'});
      b.add('errs-log', {'error-key': 'success' + i, 'run-id': run.values['run-id'], 'occurred-at': '2030-02-10T00:00:00Z'});
    }
    for (const status of ['Partial', 'Running', 'unknown']) log(b, status, {result: status, completed: '2030-02-10T00:00:00Z'});
    await cleanResults(b, {reason, now});
    const retained = reason === 'cap' ? 501 : 0;
    assert.equal(b.data['run-logs'].filter(r => r.values.result === 'Success').length, retained, reason);
    assert.equal(b.data['errs-log'].length, retained, reason);
    for (const status of ['Partial', 'Running', 'unknown']) assert.ok(b.data['run-logs'].some(r => r.values.result === status), reason + status);
  }
});
