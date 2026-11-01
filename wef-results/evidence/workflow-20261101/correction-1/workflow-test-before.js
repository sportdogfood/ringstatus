"use strict";
const test = require('node:test');
const assert = require('node:assert/strict');
const {resultsWorkflow, runResultsWorkflow} = require('../workflow');

function fixture() {
  const key = '27|981|2031-05-04|2|43|87|UUID-A';
  let sequence = 0;
  const writes = [];
  const record = (id, fields) => ({id, fields: structuredClone(fields), getCellValue(n) {return this.fields[n] ?? null;}});
  const data = {
    'wef-data.trips': [record('recTrip', {'trip-key': key, 'class': '27|981|2031-05-04|2|43', entry: '981|87', horse: '51', rider: '72', 'trip-id': '105', 'gone-in': '1',
      'class_related_data.results_verified (from class)': '1', 'last-success': '2031-05-04T10:00:00Z',
      'source-json': JSON.stringify({horse_id: 51, rider_id: 72, score1: 0, time_one: 44.2, faults_one: 0, time_fault_one: 0})})],
    'wef-data.results': [record('recResult', {'result-key': key, trip: [{id: 'recTrip'}], class: '27|981|2031-05-04|2|43', entry: '981|87', score1: '0', score6: '81', 'time-two': '33.1', 'last-success': '2031-05-04T10:01:00Z'})],
    'wef-shows.show-days': [record('recDay', {'show-day-key': '27|981|2031-05-04', 'day-priority': 0})],
    'w-horses': [record('recHorse', {'horse-id': 51, 'horse-name': 'Horse A'})],
    'w-riders': [record('recRider', {'rider-id': 72, 'rider-name': 'Rider A'})],
    snapshot: [], 'run-logs': [], heartbeat: [], 'errs-log': []
  };
  const writable = new Set(['snapshot', 'run-logs', 'heartbeat', 'errs-log']);
  const base = {id: 'appK9IX59jFwUkB2u', getTable(name) {
    assert.ok(data[name], 'known table ' + name);
    return {selectRecordsAsync: async () => ({records: data[name]}), selectRecordAsync: async id => data[name].find(r => r.id === id),
      createRecordAsync: async fields => {assert.ok(writable.has(name), 'no source writes'); const r = record('recNew' + (++sequence), fields); data[name].push(r); writes.push({name, op: 'create', fields}); return r.id;},
      updateRecordAsync: async (id, fields) => {assert.ok(writable.has(name), 'no source writes'); Object.assign(data[name].find(r => r.id === id).fields, fields); writes.push({name, op: 'update', fields});}};
  }};
  const event = () => resultsWorkflow(base, {mode: 'event', source: 'wef-data.trips', recordId: 'recTrip', now: '2031-05-04T10:02:00Z'});
  const row = k => data.snapshot.find(r => r.fields['snapshot-key'] === k + '|' + key);
  return {key, data, base, event, row, writes, record};
}

test('alert creates separate X/Y results with one nonzero value, all X phases and local identities', async () => {
  const f = fixture(); await f.event();
  assert.equal(f.row('X').fields['check-status'], 'Complete');
  assert.equal(f.row('Y').fields['check-status'], 'Complete');
  assert.deepEqual(JSON.parse(f.row('Y').fields['results-json']).values, {'time-one': 44.2});
  const x = JSON.parse(f.row('X').fields['results-json']);
  assert.deepEqual(x.instances[0].values, {score6: '81', 'time-two': '33.1'});
  assert.equal(x.identity.horse.record, 'recHorse');
  assert.equal(x.identity.rider.record, 'recRider');
  assert.equal(x.identity.entry.key, '981|87');
  assert.equal(f.row('X').fields['source-observed-at'], '2031-05-04T10:00:00Z');
  assert.equal(f.row('expected|X').fields['check-status'], 'Complete');
});
test('polling never creates X/Y even when both qualify; repeat poll has no changes', async () => {
  const f = fixture(); await resultsWorkflow(f.base, {mode: 'reconcile'});
  assert.equal(f.row('X'), undefined); assert.equal(f.row('Y'), undefined);
  assert.equal(f.row('expected|X').fields['check-status'], 'Expected');
  const before = f.writes.length; await resultsWorkflow(f.base, {mode: 'reconcile'}); assert.equal(f.writes.length, before);
});
test('repeat alert and reconciliation preserve triggered rows and complete states', async () => {
  const f = fixture(); await f.event(); const before = f.writes.length;
  await f.event(); await resultsWorkflow(f.base, {mode: 'reconcile'});
  assert.equal(f.writes.length, before); assert.equal(f.data.snapshot.length, 4);
});
test('zero, blank and nonnumeric measurements cannot qualify', async () => {
  const f = fixture(); f.data['wef-data.trips'][0].fields['source-json'] = JSON.stringify({horse_id:51,rider_id:72,score1:0,time_one:'0.0',faults_one:'',time_fault_one:null});
  Object.assign(f.data['wef-data.results'][0].fields, {score6:'0','time-two':'not known'});
  await f.event(); assert.equal(f.row('X'), undefined); assert.equal(f.row('Y'), undefined);
});
test('nonzero measurements without the respective source flags do not trigger', async () => {
  const f = fixture(); Object.assign(f.data['wef-data.trips'][0].fields, {'gone-in':'0','class_related_data.results_verified (from class)':'0'});
  await f.event(); assert.equal(f.row('X'), undefined); assert.equal(f.row('Y'), undefined);
});
test('dynamic priority zero only; null priority is not zero', async () => {
  for (const priority of [null, -1, 1]) {
    const f=fixture(); f.data['wef-shows.show-days'][0].fields['day-priority']=priority;
    await f.event(); assert.equal(f.data.snapshot.length, 0);
  }
});
test('result-created/updated alerts resolve exact matching trip', async () => {
  const f = fixture(); await resultsWorkflow(f.base,{mode:'event',source:'wef-data.results',recordId:'recResult'});
  assert.ok(f.row('X')); assert.ok(f.row('Y'));
});
test('all matching nonzero instances retained; duplicates flagged, Y independent', async () => {
  const f=fixture(); f.data['wef-data.results'].push(f.record('recOtherResult',{...f.data['wef-data.results'][0].fields,score6:'92'}));
  await f.event(); assert.equal(JSON.parse(f.row('X').fields['results-json']).instances.length,2);
  assert.equal(f.row('X').fields['check-status'],'Conflict'); assert.equal(f.row('Y').fields['check-status'],'Complete');
});
test('duplicate triggered rows flagged without deletion or another result creation', async () => {
  const f=fixture(); await f.event(); f.data.snapshot.push(f.record('recDuplicate',f.row('X').fields));
  await resultsWorkflow(f.base,{mode:'reconcile'});
  const rows=f.data.snapshot.filter(r=>r.fields['snapshot-key']==='X|'+f.key);
  assert.equal(rows.length,2); assert.ok(rows.every(r=>r.fields['check-status']==='Conflict'));
  assert.equal(f.row('expected|X').fields['check-status'],'Conflict');
});
test('changed source flagged without overwriting the triggered evidence', async () => {
  const f=fixture(); await f.event(); const original=f.row('Y').fields['results-json'];
  f.data['wef-data.trips'][0].fields['source-json']=JSON.stringify({horse_id:51,rider_id:72,time_one:55});
  await f.event(); assert.equal(f.row('Y').fields['results-json'],original); assert.equal(f.row('Y').fields['check-status'],'Conflict');
  const before=f.writes.length; await f.event(); assert.equal(f.writes.length,before);
});
test('native wrapper writes and reads run log and heartbeat', async () => {
  const f=fixture(); await runResultsWorkflow(f.base,{mode:'reconcile'});
  assert.equal(f.data['run-logs'].length,1); assert.equal(f.data.heartbeat[0].fields.result,'Complete');
  assert.equal(f.data.snapshot.length,2);
});
test('wrong base never written; uncertain write fails without retry', async () => {
  const f=fixture(); await assert.rejects(runResultsWorkflow({...f.base,id:'wrong'},{mode:'reconcile'}),/Wrong Results base/);
  assert.equal(f.writes.length,0);
  let attempts=0; const original=f.base.getTable.bind(f.base);
  f.base.getTable=name=>{const t=original(name);if(name==='snapshot')t.createRecordAsync=async()=>{attempts++;throw Error('unknown write');};return t;};
  await assert.rejects(f.event(),/unknown write/); assert.equal(attempts,1);
});
