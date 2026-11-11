"use strict";
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {runCleanup, TABLES} = require('../cleanup');
const {airtable} = require('../airtable');

function fixture(t, names = ['run-logs']) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'shows-cleanup-'));
  t.after(() => fs.rmSync(dir, {recursive: true, force: true}));
  const show = {id: 'recCurrentShow', fields: {show_id: 78, customer_id: 15, is_default: true}};
  const tables = {shows: [show], 'show-days': [], heartbeat: [], 'run-logs': [], 'err-logs': [],
    cleanup: names.map((Name, i) => ({id: 'recControl' + i, fields: {Name, cleanup: 'checked'}})),
    'cleanup-logs': [], sources: []};
  const deleted = [];
  let counter = 0, now = '2026-11-11T18:30:00Z', deleteCalls = 0;
  let fault = () => {};
  const request = async (url, options) => {
    const u = new URL(url), table = decodeURIComponent(u.pathname.split('/').at(-1));
    if (!tables[table]) throw Error('Unexpected table ' + table);
    const method = options.method;
    const response = data => ({ok: true, json: async () => structuredClone(data)});
    if (method === 'GET') {
      const filter = u.searchParams.get('filterByFormula');
      const ids = filter ? [...filter.matchAll(/RECORD_ID\(\)="([^"]+)"/g)].map(m => m[1]) : [];
      return response({records: tables[table].filter(r => !filter || ids.includes(r.id))});
    }
    if (method === 'DELETE') {
      deleteCalls++;
      const ids = u.searchParams.getAll('records[]');
      fault({table, ids, deleteCalls});
      tables[table] = tables[table].filter(r => !ids.includes(r.id));
      for (const rows of Object.values(tables)) for (const r of rows) {
        for (const key of Object.keys(r.fields)) {
          if (Array.isArray(r.fields[key])) r.fields[key] = r.fields[key].filter(id => !ids.includes(id));
        }
      }
      deleted.push(...ids);
      return response({records: ids.map(id => ({id, deleted: true}))});
    }
    const body = JSON.parse(options.body);
    const rows = body.records.map(r => {
      if (method === 'POST') {
        const row = {id: 'recCreated' + ++counter, fields: r.fields};
        tables[table].push(row);
        return row;
      }
      const row = tables[table].find(row => row.id === r.id);
      if (!row) throw Error('Missing fixture patch');
      Object.assign(row.fields, r.fields);
      return row;
    });
    return response({records: rows});
  };
  const api = airtable({request, pause: async () => {}});
  const opts = {api, clock: () => now, currentRunId: 'current-run', statePath: path.join(dir, 'cleanup-state.json')};
  return {tables, deleted, dir, api, opts, run: extra => runCleanup({...opts, ...extra}),
    setTime: value => {now = value;}, setFault: value => {fault = value;}};
}

function run(i, pending = false, sid = 77) {
  return {id: 'recRun' + i, createdTime: new Date(Date.UTC(2026, 9, 1, 0, i)).toISOString(),
    fields: {'run-id': 'run-' + i, 'show-id': sid, started: '2026-10-01T12:00:00Z',
      result: pending ? 'Running' : 'Success', ...(pending ? {} : {completed: '2026-10-01T12:01:00Z'})}};
}

test('1400 logs deletes 900 oldest eligible logs, leaves 500 and permanent history, without archives', async t => {
  const f = fixture(t);
  f.tables['run-logs'] = Array.from({length: 1400}, (_, i) => run(i));
  const result = await f.run();
  assert.equal(result.result, 'Success', result.error);
  assert.equal(f.deleted.length, 900);
  assert.equal(f.tables['run-logs'].length, 500);
  assert.equal(f.tables['run-logs'][0].id, 'recRun900');
  assert.equal(f.tables['cleanup-logs'][0].fields['records-deleted'], 900);
  assert.equal(f.tables.cleanup[0].fields.records, 1400);
  assert.equal(f.tables.cleanup[0].fields['records-archived'], 0);
  assert.deepEqual(fs.readdirSync(f.dir), ['cleanup-state.json']);
});

test('999 logs do not trigger threshold cleanup', async t => {
  const f = fixture(t);
  f.tables['run-logs'] = Array.from({length: 999}, (_, i) => run(i));
  assert.equal((await f.run()).result, 'Skipped');
  assert.equal(f.deleted.length, 0);
});

test('1000 logs reach 500 while current and pending runs remain protected', async t => {
  const f = fixture(t);
  f.tables['run-logs'] = Array.from({length: 1000}, (_, i) => run(i, i === 0));
  f.tables['run-logs'][1].fields['run-id'] = 'current-run';
  const result = await f.run();
  assert.equal(result.result, 'Success', result.error);
  assert.equal(f.deleted.length, 500);
  assert(f.tables['run-logs'].some(r => r.id === 'recRun0'));
  assert(f.tables['run-logs'].some(r => r.id === 'recRun1'));
});

test('protected rows remain even above 500 and actual remainder is recorded', async t => {
  const f = fixture(t);
  f.tables['run-logs'] = Array.from({length: 1400}, (_, i) => run(i, i < 1100));
  const result = await f.run();
  assert.equal(result.result, 'Success', result.error);
  assert.equal(f.deleted.length, 300);
  assert.equal(f.tables['run-logs'].length, 1100);
  assert.match(f.tables['cleanup-logs'][0].fields.error, /1100 total remain/);
});

test('daily clear starts after 7 Eastern once per date, preserves active heartbeat and pending errors', async t => {
  const f = fixture(t, ['err-logs', 'run-logs', 'heartbeat']);
  f.tables['run-logs'] = [run(1), run(2, true)];
  f.tables['err-logs'] = [
    {id: 'recError1', fields: {at: '2026-11-01T12:00:00Z', 'run-id': 'run-1', run: ['recRun1']}},
    {id: 'recError2', fields: {at: '2026-11-01T12:00:00Z', 'run-id': 'run-2', run: ['recRun2']}}
  ];
  f.tables['run-logs'][0].fields['err-logs'] = ['recError1'];
  f.tables.heartbeat = [{id: 'recHeartbeat', fields: {'heartbeat-key': 'WEF-default', result: 'Success'}}];
  f.setTime('2026-11-11T23:59:00Z');
  assert.equal((await f.run()).result, 'Skipped');
  f.setTime('2026-11-12T00:00:00Z');
  assert.equal((await f.run()).result, 'Success');
  assert.deepEqual(f.deleted.sort(), ['recError1', 'recRun1']);
  assert.equal(f.tables.heartbeat.length, 1);
  f.tables['run-logs'].push(run(3));
  assert.equal((await f.run()).result, 'Skipped');
  assert(f.tables['run-logs'].some(r => r.id === 'recRun3'));
});

test('summer daily clear uses Eastern DST and never backfills yesterday at midnight', async t => {
  const f = fixture(t);
  f.tables['run-logs'] = [run(1)];
  f.setTime('2026-07-02T04:05:00Z');
  assert.equal((await f.run()).result, 'Skipped');
  f.setTime('2026-07-02T23:00:00Z');
  assert.equal((await f.run()).result, 'Success');
});

test('historical heartbeats use their actual checked-at/status schema, preserving the current heartbeat', async t => {
  const f = fixture(t, ['heartbeat']);
  f.tables.heartbeat = [
    {id: 'recOldBeat', fields: {'heartbeat-key': 'old', 'show-id': 77, result: 'Success', 'checked-at': '2026-11-01T12:00:00Z'}},
    {id: 'recCurrentBeat', fields: {'heartbeat-key': 'WEF-default', 'show-id': 78, result: 'Success', 'checked-at': '2026-11-11T18:00:00Z'}},
    {id: 'recPendingBeat', fields: {'heartbeat-key': 'pending', result: 'Running', 'checked-at': '2026-11-11T18:00:00Z'}}
  ];
  const result = await f.run({previousShowId: 77});
  assert.equal(result.result, 'Success', result.error);
  assert.deepEqual(f.deleted, ['recOldBeat']);
});

test('new show clears owned previous-show data, preserving canonical and referenced rows', async t => {
  const f = fixture(t, [...TABLES, 'sources', 'cleanup-logs']);
  f.tables.shows.push({id: 'recOldShow', fields: {show_id: 77, customer_id: 15, is_default: false,
    'show-days': ['recOldDay']}});
  f.tables['show-days'] = [
    {id: 'recOldDay', fields: {show: ['recOldShow']}},
    {id: 'recCurrentDay', fields: {show: ['recCurrentShow']}}
  ];
  f.tables['run-logs'] = [run(1), run(2, false, 78)];
  f.tables.sources = [{id: 'recSource', fields: {name: 'Retained'}}];
  const result = await f.run({previousShowId: 77});
  assert.equal(result.result, 'Success', result.error);
  assert.deepEqual(f.tables.shows.map(r => r.id), ['recCurrentShow']);
  assert.deepEqual(f.tables['show-days'].map(r => r.id), ['recCurrentDay']);
  assert.deepEqual(f.tables['run-logs'].map(r => r.id), ['recRun2']);
  assert.equal(f.tables.sources.length, 1);
  const history = f.tables['cleanup-logs'].length;
  assert.equal((await f.run()).result, 'Skipped');
  assert.equal(f.tables['cleanup-logs'].length, history);
});

test('day selection alone and initial installation do not invent a new-show event', async t => {
  const f = fixture(t);
  f.tables['run-logs'] = [run(1)];
  await f.run();
  f.tables.shows[0].fields.selected_date = '2026-11-12';
  const result = await f.run();
  assert.equal(result.result, 'Skipped');
  assert.equal(f.deleted.length, 0);
});

test('other-owner references protect historical show records', async t => {
  const f = fixture(t, ['shows']);
  f.tables.shows.push({id: 'recOldShow', fields: {show_id: 77, customer_id: 15, is_default: false,
    'show-times': ['recTime']}});
  await f.run({previousShowId: 77});
  assert(f.tables.shows.some(r => r.id === 'recOldShow'));
});

test('a failed DELETE is recorded without retry; later wakes reconcile outcomes only', async t => {
  const f = fixture(t);
  f.tables['run-logs'] = Array.from({length: 1400}, (_, i) => run(i));
  f.setFault(({deleteCalls}) => {if (deleteCalls === 2) throw Error('Connection lost');});
  const first = await f.run();
  assert.equal(first.result, 'Failed');
  assert.equal(f.deleted.length, 10);
  assert.equal(f.tables['cleanup-logs'][0].fields['records-after'], null);
  const second = await f.run();
  assert.equal(second.result, 'Failed');
  assert.equal(f.deleted.length, 10);
  assert.match(second.error, /reconciled, no retry/);
  assert.equal(f.tables['cleanup-logs'].length, 1);
  assert.equal(f.tables['cleanup-logs'][0].fields['records-deleted'], 10);
});

test('time budget pauses only confirmed batches and completes the same 900 deletions next normal wake', async t => {
  const f = fixture(t);
  f.tables['run-logs'] = Array.from({length: 1400}, (_, i) => run(i));
  let elapsed = 0;
  f.setFault(({deleteCalls}) => {if (deleteCalls === 3) elapsed = 90000;});
  const first = await f.run({deadline: 100000, wallClock: () => elapsed});
  assert.equal(first.result, 'Pending', first.error);
  assert.equal(f.deleted.length, 30);
  assert.equal(JSON.parse(fs.readFileSync(f.opts.statePath)).active.phase, 'Paused');
  elapsed = 0;
  const second = await f.run({deadline: 100000, wallClock: () => elapsed});
  assert.equal(second.result, 'Success', second.error);
  assert.equal(f.deleted.length, 900);
  assert.equal(new Set(f.deleted).size, 900);
  assert.equal(f.tables['run-logs'].length, 500);
  assert.equal(f.tables['cleanup-logs'].length, 1);
  assert.equal(f.tables['cleanup-logs'][0].fields['records-deleted'], 900);
});

test('daily continuation preserves its event and cutoff across midnight', async t => {
  const f = fixture(t);
  f.tables['run-logs'] = Array.from({length: 30}, (_, i) => run(i));
  f.setTime('2026-11-12T00:01:00Z');
  let elapsed = 0;
  f.setFault(({deleteCalls}) => {if (deleteCalls === 1) elapsed = 90000;});
  assert.equal((await f.run({deadline: 100000, wallClock: () => elapsed})).result, 'Pending');
  f.setTime('2026-11-12T05:05:00Z');
  const later = run(99);
  later.fields.completed = '2026-11-12T05:00:00Z';
  f.tables['run-logs'].push(later);
  elapsed = 0;
  const result = await f.run({deadline: 100000, wallClock: () => elapsed});
  assert.equal(result.result, 'Success', result.error);
  assert.deepEqual(f.tables['run-logs'].map(r => r.id), ['recRun99']);
  assert.equal(f.tables['cleanup-logs'].length, 1);
  assert.equal(f.tables['cleanup-logs'][0].fields['event-id'], 'daily-19|2026-11-11');
});

test('failure while resuming a paused action cannot silently resume after configuration is restored', async t => {
  const f = fixture(t);
  f.tables['run-logs'] = Array.from({length: 1400}, (_, i) => run(i));
  let elapsed = 0;
  f.setFault(({deleteCalls}) => {if (deleteCalls === 1) elapsed = 90000;});
  assert.equal((await f.run({deadline: 100000, wallClock: () => elapsed})).result, 'Pending');
  f.tables.cleanup[0].fields.cleanup = null;
  elapsed = 0;
  assert.equal((await f.run()).result, 'Failed');
  assert.equal(JSON.parse(fs.readFileSync(f.opts.statePath)).active.phase, 'Failed');
  f.tables.cleanup[0].fields.cleanup = 'checked';
  assert.equal((await f.run()).result, 'Failed');
  assert.equal(f.deleted.length, 10);
});

test('an exhausted collector budget defers the genuine daily event without inventing midnight cleanup', async t => {
  const f = fixture(t);
  f.tables['run-logs'] = [run(1)];
  f.setTime('2026-11-12T00:01:00Z');
  assert.equal((await f.run({deadline: 1, wallClock: () => 0})).result, 'Pending');
  assert.equal(f.deleted.length, 0);
  f.setTime('2026-11-12T05:05:00Z');
  const result = await f.run();
  assert.equal(result.result, 'Success', result.error);
  assert.equal(f.deleted.length, 1);
  assert.equal(f.tables['cleanup-logs'][0].fields['event-id'], 'daily-19|2026-11-11');
});

test('adapter refuses direct deletes, reference deletions and changed target fields', async t => {
  const f = fixture(t);
  await assert.rejects(f.api.call('appCaSmL819KK7UFJ', 'shows', 'DELETE'), /forbidden/);
  await assert.rejects(f.api.removeCleanup('sources', [], {}), /Forbidden cleanup/);
  const snapshot = run(1);
  f.tables['run-logs'] = [structuredClone(snapshot)];
  f.tables['run-logs'][0].fields.result = 'Running';
  await assert.rejects(f.api.removeCleanup('run-logs', [snapshot], {showId: 78,
    trigger: 'quick-cleanup', authorize: async () => {}, onDeleted: async () => {}}), /changed/);
  assert.equal(f.deleted.length, 0);
});

test('malformed saved state is never reset', async t => {
  const f = fixture(t);
  fs.writeFileSync(f.opts.statePath, '{bad json');
  await assert.rejects(f.run());
  assert.equal(fs.readFileSync(f.opts.statePath, 'utf8'), '{bad json');
  assert.equal(f.deleted.length, 0);
});
