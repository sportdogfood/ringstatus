'use strict';

const assert = require('node:assert/strict');
const {test} = require('node:test');
const {cleanMessages} = require('./cleaner');

const BEFORE = '2026-11-08T23:59:59Z';
const SEVEN = '2026-11-09T00:00:00Z';
const OLD = '2026-11-07T16:00:00Z';
const BASE = 'appXjWPaCthAYs0l7';
const completed = {'run-id': 'previous', result: 'Success', completed: OLD};

function fixture() {
  const tables = new Map();
  const deletes = [];
  let state = null;
  let sequence = 0;
  function rows(table) {
    if (!tables.has(table)) tables.set(table, []);
    return tables.get(table);
  }
  function add(table, fields, createdTime = OLD) {
    const id = 'rec' + String(++sequence).padStart(14, '0');
    const row = {id, createdTime, fields: {...fields}};
    rows(table).push(row);
    return row;
  }
  const show = add('w-shows', {customer_id: 15, is_default: true, show_id: 200000077});
  const api = {
    base: BASE,
    async list(table) { return structuredClone(rows(table)); },
    async loadState() { return structuredClone(state); },
    async saveState(next, expected) {
      assert.deepEqual(state, expected);
      state = structuredClone(next);
      return true;
    },
    async remove(table, ids) {
      deletes.push({table, ids});
      const selected = new Set(ids);
      tables.set(table, rows(table).filter(row => !selected.has(row.id)));
    },
  };
  return {api, add, rows, deletes, show, checked: table => add('cleanup', {'\ufeffName': table, cleanup: {name: 'checked'}}),
    run: (now = BEFORE, options = {}) => cleanMessages({api, now, ...options})};
}

test('cap triggers at 1000, uses the oldest eligible rows and leaves exactly 500', async () => {
  const f = fixture();
  f.checked('run-logs');
  for (let i = 998; i >= 0; i--) {
    f.add('run-logs', completed, new Date(Date.parse(OLD) + i * 1000).toISOString());
  }
  assert.deepEqual((await f.run()).actions, []);
  f.add('run-logs', completed, '2026-11-07T17:00:00Z');
  const expected = f.rows('run-logs').slice(499, 999).reverse().map(row => row.id);
  assert.equal((await f.run()).actions[0].deleted, 500);
  assert.deepEqual(f.deletes.flatMap(batch => batch.ids), expected);
  assert.equal(f.rows('run-logs').length, 500);
  assert.deepEqual((await f.run()).actions, []);
});

test('an unfinished trim continues below 1000 after protected runs complete', async () => {
  const f = fixture();
  f.checked('run-logs');
  for (let i = 0; i < 1200; i++) {
    f.add('run-logs', i < 700 ? {'run-id': 'active', result: 'Running'} : completed);
  }
  assert.equal((await f.run()).actions[0].deleted, 500);
  assert.equal(f.rows('run-logs').length, 700);
  assert.deepEqual((await f.run()).actions, []);
  for (const row of f.rows('run-logs')) Object.assign(row.fields, {result: 'Success', completed: OLD});
  assert.equal((await f.run()).actions[0].deleted, 200);
  assert.equal(f.rows('run-logs').length, 500);
});

test('7 p.m. Eastern clears checked eligible logs once and protects active runs', async () => {
  const f = fixture();
  for (const table of ['heartbeat', 'run-logs', 'errs-log', 'alert-snapshots', 'w-days']) f.checked(table);
  f.add('heartbeat', completed);
  f.add('run-logs', completed);
  f.add('errs-log', {'run-id': 'previous', 'occurred-at': OLD});
  const active = f.add('heartbeat', {'run-id': 'current', result: 'Running'});
  const activeError = f.add('errs-log', {'run-id': 'current|class', 'occurred-at': OLD});
  const malformed = f.add('errs-log', {'occurred-at': '2026-02-30T08:00:00Z'});
  f.add('alert-snapshots', {});
  f.add('w-days', {});

  assert.deepEqual((await f.run()).actions, []);
  const atCutoff = await f.run(SEVEN);
  assert.equal(atCutoff.dailyDue, true);
  assert.deepEqual(atCutoff.actions.map(action => action.table), ['heartbeat', 'run-logs', 'errs-log']);
  assert.deepEqual(f.rows('heartbeat'), [active]);
  assert.deepEqual(f.rows('errs-log'), [activeError, malformed]);
  assert.equal(f.rows('alert-snapshots').length, 1);
  assert.equal(f.rows('w-days').length, 1);

  f.add('run-logs', completed);
  assert.equal((await f.run('2026-11-09T01:00:00Z')).dailyDue, false);
  assert.equal(f.rows('run-logs').length, 1);
  assert.equal((await f.run('2026-11-10T00:01:00Z')).dailyDue, true);
  assert.equal(f.rows('run-logs').length, 0);
});

test('a changed show resets checked owned live rows from both shows', async () => {
  const f = fixture();
  for (const table of ['alert-snapshots', 'run-logs', 'wef-live.trips', 'live-rings']) f.checked(table);
  f.add('alert-snapshots', {'show-id': 200000077});
  f.add('alert-snapshots', {'show-id': 200000078});
  f.add('wef-live.trips', {});
  f.add('live-rings', {});
  f.add('run-logs', completed);
  f.add('errs-log', {'occurred-at': OLD});
  assert.equal((await f.run()).newShow, false);
  f.show.fields.selected_date = '2026-11-09';
  assert.equal((await f.run()).newShow, false);
  assert.equal(f.rows('alert-snapshots').length, 2);
  f.show.fields.show_id = 200000078;
  assert.equal((await f.run()).newShow, true);
  assert.equal(f.rows('alert-snapshots').length, 0);
  assert.equal(f.rows('run-logs').length, 0);
  assert.equal(f.rows('errs-log').length, 1);
  assert.equal(f.rows('wef-live.trips').length, 1);
  assert.equal(f.rows('live-rings').length, 1);
  f.add('alert-snapshots', {'show-id': 200000078});
  assert.deepEqual((await f.run()).actions, []);
  assert.equal(f.rows('alert-snapshots').length, 1);
});

test('an uncertain delete leaves its intent and is never automatically replayed', async () => {
  const f = fixture();
  f.checked('errs-log');
  f.add('errs-log', {'occurred-at': OLD});
  await f.run();
  let attempts = 0;
  f.api.remove = async () => { attempts++; throw new Error('unknown outcome'); };
  await assert.rejects(f.run(SEVEN), /unknown outcome/);
  assert.ok((await f.api.loadState()).pending);
  await assert.rejects(f.run(SEVEN), /requires reconciliation/);
  assert.equal(attempts, 1);
});
