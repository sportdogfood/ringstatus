'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { base } = require('./schema.json');
const { due, plan, cleanup } = require('./cleanup');

const now = '2026-11-08T23:00:00.000Z';
const show = 200000077;
const event = { trigger: 'log-limit', show, day: '2026-11-08' };
const protection = { complete: true, pending: false, recordIds: [], runIds: [], resolvedErrorIds: [] };
const rows = Array.from({ length: 1340 }, (_, index) => ({
  id: 'rec' + String(index).padStart(14, '0'),
  createdTime: new Date(Date.parse(now) - 3600000 + index).toISOString(),
  fields: { result: 'Success', completed: now, 'show-id': show, 'run-id': `run-${index}` },
}));
const args = { table: 'run-logs', rows, event, now, checked: true, protection };

test('trim begins at 1,000 and removes the oldest eligible rows down to 500', () => {
  assert.equal(plan({ ...args, rows: rows.slice(0, 999) }).length, 0);
  assert.deepEqual(plan({ ...args, rows: rows.slice(0, 1000) }), rows.slice(0, 500));
  assert.deepEqual(plan(args), rows.slice(0, 840));
});

test('a protected trim resumes below the trigger on a later normal run', async () => {
  let saved = new Map(rows.map(row => [row.id, row]));
  let pending;
  let persistedResult;
  const api = {
    base,
    read: async (_table, ids) => ids.map(id => saved.get(id)).filter(Boolean),
    beginIntent: async intent => { pending = intent; return 'intent'; },
    remove: async (_table, ids) => {
      assert(pending);
      for (const id of ids) saved.delete(id);
    },
    finishIntent: async (_id, result) => { persistedResult = result; pending = null; },
  };
  const result = await cleanup({ ...args, api, protection: {
    ...protection, recordIds: rows.slice(0, 600).map(row => row.id),
  } });
  assert.deepEqual(result, { selected: 740, deleted: 740, remaining: 600, trimPending: true });
  assert.deepEqual(persistedResult, result);
  assert.equal(saved.size, 600);

  const later = await cleanup({ ...args, api, rows: [...saved.values()], pendingTrim: persistedResult.trimPending });
  assert.deepEqual(later, { selected: 100, deleted: 100, remaining: 500, trimPending: true });
  assert.equal(saved.size, 500);
  const confirmed = await cleanup({ ...args, api, rows: [...saved.values()], pendingTrim: persistedResult.trimPending });
  assert.deepEqual(confirmed, { selected: 0, deleted: 0, remaining: 500, trimPending: false });
});

test('an append during deletion keeps the trim open until a fresh count confirms 500', async () => {
  const saved = new Map(rows.slice(0,1000).map(row => [row.id,row]));
  let state;
  const api={base,read:async(_table,ids)=>ids.map(id=>saved.get(id)).filter(Boolean),
    beginIntent:async()=> 'intent',
    remove:async(_table,ids)=>{ for(const id of ids)saved.delete(id); if(!state)saved.set(rows[1000].id,rows[1000]); },
    finishIntent:async(_id,result)=>{state=result;}};
  const first=await cleanup({...args,api,rows:[...saved.values()]});
  assert.equal(first.trimPending,true);
  assert.equal(saved.size,501);
  await cleanup({...args,api,rows:[...saved.values()],pendingTrim:state.trimPending});
  assert.equal(saved.size,500);
  const confirmed=await cleanup({...args,api,rows:[...saved.values()],pendingTrim:state.trimPending});
  assert.equal(confirmed.trimPending,false);
});

test('an entirely protected table retains the unfinished trim without deleting', async () => {
  const result = await cleanup({ ...args, api: { base }, protection: {
    ...protection, recordIds: rows.map(row => row.id),
  } });
  assert.deepEqual(result, { selected: 0, deleted: 0, remaining: 1340, trimPending: true });
});

test('daily clear is due at 7 p.m. Eastern once per local cutoff in winter and summer', () => {
  for (const [day, before, cutoff] of [
    ['2026-11-08', '2026-11-08T23:59:59Z', '2026-11-09T00:00:00Z'],
    ['2026-07-01', '2026-07-01T22:59:59Z', '2026-07-01T23:00:00Z'],
  ]) {
    assert.equal(due({ show }, show, before).trigger, 'log-limit');
    assert.deepEqual(due({ show }, show, cutoff), { trigger: 'daily-1900', show, day });
    assert.equal(due({ show, dailyDay: day }, show, cutoff).trigger, 'log-limit');
  }
  assert.equal(plan({ ...args, event: { ...event, trigger: 'daily-1900' } }).length, 1340);
});

test('new show clears eligible checked logs across show IDs and can satisfy the daily cutoff', () => {
  const reset = due({ show: show - 1 }, show, '2026-11-09T00:00:00Z');
  assert.equal(reset.trigger, 'new-show');
  assert.equal(reset.dailyDue, true);
  const mixed = [rows[0], { ...rows[1], fields: { ...rows[1].fields, 'show-id': show - 1 } }];
  assert.deepEqual(plan({ ...args, rows: mixed, event: reset }), mixed);
  assert.equal(due({ show, dailyDay: '2026-11-08' }, show, '2026-11-09T06:00:00Z').trigger, 'log-limit');
});

test('unchecked or unsupported tables and pending intents cannot select rows', () => {
  const reset = due({ show: show - 1 }, show, now);
  for (const table of ['live-rings', 'wef-live.trips', 'trip-status', 'timing-answers', 'engine-checkpoints', 'cleanup']) {
    assert.deepEqual(plan({ ...args, table, event: reset }), []);
  }
  for (const checked of [false, 'unchecked']) assert.deepEqual(plan({ ...args, checked }), []);
  assert.deepEqual(plan({ ...args, protection: { ...protection, pending: true } }), []);
});

test('active runs, protected records and unresolved errors survive a daily clear', () => {
  const daily = { ...event, trigger: 'daily-1900' };
  const active = { ...rows[0], fields: { ...rows[0].fields, result: 'Running', completed: null } };
  assert.deepEqual(plan({ ...args, rows: [active, rows[1], rows[2]], event: daily, protection: {
    ...protection, runIds: [rows[1].fields['run-id']],
  } }), [rows[2]]);
  const errors = rows.slice(0, 2).map(row => ({ ...row, fields: { ...row.fields, 'occurred-at': now } }));
  assert.deepEqual(plan({ ...args, table: 'errs-log', rows: errors, event: daily, protection: {
    ...protection, resolvedErrorIds: [errors[1].id],
  } }), [errors[1]]);
});

test('changed rows prevent deletion and uncertain deletion leaves intent pending', async () => {
  const selected = rows.slice(0, 840);
  let deleted = false;
  let finished = false;
  const api = {
    base,
    read: async () => selected,
    beginIntent: async () => 'intent',
    remove: async () => { deleted = true; },
    finishIntent: async () => { finished = true; },
  };
  await assert.rejects(() => cleanup({ ...args, api: { ...api, read: async () => [] } }), /changed/);
  assert.equal(deleted, false);
  await assert.rejects(() => cleanup({ ...args, api }), /absence unverified/);
  assert.equal(deleted, true);
  assert.equal(finished, false);
});
