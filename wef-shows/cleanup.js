"use strict";
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const {local} = require('./discovery-policy');
const {atomicReport} = require('../wef-schedules/collect');
const settings = require('./settings');

// Fixed owned targets. Checking a reference table never authorizes its deletion.
const TABLES = ['err-logs', 'run-logs', 'heartbeat', 'show-days', 'shows'];
const LOGS = ['err-logs', 'run-logs', 'heartbeat'];
const TERMINAL = new Set(['Success', 'Partial', 'Failed', 'Skipped', 'Error']);
const REFERENCES = ['show-times', 'health-table', 'warning-log', 'decoder-run-log', 'decoder-fail-alerts'];
const validTime = value => typeof value === 'string' && Number.isFinite(Date.parse(value));
const digest = fields => crypto.createHash('sha256').update(JSON.stringify(fields)).digest('hex');

function checked(row) {
  return row.fields.cleanup === 'checked' || row.fields.cleanup === true;
}

function completed(row) {
  return TERMINAL.has(row.fields.result) && validTime(row.fields.completed);
}

function context(shows, runs, currentRunId) {
  const defaults = shows.filter(r => r.fields.is_default === true && r.fields.customer_id === 15);
  if (defaults.length !== 1 || !Number.isSafeInteger(defaults[0].fields.show_id) || defaults[0].fields.show_id <= 0) {
    throw Error('Cleanup requires exactly one canonical default show');
  }
  const show = defaults[0];
  const activeRuns = new Set(runs.filter(r => !completed(r) || r.fields['run-id'] === currentRunId).map(r => r.id));
  const runKeys = new Map(runs.map(r => [r.fields['run-id'], r]));
  const runsById = new Map(runs.map(r => [r.id, r]));
  return {show, shows: new Map(shows.map(r => [r.id, r])), activeRuns, runKeys, runsById, currentRunId};
}

function eligible(table, row, trigger, ctx) {
  if (!TABLES.includes(table)) return false;
  const f = row.fields;
  if (REFERENCES.some(key => Array.isArray(f[key]) && f[key].length)) return false;
  const currentShow = ctx.show.fields.show_id;
  if (table === 'shows') {
    return trigger === 'new-show' && f.customer_id === 15 && f.is_default !== true &&
      Number.isSafeInteger(f.show_id) && f.show_id !== currentShow && !(f['show-days'] || []).length;
  }
  if (table === 'show-days') {
    const parent = (f.show || []).length === 1 && ctx.shows.get(f.show[0]);
    return trigger === 'new-show' && parent && parent.fields.customer_id === 15 &&
      parent.fields.show_id !== currentShow && parent.fields.is_default !== true;
  }
  if (table === 'heartbeat') {
    if (f['heartbeat-key'] === 'WEF-default' || !TERMINAL.has(f.result) || !validTime(f['checked-at'])) return false;
    if (trigger === 'daily-clear' && Date.parse(f['checked-at']) > Date.parse(ctx.cutoff)) return false;
    return trigger !== 'new-show' || Number.isSafeInteger(f['show-id']) && f['show-id'] !== currentShow;
  }
  if (table === 'run-logs') {
    if (!completed(row) || ctx.activeRuns.has(row.id) || f['run-id'] === ctx.currentRunId) return false;
    if (trigger === 'daily-clear' && Date.parse(f.completed) > Date.parse(ctx.cutoff)) return false;
    if ((f['err-logs'] || []).some(id => !ctx.deletedErrors?.has(id))) return false;
    return trigger !== 'new-show' || Number.isSafeInteger(f['show-id']) && f['show-id'] !== currentShow;
  }
  if (!validTime(f.at) || f['run-id'] === ctx.currentRunId) return false;
  const parents = (f.run || []).map(id => ctx.runsById.get(id));
  if (!parents.length && ctx.runKeys.has(f['run-id'])) parents.push(ctx.runKeys.get(f['run-id']));
  if (!parents.length || parents.some(r => !r || !completed(r) || ctx.activeRuns.has(r.id))) return false;
  if (trigger === 'daily-clear' && (Date.parse(f.at) > Date.parse(ctx.cutoff) ||
      parents.some(r => Date.parse(r.fields.completed) > Date.parse(ctx.cutoff)))) return false;
  return trigger !== 'new-show' || parents.every(r => Number.isSafeInteger(r.fields['show-id']) && r.fields['show-id'] !== currentShow);
}

function choose(rows, available, trigger, draining = false) {
  if (trigger === 'quick-cleanup' && (!draining && rows.length < 1000 || rows.length <= 500)) return [];
  const ordered = [...available].sort((a, b) => {
    const time = r => Date.parse(r.createdTime || r.fields.started || r.fields.at || r.fields['checked-at'] || 0);
    return time(a) - time(b) || a.id.localeCompare(b.id);
  });
  return trigger === 'quick-cleanup' ? ordered.slice(0, rows.length - 500) : ordered;
}

async function runCleanup({api, clock = () => new Date().toISOString(), currentRunId,
  previousShowId, deadline = Infinity, wallClock = Date.now,
  statePath = path.join(settings.RUNTIME, 'cleanup-state.json')} = {}) {
  const started = clock();
  const report = {run_id: crypto.randomUUID(), started, result: 'Skipped', actions: [], counts: {}, archives: 0};
  let state = fs.existsSync(statePath) ? JSON.parse(fs.readFileSync(statePath, 'utf8')) :
    {version: 1, show: null, daily_date: null, draining: [], active: null};
  if (state.version !== 1 || !Array.isArray(state.draining)) throw Error('Invalid Shows cleanup checkpoint');
  const save = () => atomicReport(statePath, state);
  let ctx;

  async function execute(action, targets) {
    const deletedIds = new Set(action.deleted_ids);
    const outcome = targets.length ? await api.removeCleanup(action.table, targets, {
      showId: action.show, trigger: action.trigger, deletedIds,
      shouldPause: () => wallClock() >= deadline - 15000,
      authorize: async current => {
        const liveShows = await api.list('shows');
        let liveRuns = [];
        if (action.table === 'err-logs') {
          const ids = [...new Set(current.flatMap(r => (r.fields.run || []).length ? r.fields.run :
            [ctx.runKeys.get(r.fields['run-id'])?.id]).filter(Boolean))];
          const filter = 'OR(' + ids.map(id => `RECORD_ID()="${id}"`).join(',') + ')';
          liveRuns = await api.list('run-logs', settings.BASE_ID, filter);
        }
        const live = context(liveShows, liveRuns, currentRunId);
        live.deletedErrors = ctx.deletedErrors;
        live.cutoff = action.cutoff;
        if (live.show.fields.show_id !== action.show) throw Error('Canonical show changed during cleanup');
        if (!(await api.list('cleanup')).some(r => r.fields.Name === action.table && checked(r))) {
          throw Error('Cleanup table selection changed');
        }
        if (current.some(r => !eligible(action.table, r, action.trigger, live))) throw Error('Cleanup target became protected');
      },
      beforeDelete: async ids => {
        action.phase = 'Deleting';
        action.pending_ids = ids;
        await save();
      },
      onDeleted: async ids => {
        action.deleted_ids.push(...ids);
        action.deleted = action.deleted_ids.length;
        for (const id of ids) {deletedIds.add(id); if (action.table === 'err-logs') ctx.deletedErrors.add(id);}
        action.pending_ids = [];
        action.phase = 'Running';
        await save();
      }
    }) : null;
    const after = await api.list(action.table);
    if (after.some(r => action.deleted_ids.includes(r.id))) throw Error('Cleanup deletion readback failed');
    action.after = after.length;
    if (outcome?.paused) {
      action.phase = 'Paused';
      action.error = 'Continuing confirmed remaining targets on the next normal scheduled run';
      await save();
      await publish(action);
      return false;
    }
    action.completed = clock();
    action.result = action.targets.length ? 'Success' : 'Skipped';
    action.error = action.trigger === 'quick-cleanup' && after.length > 500 ?
      `Protected records or concurrent inserts retained; ${after.length} total remain` : '';
    if (action.trigger === 'quick-cleanup' && after.length > 500) {
      state.draining = [...new Set([...state.draining, action.table])];
    } else state.draining = state.draining.filter(t => t !== action.table);
    await publish(action);
    state.event.completed.push(action.table);
    state.active = null;
    await save();
    return true;
  }

  async function publish(action) {
    const fields = {'cleanup-id': action.id, 'event-id': action.event_id, 'run-id': action.run_id,
      table: action.table, trigger: action.trigger, started: action.started, result: action.result,
      'old-show-id': action.old_show || null, 'new-show-id': action.show,
      records: action.before, 'records-after': action.after ?? null,
      'records-deleted': action.deleted || 0, 'records-protected': action.protected,
      'records-archived': 0, 'record-ids': action.targets.map(r => r.id).join('\n'),
      'deleted-ids': (action.deleted_ids || []).join('\n'), error: action.error || '',
      ...(action.completed ? {completed: action.completed} : {})};
    await api.upsert('cleanup-logs', 'cleanup-id', [fields]);
    await api.upsert('cleanup', 'Name', [{Name: action.table, records: action.before,
      started: action.started, completed: action.completed || null, result: action.result,
      'old-show-id': action.old_show || null, 'new-show-id': action.show,
      'records-deleted': action.deleted || 0, 'records-archived': 0,
      'archive-file': '', error: action.error || '', 'run-id': action.run_id}]);
  }

  try {
    const controls = await api.list('cleanup');
    const selected = controls.filter(r => checked(r) && TABLES.includes(r.fields.Name)).map(r => r.fields.Name);
    if (new Set(selected).size !== selected.length) throw Error('Duplicate checked Shows cleanup table');
    const shows = await api.list('shows');
    const runs = await api.list('run-logs');
    ctx = context(shows, runs, currentRunId);
    ctx.deletedErrors = new Set();
    const show = ctx.show.fields.show_id;
    report.show_id = show;
    if (state.active) {
      // Reconcile receipts only. Never repeat an interrupted DELETE automatically.
      const a = state.active;
      if (a.phase === 'Paused') {
        if (a.show !== show || !selected.includes(a.table)) throw Error('Paused cleanup scope changed');
        const rows = await api.list(a.table);
        const remaining = a.targets.filter(r => !a.deleted_ids.includes(r.id));
        const targets = remaining.map(r => rows.find(row => row.id === r.id));
        if (targets.some((row, i) => !row || digest(row.fields) !== remaining[i].hash)) {
          throw Error('Paused cleanup target changed; no retry performed');
        }
        report.actions.push(a);
        a.phase = 'Running';
        await save();
        if (!await execute(a, targets)) return {...report, result: 'Pending', completed: clock()};
      } else {
      if (a.failure_recorded) throw Error(a.error);
      const remaining = await api.list(a.table);
      const ids = new Set(remaining.map(r => r.id));
      a.deleted_ids = a.targets.filter(r => !ids.has(r.id)).map(r => r.id);
      a.deleted = a.deleted_ids.length;
      a.after = remaining.length;
      a.result = 'Failed';
      a.phase = 'Failed';
      a.error = 'Previous cleanup action incomplete; deletion outcomes reconciled, no retry performed';
      a.completed = clock();
      await publish(a);
      a.failure_recorded = true;
      await save();
      throw Error(a.error);
      }
    }
    const oldShow = state.show || previousShowId || show;
    if (!state.show) {state.show = oldShow; await save();}
    const localNow = local(started);
    const events = state.event ? [{trigger: state.event.trigger, id: state.event.id,
      date: state.event.date, cutoff: state.event.cutoff}] : [];
    if (state.event && state.event.show !== show) throw Error('Pending cleanup event show changed');
    if (oldShow !== show) events.push({trigger: 'new-show', id: `new-show|${oldShow}|${show}`});
    if (localNow.hour >= 19 && state.daily_date !== localNow.date) {
      events.push({trigger: 'daily-clear', id: `daily-19|${localNow.date}`, date: localNow.date, cutoff: started});
    }
    events.push({trigger: 'quick-cleanup', id: `log-check|${currentRunId || report.run_id}`});

    for (const event of events.filter((e, i) => events.findIndex(other => other.id === e.id) === i)) {
      if (state.event?.id !== event.id) state.event = {...event, show, completed: []};
      await save();
      ctx.cutoff = event.cutoff;
      for (const table of TABLES.filter(t => selected.includes(t) && (event.trigger === 'new-show' || LOGS.includes(t)))) {
        if (state.event.completed.includes(table)) continue;
        if (wallClock() >= deadline - 30000) return {...report, result: 'Pending', completed: clock()};
        const rows = await api.list(table);
        report.counts[table] = rows.length;
        const available = rows.filter(r => eligible(table, r, event.trigger, ctx));
        const targets = choose(rows, available, event.trigger, state.draining.includes(table));
        if (event.trigger === 'quick-cleanup' && rows.length < 1000 && !state.draining.includes(table)) continue;
        const action = {id: `${event.id}|${table}`, event_id: event.id, run_id: report.run_id, table, trigger: event.trigger,
          cutoff: event.cutoff,
          show, old_show: oldShow !== show ? oldShow : null, started: clock(), before: rows.length,
          protected: rows.length - available.length, targets: targets.map(r => ({id: r.id, hash: digest(r.fields)})),
          deleted: 0, deleted_ids: [], phase: 'Running', result: targets.length ? 'Running' : 'Skipped'};
        report.actions.push(action);
        state.active = action;
        await save();
        await publish(action);
        if (!await execute(action, targets)) return {...report, result: 'Pending', completed: clock()};
      }
      if (event.trigger === 'new-show') state.show = show;
      if (event.trigger === 'daily-clear') state.daily_date = event.date;
      state.event = null;
      await save();
    }
    report.result = report.actions.length ? 'Success' : 'Skipped';
  } catch (error) {
    report.result = 'Failed';
    report.error = error.message;
    if (state.active && !(state.active.phase === 'Failed' && state.active.failure_recorded)) {
      state.active.phase = 'Failed';
      state.active.result = 'Failed';
      state.active.error = error.message;
      state.active.completed = clock();
      try {await save(); await publish(state.active);} catch (logging) {report.logging_error = logging.message;}
    }
  }
  report.completed = clock();
  // Action targets contain IDs/hashes only, never deleted record contents or backups.
  return report;
}

module.exports = {runCleanup, TABLES, LOGS, completed, context, eligible, choose};
