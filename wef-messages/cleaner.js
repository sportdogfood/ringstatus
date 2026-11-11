'use strict';

const BASE_ID = 'appXjWPaCthAYs0l7';
const LOGS = ['heartbeat', 'run-logs', 'errs-log'];
// The only Messages-owned live snapshot. Synced source tables are never targets.
const LIVE = ['alert-snapshots'];
const TERMINAL = new Set(['Complete', 'Success', 'Partial', 'Failed', 'Skipped', 'Error']);
const eastern = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', hourCycle: 'h23',
});

function timestamp(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d\d-\d\dT(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)$/.test(value)) return NaN;
  const date = new Date(value.slice(0, 10) + 'T00:00:00Z');
  return Number.isFinite(+date) && date.toISOString().slice(0, 10) === value.slice(0, 10)
    ? Date.parse(value) : NaN;
}

function cutoff(now) {
  const parts = Object.fromEntries(eastern.formatToParts(now).map(({type, value}) => [type, value]));
  const day = `${parts.year}-${parts.month}-${parts.day}`;
  const afterSeven = Number(parts.hour) >= 19;
  const previousDay = new Date(Date.parse(`${day}T00:00:00Z`) - 86400000).toISOString().slice(0, 10);
  return {key: afterSeven ? day : previousDay, afterSeven};
}

function checked(rows, table) {
  const controls = rows.filter(({fields}) => (fields['\ufeffName'] ?? fields.Name) === table);
  if (controls.length !== 1) return false;
  const value = controls[0].fields.cleanup;
  return (value?.name ?? value) === 'checked';
}

function defaultShow(rows) {
  const defaults = rows.filter(({fields}) => fields.customer_id === 15 && fields.is_default === true);
  const id = defaults[0]?.fields.show_id;
  if (defaults.length !== 1 || !Number.isSafeInteger(id) || id <= 0) {
    throw new Error('Cleanup requires one verified WEF default show');
  }
  return id;
}

function completed(row, now) {
  return TERMINAL.has(row.fields.result) && timestamp(row.fields.completed) <= now;
}

function isProtectedRun(id, protectedRuns) {
  return typeof id === 'string' && protectedRuns.some(run => id === run || id.startsWith(`${run}|`));
}

function eligibleLogs(table, rows, now, protectedRecords, protectedRuns) {
  return rows.filter(row => {
    if (!(timestamp(row.createdTime) <= now) || protectedRecords.has(row.id) || isProtectedRun(row.fields['run-id'], protectedRuns)) return false;
    return table === 'errs-log' ? timestamp(row.fields['occurred-at']) <= now : completed(row, now);
  }).sort((a, b) => timestamp(a.createdTime) - timestamp(b.createdTime) || a.id.localeCompare(b.id));
}

/**
 * Called by the normal Messages runner. api.list returns all rows with Airtable
 * REST fields and createdTime. api.remove deletes the supplied IDs only.
 * api.loadState returns the last durable state (null on first run). api.saveState
 * atomically replaces that state only if it equals the supplied expected state,
 * and returns true after persistence. An interrupted deletion leaves its intent.
 */
async function cleanMessages({api, now = new Date().toISOString(), protectedRunIds = [], protectedRecordIds = []}) {
  if (api?.base !== BASE_ID) throw new Error('Messages base isolation required');
  const epoch = timestamp(now);
  if (!Number.isFinite(epoch)) throw new Error('Invalid cleanup clock');
  const previous = await api.loadState();
  if (previous?.pending) throw new Error('Prior cleanup deletion requires reconciliation');

  const [controls, shows] = await Promise.all([api.list('cleanup'), api.list('w-shows')]);
  const show = defaultShow(shows);
  const daily = cutoff(new Date(epoch));
  const dailyDue = previous?.dailyCutoff ? previous.dailyCutoff < daily.key : daily.afterSeven;
  const newShow = previous?.show != null && previous.show !== show;
  const logTables = LOGS.filter(table => checked(controls, table));
  const liveTables = newShow ? LIVE.filter(table => checked(controls, table)) : [];
  const reads = new Set([...logTables, ...liveTables]);
  if (logTables.length) {
    // An active run can own errors even when its parent log table is unchecked.
    reads.add('heartbeat');
    reads.add('run-logs');
  }
  const rowsByTable = Object.fromEntries(await Promise.all([...reads].map(async table => [table, await api.list(table)])));
  const activeRuns = [...protectedRunIds];
  for (const table of ['heartbeat', 'run-logs']) {
    for (const row of rowsByTable[table] ?? []) {
      if (!completed(row, epoch) && row.fields['run-id']) activeRuns.push(row.fields['run-id']);
    }
  }

  const protectedRecords = new Set(protectedRecordIds);
  const actions = [];
  const trimming = new Set();
  const draining = new Set(previous?.drainingLogs ?? []);
  for (const table of [...liveTables, ...logTables]) {
    const rows = rowsByTable[table];
    const isLive = liveTables.includes(table);
    const trim = !isLive && (rows.length >= 1000 || draining.has(table));
    if (trim) trimming.add(table);
    const trigger = newShow ? 'new-show' : dailyDue ? 'daily-1900' : 'log-limit';
    let selected = isLive ? rows : eligibleLogs(table, rows, epoch, protectedRecords, activeRuns);
    if (trigger === 'log-limit') selected = trim ? selected.slice(0, Math.max(0, rows.length - 500)) : [];
    if (selected.length) actions.push({table, trigger, ids: selected.map(row => row.id), before: rows.length});
    if (trim && rows.length - selected.length > 500) draining.add(table);
    else draining.delete(table);
  }

  const next = {
    show,
    dailyCutoff: dailyDue ? daily.key : previous?.dailyCutoff ?? daily.key,
    drainingLogs: LOGS.filter(table => draining.has(table)),
  };
  const save = async (state, expected) => {
    if (await api.saveState(state, expected) !== true) throw new Error('Cleanup state changed or was not saved');
  };

  if (actions.length) {
    const intent = {...previous, pending: {now, show, actions}};
    await save(intent, previous);
    const [freshControls, freshShows] = await Promise.all([api.list('cleanup'), api.list('w-shows')]);
    if (defaultShow(freshShows) !== show || actions.some(action => !checked(freshControls, action.table))) {
      throw new Error('Cleanup controls or default show changed after planning');
    }
    for (const {table, ids} of actions) {
      for (let i = 0; i < ids.length; i += 10) await api.remove(table, ids.slice(i, i + 10));
      const remaining = await api.list(table);
      const deleted = new Set(ids);
      if (remaining.some(row => deleted.has(row.id))) throw new Error(`Cleanup delete readback failed for ${table}`);
      if (trimming.has(table) && remaining.length > 500) draining.add(table);
      else draining.delete(table);
    }
    next.drainingLogs = LOGS.filter(table => draining.has(table));
    await save(next, intent);
  } else if (JSON.stringify(previous) !== JSON.stringify(next)) {
    await save(next, previous);
  }

  return {
    show, dailyDue, newShow,
    actions: actions.map(({table, trigger, ids, before}) => ({table, trigger, before, deleted: ids.length})),
  };
}

module.exports = {cleanMessages};
