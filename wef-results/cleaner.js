"use strict";

async function cleanResults(base, {now = new Date().toISOString()} = {}) {
  const C = typeof resultsCore === 'function' ? resultsCore(base) : require('./event-core').resultsCore(base);
  const logs = ['heartbeat', 'errs-log', 'run-logs'];
  const supported = ['snapshot', ...logs];
  const controls = await C.read('cleanup');
  const selected = new Map();
  for (const row of controls) {
    const name = C.text(row, 'Name');
    if (!supported.includes(name) || C.cell(row, 'cleanup')?.name !== 'checked') continue;
    if (selected.has(name)) throw Error('Duplicate checked cleanup table: ' + name);
    selected.set(name, row);
  }
  const report = {deleted: 0, daily: false, newShow: false, draining: []};
  if (!selected.size) return report;

  const eastern = new Intl.DateTimeFormat('en-CA', {timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit'});
  const localDay = value => eastern.format(new Date(value));
  const today = localDay(now);
  const hour = Number(new Intl.DateTimeFormat('en-GB', {timeZone: 'America/New_York', hour: '2-digit', hourCycle: 'h23'}).format(new Date(now)));
  const cutoff = hour >= 19 ? today : new Date(Date.parse(today + 'T12:00:00Z') - 86400000).toISOString().slice(0, 10);
  const stateName = 'Results cleaner state';
  const stateRows = controls.filter(r => C.text(r, 'Name') === stateName);
  if (stateRows.length > 1) throw Error('Duplicate Results cleaner state');
  let stateRow = stateRows[0];
  const previous = stateRow ? C.parse(C.cell(stateRow, 'purpose')) : {cutoff: hour < 19 ? cutoff : null, draining: []};
  if (previous.pending) throw Error('Previous cleanup deletion requires review');
  const [shows, days, runs, errors, beats] = await Promise.all(['w-shows', 'w-days', 'run-logs', 'errs-log', 'heartbeat'].map(C.read));
  const defaults = shows.filter(r => C.cell(r, 'is_default') === true && /^\d+\|\d+$/.test(C.text(r, 'show-key')));
  const show = defaults.length === 1 ? C.text(defaults[0], 'show-key') : null;
  const newShow = Boolean(show && previous.show && show !== previous.show);
  const daily = (!previous.cutoff || previous.cutoff < cutoff) && logs.some(name => selected.has(name));
  const rows = {heartbeat: beats, 'errs-log': errors, 'run-logs': runs};
  const draining = new Set(previous.draining || []);
  const capDue = new Set(logs.filter(name => selected.has(name) && (rows[name].length >= 1000 || draining.has(name))));
  const persist = async state => {
    const value = JSON.stringify(state);
    if (stateRow && C.text(stateRow, 'purpose') === value) return;
    stateRow = await C.write('cleanup', stateRow?.id, {Name: stateName, purpose: value});
  };
  // Remember the drain before deletes so a partial drain below 1,000 retains its target.
  for (const name of capDue) if (rows[name].length >= 1000) draining.add(name);
  await persist({...previous, draining: [...draining]});

  const protectedShows = new Set(days.filter(r => C.cell(r, 'day-priority') === 0).map(r => C.text(r, 'show-day-key').split('|')[1]).filter(Boolean));
  const timestamp = (r, field) => Date.parse(C.text(r, field));
  const old = time => Number.isFinite(time) && time <= Date.parse(now) && localDay(time) < today;
  const terminal = r => ['Complete', 'Success', 'Failed'].includes(C.text(r, 'result')) && old(timestamp(r, 'completed')) && !protectedShows.has(C.text(r, 'show-id'));
  const runById = new Map();
  for (const run of runs) {
    const id = C.text(run, 'run-id');
    if (id) runById.set(id, runById.has(id) ? null : run);
  }
  const latestBeat = new Map();
  for (const beat of beats) {
    const key = C.text(beat, 'heartbeat-key');
    const at = timestamp(beat, 'checked-at') || timestamp(beat, 'completed');
    if (Number.isFinite(at)) latestBeat.set(key, Math.max(latestBeat.get(key) ?? -Infinity, at));
  }
  const eligible = {
    heartbeat: r => terminal(r) && (timestamp(r, 'checked-at') || timestamp(r, 'completed')) < latestBeat.get(C.text(r, 'heartbeat-key')),
    'errs-log': r => {
      const run = runById.get(C.text(r, 'run-id'));
      return run && terminal(run) && old(timestamp(r, 'occurred-at')) && !protectedShows.has(C.text(r, 'show-id'));
    },
    'run-logs': r => terminal(r)
  };

  let deleting = false;
  async function remove(name, records, targets) {
    const table = base.getTable(name), row = selected.get(name);
    const started = new Date().toISOString();
    let remainingCount = records.length;
    try {
      if (targets.length && !deleting) {
        await persist({...previous, draining: [...draining], pending: true});
        deleting = true;
      }
      for (let offset = 0; offset < targets.length; offset += 50) await table.deleteRecordsAsync(targets.slice(offset, offset + 50).map(r => r.id));
      if (targets.length) {
        const remaining = new Set((await C.read(name)).map(r => r.id));
        if (targets.some(r => remaining.has(r.id))) throw Error('Cleanup delete readback mismatch: ' + name);
        remainingCount = remaining.size;
      }
      await C.write('cleanup', row.id, {records: records.length, started, completed: new Date().toISOString(), result: {name: 'Success'},
        'records-deleted': targets.length, 'records-archived': 0, error: '', ...(newShow ? {'old-show-id': Number(previous.show.split('|')[1]), 'new-show-id': Number(show.split('|')[1])} : {})});
    } catch (error) {
      await C.write('cleanup', row.id, {records: records.length, started, completed: new Date().toISOString(), result: null,
        'records-deleted': null, error: String(error.message)});
      throw error;
    }
    report.deleted += targets.length;
    return remainingCount;
  }

  if (newShow && selected.has('snapshot')) {
    const snapshots = await C.read('snapshot');
    await remove('snapshot', snapshots, snapshots);
  }
  let retainedErrors = errors;
  for (const name of logs) {
    if (!selected.has(name) || (!daily && !newShow && !capDue.has(name))) continue;
    const records = rows[name];
    const retainedRunIds = new Set(retainedErrors.map(r => C.text(r, 'run-id')).filter(Boolean));
    const timeField = name === 'errs-log' ? 'occurred-at' : 'completed';
    const available = records.filter(r => eligible[name](r) && (name !== 'run-logs' || !retainedRunIds.has(C.text(r, 'run-id'))))
      .sort((a, b) => timestamp(a, timeField) - timestamp(b, timeField) || a.id.localeCompare(b.id));
    const targets = daily || newShow ? available : available.slice(0, Math.max(0, records.length - 500));
    const remaining = await remove(name, records, targets);
    if (name === 'errs-log') {
      const removed = new Set(targets.map(r => r.id));
      retainedErrors = errors.filter(r => !removed.has(r.id));
    }
    if (remaining <= 500) draining.delete(name);
  }
  await persist({show: show || previous.show || null, cutoff: daily ? cutoff : previous.cutoff, draining: [...draining]});
  return {...report, daily, newShow, draining: [...draining]};
}

if (typeof module !== 'undefined') module.exports = {cleanResults};
