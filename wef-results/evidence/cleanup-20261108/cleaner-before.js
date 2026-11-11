"use strict";

async function cleanResults(base, {reason, now = new Date().toISOString()} = {}) {
  const C = typeof resultsCore === 'function' ? resultsCore(base) : require('./event-core').resultsCore(base);
  if (!['daily', 'cap', 'new-show'].includes(reason)) throw Error('Unknown Results cleanup');
  const today = new Intl.DateTimeFormat('en-CA', {timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit'}).format(new Date(now));
  const epoch = value => {const n = Date.parse(value); return Number.isFinite(n) ? n : null;};
  const oldTime = value => {const n = epoch(value); return n !== null && n <= Date.parse(now) && new Intl.DateTimeFormat('en-CA', {timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit'}).format(new Date(n)) < today;};
  const [controls, runs, errors, rows, shows, days] = await Promise.all(['cleanup', 'run-logs', 'errs-log', 'snapshot', 'w-shows', 'w-days'].map(C.read));
  const defaults = shows.filter(r => C.cell(r, 'is_default') === true);
  const defaultIds = new Set(defaults.map(r => C.cell(r, 'show_id')));
  const currentDays = new Set(days.filter(r => C.cell(r, 'day-priority') === 0).map(r => C.text(r, 'show-day-key')));
  // Logs carry show-id only: preserve the show when any of its days is protected.
  const currentShows = new Set([...currentDays].map(key => key.split('|')[1]));
  const terminal = r => ['Complete', 'Success', 'Failed'].includes(C.text(r, 'result')) && oldTime(C.text(r, 'completed'));
  // Delete only explicit owned tables. A checked source/reference row never grants deletion.
  const owned = {'errs-log': errors, 'run-logs': runs, snapshot: rows};
  const report = {reason, deleted: 0};
  for (const [name, records] of Object.entries(owned)) {
    const control = controls.filter(r => C.text(r, 'Name') === name);
    if (control.length !== 1 || C.cell(control[0], 'cleanup')?.name !== 'checked') continue;
    if (name === 'snapshot' && reason !== 'new-show') continue;
    if (reason === 'cap' && records.length <= 1000) continue;
    let eligible = records.filter(r => {
      if (name === 'snapshot') {
        const show = C.cell(r, 'show-id'), customer = C.cell(r, 'customer-id'), date = C.text(r, 'date');
        return defaults.length === 1 && show && !defaultIds.has(show) && shows.some(s => C.cell(s, 'show_id') === show && C.cell(s, 'customer_id') === customer) &&
          C.text(r, 'check-status') === 'Complete' && /^\d{4}-\d{2}-\d{2}$/.test(date) && date < today && !currentDays.has(customer + '|' + show + '|' + date);
      }
      const run = name === 'run-logs' ? r : runs.find(run => C.text(run, 'run-id') === C.text(r, 'run-id'));
      if (!run || !terminal(run)) return false;
      if (currentShows.has(C.text(run, 'show-id'))) return false;
      if (name === 'errs-log' && !oldTime(C.text(r, 'occurred-at'))) return false;
      if (reason === 'new-show') return defaults.length === 1 && C.cell(run, 'show-id') && !defaultIds.has(C.cell(run, 'show-id'));
      return true;
    });
    eligible.sort((a, b) => String(C.cell(a, name === 'snapshot' ? 'accepted-at' : name === 'errs-log' ? 'occurred-at' : 'completed') || '').localeCompare(String(C.cell(b, name === 'snapshot' ? 'accepted-at' : name === 'errs-log' ? 'occurred-at' : 'completed') || '')) || a.id.localeCompare(b.id));
    if (reason === 'cap') eligible = eligible.slice(0, 500);
    const started = new Date().toISOString();
    let deleted = 0;
    try {
      // Read back each delete batch. Never repeat an ambiguous deletion.
      for (let i = 0; i < eligible.length; i += 50) {
        const ids = eligible.slice(i, i + 50).map(r => r.id);
        await base.getTable(name).deleteRecordsAsync(ids);
        for (const id of ids) if (await base.getTable(name).selectRecordAsync(id)) throw Error('Cleanup delete readback mismatch');
        deleted += ids.length;
      }
      await C.write('cleanup', control[0].id, {records: records.length, started, completed: new Date().toISOString(), result: {name: 'Success'},
        'records-deleted': deleted, 'records-archived': 0, error: '', ...(reason === 'new-show' && defaults.length === 1 ? {'new-show-id': C.cell(defaults[0], 'show_id')} : {})});
    } catch (e) {
      await C.write('cleanup', control[0].id, {records: records.length, started, completed: new Date().toISOString(), result: null, 'records-deleted': deleted, error: String(e.message)});
      throw e;
    }
    report.deleted += deleted;
  }
  return report;
}

if (typeof module !== 'undefined') module.exports = {cleanResults};
