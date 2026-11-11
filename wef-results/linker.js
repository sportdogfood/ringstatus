"use strict";

async function linkResults(base) {
  const C = typeof resultsCore === 'function' ? resultsCore(base) : require('./event-core').resultsCore(base);
  const [rows, shows, days, results, horses, riders] = await Promise.all(['snapshot', 'w-shows', 'w-days', 'wef-data.results', 'w-horses', 'w-riders'].map(C.read));
  const report = {linked: 0, unresolved: 0};
  for (const row of rows) {
    const key = C.text(row, 'snapshot-key');
    if (!/^(expected\|)?[XY]\|/.test(key)) continue;
    const payload = C.parse(C.cell(row, 'results-json')), identity = payload.identity;
    if (!identity?.tripKey) {report.unresolved++; continue;}
    const parts = identity.tripKey.split('|'), fields = {};
    const lookups = [
      ['w-shows', shows.filter(r => C.text(r, 'show-key') === parts.slice(0, 2).join('|')), true],
      ['w-days', days.filter(r => C.text(r, 'show-day-key') === parts.slice(0, 3).join('|')), true],
      ['wef-data.results', results.filter(r => C.text(r, 'result-key') === identity.tripKey), false],
      ['w-horses', horses.filter(r => C.text(r, 'horse-id') === identity.horse?.id), true],
      ['w-riders', riders.filter(r => C.text(r, 'rider-id') === identity.rider?.id), true]
    ];
    for (const [field, matches, single] of lookups) {
      if ((C.cell(row, field) || []).length) continue;
      if (!matches.length || (single && matches.length !== 1)) {report.unresolved++; continue;}
      fields[field] = matches.map(r => ({id: r.id}));
    }
    if (Object.keys(fields).length) {await C.write('snapshot', row.id, fields); report.linked++;}
  }
  return report;
}

if (typeof module !== 'undefined') module.exports = {linkResults};
