"use strict";
const keys = {'live-rings':'ring-key','live-groups':'group-key','live-classes':'class-key','live-entries':'live-entry-key'};
function validScope(scope) {
  return scope && Number.isSafeInteger(scope.show) && scope.show > 0 && /^\d{4}-\d{2}-\d{2}$/.test(scope.date);
}
function outsideScope(fields, scope) {
  return fields.date && (fields.date !== scope.date || Number(fields['show-id']) !== scope.show);
}
function canRetire(fields, scope) {
  if (!validScope(scope) || !/^\d{4}-\d{2}-\d{2}$/.test(fields.date || '')) return false;
  return fields.date < scope.date || (fields.date === scope.date &&
    Number.isSafeInteger(Number(fields['show-id'])) && Number(fields['show-id']) > 0 && Number(fields['show-id']) !== scope.show);
}
async function labelHistorical(api, scope, keep, now) {
  if (!validScope(scope)) throw Error('Invalid rollover scope');
  for (const [table,key] of Object.entries(keys)) {
    const rows = (await api.list(table)).filter(r=>outsideScope(r.fields,scope) &&
      (r.fields['scope-status'] !== 'historical' || (table === 'live-entries' && r.fields['participation-link-status'] === 'current')));
    if (!rows.length) continue;
    await keep('rollover-before-'+table,rows);
    await api.upsert(table,key,rows.map(r=>({[key]:r.fields[key],'scope-status':'historical',
      ...(table === 'live-entries' && r.fields['participation-link-status'] === 'current' ? {
        'participation-link-status':'historical',
        'participation-link-reason':'Outside canonical show/date; existing observation and links retained',
        'participation-checked-at':now,
      } : {})})));
  }
}
module.exports={keys,canRetire,labelHistorical};
