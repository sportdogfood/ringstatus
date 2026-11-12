"use strict";
// Describe future logs from saved identities only; no source or Airtable calls.
const KEYS = {
  'live-rings': 'ring-key', 'live-groups': 'group-key',
  'live-classes': 'class-key', 'live-class-appearances': 'appearance-key',
  'live-entries': 'live-entry-key', 'live-trips': 'trip-key',
};
const TYPES = {
  'live-rings': 'ring', 'live-groups': 'group', 'live-classes': 'class',
  'live-class-appearances': 'class-appearance', 'live-entries': 'entry', 'live-trips': 'trip',
};
const id = value => value !== null && value !== undefined && value !== '' &&
  Number.isSafeInteger(Number(value)) && Number(value) > 0 ? Number(value) : null;
function logFields(lane, runId, change, data = {}) {
  if (!change) return {lane, 'record-type': 'run', 'record-key': runId};
  const row = (data[change.table] || []).find(r => change.key ?
    r.fields[KEYS[change.table]] === change.key : r.id === change.id);
  const f = row?.fields || {};
  const linked = (table, links) => Array.isArray(links) && links.length === 1 ?
    (data[table] || []).find(r => r.id === links[0])?.fields : null;
  const cls = change.table === 'live-classes' ? f : linked('live-classes', f.class);
  const group = linked('live-groups', f.group);
  const ring = linked('live-rings', f.ring) || linked('live-rings', cls?.ring) || linked('live-rings', group?.ring);
  const out = {lane, 'record-type': TYPES[change.table],
    'record-key': change.key || f[KEYS[change.table]] || change.id};
  for (const [field, value] of Object.entries({
    'trip-id': id(f['trip-id']), 'class-id': id(f['class-id']) || id(cls?.['class-id']),
    'ring-id': id(f['ring-id']) || id(ring?.['ring-id']),
  })) if (value !== null) out[field] = value;
  return out;
}
module.exports = {logFields};
