'use strict';

const LINK_FIELDS = [
  'w-shows', 'w-days', 'wef-live.rings', 'wef-data.schedules',
  'current-group', 'current-classes', 'current-trips',
  'next-group', 'next-classes', 'next-trips',
];

function fillLinks(fields, matches, issues) {
  for (const name of LINK_FIELDS) {
    if (fields[name]?.length) continue;
    const { rows, single = false, expected = false } = matches[name];
    const ids = [...new Set(rows.map(row => row.id))];
    if (single && ids.length > 1) {
      issues.push(name + '_link_ambiguous');
      fields[name] = [];
    } else {
      if (expected && !ids.length) issues.push(name + '_link_missing');
      fields[name] = ids;
    }
  }
  return fields;
}

module.exports = { LINK_FIELDS, fillLinks };
