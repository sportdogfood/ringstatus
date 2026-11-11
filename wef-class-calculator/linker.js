'use strict';

function recordUrl(base, table, row) {
  return row?.id ? `https://airtable.com/${base}/${table}/${row.id}` : null;
}

// Use the calculator's show-day inputs and its already matched ring and group.
function fillSnapshotLinks(snapshot, {base, tables, classes, schedules, ring, group, ringSnapshot}) {
  const matches = classes.filter(row => row.fields['class-key'] === snapshot['class-key']);
  const classRow = matches.length === 1 ? matches[0] : null;
  const issues = [];

  function fill(field, value, reason = `${field}_link_missing`) {
    const existing = snapshot[field];
    if (existing != null && existing !== '') return;
    if (value) snapshot[field] = value;
    else issues.push(reason);
  }

  const classIssue = matches.length > 1 ? 'class_link_ambiguous' : 'class_link_missing';
  const scheduleUrls = classRow
    ? schedules.map(row => recordUrl(base, tables.schedules, row)).filter(Boolean)
    : [];

  fill('class', recordUrl(base, tables.classes, classRow), classIssue);
  fill('ring', recordUrl(base, tables.rings, ring));
  fill('group', recordUrl(base, tables.groups, group));
  fill('schedule', [...new Set(scheduleUrls)].join('\n'));
  fill('ring-snapshot', recordUrl(base, tables.ringStatus, ringSnapshot));
  return issues;
}

module.exports = {fillSnapshotLinks, recordUrl};
