'use strict';

const { base, table } = require('./schema.json');
const CLASS_BASE = 'appeqxxJyPGS1iqdm';
const TABLES = Object.freeze({
  shows: 'tbllaaTn2zFB15cAn', days: 'tblVycMtB1Bb0rJ9v',
  trips: 'tblUBWZnHbhDyxjwG', classes: 'tbld3Ei7U9iBfgpGJ',
  groups: 'tbl0KOG0ErkhqecA0', rings: 'tblVa5DhhuuZtjH7x',
  schedules: 'tblYnbZti8uJcHQvm', ringStatus: 'tblFABwxZsbm4angZ',
  classStatus: 'tblxhS1xWpfHKTd6l', snapshots: table.id,
  runs: 'tblT0CVaBcvW4oybM', heartbeat: 'tblBAp64wopJCzjlZ',
  errors: 'tblGgFWqJ8ipHkPd', cleanup: 'tblvMd1YKJK3AineJ',
});

const links = value => (Array.isArray(value) ? value.map(item => typeof item === 'string' ? item : item.id) : []);
const unique = rows => rows.length === 1 ? rows[0] : null;
const url = (name, row) => row ? `https://airtable.com/${name === 'classStatus' ? CLASS_BASE : base}/${TABLES[name]}/${row.id}` : null;

function fillReferences(fields, refs, onIssue) {
  const mapping = { trip: 'trips', class: 'classes', schedule: 'schedules', 'ring-status': 'ringStatus', 'class-status': 'classStatus' };
  const linked = { ...fields };
  for (const [field, name] of Object.entries(mapping)) {
    if (linked[field]) continue;
    linked[field] = url(name, refs[name]);
    if (!linked[field]) onIssue(`missing_or_ambiguous_${field}_reference`);
  }
  return linked;
}

module.exports = { base, CLASS_BASE, TABLES, links, unique, url, fillReferences };
