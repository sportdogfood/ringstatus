'use strict';

function isSourceId(value) {
  return (typeof value === 'string' && value.trim().length > 0) ||
    (Number.isSafeInteger(value) && value > 0);
}

/**
 * Fill native Airtable links in the fields that will be saved once by Messages.
 * Each lookup supplies the verified destination field schema, already-read
 * Airtable REST rows, and exact source field values in `source`. Use a scoped
 * key or include show, date and class where an ID can repeat. The caller logs
 * the returned issue codes along with its event context.
 */
function fillLinks(fields, lookups, issues) {
  for (const {field, rows, source, expected = true} of lookups) {
    if (field?.type !== 'multipleRecordLinks') continue;
    const name = field.name;
    const existing = fields[name];
    if (existing != null && (!Array.isArray(existing) || existing.length > 0)) continue;

    const keys = Object.entries(source ?? {});
    const ids = new Set();
    if (keys.length && keys.every(([, value]) => isSourceId(value))) {
      for (const row of rows ?? []) {
        if (/^rec[A-Za-z0-9]{14}$/.test(row.id) && keys.every(([key, value]) => row.fields?.[key] === value)) {
          ids.add(row.id);
        }
      }
    }

    if (ids.size === 1) fields[name] = [...ids];
    else if (ids.size > 1) issues.push(`${name}_link_ambiguous`);
    else if (expected) issues.push(`${name}_link_missing`);
  }
  return fields;
}

module.exports = {fillLinks};
