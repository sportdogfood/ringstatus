"use strict";
// Entries owns downstream linking. Never read or requalify its publication here.
// Omit native link and verification timestamps from writes so Airtable preserves them.
function preserve(previous = {}) {
  if (previous['participation-link-status'] !== 'current') return {};
  return {
    'participation-link-status': 'historical',
    'participation-link-reason': 'Existing link retained; current participation qualification belongs to Entries.',
  };
}
module.exports = { preserve };
