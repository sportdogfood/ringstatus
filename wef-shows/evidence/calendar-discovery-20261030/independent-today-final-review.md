# Independent frozen TODAY-consumer review — 2026-10-30

**Code review: PASS for the exact 16-file frozen manifest. No unresolved
actionable implementation finding identified.** Deployment and scheduled
acceptance remain separate, unverified gates for this review.

The owner explicitly chose to keep Live and Timing on TODAY. This supersedes the
earlier policy decision blocker in `independent-review.md`; that earlier review
and its 14-file hashes remain historical evidence.

## Corrected findings

- The source-time validation bypass found during the initial calendar review
  remains corrected in JS/Python. Original source time stays distinct from the
  authority heartbeat; missing, malformed or future source time is rejected.
- The first TODAY candidate expanded Live retirement eligibility by changing the
  date used by the existing cleanup guard. Actual offline `retireOld` calls
  reproduced newly eligible same-day different-show rows, then newly eligible
  prior-day same-show rows when canonical selection lagged TODAY. Reproductions
  remain in `independent-today-retirement-reproduction.log` and
  `independent-retirement-past-selection.log`.
- The frozen correction keeps `scope.selected_date` canonical, uses TODAY only
  for active observation, and derives BOTH retirement cutoff and future-selected
  protection from canonical selection. Inactive upcoming/ended shows retain their
  prior scoped date. No cleanup expansion is included.

## Verified scope and checks

- All 16 prepared-release and isolated test-tree files exactly match the frozen
  deployment manifest: `independent-today-final-hashes.json`.
- Independently ran the entire isolated Shows suite: **47/47 PASS, exit 0**.
  Exact command/runtime/timestamps/duration are in
  `independent-today-final-tests.json`; complete output is in
  `independent-today-final-tests.tap`.
- Independently compared actual baseline/candidate Live retirement functions for
  30 combinations of selected date before/on/after TODAY, record dates and
  same/different show IDs. Eligibility is identical in all cases:
  `independent-retirement-equivalence.json`. Mock archive callbacks stop before
  any writes/deletes; these are offline checks, not business runs.
- The actual Timing runner fixture runs twice using TODAY inventories, sync
  filters, Ring groups and Class adaptation while retaining tomorrow in the
  canonical Data context/fence. Future-selected purge protection remains; no
  purge occurs. Native identifiers, source clocks and unrelated evidence-age
  rules remain unchanged.
- `discovery-policy.js` contains the already-reviewed Sunday/daily policy under
  a new filename; existing deployed `discovery-window.js` remains untouched for
  compatibility during rollout. No remaining reader references the obsolete
  candidate policy filename.

## Deployment sequencing and remaining proof

Revalidate hashes and natural idle execution boundaries. Install new helper
`discovery-policy.js` and the extended `authority-age.js` first; install Live
`retirement.js` before its model; deploy other companion readers before Shows
collector last. The retirement fallback preserves the old model during this
transition. Deploy only manifest files, never the full staging tree. Preserve
unrelated concurrent changes, existing locks and the count-publication stop.

Normal scheduled discovery/calendar/TODAY-consumer receipts and Airtable readback
must still establish runtime acceptance. Sunday checks, retries and real rollover
are not proved by Friday fixtures. Existing count replay and unrelated consumer
failures remain separate; this review does not authorize their retry or repair.

Reviewer used the Code Review skill and applicable Shows/common instructions.
Only reviewer evidence files were written; no production code, records, tasks,
source calls, collectors or failure stops were changed.
