# Prepared, not deployed

Target: existing wef-live-collector Windows task and the existing wef-live module. Owner authorized edit/test, not deployment. No extra task, worker, observer, schema or dependency is required.

Ready artifacts:

- candidate/live-cadence.js (new)
- candidate/lanes.js (small scheduled-entry/scope handoff change)
- task-prepared.xml (existing task, all-day quiet dispatcher)
- offline-final.json (71 offline PASS)
- review.md and review-receipt.json (author review, not independent)
- baseline/ and task-before.xml (exact pre-change runtime/task)

Deployment after approval: confirm all deployed JS still match baseline.json and installed task matches task-before.xml, no active runner or cleanup-stop is present, and no intervening owner changes are overwritten. Install new module before lanes.js; update only the existing task to task-prepared.xml. Verify installed hashes, task account/action/interval/trigger/settings and next scheduled time. Do not launch it. Inspect the next two normal due registered checks and local quiet dispatch receipts; actual hourly spacing takes an hour to prove. Active matching and overnight collection branches require naturally occurring evidence and remain pending when not encountered.

The dispatcher still wakes every3minutes, including overnight. Before due time it only reads local due state, calls no endpoint, writes no Airtable row and does no cleanup. Actual source/status checks and registered inactive runs are hourly; active matching checks collect three-minutely only05:00–19:00 Eastern. This distinction must remain visible to the owner.

Rollback is prepared, not executed or newly auto-authorized: under applicable owner authorization, first confirm candidate/task identities and runner inactivity, restore baseline/lanes.js and task-before.xml. Remove only the added live-cadence.js if its hash still matches this candidate. Preserve all runtime receipts/state, archives, logs and stops; no record repair or manual collector run. On a failed approved normal verification, report exact evidence and preserve its stop rather than patching/retrying automatically.

Known unchanged work: two cleanup-reporting P2 findings; revised new-show/new-day cleanup trigger agreement not implemented. These are not silently included in this cadence change.
