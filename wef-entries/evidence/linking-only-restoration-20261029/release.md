# Entries linking-only restoration - 29 October 2026

Owner approval: restore linking-only scope; preserve all records/links and compact-input, bounded-readback and archive-log fixes. Test/review/deploy via existing route and verify two normal scheduled runs. No manual runs/tasks/records repaired.

Status: ACCEPTED. Deployed19:37:05Eastern; two normal scheduled runs and complete preservation readbacks PASS at19:40 and19:45.
Candidate: staged/; exact before source and obsolete feature tests archived in before/. Five runtime files edited, inactive Live helper retired from runtime. No matcher/freshness/runner changes; no schema/task changes.

Offline: 41 current-contract Node tests PASS; compact projection Python test PASS (400 classes, one retained inventory, <8MB; expanded old representation >8MB). Three intentional red scope checks first reproduced the scope regressions. Unchanged source retry tests run in original repository location. Independent native reviewer /root/review_entries_restoration cleared runtime restoration; verifier findings are corrected before use. Full historical test glob is not a passing gate: obsolete CWF/feed collector tests fail also on untouched baseline. Kept evidence and did not reintroduce that behavior.

Deploy: unchanged registered wef-entries-cwf-collector -> run-hidden.vbs -> run.ps1 -> entry.js. Existing lock/idle window, matching before/candidate hashes, stable GET-only preservation snapshot, exact task XML unchanged. No scheduled collector invoked by Codex.

Acceptance: two distinct post-deployment normal receipts and task results; all original entry/collection IDs and native/helper links preserved, outside-shared records untouched, source timestamps valid/non-regressing, old lane/live JSON retained without generation or reads, no new outside-scope records, heartbeat/run-log agreement, deployed hashes and task XML unchanged. Partial for actual unresolved relationships is not a processing failure. No manual repairs/deletion to force proof.

Rollback not automatic: preserve files/data/evidence and stop affected verification path if a scheduled run fails; fresh applicable approval required before further code correction/resumption.

Independent runtime and proof-script review complete: all five verification-script findings resolved. Exact deployed hashes/task XML in deployment.json. Stable predeployment snapshot contains 513 existing participation records.

First normal scheduled receipt PASS: db51c5bc-4270-4194-993f-74b54dc6a8b5;23:39:02-23:40:22Z /19:39-19:40Eastern; task0, businessPartial with no errors. 471processed,86outsideclassesexcluded;513originalrecords and446collectionIDs preserved;988entry native/helper fields,175collectionnativefields,496historicalJSONfields checked;28outside-scope records byte-value-equivalent; no source-time regression, duplicates, newoutside rows, Liveexecution or task/hash changes. Readbacks: run-1.json/run-1-summary.json/run-1-task.json. Second normal run subsequently passed; see acceptance below.

## Normal scheduled acceptance - PASS

| Run | Eastern | Receipt | Outcome |
| --- | --- | --- | --- |
| First |19:39:02-19:40:22|db51c5bc-4270-4194-993f-74b54dc6a8b5|Task0, businessPartial, no errors|
| Second |19:44:03-19:45:48|c09f9521-a35f-4cb9-9172-dce34de9a48c|Task0, businessPartial, no errors|

Each processed471participation rows and excluded86outside class scopes. All513baseline Entries records and446collection records retained exact IDs/keys.988entry native/helper fields and175collection native fields were preserved.496historical lane/Live JSON fields retained;28outside-scope records unchanged. No duplicate keys, timestamp regression, newoutside records, Live execution, deployedhash drift or taskXML changes. Heartbeat/run-log match both receipts; run2 is distinct and ordered after run1. Partial means actual unresolved/Conflict relationships, not processing failure; no links repaired.

Evidence: acceptance.json, run-1/2.json, run-1/2-summary.json, run-1/2-task.json, deployment.json, task-before.xml, candidate/baseline hashes, current-tests.txt, saved-replay-check.json and review.md. No manual collector/source runs, record repairs/deletions, schema/sync/task/cadence changes. The restoration is accepted through the required two normal runs; prior unrelated releases remain historical evidence.
