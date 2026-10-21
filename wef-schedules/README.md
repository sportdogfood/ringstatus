# Independent WEF schedules staging
Current verified deployment:15 October2026.42 local checks passed. Normal13:40 and13:45 Eastern runs completed successfully. Own five-minute Windows task remains enabled (Interactive account, IgnoreNew). No manual business runs. Existing shared workflows remain untouched.

## Airtable
Base appUq5Qz9kARqFija. schedules holds one current row per show/date/existing source schedule key.473 rows currently; collections holds one latest status per show/day (4 currently), updated in place. Historical snapshots are no longer multiplied in Airtable.
Readable fields: show-id,date,ring,group-id,group,sequence,class-id,class-number,class-name,type,sequence-type,start-time,end-time,total-trips,completed-trips,status,changed-at. Missing source values remain blank. status is active or dropped. The existing stage-key, dataset-id and row-json provide stable addressing, complete source evidence and archive integrity; they are not extra user input requirements. Source-owned columns refresh from SGL; unrelated columns are not modified. Existing manual source overrides remain private and show-scoped.
Check collections status before treating a day as successfully refreshed. Failed retrieval is distinct from a successful empty date; empty in-range days warn. Do not invent days between shows. Default-show logs record previous/current IDs/ranges and phase; all days of the discovered default show are collected even before its start.

## Separate history
runtime/history/initial-airtable.json.gz preserves the exact946 prior schedule records and24 prior collection records. datasets/ holds full versioned snapshots; attempts/ holds run history; removed/ and retired/ preserve records removed from Airtable; current.json records last complete day state. Archives are written, decompressed and verified before cleanup. History is private local storage within this deployment, not another Airtable table or cloud service. Preserve this directory for recovery. runtime/before-current-staging contains pre-release code copies; reverting code alone is not a data rollback.
A successful whole-show collection retires older-show Airtable rows after archive verification. Failed/incomplete source responses do not drop established rows.4900-row capacity guard stops a write rather than silently truncating. A single default show that exceeds available capacity needs an owner decision; no extra base is created automatically.

## Isolation and execution
Only this package is deployed. Never promote the superseded repository-root prototypes or replace shared helpers. settings.js fixes the base/customer and accepts only WEF_SCHEDULES_* transport/credential settings. All state, cache, manual inputs, logs and history are under runtime/. Never print or commit collector.env. run.ps1 is invoked only by wef-schedules-collector on its five-minute schedule; do not invoke business commands manually.
Token rotation is excluded from semantic comparison; full saved fingerprints still verify integrity. Latest source payload is retained in the private cache. Real changes retain archive history while updating the same Airtable record. Separate tests cover adds/drops/reactivation, failed fetches, interrupted storage, default-show gaps and archive preservation. Live proof covers the two observed normal runs, not unobserved future show transitions.

## Evidence and resume
runtime/current-staging-verification.json is the latest acceptance evidence; runtime/deployment-verification.json is the deployment checkpoint. See docs/coordination/wef-schedules-only-activation-plan-20261015.md in the main repository for approval/failure history. Source-manifest.json contains exact private-package and protected-file hashes. The separate maintenance chat remains stopped; parent completed this release before handoff.


## Heartbeat

The heartbeat table holds one latest completed-run status for schedules.
checked-at shows the last confirmed completion; run-id locates its detailed
run-logs record. It copies result, source-selected date, counts, summary and
errors after the run-log is saved and read back. Warning stays Warning; failed
runs show Failed when logging/status storage remains available. A stale timestamp
can mean the collector did not start or could not publish status. The table does
not trigger runs. Detailed history and archive paths/hashes remain in run-logs.


## Shows index — 20 October 2026

Shared show-details is now shows; collections is now show-collection. Table IDs
and existing linked fields are preserved. The normal collector indexes verified
retired archive locations/counts in shows, links run-logs/show-collection/heartbeat,
and retains previous show metadata without restoring archived schedules.
Both tenants and wef-tenant-index remain. Original meta notes are preserved in
docs/coordination/wef-schedules-meta-notes-preserved-20261020.md; meta is retired.

Initial acceptance stopped after the14:35 fetch failure. Under separate owner
approval, read failure handling and wrapper logging were corrected and normal
15:10/15:15 runs passed full archived-source checks. Historical failure evidence
is preserved; see docs/coordination/wef-fetch-failure-release-20261020.md and
evidence/fetch-failure-20261020/. No manual runs or record repairs.


## Airtable reads and runner logs

GET reads validate records and cover both headers/body within the existing
three-attempt timeout/backoff policy. Failed/invalid reads cannot masquerade as
empty tables. Final errors identify request scope and nested cause codes without
headers, query values or response contents. Native stderr, native exit and END
timestamps are preserved in the daily runner log. Write retry behavior is unchanged.
