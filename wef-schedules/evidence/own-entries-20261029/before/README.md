# Independent WEF schedules staging

## Collection attempts and separate err-logs - 29 October 2026

The existing task still calls run.ps1 every five minutes. The wrapper now assigns
an attempt ID and writes start/end receipts before private-environment loading
or Node startup. attempt-entry.js creates the matching Airtable run-log before
collector imports or the business lock, then invokes the unchanged collection.
Normal completion, warnings and row/archive counts stay on that same run record.

The err-logs table (tblntJGnYFpLxjFSr) records deterministic error IDs, attempt/run ID, stage,
recording/start times, optional show/date, redacted message and a direct run-log
link. Informational empty-day warnings are not errors. Error rows are bounded to
1000, with verified archive-before-slot-reuse and readable archive receipts.

If credentials, Node or Airtable are unavailable, the wrapper's local metadata
receipts remain under runtime/attempt-receipts/<attempt-id>.jsonl. A later normal
invocation publishes only these newly marked attempt/error receipts; it never
reruns collection or repairs old historical log rows. Interrupted receipts retain
unknown outcome until actual completion evidence exists. Recovery acknowledges
exact remotely verified evidence and handles at most ten pending attempts per
invocation. Older fully finished, acknowledged, inactive receipts are compressed,
read back and archive-logged before removal. A missing ACK is not success.

Logging mutations have a separate short-lived serialization lock. Invalid/dead
logging locks fail visibly and require inspection; they are not automatically
cleared. The existing business lock/collection/archive logic is unchanged.
A suppressed Windows trigger or a sleeping/unavailable PC is not a launched
collection attempt. If even local storage is unwritable, no software log write
can be guaranteed; task/process failure remains a separate signal.

Operational acceptance: the normal 11:20 and 11:25 Eastern starts on 29 October
completed Success at 11:20:57.900 and 11:25:49.008 respectively. Both wrappers
returned exit0; attempt IDs/start times match Airtable, latest heartbeat matches,
and the second normal process acknowledged the first receipt without collection
replay. All504 existing schedules,56 rings,3 local shows and14 helpers/links were
preserved.165 offline tests and independent review passed. Err-logs remained
empty because both observed normal runs were healthy; failure paths are fixture
proof only. Eleven logged archive files matched their decompressed SHA256.
Task XML/five-minute cadence and protected collection/store/guard/settings files
are unchanged. No manual business runs or historical record repairs.

See evidence/attempt-errors-20261029/ for source baseline, tests, review, schema,
deployment and two normal scheduled-run receipts. Offline failure tests do not
claim production failures were induced. No manual runs or record repairs.

## Schooling sessions and breaks - 29 October 2026

Explicit source schedule_break groups with empty class lists now appear in
schedules alongside classes. Their keys are block:<source group ID>, scoped by
the existing show/date stage-key. Class IDs/numbers remain blank; type is
schooling or break, and group retains the source label. Existing class keys,
record identities and downstream links are preserved. Source counts and times
are retained; no attendance or times are inferred from labels or top-level counts.
Class-only browser time enrichment does not run for block-only days. Manual
time overrides, archive-before-delete and destructive-empty guards remain.

Normal run-logs and show-collection distinguish class and schooling/break counts.
The empty_show_day message applies only when no classes, schooling sessions or
breaks were returned for the specific show/date. A schooling-only day is not empty.

139 offline tests passed, including repeat collection and preserved entry links.
Saved-source replay preserved all 471 existing class rows and keys exactly and
identified 26 blocks on October 29, five on October 30 and two on October 31.
Normal 08:30 and 08:35 scheduled runs passed on 29 October, completing at
08:31:07 and 08:36:04 Eastern with matching heartbeat/run-logs and wrapper exit 0.
All 471 existing class record IDs and show/ring links were retained; 33 source
blocks were added across the show, including 24 schooling sessions and two breaks
on October 29. All 504 record IDs remained stable on repeat. Today reports zero
source entries/trips, not measured open-schooling attendance. No empty-day warning,
manual collector runs, record repairs, schema or task/cadence changes.
Acceptance: evidence/schedule-blocks-20261029/.

## Maintenance source aligned - 28 October 2026

The owner-approved maintenance copy alignment is complete. The designated editable
package at C:/Users/gombc/.codex/worktrees/wef-schedule-collector/ringstatus/wef-schedules
now matches the verified deployed package across 58 inspected code/config/test/doc
files, including canonical show-source consumption and completion-guard protection.
134 local maintenance fixtures passed. The deployed schedules collector was not
replaced or redeployed. Private runtime, credentials, dependencies and evidence
were not copied or deleted. Exact prior files and alignment hashes are retained in
wef-tenants/evidence/failure-reporting-20261028/maintenance-before/ and
maintenance-alignment.json. Existing business tasks/cadences remain unchanged.
The PC's AC-only automatic/lid sleep prevention was configured and read back in
that same approved release; DC settings remain unchanged. This does not resolve
future host migration, new retention design or natural all-consumer rollover gates.

## Current operating agreement - 24 October 2026

This section supersedes dated status claims below. It locks the agreed ownership
and maintenance plan; it does not freeze live source data or authorize code changes.

**Completion reporting correction deployed and verified - 24 October 2026, 12:06 Eastern.**
The collector now defaults to exit 1 until its existing final run-log/heartbeat
path returns a terminal report. Exiting while still pending writes an explicit
incomplete_run error to the local runner log and returns failure to Task Scheduler.
The guard does not retry collection or change historical Airtable records. If a
process cannot finish reporting, its prior Running row may remain; heartbeat
continues to represent the last verified completion. Use task result/local errors
alongside Airtable freshness instead of treating exit 0 alone as completion proof.

All 134 offline tests passed, including unresolved finalization, explicit premature
zero exit, rejected completion and real entry-point fixtures with inert adapters.
Normal 12:00 and 12:05 triggers completed at 12:01:48 and 12:05:57, exit 0, each
with the new completion marker and matching finished Airtable log/heartbeat.
Both collected 214 active rows, all four dates, zero failed dates or errors; only
the owner-confirmed empty October 22 Warning. Fault behavior is fixture-proven;
no production failure was induced. Collection/adapters/archive logic, launcher
and task XML are unchanged; the observed existing cadence remains five minutes
(PT5M), despite the approval wording referring to three minutes.

Evidence: runtime/incomplete-run-release-20261024/ (tests.txt, deployment.json,
normal-run-1200.json, normal-run-1205.json and identical task-before/task-after XML).
No manual collector runs or record repairs. The exact network/runtime cause of
the historical 11:20 interruption remains unproven; this release fixes its false
successful-exit behavior, not an asserted network root cause.

### Confirmed empty date - do not reopen as an unexplained error

The owner confirmed on 24 October 2026 that **22 October 2026 has no scheduled
classes for show 200000075, 2026 ESP Fall III (22-25 October)**. A validated source
response with zero rows for that exact show/date is an accepted successful empty
collection. The existing empty_show_day Warning is informational in this case,
not a failed date. This confirmation does not excuse failed, invalid, missing or
stale source responses, or apply automatically to another show/date.

Normal run-logs already record the dated zero-row result and empty_show_day
message. This README records the owner's confirmation so it need not be requested
again. Logging behavior and historical records have not been changed.

### Evidence recorded 24 October 2026, Eastern

| Observation | Evidence |
| --- | --- |
| Confirmed Airtable completion | 11:15:05.507-11:16:00.217; run 96cfcb93-367e-4d2b-9e52-aaed637298d2; heartbeat and run-log agree; 214 active rows, four complete dates, zero failed dates, no errors, five archive entries. Only warning: confirmed empty October 22. |
| Newest task/checkpoint | 11:20:01 scheduled trigger; wrapper 11:20:02-11:21:40, exit 0; runtime/focus.json reports valid 80/95/0/39 rows for October 24/25/22/23. |
| Historical completion discrepancy (not repaired) | Run 26a290ac-3eb4-4863-acce-e451cc555619, Airtable record recgnCyNE4EjQjpOG, remains Running with archive details through 11:20:57; heartbeat remains on preceding run. Saved checkpoint alone does not prove final log/heartbeat publication. |

Airtable: [heartbeat](https://airtable.com/appUq5Qz9kARqFija/tblj077efYbhlznOJ)
and [run-logs](https://airtable.com/appUq5Qz9kARqFija/tbluqwuXxOvgDRUBq).
Local evidence: runtime/focus.json and runtime/collector-2026-10-24.log.
These are dated observations, not a promise that future runs have completed.

## Responsibilities and locked plan

| Owner | Responsibility |
| --- | --- |
| wef-shows | Canonical default show, date range and source-selected context. Schedules reads this authority; it does not rediscover or trigger shows. |
| This wef-schedules chat | Dated schedules, groups/classes, daily rings, stable source keys and links; archive protection; collection status, heartbeat and run-logs in appUq5Qz9kARqFija. |
| Scheduled runner | Executes the existing five-minute collector. Codex inspects evidence and makes separately approved repeatable changes; it does not substitute manual execution or record repairs. |
| wef-entries | Tenant-selected participation and links to schedules; CWF consumption/helper migration belongs to its owner. |
| wef-live | Independent live observations and trip identities; reads schedule/entry references without triggering their collectors. |
| wef-records-coordinator | Records contracts, acknowledgments and cross-module gaps. |

Preserve schedules.stage-key unchanged; ring is ring number, class-number is a
display number, and status active/dropped is retention state. Source-selected day
is not automatically TODAY. Consumers resolve native links to destination-local
synced records. Entries' full sync parity includes retained dropped rows, while
active participation matching excludes them. Freshness comes from completed
collection/heartbeat evidence, not unchanged row changed-at.

Remaining plan, without expanding implementation scope:

- COMPLETE: fail incomplete invocations visibly, verified through two normal runs.
  Preserve the historical 11:20 evidence; do not repair its record. A future actual
  failure requires diagnosis before separately authorized edits or retries.
- Keep the five-minute cadence, source keys, existing links and archive-before-delete
  protection. No redesign or repeat deployment of completed work.
- Next planned routine code review: Monday 26 October, 9:00 a.m. Eastern.
- Desktop isolation remains unresolved: the task still uses Interactive logon.
  Hidden launch arguments do not prove execution outside the desktop session.
- Coordinate multiple valid schedule associations and consumer freshness rules
  with entries/live owners; no guessed joins or changes to their collectors.

Detailed accepted ownership and remaining downstream gaps:
[owner acknowledgments](../docs/coordination/wef-records-owner-acknowledgments-20261023.md).

## Historical deployment reference

The following 15 October deployment figures are historical, not current counts.
The then-observed current schedule count was 473; the table subsequently became
show-collection. Later sections record the rename and subsequent releases.

## Airtable
Base appUq5Qz9kARqFija. schedules holds one current row per show/date/existing source schedule key.At the 15 October checkpoint, 473 rows were present; collections held one latest status per show/day (4 then), updated in place. Historical snapshots are no longer multiplied in Airtable.
Readable fields: show-id,date,ring,group-id,group,sequence,class-id,class-number,class-name,type,sequence-type,start-time,end-time,total-trips,completed-trips,status,changed-at. Missing source values remain blank. status is active or dropped. The existing stage-key, dataset-id and row-json provide stable addressing, complete source evidence and archive integrity; they are not extra user input requirements. Source-owned columns refresh from SGL; unrelated columns are not modified. Existing manual source overrides remain private and show-scoped.
Check collections status before treating a day as successfully refreshed. Failed retrieval is distinct from a successful empty date; empty in-range days warn. Do not invent days between shows. Default-show logs record previous/current IDs/ranges and phase; all days of the discovered default show are collected even before its start.

## Separate history
runtime/history/initial-airtable.json.gz preserves the exact946 prior schedule records and24 prior collection records. datasets/ holds full versioned snapshots; attempts/ holds run history; removed/ and retired/ preserve records removed from Airtable; current.json records last complete day state. Archives are written, decompressed and verified before cleanup. History is private local storage within this deployment, not another Airtable table or cloud service. Preserve this directory for recovery. runtime/before-current-staging contains pre-release code copies; reverting code alone is not a data rollback.
A successful whole-show collection retires older-show Airtable rows after archive verification. Failed/incomplete source responses do not drop established rows.4900-row capacity guard stops a write rather than silently truncating. A single default show that exceeds available capacity needs an owner decision; no extra base is created automatically.

## Isolation and execution
Only this package is deployed. Never promote the superseded repository-root prototypes or replace shared helpers. settings.js fixes the base/customer and accepts only WEF_SCHEDULES_* transport/credential settings. All state, cache, manual inputs, logs and history are under runtime/. Never print or commit collector.env. run.ps1 is invoked only by wef-schedules-collector on its five-minute schedule; do not invoke business commands manually.
Token rotation is excluded from semantic comparison; full saved fingerprints still verify integrity. Latest source payload is retained in the private cache. Real changes retain archive history while updating the same Airtable record. Separate tests cover adds/drops/reactivation, failed fetches, interrupted storage, default-show gaps and archive preservation. Live proof covers the two observed normal runs, not unobserved future show transitions.

## Evidence and resume
runtime/current-staging-verification.json is the historical 15 October acceptance evidence; runtime/deployment-verification.json is the deployment checkpoint. See docs/coordination/wef-schedules-only-activation-plan-20261015.md in the main repository for approval/failure history. Source-manifest.json contains exact private-package and protected-file hashes. The separate maintenance chat remains stopped; parent completed this release before handoff.


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
