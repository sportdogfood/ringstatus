## Approved Live publication v1 — 29 October 2026 (scope v1.7)

Live independently collects TODAY observations. Entries is linking-only and is neither read nor written by Live; CWF has no role. Shows remains canonical show/day authority. Source collection, schedule links, logs, three-minute cadence and recurring verified archive-before-delete retirement are preserved.

The narrow approved exception lets Live GET existing destinations and PATCH only `live-observation-json` on Schedules rings/schedules and Data rings/groups/classes/schedules/trips. Seven multiline fields were created and read back. Live owns this overlay, exact matching and its durable audit. Schedules and Data retain their independent collection, fields, keys, records, native links and lifecycle. No sibling runtime, sync, task or record repairs are included.

Only validated observations within six minutes of their original receipt (one minute future tolerance) may publish. Empty/missing/stale detail does not clear or renew overlays. Class counters require successful class detail; ambiguous multi-group occurrences are withheld. Trips require exact dated identity and supported source provenance. Lanes run independently with bounded requests; uncertain writes retain durable intent and require readback on a later normal invocation. Source/retirement safety stops remain unchanged.

Publication audit files retain full source evidence, destination before/proposed-after, intent, outcome and readback. Existing logs also report lane results and separate publication errors. Original acquisition fields and links are never publication writes. Entries has no new enrichment assignment.

Deployment and review evidence: `evidence/publication-v1-20261029/deployed.json`, `review.md`, `tests.txt` (80 passing offline tests). Two NEW normal runs at 20:29 and 20:32 Eastern passed with empty activity, both lanes Skipped, matching logs and no errors. All baseline destination IDs/keys/native links and seven archive catalogs/hashes are preserved; only Data-owned last-success timestamps advanced. Acceptance remains PARTIAL: two naturally populated runs with PATCH/readback and repeat behavior are still required. Old v1.6 runs do not count. No manual runs or seeded proof.

Earlier locks and descriptions below remain historical except for this explicitly approved publication addition.

## LOCKED - wef-live approved scope v1.6 - 29 October 2026

Owner directs this module to remain locked to its latest approved implementation. This scope lock does not stop the three-minute collector. Preserve independent TODAY source collection and identity validation, Shows/Schedules reference reads, logging, existing links/timestamps, verified recurring archive/catalog retirement and current/future protections.

CURRENT dependency correction: Live does not read, qualify or wait for Entries. Its v1.6 reverse-read removal was separately approved, deployed19:33:37Eastern and verified by normal19:35/19:38runs (evidence/no-entries-read-20261029/release.md). Entries' separately approved linking-only restoration also removed its added Live reader and enrichment. Therefore neither current collector reads the other. Keep existing sync tables/native links/historical evidence intact; their presence does not create an active dependency. No Live preparation, enrichment reader, comparison work or new cross-module contract is pending.

Both v1.6 normal Live runs were empty activity; populated link preservation is offline-tested, not naturally proved. Preserve this limit; do not reopen work or manufacture activity to fill it. Separately approved v1.4 recurring retirement remains: verified archives/catalogs before eligible removal on successful valid empty/skipped runs; failed/partial/unresolved inputs do not authorize cleanup; current/future protected.

Reopen only for a verified defect or explicitly approved requirement. Defect investigation does not itself authorize fixes/deployment/task/schema/sync changes or record repairs. Consumer owners own their future reads; no future Entries input is implicitly assigned here. Older v1.5/Entries-read wording below is historical, superseded and not an active assignment. No runtime changes are made by this documentation lock.

## Reverse dependency removed - 29 October 2026 (v1.6)

Entries reads wef-live for downstream linking; Live independently collects observations. Live no longer reads the Entries reference, inspects its publication, or qualifies its schedule/participation results. The Airtable adapter rejects Entries reads and metadata discovery; the unused Entries base setting is removed. The existing sync/table/link fields remain intact to preserve native links and history, not as a collection dependency.

For collected observations, existing native entry links and original verification/check timestamps are omitted from writes and preserved. Previously current participation qualification is marked historical because Live no longer validates it. New observations do not create Entries links. Source identity validation, class/ring/group schedule links, class-error logging and recurring verified archive retirement are unchanged. Entries owns current downstream qualification. No manual runs, record repairs or source sync changes.

58 offline tests passed, including populated repeat collection with Entries reads forbidden, exact link/timestamp preservation, unresolved classes, logging and archive/retirement safeguards. Deployment: 19:33:37 Eastern through the existing scheduled task path. Two normal runs at 19:35 and 19:38 Eastern passed: matching Success logs, no errors, unchanged archive catalog and verified file hashes. Evidence: evidence/no-entries-read-20261029/release.md. Both were empty activity; populated behavior remains offline-tested.

## Prior v1.5 inspection - superseded by v1.6 above

**Entries reads wef-live for downstream linking. Live owns independent observation collection.** Entries is linking-only, not a collector, lane-comparison or tenant-selection engine. CWF is not a core input, dependency or acceptance requirement. Season is retention-only and never a current collection gate.

Implementation distinction: Entries collector.js now invokes live-input.js and allows GETs of Live live-entries, live-classes and collections. Live collector.js still reads the older wef-entries.entries reference and calls participation.resolve. This remaining reverse dependency is existing compatibility behavior, not the intended ownership direction. It was not removed by this documentation update; removing it requires separately approved code work preserving links. No populated downstream acceptance is claimed here.

Health checked 29 October at 19:30 Eastern: normal 19:26 and 19:29 runs completed Success, with matching heartbeat/run-logs and no error rows or logging errors. Three-minute cadence remains. All seven old-show archive catalogs are Retired; hashes and exact fields/links verified for 550 archived records. No show 200000075 observations remain active. Current activity is empty; no duplicate retirement occurred.

# Independent WEF live collector

Destination: `wef-live` (`appgzosUcVqA4vZdf`). Built 23 October 2026.
Current task is enabled and runs every three minutes. Earlier failure/recovery notes are historical evidence, not current task status.

## Existing reverse reference - prior implementation evidence — 29 October 2026

Live now consumes the accepted Entries publication through the existing read-only sync, including its five status/evidence fields. It checks exact identity, original-source freshness (30 minutes), unresolved newer attempts and local schedule availability. It no longer replays upstream schedule completeness/parity. A missing/stale mapping does not stop observations; previous links stay historical. Existing class/group schedule links and logs remain unchanged. See evidence/entries-contract-20261029/release.md for acceptance and scope.md v1.5 for the current contract.

## Current boundary — 29 October 2026

Live has no tenant-code imports or tenant-specific collection prerequisite. Entries is linking-only, reading accepted participation/schedule evidence and wef-live observations. Season is not a current collection gate. Historical release notes below are dated evidence, not current dependencies. See scope.md and HANDOFF.md for current ownership and cadence.

## Scope and authority

Read the current default record from `wef-shows.shows`: show ID, customer ID,
selected date, available dates and show start/end. Require live source identities
and dates to match. The selected date must also be the current New York day;
this is an upcoming-day guard, not an alternate show selector. A future selected
date produces an explicit no-activity success, with no live source request.
Canonical authority is base `appCaSmL819KK7UFJ`. The live collector requires one
customer-15 default with a valid show key and a checked timestamp within thirty
minutes. There is no schedules-show fallback and no discovery request.

Collect rings/groups and each available class's live trips independently of
tenant-specific feeds. Tenant slices happen downstream. Public ListAjax and
LiveScoreWidget supply the show/day graph. Public getLiveClassData supplies
trip observations; the unchanged decoded classes Worker supplies exact trip,
entry, horse, rider and trainer identities. No proxy routes were changed.

## Tables and references

`live-rings`, `live-groups`, `live-classes`, `live-entries`, `collections`,
`run-logs`, `heartbeat` and `errs-log` are destination-owned. `schema.js` lists fields and
native links; `evidence/schema-configured.json` records their Airtable IDs.

`wef-schedules.schedules` and `wef-entries.entries` are automatic read-only
native syncs. Their local record IDs are native link targets. The owner approved
one exception to keeping entries untouched: the `wef-live-reference` view,
containing only entry-key, show-id, date, entry-id, class-id, horse-id, rider-id
and status. External-workspace sync and edits from other bases are off. No
private collection/follow associations are shared. Existing entries records,
collector, field schema and task remain unchanged.

Missing reference rows do not prevent live observations. Missing or ambiguous
schedule matches are reported; entries without an available reference may have no binder
link. A failed reference read preserves existing links. No names-only joins.

## Execution and verification

Task `wef-live-collector` repeats every three minutes. Its hidden WScript launcher
runs PowerShell/Node without a console window; Windows sign-in and an awake
computer are still required. This is a separate task, not a Codex heartbeat.
Overlapping task instances are ignored; the execution limit is ten minutes.
`entry.js` is scheduled only. Never invoke it or `run.ps1` manually.

`configure.js` configures destination metadata only. `verify.js` reads normal-run
receipts, Airtable rows and archives, and writes local verification evidence.
The retirement release passed 62 offline tests and two normal scheduled runs. Heartbeat records each invocation; run-logs records outcomes and archives; errs-log records individual errors. Logging failures are isolated. A failed receipt does not permanently stop later scheduled invocations.

Verified archives precede eligible retirement, including on successful empty/skipped runs. Permanent catalogs remain in collections. Current/future records are protected. No manual runs or repairs are part of this workflow.

Schemas and runtime code for schedules and entries are protected. During
preflight, two schedules test files changed outside this work; their paths are
recorded in evidence/preflight.json. Collector runtime hashes stayed unchanged.

The local archives/credentials/operational receipts are under ignored runtime/.
Source inspection fixtures are diagnostic inputs, never scheduled-run proof.

## Normal-run evidence and required correction

Normal start: 23 October 2026, 10:32:12 Eastern; completed 10:34:30.
Run ID: 14a119c2-f1c6-4f77-a31e-6af424a84793. Task exit code 0.
Collected 6 rings, 17 groups, 30 classes and 195 trips; 68 archives verified.
Heartbeat/run-log readback passed. Four exact links to today's entry references
passed. Twenty-one classes linked to schedules; nine were left unlinked.

The source schedules are not established as broken: the new matcher counted
both active and dropped rows. Eight affected classes have one active schedule;
class 200030243 has two active schedules, one for each of its two live groups.
The existing stage keys identify these group contexts. Evidence is in
schedule-link-diagnosis.json and the normal-run JSON under evidence/.

Approved correction deployed on 23 October 2026: match only active schedule
rows using the existing show/date/class and group-specific stage keys; allow
multiple valid schedule links for multi-group classes; give each live group only
its own schedules. Add offline cases for dropped rows and multi-group classes,
then verify two normal runs. No source schedule changes or record repairs.

The task was re-enabled after all 35 entries/live offline tests passed, including
dropped schedules, multiple group-specific schedules, repeat identity stability,
canonical source reads, stale authority rejection and protected writes. Normal
acceptance is recorded under `../wef-shows/evidence/consumer-connection/`.


Final readback for this change: both consumers used canonical show record
`rec4En9UWd6lkT1Oh` directly during their first normal post-deployment run.
Live's 30 classes have 31 correct active schedule links across 17 groups;
all 248 pre-existing live identities and seven entries identities were retained.
This is partial verification, not two successful runs. Live failed on six HTTP
502 class responses and missing decoded identity for trip 200398019 in class
200030319. Entries failed on trainer 50121 HTTP 502. Both wrote Failed to
heartbeat/run-logs. Live remains disabled with its original five-minute trigger;
entries and the other existing task configurations remain unchanged. Source
investigation/correction and renewed normal-run acceptance remain outstanding.


## Source handling update — 23 October 2026, 13:02 Eastern

Approved source correction adds one bounded transient HTTP/timeout retry and
one paired snapshot refresh for a missing trip identity, with timestamped
public detail requests. It never substitutes guessed or historical identities.
Forty offline/deployed tests pass. Worker and schedules are unchanged.
The first normal live run still encountered a persistent 502 for class
200030347; further live starts are disabled. Full live acceptance remains blocked.
Evidence: `evidence/source-fix/`. No additional production changes were made
after this failed gate.


Final acceptance at 13:11 Eastern: entries completed normal runs at 13:04 and
13:09, with seven and nine entries respectively. Both had no source errors and
passed record/link/archive checks. The first full verification passed. The
second full verification was blocked by concurrent changes to
wef-tenants/airtable.js, collector.js and settings.js at 13:07, outside this
task's deployment file list. Those changes were left untouched; expected hashes
were not changed to hide the drift.
Live's 13:02 normal run ended Failed with persistent class 200030347 HTTP 502;
no missing-trip identity error remained and 68 archives verified. Live is disabled.
No production patch or manual retry followed that failed gate. Overall work is
incomplete: persistent 502, concurrent protected drift, and two live runs remain.
Full evidence: wef-live/evidence/source-fix/release-status.json.
