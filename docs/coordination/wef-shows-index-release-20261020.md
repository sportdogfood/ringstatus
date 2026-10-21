# WEF shows index and table names — 20 October 2026

Owner approved in-place show-details -> shows and collections -> show-collection,
archive details/links, both tenant tables retained, meta notes preserved before
retirement, test/deploy/two normal runs. No manual runs or record repairs.

## Baseline and scope

Source: C:/Users/gombc/.codex/worktrees/wef-schedule-collector/ringstatus/wef-schedules.
Deployed: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-schedules.
Pre-edit source hashes matched deployed hashes for every existing runtime file
changed here. Evidence: wef-schedules/evidence/show-index-20261020/. Existing
uncommitted releases are the proven baseline and were preserved.

Shared shows table remains tblBq3gXC0TYBCmMZ; show-collection remains
tblvGpwxGjXXNLKAU. Existing records and linked fields retain their IDs/names;
only table labels change. Both schedules and tenant shared-show reads use stable
table IDs. The tenant's own show-details/collections tables are unchanged.
Source selection, schedule keys, archive-before-delete, current-row behavior,
standing-roster/static helpers and all task cadences remain unchanged.

The existing shared shows table gains show-status, archive-status, archive-files,
archived-schedule-count, archived-collection-count, archive-verified-at and
archive-notes. New shows links on show-collection/run-logs/heartbeat provide
reciprocal navigation. Existing schedules/rings/tenant-index links remain intact.
The normal collector verifies retired gzip files against logged SHA256 hashes
before any index writes. Historical metadata comes from existing show records
or the explicit archived-show description in run-logs; unknown dates/times are
not invented. Counts identify unique archived records. File paths are local
runner locations, not hosted downloads. No archived schedules are restored.

Run-log completion refreshes catalog relations before heartbeat publication.
Uncertain creates are reconciled once by stable key; POST is not repeated.
Catalog failures are recorded and fail the invocation. Unknown failed scope
clears prior show attribution before terminal heartbeat updates. Existing
archive-files remain an index source even after old run-logs rotate out.

ESP Fall II preflight independently verified show200000074, October15–18,2026,
499 schedules and four collection records in two existing files. Population of
its shows entry and links is performed by normal scheduled collection only.

## Meta retirement

Original meta table tblwBvigcmqRlSYUb held one historical start-here note.
Exact schema/record backup: evidence/show-index-20261020/meta-preserved.json.
Readable preservation: docs/coordination/wef-schedules-meta-notes-preserved-20261020.md.
Original notes were read back verbatim before approved delete_table action
actHJNCp7RFuEAX8e. No operational collector read or wrote this table.
Both tenants and wef-tenant-index remain, including home pin/example walks.

## Verification

113 deployed schedules fixtures,116 source fixtures and42tenant fixtures PASS.
Independent read-only review, including retention, found no material findings.
The renamed-table compatibility run at14:25 completed exit0. First full catalog run14:30:01 exited0, completed14:30:46 Eastern, UUID
cc63fbd3-dd95-479f-8ec5-810c98b31445. Passive verification PASS: same table IDs,
435 unchanged schedule records,24rings,sixhelpers,four collection record IDs;
new show relations verified. ESP FallII archived record recNXbBZ94kin9R3d exposes
October15–18,499schedules and4collections in two SHA256-verified existing files.
No archived data restored. The Warning remains the existing empty-October22
source day; no failed days or errors.

Tenant normal14:27:01 exited0, completed14:27:44, UUID
7ae7cce6-4c65-4ef7-85c6-2db17840c3cc;71horses33riders,6archives, static IDs and
values preserved through the shared table rename. Second tenant normal14:32:01 exited0, completed14:32:43, UUID
0b6ad1b6-0a63-46df-92e7-ac8bbbd1dafc. Comparison PASS:12stable tenant tables,
one new run-log,12verified archive events across tenant runs.

## Required second schedules run failed — verification stopped

Normal14:35:01 schedules start returned task exit1. Windows PowerShell event4100
at14:35:25 reports NativeCommandError: fetch failed. The wrapper's existing
ErrorActionPreference=Stop prevented its END line. No14:35 Airtable run-log was
found in a single successful diagnostic read of the latest three logs; latest is
still14:30. focus.json likewise remains14:30. No collector lock remained.
Exact failed request/underlying network cause is not established by this message.

Result: DEPLOYED, ACCEPTANCE INCOMPLETE (1 of2 full schedules runs verified).
Do not call this release fully accepted. The failed execution path was stopped;
no further normal run is counted as a retry or substitute. Existing tasks remain
enabled/unchanged; no manual run, endpoint workaround, record repair, rollback or
runtime patch occurred. Final runtime hashes match deployment.

An earlier optional whole-record shared tenants GET also returned fetch failed;
recorded in optional-tenants-readback.json without another request. Both tenant
normal receipts and their full repeat comparison passed; schema confirms both
shared tenant tables remain. No collector write targets the tenants table.

Failure evidence: failed-normal-run-event.json (Windows event/task result),
failed-normal-run-1435.json (diagnostic latest logs, no14:35 row),
final-code-check.json, tenant-repeat-comparison.json and first full scheduled
snapshot. Independent code/first-run review PASS; second full acceptance FAIL.

Next owner decision: authorize the next investigation/correction/normal-run
verification stage. Under AGENTS.md a failed cadence path cannot be manually
substituted, repaired or patched without separate APPROVED TO EDIT authority.
No manual runs or record repairs were performed.


## Later separately approved recovery acceptance — 20 October, 15:17 Eastern

Owner separately approved investigating/fixing fetch failed and two fresh normal
runs. Read-body recovery and wrapper error capture corrected without altering
source selection, archive protection, schema or mutation behavior. Normal15:10
and15:15 runs both exited0;435rows per run matched their archived datasets, eight
current archive events plus ESP FallII's two existing files verified, all record
IDs and static helper/archive values preserved. First failed14:35 acceptance
remains historical evidence. Current two-run acceptance is PASS under the new
approval. See docs/coordination/wef-fetch-failure-release-20261020.md and
wef-schedules/evidence/fetch-failure-20261020/. No manual runs or record repairs.
