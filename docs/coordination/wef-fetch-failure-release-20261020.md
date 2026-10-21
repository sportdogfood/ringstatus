# WEF Airtable read failure handling — 20 October 2026

Owner separately approved investigation/correction, testing, deployment and two
normal scheduled runs after the14:35 failure. Existing workflows unchanged;
no manual collector runs, direct SGL requests or record repairs.

## Evidence and cause boundary

14:35:25 Windows PowerShell event4100 recorded fetch failed. No14:35 saved
run-log or focus update was observed. Collector order points to the initial
Airtable log-read boundary. The old collector printed only error.message;
underlying DNS/TLS/socket cause was not retained and cannot now be established.
A read-only diagnostic Airtable GET later returnedHTTP200 in265ms, with IPv4
DNS addresses. Unchanged normal14:40,14:45,14:50,14:55 runs succeeded. This is
an intermittent request failure, not evidence of a persistent rename/schema
fault. No speculative IPv4/proxy/DNS/dependency/credential change was made.
A separate pre-deployment15:00 task interruption returned3221225786; it was not
repaired or counted as acceptance. Its existing run-log was left untouched.

Two reproducible code weaknesses were identified independently of the unknown
historical network cause:
1. Airtable list reads retried fetch headers, then swallowed JSON/body failures
   as an empty object. Failed reads could appear as empty tables. Timeout also
   ended before body parsing completed.
2. Windows PowerShell5.1 ErrorActionPreference=Stop treated native stderr as a
   terminating wrapper error. The intended capture/END line and native exit
   code were lost. Copied-wrapper console-only fixtures reproduced this,
   including false failure on stderr with a successful native exit.

## Exact change and protected baseline

Source: C:/Users/gombc/.codex/worktrees/wef-schedule-collector/ringstatus/wef-schedules.
Deployed: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-schedules.
Existing source hashes matched deployed collector-core.js/run.ps1 before edits.
Snapshots/evidence: wef-schedules/evidence/fetch-failure-20261020/.

Added GET-only airtable-read.js and changed only airtableList's request/parse
boundary in collector-core.js. Existing three attempts,20second per-attempt
limit and bounded backoff remain. Headers AND body parsing share that timeout;
transient body/socket failures retry safely. Invalid JSON/list shapes and
nonretryable HTTP failures are explicit, never an empty-table fallback. Final
errors identify GET host/path, attempts and nested error codes, excluding query
values, credentials, headers and response contents. Creation/update/delete
request paths and all existing mutation retry protections are unchanged.

run.ps1 scopes Continue to native output capture, restores Stop, preserves
stderr and exact native exits, and writes END with timestamp/exit in finally.
A null native exit keeps the failure default. No scheduler/task definition,
collection/archive/source-selection/identity/static helper/tenant code changed.
No additional package dependencies or transport alternative was introduced.

## Verification

Red fixtures demonstrated wrapper failures before correction.124 deployed tests
and127 source tests PASS. Coverage includes body disconnect/recovery, body
abort timeout, exhausted reads with nested codes, auth/invalid JSON/invalid shape,
429response release, actual Airtable-list integration, and five copied-wrapper
console-only scenarios (never business collection). Independent read-only code
review found no remaining material findings.

Final wrapper change completed after the15:05 trigger, so that invocation is
not counted as final-version acceptance. Planned final-version acceptance uses
ordinary15:10 and15:15 task starts. No manual run or forced live failure.
Underlying network reliability is external: a sustained outage still fails
explicitly; normal-run success is not proof the historical outage cannot recur.


## Final acceptance — 15:17 Eastern

PASS. Ordinary15:10:01 and15:15:01 task starts exited0. First completed
15:10:47.154, run66a332a1-f19f-4dd7-ae07-d2519aca9f1d; wrapper END15:10:49.247
exit0. Second completed15:15:46.783, run8ed835ef-f19e-4df7-a8ca-f2a37966ebfb;
wrapper END15:15:48.921 exit0. Both Warning results are the existing empty
October22 source day, not fetch errors. Four dates and435rows each verified.

The existing verify-heartbeat checks passed both runs, including four archive
hashes per run, current show/date, matching status/logs, collections and links.
The older verify-shows utility then asserted schedule values equal a14:22
snapshot. That comparison failed because normal source updates had refreshed
435datasetIDs/sourceJSON/timestamps and67estimated start times. The failed frozen
comparison was stopped and recorded in obsolete-snapshot-comparison.json.
No product or verifier code was changed after this assertion. Independent
read-only analysis of saved normal receipts matched every saved row JSON and
start/end/class/ring/count field to its respective archived dataset instead.
Both435row checks passed, matching the actual approved normal-update behavior.

Eight current attempt archives plus both existing ESP FallII archive files were
read back and SHA256 verified. Stable IDs across schedules,shows,show-collection,
rings and rings-helper; static ring/helper values and show/archive metadata
preserved. One additional run-log, no earlier log changed. No stale15:00 record
was repaired. Both normal runs retain all archive-before-delete protections.

Independent final review PASS. All35deployed files matched manifest hashes
through verification; task XMLs unchanged. Only documentation and release
manifest bookkeeping follow acceptance. Original14:35 failure and its earlier
incomplete release remain historical evidence; separately approved recovery
now completes the two-run acceptance. No manual business runs, record repairs,
new dependencies, alternate transport, credentials or task changes.

The exact historical DNS/TLS/socket cause is still unconfirmed. This release
corrects the proven read-body and logging defects, with existing bounded retry
policy, rather than claiming an external network outage cannot recur.
