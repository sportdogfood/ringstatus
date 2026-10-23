# Isolated decoder redirect correction — 21 October 2026

Owner explicitly approved fixing the isolated Worker's redirect setting, testing
before switching collectors and normal scheduled verification. Legacy routes
must remain untouched; no manual collector runs or record repairs.

## Scope and source

Worker source: workers/wef-schedules-decoded/worker.js.
Changed only redirect:error to redirect:manual and explicit sanitized502 for3xx.
This avoids unsupported Workers behavior and never forwards source credentials to
a redirect destination. The new regression test failed before the fix and all
seven Worker tests passed after it. Independent read-only review found no
actionable issues.

Deployment2026-10-21T23:58:41.299934Z:
fb3269d8-e190-4b9b-b8bf-4564c0a6dcea,
version817d56e7-59ba-4deb-8c50-63d196a2808d at100 percent.
Source readback matches local candidate; secret bindings unchanged.

Before switching: read-only diagnostic GETs to the isolated ring, Oct25 schedule
and people50121 endpoints all returned200 with X-RS-Decoded:1. The schedule
contained139 class IDs for139 rows with the same keys as the cached baseline.
The people response contained four trips with entry/class/horse/rider IDs.
Evidence: workers/wef-schedules-decoded/evidence/pre-switch-live-tests-fixed.json.
These endpoint tests are diagnostics, not scheduled-workflow proof.

At23:59:19.531Z, with both tasks Ready, switched only the independent schedules
default source base in wef-schedules/settings.js and the tenant people source in
wef-tenants/source.js. Existing namespaced schedule overrides remain supported.
No normalizer, model, current-store, archive, roster policy or task code changed.
The one source-host assertion in the schedules test was updated to the new
approved default.173 deployed-package offline tests PASS.

## Normal scheduled proof

Schedules normal20:00 Eastern trigger: run
6da4b8c3-d226-409d-a3b6-0dc14b74e853, started00:00:03.280Z,
completed00:00:56.852Z on22 October UTC; task exit0.435 records now contain435
class IDs, with435 unique keys and every prior schedule record ID/key preserved.
Result Warning is only the already-known Oct22 empty show day, with four complete
days and no failed days. Existing source-selected date remainsOct22.

Tenant normal20:02 trigger: runb0e809a1-908c-48e0-a405-8a20a1a42bee,
started00:02:01.993Z, completed00:03:18.466Z; task exit0, result Success,
four active entries. Each entry's class ID/date/schedule key/link matches the
shared schedule record.8778 remains main;50121 remains additional slot2.
Two horses added by normal collection:73 total;33 riders remain. All previously
present horse/rider owner helper values and IDs unchanged. Six ring helpers and
six walk values unchanged.18 archive references across the first paired runs
have matching decompressed hashes. Heartbeat/run-logs updated normally.

First paired receipt:
workers/wef-schedules-decoded/evidence/normal-2026-10-22T00-03-56-318Z.json.

Repeat-run acceptance PASS through ordinary20:05 schedules and20:07 tenant
triggers. Schedules runb71f4811-2d5f-4a29-862b-45e5624eef65 started00:05:03.193Z,
completed00:05:46.550Z; tenant run15565090-09d8-432a-be5f-ce8dbd854313
started00:07:02.005Z, completed00:08:19.957Z. Both task exits0. The schedules
result remains Warning only for the known emptyOct22 day; tenant result Success.
All435 original schedule record IDs/keys remain, all435 class IDs populated.
Exactly four entry records retain the same IDs/keys and correct schedule links
across both tenant runs; no duplicate keys.73 horses and33 riders remain stable;
original helper values, six ring helpers and six walks remain unchanged.
Both heartbeat tables match their latest completed runs; run-logs contain both
normal completions.33 recorded archive references across the four runs have
matching decompressed SHA256 hashes. Both five-minute task definitions match
their pre-switch exports. No manual collector runs or record repairs performed.

Final paired receipt:
workers/wef-schedules-decoded/evidence/normal-2026-10-22T00-08-59-337Z.json.
Final comparison:
workers/wef-schedules-decoded/evidence/final-verification.json.

## Protected legacy state

broad-tooth-b8ed remains restored to exact original deployed source:
deployment8d671fb4-bfa1-4294-8d0a-b30cf736b1e1,
version291feca0-f686-4276-b44d-3b1b21f30f38.
ringstatus-proxy remains deployment0263a897-f45e-4d89-8503-0d8953ea151c,
versionde38179f-b130-41bb-9f78-f14ca179bf11. Neither legacy Worker was modified
during this correction. Existing Windows task definitions and five-minute cadence
are protected by before/after exported XML checks. Unrelated user changes remain.
