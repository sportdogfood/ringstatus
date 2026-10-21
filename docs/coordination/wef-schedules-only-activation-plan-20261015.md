# WEF schedules-only: independent package and activation



## Owner clarification - default discovery only (15 October 2026)



Current planned discovery contract; supersedes earlier plan language coupling discovery to every five-minute collection run. Documentation only: deployed code/tasks have NOT changed.



- Endpoint: https://broad-tooth-b8ed.gombcg.workers.dev/ring?customer_id=15. No show ID or date supplied. Record initial default show ID and default show day only.

- Expected change window: Sunday 4-8 PM America/New_York (Eastern, following daylight saving). Owner estimate, not a guaranteed source-change time.

- Compare recorded defaults. On change, record them and STOP that discovery polling cycle until discovery logic calls again. No five-minute discovery cadence; interval within the Sunday window remains unspecified.

- Optional six-hour background check: owner-proposed safeguard for unexpected changes, not yet locked or enabled. If adopted, each check is discovery only; it does not continue the stopped polling cycle.

- No downstream triggers, schedule collection, focus changes, processing, publication or other business action may derive from discovery.

- Separate planning context: schedule data expected 4-8 PM Eastern on show start date minus one day (October14 for October15-18). This is neither a guarantee nor a discovery-triggered collection action.

- No code, scheduler, automation or runtime changes authorized by this documentation update.



Endpoint record: https://airtable.com/appnOAk2pW8JawiuP/tblcgmHa2XJkGRvar/recxd60CQFIhJ9UKy





## Default-day priority deployed and verified — 15 October,17:37 Eastern

Owner-approved collect.js deployed only to independent package;64 local tests passed. Normal17:30 and17:35 triggers exited0. Both API discoveries returned default day2026-10-15; each collected15,16,17,18 in that order, with all show days included. Per-day captures0/46/155/139 matched, no capture errors/unmatched. Existing emptyOct15 warning retained. Airtable collection readback confirmed completion. Manual focus remains unchanged. No manual runs, scheduler or protected workflow changes. Source-day16 priority is fixture-proven, NOT live-observed: API still returned15 despite owner's website observation16. Preserve that distinction; do not invent or force a default day. Evidence: wef-schedules/runtime/default-day-deployment.json. Maintenance agent should adopt this update on its next authorized task, not redeploy or repeat completed work.



## Current status — 15 October 2026, 16:51 Eastern

Group-matching release acceptance PASS. Two consecutive normal scheduled triggers16:40:01 and16:45:01 exited0; completed16:43:14 and16:50:42. Each captured Friday46, Saturday155, Sunday139 active rows; Thursday0 (owner-confirmed empty). Every populated day had zero capture errors and zero unmatched rows. Airtable readback matched all340 active times, zero blanks/mismatches. Repeated classes and zero-entry groups included.28 previously blank dropped records stayed blank; missing times were not invented. Manual override tests pass, but no flagged live override cases existed.



The16:45 run overlapped16:50; IgnoreNew prevented another instance. Five-minute trigger is not a guarantee of five-minute completion. Task remains enabled and unchanged. Earlier16:35 Saturday capture failure is retained: zero parsed timed rows,130 prior values preserved; exact underlying cause unavailable because HTML/browser diagnostics were not saved. Two later clean runs do not erase that failure.



No immediate repair, redeployment, schema migration or re-verification assignment remains. Maintenance is read-only until a new authorized change. Future default-show transition across a gap and live manual-override cases remain naturally observed coverage, not failures or permission to manufacture data.



Evidence: wef-schedules/runtime/two-clean-runs-20261015.json; group-matching-deployment.json; group-matching-airtable-readback.json; collector-2026-10-15.log; history/attempts and history/datasets. Local suite61 passed. Source repeated-group-verification.md contains test scope.



## Historical plan and observations (superseded status statements retained)

Current authority: maintenance handoff prepared; group-matching release verified below. Earlier sections are historical and do not authorize replaying old repairs or deployments.



## Exact artifact

C:/Users/gombc/.codex/worktrees/wef-schedule-collector/ringstatus/wef-schedules/

Deploy ONLY this folder after explicit approval. Do not replace any repository-root JS, library, PowerShell helper, environment file or scheduled task. The prior three-root-file deployment proposal is superseded.



## Proven baseline and copies

collect.js and store.js preserve the approved all-show-day/default-discovery/staging/drop logic. Private collector-core.js copies the prepared working collector; normalizer.js, token-utils.js, lib/soft_payload_guard.js, lib/scope_fields.js, lib/sgl_fetch_adapter.js and sgl_fetch.ps1 are private copies of existing dependencies. Internal imports stay inside this directory. source-manifest.json records the exact copied source paths/hashes, final package hashes and protected-file verification. No new SGL transport or normalization method.

All pre-existing files in both main checkout and worktree were unchanged during isolation (hash verified). The previously modified root worktree prototype remains untouched and must not be deployed. No shared code is loaded by the private package.



## Behavior

Discover default show using the proven /ring customer 15 request, then default-date schedule metadata. Compare saved show ID, create/update private focus state and fetch EVERY calendar day from show start through end on each invocation. Focus does not restrict collection. Preserve manual show-specific settings and existing manual payload/HTML fallback methods using PRIVATE copies of any needed inputs.

Write only wef-schedules appUq5Qz9kARqFija collections/schedules. Existing schema remains unchanged. Reuse identical datasets; publish completion only after readback. Compare keys within each show/day. Add/update returned rows, retain missing rows as inactive with dropped_at, retain old drop dates and reactivate returned rows. Track counts/keys in existing message/context-json. Failed, malformed, wrong-scope and destructive zero-row responses cannot replace an established day's snapshot. Failed fetch fallback cannot generate drops. As explicitly approved, silently truncated but valid nonempty responses are subject to the legacy comparison policy; no invented completeness gate or always-pending queue.



## Isolation

Only WEF_SCHEDULES_* credential/transport settings are accepted. Legacy environment settings are ignored. Destination/customer/table names are fixed in private settings.js. Mutable paths are package-local runtime/: collector.env, focus.json, collector.lock, cache/, manual/, session.json, tmp/ and dated logs. No legacy session, manual input, cache or log directory is used. The copied PowerShell helper does not load User/Machine business settings. OS runtime/executable settings are the only shared host facilities.

No actual credentials, live session files or manual business payloads have been copied. If required for the existing transport/manual workflow, deliberately copy them into private runtime locations during approved setup; never point back to shared paths.



## Exact activation still required

1. Explicit approval to deploy this private folder and create/enable ONE task.

2. Place folder at an approved dedicated deployment directory; confirm Node v22+ and PowerShell executable paths under the runner account and private-directory read/write permissions. No existing source replacement.

3. Populate runtime/collector.env with WEF_SCHEDULES_AIRTABLE_TOKEN and only needed namespaced transport settings; copy required session/manual inputs into runtime. Keep these private and outside version control. No Airtable schema changes.

4. Register wef-schedules-collector: every five minutes, IgnoreNew for overlapping starts, absolute PowerShell executable with -NoProfile -File <package>/run.ps1, working directory <package>. run.ps1 invokes collect.js, captures output/exit in private dated logs, and reads the private env file. No heartbeat or epoch task edits.

5. Wait for two NORMAL scheduled runs; do not click Run or invoke the business script manually. Verify timestamps/exit codes, default-show/all-days scope, saved Airtable rows/readbacks, unchanged reuse and naturally occurring added/changed/dropped events. Failure is reported without manual retry/repair. Future show transitions/outages require future normal-run evidence if not naturally observed.



## Verification

33 local checks passed, including 22 functional checks from a detached copy without access through imports to the original checkout. Covered import graph, inherited-setting rejection, private paths, all-day source collection, change/drop/reactivation history, empty/failure/wrong-date protections, snapshots, repeat runs, default discovery, and overlap. PowerShell task wrapper parsed successfully without execution. Protected pre-existing file hashes unchanged. git diff --check passed.

No deployment, task registration/change, business endpoint call, manual business run, or Airtable record/schema change occurred.



## Deployment observation — 15 October, Eastern time

Private package deployed and task registered for 12:35 then every five minutes. First normal run started at 12:35; no manual trigger. It stalled before default-show state/Airtable writes. Static AST comparison confirms private sgl_fetch.ps1 Invoke-WebRequest lost all five parameter bindings due to doubled CR line endings introduced during copying; original helper has all five. New task stopped/disabled to prevent repetition. Original tasks and sources unchanged. Airtable collections readback returned zero records. Two successful normal runs remain unverified. Separate approval requested for private-file correction, regression assertion, redeployment and re-enable; no repair performed.



## Approved correction and resumed observation

At 12:41 Eastern, private PowerShell LF correction and parameter-binding regression test deployed. All 34 local checks pass. Deployed AST confirms UseBasicParsing, Uri, WebSession, Headers and TimeoutSec. Protected source hashes and all package hashes verified. Task re-enabled; next normal trigger 12:45. No manual business runs. Prior failed observation retained above; two successful scheduled runs still to verify.



## Two scheduled runs observed; repeat acceptance failed

Normal triggers 12:45:01 and 12:50:01 Eastern both exited 0, ending 12:46:27 and 12:50:46. Each collected all dates October 15–18 for default show 200000074: 0, 138, 196, 139 schedule rows. Connector readback verified 8 complete collections and 946 snapshot rows. Comparing every matched row proves only sgl_token_raw, sgl_token_prefix and sgl_token_hash changed (473 each); no business schedule fields changed. store.js fingerprints and row comparisons include these rotating transport values, creating unnecessary new snapshots and false change counts. Empty October 15 reused its dataset. Repeat acceptance therefore FAILS despite successful execution. Disabled only wef-schedules-collector before its next trigger. No business records deleted or repaired, no manual runs, no source correction after failure. Existing RingStatus heartbeat, pipeline and epoch task XML definitions match their original hashes. Some unrelated Windows system task definitions changed/vanished during the observation; no edits were made to them.

Exact next step: obtain APPROVED TO EDIT for private change-comparison correction, preserving necessary source evidence separately from semantic schedule comparisons; add rotating-token repeat regression, locally verify, redeploy private package and observe two normal scheduled runs. Do not resume or modify existing tasks. Evidence: wef-schedules/runtime/recurring-verification.json and deployment-verification.json.



## Parent takeover and local verification â€” 15 October

Owner requested parent complete the approved corrections before assigning maintenance. rs-wef-schedules stopped with acknowledgment, no active commands; it added two regression tests only. Parent implemented semantic comparisons excluding four rotating token fields while retaining full snapshot integrity fingerprints, reuse of previous dataset/fingerprint, rotation counts and observed fingerprint in existing context-json. Added successful-empty in-range warnings and default-show ID/range/phase logs; future default shows do not imply missing gap days. Added October19 -> October21 show-transition fixture.

Local suite: 36 passed, 1 failed. All 25 detached functional tests passed, including new regressions. Sole failure: isolation.test.js hardcodes expected test count 22, now 25; assertion rejects the successful detached suite. No product correction made after that failure. Task remains disabled, corrected source not deployed. Exact next action: approval to update stale test-count assertion and repeat verification, then scoped deployment and two normal scheduled triggers. No manual business runs or existing workflow edits.



## Approved correction deployed â€” 13:05 Eastern

Owner approved the stale test assertion correction and completion here before maintenance handoff. Parent changed expected detached test count from22 to25; all37 tests pass. Reviewed exact store.js/collect.js delta against deployed package. Semantic comparisons ignore sgl_token_raw/prefix/length/hash only; stored snapshots and full integrity fingerprints remain intact, and prior records need no migration. Latest source payload remains in private source cache; context logs observed fingerprint and token-rotation count. Real changes still save new snapshots. Successful empty in-range days emit WARNING empty_show_day in existing message/context and run report. Failed fetches remain failures, not empty-day warnings. Default discovery report/state logs old/new ID/ranges and upcoming/current/ended phase. October19 switch to October21 start tested locally without inventing gap-day collection.

Copied only approved private files and tests/manifest; backup in runtime/before-token-comparison-correction. Full package/protected-source hashes verified. Task re-enabled for normal13:10 and13:15 triggers. No manual runs, no existing workflow changes. New maintenance chat remains stopped.



## Final acceptance â€” PASS, 15 October 2026 13:16 Eastern

Two NORMAL scheduled triggers at13:10:01 and13:15:01 both exited0. Connector readback verified8 complete day collections for show200000074 covering October15â€“18, counts0/138/196/139 each run. All datasets reused. Each run recorded473 token rotations with zero semantic added/changed/dropped rows. Historical schedule snapshot row count stayed946. Empty October15 warning confirmed in Airtable message/context, focus state and dated runner log. Default-show/current-range log confirmed. Future default transition across a gap is locally fixture-proven, not yet live-observed. Real changes/drop/reactivation and failure guards retain local regression coverage; no live events fabricated.37 tests passed. Final deployed hashes and protected source hashes verified unchanged. Existing heartbeat/pipeline/epoch task definitions match baseline. Five-minute independent task stays enabled with IgnoreNew and existing Interactive account. No manual business runs or record repairs. New maintenance agent remains stopped per owner. Evidence: wef-schedules/runtime/corrected-recurring-verification.json; deployment-verification.json; collector-2026-10-15.log. Parent finished acceptance before maintenance handoff.



## Usable current staging â€” owner-approved revision, 15 October

Owner approved readable staging fields, one current row per schedule key, separate history, local tests and normal scheduled proof; no manual runs or existing workflow changes. Disabled only wef-schedules-collector before schema/code work. Created17 actual projection fields (show/date/ring/group/sequence/class/type/times/trips/status/changed-at); retained3 internal original fields for key/source integrity. No data records changed manually.

Prepared private current-store.js adapter, retaining existing discovery/normalization/drop guards. Normal scheduled initialization first archives and verifies exact historical Airtable records in private runtime/history/*.json.gz, then materializes latest source rows in place and removes archived duplicate snapshots only after readback. Collections becomes one status row per show/day; per-attempt history and immutable datasets stay in private compressed files. Successful whole-show collection retires previous-show rows only after verified archive. Existing source keys retained with show/date scope. Explicit capacity guard refuses writes at4900 current rows rather than silently truncating. No new backend service or tables.

Current local result:41 of42 checks pass. Sole failure is new current-store integration fixture missing customer_id15; collector correctly rejects missing show/customer ID. No implementation edit after failure. Exact next action is separate approval to correct that fixture and finish local verification, deploy approved package and observe two normal triggers. Task remains disabled; new storage code NOT deployed. All existing Airtable records retained. No manual business execution.



## Current staging deployment â€” 13:36 Eastern

Owner separately approved fixture correction. Added missing customer_id15 to the test show; all42 checks pass. Reviewed archive ordering, readback and cleanup pacing. Deployed private current-store.js, store adapter and tests/manifest only; protected-source and package SHA256 checks passed. Existing heartbeat/pipeline/epoch and settings remain untouched. First normal migration trigger13:40, next13:45. No manual data migration or business execution.

Airtable columns: show-id,date,ring,group-id,group,sequence,class-id,class-number,class-name,type,sequence-type,start-time,end-time,total-trips,completed-trips,status,changed-at. Original stage-key scopes existing source key by show/date; row-json and dataset-id retain complete source integrity and archive reference. No invented missing times or identifiers. Separate history lives at deployed wef-schedules/runtime/history: initial-airtable.json.gz exact pre-migration record archive; datasets/*.json.gz immutable snapshots; attempts/*.json.gz run history; removed and retired exact archived records; current.json latest complete day checkpoints. Archive write/decompress/readback verification occurs before cleanup. History remains private on this machine; no new cloud service or base is introduced.

Target on existing data:473 schedules,4 collections. Normal successful future-show collection retires previous-show Airtable rows after verified archive. Recurrence must not add schedule snapshots or collection-status records. Migration and final acceptance pending normal scheduled proof.



## First scheduled current-staging proof â€” 13:40 trigger

Normal task start13:40:01, completed13:41:14 Eastern, exit0. Connector readback confirms473 rows/473 unique scoped keys, counts138 October16,196 October17,139 October18. Four collection-status records complete; October15 remains zero with warning. Readable class/ring/date/source key values compared against row-json: zero mismatches. Exact prior946 schedule records and24 collection records archived in initial-airtable.json.gz before cleanup; SHA256 b443956c84e1490ff61da56b879be24757b8d47ed9fc8973db4e54cb00a2e4e8. All19 compressed JSON history artifacts decompress and parse successfully (699222 bytes total). No manual runs or data changes. Next normal13:45 run must prove stable IDs/counts and continued history without Airtable growth.



## Current staging final acceptance â€” PASS at13:46 Eastern

Normal13:40/13:45 triggers exited0; first ended13:41:14, second13:45:20. Full connector readback after both confirms473 rows,473 unique scoped schedule keys and identical record IDs and field values on repeat. All17 exposed fields verified against row-json/source values; no mismatches. Collections remains4 records with same IDs, all refreshed complete, retaining empty-October15 warning. Source-supplied times/total-trip counts absent today remain blank; class IDs present only for10 source-enriched rows.23 separate gzip history artifacts verified;8 current scheduled attempts, plus all946 original schedule records and24 prior collection records in exact initial archive. No historical records lost. Final protected/source hashes pass; existing task definitions unchanged. New collector stays enabled every5minutes. No manual runs, manual repairs or changes to existing workflows. Evidence runtime/current-staging-verification.json supersedes previous snapshot-store acceptance. Old-show transition is fixture-proven and awaits natural occurrence for live evidence. Maintenance agent remains stopped.





## Estimated-time release observed 15 October 2026, 14:26 Eastern

Approved tested time capture deployed at18:16:15Z; exact source and dependency hashes verified. Normal task starts14:20:01 and14:25:01 Eastern completed exit0. No manual business runs, task definition edits, shared workflow changes or test records.

Airtable connector readback after each run:473 records with identical IDs and times. Timed rows:Oct16 55/138;Oct17 41/196;Oct18 45/139. Oct15 no scheduled classes. All four day snapshots reused on second run; no duplicate growth. Unmatched332 rows include actual classes: source time availability versus matcher coverage is not yet established. Do not call complete time coverage verified. No flagged manual override rows exist in current snapshots: preservation verified by local tests only, not a live override case. No fabricated test records added.

Protected16 file hashes unchanged. All scheduled task definitions checked against deployment checkpoint. Evidence:wef-schedules/runtime/estimated-times-deployment.json and estimated-times-task-check.json.

Next authorized work would be read-only diagnosis of the332 unmatched rows using existing captured evidence; no live manual scrape or correction has been performed. Further code correction requires separate approval after failed acceptance. Existing successful collector remains on its original five-minute schedule; warnings expose missing times.





## Inheritance deployment â€” failed live acceptance,15October2026

Deployed hash-verified tested package at18:51:34Z. First normal14:55:01Eastern run exited0 but failed functional acceptance. Owner screenshot classes720/787/835 onOct16 ring3 remain blank instead of08:00. Classes744/715 remain08:27/08:39. Time counts unchanged55/138,41/196,45/139 forOct16/17/18. Oct15empty. No live manual override rows present; only local override proof exists. Protectedsourcehashes unchanged. No manual runs or task changes.

Stopped second-run verification after observed failed acceptance under owner instructions. Did not disable or modify the existing scheduled task; it remains enabled. No corrective edits performed. Do not present54 passing synthetic tests as live proof. Runtime does not retain rendered HTML, so exact omitted DOM structure is not yet established; do not invent cause or another correction. Next step requires actual rendered-source evidence against the supplied screenshot before a separately approved minimal correction. Evidence:wef-schedules/runtime/inheritance-deployment.json.





## Table-ring release15October2026

Deployed exact58-test package at19:54:35Z between scheduled runs; hashes verified. First normal15:55:01Eastern task trigger ended with3221225786 (0xC000013A), with no new collector START line; latest completed snapshots remain from prior15:50trigger. Cause of interruption not established. No claim of live720/787/83508:00 or two-run verification. No flagged manual overrides in baseline: protection locally tested only. No manual runs, restarts, repair, task changes or further code edits. Stopped verification after scheduler failure per runner boundary. Package remains deployed; task remains enabled and unchanged. Evidence:wef-schedules/runtime/table-ring-deployment.json. Next action: establish cause of task interruption before resuming normal-run observation; no manual substitute.





## Two subsequent normal runs verified â€” 15October2026,16:08Eastern

Read-only interruption investigation: TaskScheduler Operational history disabled; no collectorSTART for15:55trigger; reported0xC000013A. Available System/Application/PowerShell events do not establish who/what terminated it. No cause asserted and no settings changed.

Unmodified schedule subsequently ran16:00(start16:00:03,end16:01:06exit0) and16:05(start16:05:02,end16:08:09exit0; exact END retained in collector log). Both four-day attempt archives confirm720,787,835 onOct16 ring3 at08:00. Airtable readback confirms same three existing record IDs with08:00. No manual runs/restarts/repair required. Protectedsourcehashes unchanged. No flagged live overrides existed; preservation proof remains local tests only. Other unmatched rows are not claimed complete. Full evidence updated in runtime/table-ring-deployment.json.

## Run/archive logging verified - 20 October 2026, 09:26 Eastern

Airtable run-logs is live: one record per actual collector run, readable per-day rows/changes/warnings/errors and incremental archive details (kind, show/date scope, count, file, SHA256 and cleanup outcome). Normal triggers 09:20:01/09:25:01 exited 0, completed 09:20:48/09:25:44. Exactly two distinct run records; four verified attempt archives each. Both report 435 active rows, all four October 22-25 dates complete, no time errors/unmatched rows and only the empty-October-22 warning. All 435 schedule IDs and values remained unchanged; no duplicate log rows or schedule growth. Local suite 75 PASS; release review no material findings. Single-attempt log POST avoids duplicate-create retries; uncertain outcomes use read-only stable-ID lookup. Existing heartbeat/epoch/pipeline/collector task definitions and protected sources unchanged. No manual runs or repairs.

Latest 1000 runs stay in Airtable; older completed logs are verified locally before slot reuse, with that archive noted on the new run. Existing previous-show archives are preserved locally, not retroactively imported. Retirement, failure and retention branches are fixture-proven, not live-observed during these two unchanged runs. Logging cannot report to Airtable when Airtable itself is unavailable or process startup fails; existing local log retains available errors. Do not repeat release implementation/acceptance. Evidence: runtime/run-logs-release-20261020/deployment.json and two-runs-airtable.json.



## Duplicate schedule-key correction verified - 20 October 2026, 09:52 Eastern

Owner-approved P2 correction deployed to private current-store.js and its tests only. Verified survivor first, then exact archive-before-delete of scoped extras, then final unique survivor/field readback. Post-write duplicates from repeated creates now reach the existing safe reconciliation path. Existing transport, schedule/manual/drop behavior and other workflows preserved. Local suite 80 PASS, targeted 14 PASS; release reviewer no material findings. Two normal triggers 09:45:01/09:50:01 exited 0, ended 09:45:49/09:50:52. Each reports all four October 22-25 dates complete, 435 rows, no errors/unmatched times and only the empty-October-22 warning. All 435 record IDs/values unchanged; zero duplicates; eight actual attempt archive files/hash entries verified. No live duplicate case was fabricated: duplicate/failure recovery is fixture-proven, normal operation independently observed. No manual runs or record repairs. Protected source hashes and collector/heartbeat/epoch/pipeline task definitions unchanged. P2 review finding resolved. Next Monday code review remains October 26 at 09:00 Eastern. Evidence: runtime/duplicate-key-release-20261020/deployment.json; root docs/coordination/wef-schedules-duplicate-key-fix-20261020.md.

