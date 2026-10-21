# Show/ring tables verified â€” 20 October 2026, 11:43 Eastern

Owner-approved show-details, rings and rings-helper addition is deployed and accepted. Source-selected date is October 22; calendar date and legacy focus_day do not select these records. Normal 11:35:01 and 11:40:01 triggers completed 11:36:14/11:40:50, exit 0. Both verify 1 show, 24 daily rings, 6 selected October 22 rings, 6 static helpers and all 435 schedule links. Repeat record IDs/data stable, zero duplicate creation, all original schedule IDs/fields unchanged. Both run logs include reference totals and four actual archive files each; no errors and only the existing empty-October-22 warning. 97 offline tests PASS; final independent review no material findings. Retained dropped rows use historical ring links or a documented blank if no source/historical identity exists. Owner static helper fields preserved. No additional SGL calls, triggers, deletions, manual runs or record repairs. Six existing private modules (including archive-protected current-store.js), 16 protected sources and all four task definitions unchanged. Do not repeat deployment/acceptance. Evidence: runtime/show-rings-release-20261020; docs/coordination/wef-show-rings-release-20261020.md.

# WEF schedules: resume here

## Duplicate schedule-key correction verified - 20 October 2026, 09:52 Eastern

Owner-approved P2 correction deployed to private current-store.js and its tests only. Verified survivor first, then exact archive-before-delete of scoped extras, then final unique survivor/field readback. Post-write duplicates from repeated creates now reach the existing safe reconciliation path. Existing transport, schedule/manual/drop behavior and other workflows preserved. Local suite 80 PASS, targeted 14 PASS; release reviewer no material findings. Two normal triggers 09:45:01/09:50:01 exited 0, ended 09:45:49/09:50:52. Each reports all four October 22-25 dates complete, 435 rows, no errors/unmatched times and only the empty-October-22 warning. All 435 record IDs/values unchanged; zero duplicates; eight actual attempt archive files/hash entries verified. No live duplicate case was fabricated: duplicate/failure recovery is fixture-proven, normal operation independently observed. No manual runs or record repairs. Protected source hashes and collector/heartbeat/epoch/pipeline task definitions unchanged. P2 review finding resolved. Next Monday code review remains October 26 at 09:00 Eastern. Evidence: runtime/duplicate-key-release-20261020/deployment.json; root docs/coordination/wef-schedules-duplicate-key-fix-20261020.md.

## Run/archive logging verified - 20 October 2026, 09:26 Eastern

Airtable run-logs is live: one record per actual collector run, readable per-day rows/changes/warnings/errors and incremental archive details (kind, show/date scope, count, file, SHA256 and cleanup outcome). Normal triggers 09:20:01/09:25:01 exited 0, completed 09:20:48/09:25:44. Exactly two distinct run records; four verified attempt archives each. Both report 435 active rows, all four October 22-25 dates complete, no time errors/unmatched rows and only the empty-October-22 warning. All 435 schedule IDs and values remained unchanged; no duplicate log rows or schedule growth. Local suite 75 PASS; release review no material findings. Single-attempt log POST avoids duplicate-create retries; uncertain outcomes use read-only stable-ID lookup. Existing heartbeat/epoch/pipeline/collector task definitions and protected sources unchanged. No manual runs or repairs.

Latest 1000 runs stay in Airtable; older completed logs are verified locally before slot reuse, with that archive noted on the new run. Existing previous-show archives are preserved locally, not retroactively imported. Retirement, failure and retention branches are fixture-proven, not live-observed during these two unchanged runs. Logging cannot report to Airtable when Airtable itself is unavailable or process startup fails; existing local log retains available errors. Do not repeat release implementation/acceptance. Evidence: runtime/run-logs-release-20261020/deployment.json and two-runs-airtable.json.



## Default-day priority deployed and verified ï¿½ 15 October,17:37 Eastern
Owner-approved collect.js deployed only to independent package;64 local tests passed. Normal17:30 and17:35 triggers exited0. Both API discoveries returned default day2026-10-15; each collected15,16,17,18 in that order, with all show days included. Per-day captures0/46/155/139 matched, no capture errors/unmatched. Existing emptyOct15 warning retained. Airtable collection readback confirmed completion. Manual focus remains unchanged. No manual runs, scheduler or protected workflow changes. Source-day16 priority is fixture-proven, NOT live-observed: API still returned15 despite owner's website observation16. Preserve that distinction; do not invent or force a default day. Evidence: wef-schedules/runtime/default-day-deployment.json. Maintenance agent should adopt this update on its next authorized task, not redeploy or repeat completed work.

## Current status ï¿½ 15 October 2026, 16:51 Eastern
Group-matching release acceptance PASS. Two consecutive normal scheduled triggers16:40:01 and16:45:01 exited0; completed16:43:14 and16:50:42. Each captured Friday46, Saturday155, Sunday139 active rows; Thursday0 (owner-confirmed empty). Every populated day had zero capture errors and zero unmatched rows. Airtable readback matched all340 active times, zero blanks/mismatches. Repeated classes and zero-entry groups included.28 previously blank dropped records stayed blank; missing times were not invented. Manual override tests pass, but no flagged live override cases existed.

The16:45 run overlapped16:50; IgnoreNew prevented another instance. Five-minute trigger is not a guarantee of five-minute completion. Task remains enabled and unchanged. Earlier16:35 Saturday capture failure is retained: zero parsed timed rows,130 prior values preserved; exact underlying cause unavailable because HTML/browser diagnostics were not saved. Two later clean runs do not erase that failure.

No immediate repair, redeployment, schema migration or re-verification assignment remains. Maintenance is read-only until a new authorized change. Future default-show transition across a gap and live manual-override cases remain naturally observed coverage, not failures or permission to manufacture data.

Evidence: wef-schedules/runtime/two-clean-runs-20261015.json; group-matching-deployment.json; group-matching-airtable-readback.json; collector-2026-10-15.log; history/attempts and history/datasets. Local suite61 passed. Source repeated-group-verification.md contains test scope.

Plan: ../docs/coordination/wef-schedules-only-activation-plan-20261015.md
Handoff: ../docs/coordination/wef-schedules-maintenance-handoff-20261015.md


20 October 2026 — shows/show-collection migration deployed; acceptance INCOMPLETE. First full14:30 normal run PASS; second14:35 exited1, fetch failed. No patches/retries/repairs after failure. See docs/coordination/wef-shows-index-release-20261020.md.


20 October2026,15:17Eastern — Separately approved read-failure handling recovery VERIFIED.124deployed/127source tests; normal15:10/15:15exit0,435rows each matched archived datasets; show/index/static helpers/IDs intact. Original network cause unconfirmed. No manual runs, record repairs or task changes. See docs/coordination/wef-fetch-failure-release-20261020.md.
