# WEF schedules: resume here

## Default-day priority deployed and verified — 15 October,17:37 Eastern
Owner-approved collect.js deployed only to independent package;64 local tests passed. Normal17:30 and17:35 triggers exited0. Both API discoveries returned default day2026-10-15; each collected15,16,17,18 in that order, with all show days included. Per-day captures0/46/155/139 matched, no capture errors/unmatched. Existing emptyOct15 warning retained. Airtable collection readback confirmed completion. Manual focus remains unchanged. No manual runs, scheduler or protected workflow changes. Source-day16 priority is fixture-proven, NOT live-observed: API still returned15 despite owner's website observation16. Preserve that distinction; do not invent or force a default day. Evidence: wef-schedules/runtime/default-day-deployment.json. Maintenance agent should adopt this update on its next authorized task, not redeploy or repeat completed work.

## Current status — 15 October 2026, 16:51 Eastern
Group-matching release acceptance PASS. Two consecutive normal scheduled triggers16:40:01 and16:45:01 exited0; completed16:43:14 and16:50:42. Each captured Friday46, Saturday155, Sunday139 active rows; Thursday0 (owner-confirmed empty). Every populated day had zero capture errors and zero unmatched rows. Airtable readback matched all340 active times, zero blanks/mismatches. Repeated classes and zero-entry groups included.28 previously blank dropped records stayed blank; missing times were not invented. Manual override tests pass, but no flagged live override cases existed.

The16:45 run overlapped16:50; IgnoreNew prevented another instance. Five-minute trigger is not a guarantee of five-minute completion. Task remains enabled and unchanged. Earlier16:35 Saturday capture failure is retained: zero parsed timed rows,130 prior values preserved; exact underlying cause unavailable because HTML/browser diagnostics were not saved. Two later clean runs do not erase that failure.

No immediate repair, redeployment, schema migration or re-verification assignment remains. Maintenance is read-only until a new authorized change. Future default-show transition across a gap and live manual-override cases remain naturally observed coverage, not failures or permission to manufacture data.

Evidence: wef-schedules/runtime/two-clean-runs-20261015.json; group-matching-deployment.json; group-matching-airtable-readback.json; collector-2026-10-15.log; history/attempts and history/datasets. Local suite61 passed. Source repeated-group-verification.md contains test scope.

Plan: ../docs/coordination/wef-schedules-only-activation-plan-20261015.md
Handoff: ../docs/coordination/wef-schedules-maintenance-handoff-20261015.md
