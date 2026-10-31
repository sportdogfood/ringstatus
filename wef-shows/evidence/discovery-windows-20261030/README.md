# Discovery windows — October 30, 2026

Status: DEPLOYED October 30 at 18:15:02 Eastern; two normal daily-window discovery runs verified at 18:19 and 18:24 Eastern. Full-run health remains Partial because of the prior count-publication stop; Sunday/overnight branches remain unobserved in normal runs. The owner approved the Shows change and companion authority checks. Each module was updated at its idle boundary; no active collector was interrupted. No manual collector or provider runs, record repairs, task changes or stop-file clearing were performed.

## Approved behavior

- New default show: Sunday 16:00 inclusive to 20:00 exclusive America/New_York.
- Source-selected show-day for the retained default show: daily 16:00 inclusive to 20:00 exclusive in that timezone.
- Existing five-minute task, heartbeat, run logs and error logs continue. Checks within windows use those wakes. DST follows Eastern civil time.
- Sunday reuses the proven ring then default-schedule discovery helper. Other days query only default-schedule for the existing canonical show. A mismatched source show is rejected.
- Outside windows, keep the canonical record, IDs, links, archived source payload and original last_checked unchanged. Log the discovery skip, continue existing count publication when not already stopped, and update heartbeat.
- Do not manufacture tomorrow's date. Use only the date returned by the source. No stop-after-first-change or new timer was requested.

## Prepared artifact and tests

[Deployment manifest](deployment-manifest.json) identifies the exact 10 runtime and four test files under prepared-release/. The initial Shows-only artifact under prepared/ is historical. The isolated runnable tree is ../../staged/discovery-windows-20261030 with copied dependencies, no production credentials or runtime. Tests use mock IO and retained test fixtures only. All 55 offline tests passed October 30; transcript: [tests.tap](tests.tap). [Independent review](review.md) passed 27 relevant tests and confirmed the compatibility blocker, with no additional actionable finding. Tests are not deployment or scheduled proof.

The deployment route remains the same checkout's wef-shows/collector.js loaded by wef-shows-collector through run-hidden.vbs, run.ps1 and entry.js. All seven companion reader files and the shared authority helper were installed before the Shows collector. Exact readback hashes and protected files: [deployment receipt](deployment.json).

## Approved consumer compatibility change

Schedules show-source.js, Entries model.js, Live model.js, Data's wef-schedules/tests/wef_data_context.py validator and Timing authority/read paths require a source check within 30 minutes. Off-window checks will intentionally cease. Renewing last_checked on skipped runs would falsify source time. Deploying Shows alone would make readers reject retained authority overnight.

Approved and prepared companion change: make only canonical show-authority age checks aware of the approved daily discovery window. Retained authority remains usable outside the window if the last successful source check met the existing 30-minute allowance at the previous window's close. During an open window retain the 30-minute check, allowing the first 30 minutes for the first natural scheduled check. A missed/failed full window expires authority; old evidence is never renewed. Preserve original timestamps, identity/hash/date/native-link checks, count five-minute readiness and unrelated Data/Live/Timing observation limits. Exact prepared files are listed in deployment-manifest.json. Independent combined review PASS; 380 offline tests produced 341 passes and the same 39 Entries/Timing failures reproduced against the unchanged baseline, plus 13/13 Data contract passes. See review.md and verification.json.

## Separate pre-existing count stop

The production stop at 18:07:49.560Z (14:07:49 Eastern) October 30 records Count replay timed out, run 5b52891a-0ad5-41dc-a264-3c2965ccb6c4. This preceded this work. Preserve it; this discovery approval does not fix replay, clear its stop or authorize a failed-path retry. Logs/heartbeat may remain Partial because of it even after discovery acceptance succeeds.

## Remaining acceptance

Deployed hash readback PASS; real scheduled receipts and Airtable heartbeat/run-log readback. Verify daily-window and outside-window behaviors separately. A normal Sunday check/rollover cannot be claimed from Friday tests or a forced run. Preserve the five-minute task and existing archives/count safeguards.

## Observed scheduled verification

Normal runs `1db4a696-5728-48d2-b009-900900a26c24` (18:19) and `e2cbcc75-63ae-4b99-bcbc-dbc366f46b09` (18:24) passed daily discovery. Each requested default-schedule only, retained show 200000076, and published source-selected October 30. Airtable heartbeat and exact run-log timestamps matched; source links and three archive checksums per run verified. Default-ring last attempt stayed 22:14:16.118Z. See normal-*.json, readback-1.json, readback-2.json and verification.json.

All 14 deployed file hashes remain exact. The five-minute task remains unchanged. Both overall runs are Partial solely for the preserved count-publication stop; its hash is unchanged. No manual runs or record repairs. Live subsequently read Shows and Timing completed normally. Existing Schedules lock and Entries uniqueness blockers remain separate. Overnight companion behavior and Sunday discovery are not claimed as normal-run verified.
