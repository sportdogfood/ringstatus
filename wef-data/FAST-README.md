# Fast operating README

Date: **November 12, 2026 — America/New_York**  
Document/lock revision: **2026.11.12.3**; frozen Fast collector baseline: **2026.11.12.1**  
Operational owner: **wef-decoder**, chat `01a1a753-dce7-7951-a147-1b096e1538ba`  
Collector lock: [FAST-LOCK.json](FAST-LOCK.json)  
Exact accepted workflow and dated results: [fast-test.md](fast-test.md)

## Ownership

The owner deprecated **wef-fast as the operational owner** and instructed wef-decoder to retain this workflow, own wef-data cleanup and remain responsible for show-times. This does not disable the working Windows collector or its separate saving/linking tasks. Windows tasks execute the business workflow; Codex inspects, makes explicitly approved changes, reviews and verifies.

On November 12, owner-directed notices were submitted to `wef-fast` (`01a19273-bdf8-71b2-8503-cc8079efc129`) and `wef-records-coordinator` (`01a169bc-e58a-7de0-94c6-5d3931fcf745`). Fast was asked to identify every remaining responsibility not already covered. Records was asked to identify cleanup/alert routing still directed to deprecated Fast. Both recipient chats acknowledged the change. The existing Records cleanup reminder is now routed to wef-decoder. Its three-hour schedule, event IDs, completed-event receipts and genuine-event-only task dispatch remain unchanged; no old event is replayed. No additional chats were notified.

## Frozen collector

The lock records the actual code hashes, Git HEAD and task snapshot at creation. It is a documented approval boundary and checksum manifest; it does not introduce a runtime gate or alter permissions, schedules, credentials or code. A future approved change requires its own reviewed delta and dated lock revision. Support components with known gaps are identified separately; their presence in the inventory is not acceptance of those gaps.

No optional enhancement pass, rewrite, added source retry, automatic record repair or patch over a failed verification is authorized by this README. Preserve source URLs, pause, native keys/records/links and actual source timestamps. A failed scheduled verification requires fresh owner edit approval. Do not run manual collectors or use direct source calls as scheduled proof.

## Existing separate execution paths

| Work | Existing task | Entry | Observed cadence |
|---|---|---|---|
| Collection | `wef-data-collector` | `fast-hidden.vbs /day:0` through `/day:4`, sequential actions | Three-minute wake |
| Saving | `wef-data-save` | `fast-hidden.vbs /saving` → `fast-entry.js` → `fast-day.py --saving` | Three-minute wake |
| Linking | `wef-data-linking` | `fast-hidden.vbs /linking` → `fast/linker.js` → `fast_linker.py` | Three-minute wake |
| Cleanup | `wef-data-cleanup` | `cleanup-hidden.vbs` → `cleanup-run.js` → `cleanup-lane.js` | Daily 7 p.m. Eastern plus the existing genuine Records-event dispatch |
| Show-times clock | `wef-decoder-show-time-health` | `workers/wef-schedules-decoded/run-show-time-health.ps1` | One-minute wake; source checks in the existing daytime three-minute/overnight three-hour slots |

These are observed existing configurations, not new schedules created by this documentation.

### Collection

1. Read the selected show and actual date from synced Shows data. The same code handles Days 0–4 within the show date range.
2. Another pass for that actual show/date waits until its previous independent saving **and** linking have completed. Other dates continue.
3. Preparation confirms schedule and `getLiveClassData` access. Check the saved working schedule lane first; use the existing recovery path only after a failed preparation check. Retain successful preparation responses.
4. Schedule requests use the decoder. Repeat comparison uses `source-position` and decoded `source-json`, excluding encrypted `_`. Capture field differences as **old → new** and save only new/changed schedule rows.
5. Keep records but exclude requests for No entries/zero trips, Flat/Under Saddle, Model, breaks and priority 999. Keep First trip → Second trip → Classic → other order. Previously failed classes remain excluded until their failure mark is cleared.
6. Classes use the public `getLiveClassData` route. Capture each eligible response once; no retries or fallback discovery inside the ordinary class loop. Day 0 stops requests only for classes with confirmed fresh saved Live data for the exact customer/show/date/class.
7. Finish acquisition without waiting for Airtable saving, linking or log delivery; proceed to the next date.

### Saver and linker

The saver reads existing local captures and their operation index, saves mapped facts/basic links with existing batching and records its completion receipt. It never invokes a source. The linker reads already saved records, fills required empty links only, preserves populated links, leaves ambiguity empty and checks its writes. It confirms a pass linked only when saving finished before that linker run began. A date-specific linking error does not stop other dates.

Both remain separate task executions. The collector, saver and linker share some source files; their hashes are frozen in the collector lock. This README does not alter them.

## Cleanup: responsibility and current coverage

**wef-decoder owns wef-data cleanup.** The existing independent cleanup lane remains the executor. Records supplies existing genuine new-show/calendar-day/log-check events; the daily 7 p.m. trigger also remains. Cleanup is not called by the collector or linker.

`cleanupV2` checked selections control the existing native-table cleanup. Existing daily/new-show/log-retention rules remain unchanged. The last inspected receipt `runtime/cleanup/simple-last-run.json` records the November 11 daily event; that historical success is not proof of complete coverage of the new Fast paths.

The independent scanner now covers the existing legacy log folders and every actual `runtime/fast/dayN/logging`, `runtime/fast/fast-save/logging` and `runtime/fast/fast-linker/logging` folder. Retention is counted separately per folder. Existing native cleanup selections, daily/new-show/log-check rules, active/latest/pending-delivery protection and acknowledgement-file retention remain unchanged. Fast failure families stay protected locally and in Airtable until their actual owner notice is submitted; malformed notification receipts preserve evidence and report the failure. This does not block collection, saving or linking. No new capture/manifest deletion policy was added.

Offline fixtures and independent Code Review are recorded in `C:/Users/gombc/Documents/Codex/outputs/wef-fast-support-20261112/`. Actual cleanup under this revision still requires the next genuine Records event or the normal 7 p.m. task; deployment is not proof of deletion. Do not manually run or replay cleanup to obtain proof.

## Failure logging and owner alerts

Collection, saving and linking retain actual redacted errors and deliver their own results through the existing Data `run-logs`, `err-logs` and `heartbeat` tables. A source failure, saving failure and linking failure are distinct outcomes. The scheduled UTF-8 saving correction was verified using the actual Windows Python and native Airtable readback.

The existing hourly `check-wellington-decoder` monitor now also reads saved Fast collection, saving, linking, log-delivery and cleanup outcomes. It does not invoke a collector or source. It examines all retained errors, so a healthy latest wake cannot hide a preceding failure. Notices go to this chat, grouped by actual date/lane/cause, once per saved failure identity. Existing old errors are labeled historical rather than treated as failures of the latest run.

Submission receipts live only at `runtime/fast/owner-alerts.json`; they mean submitted to chat, never owner-read acknowledgement. A failed/malformed receipt is reported and left pending for the next existing check. A class exclusion, successful other class or healthy schedule is never class recovery. Recovery requires a saved later success for the same exact operation and scope. No new notification service, task, cadence, class retry or collector hook was introduced. The legacy Windows/Records notifier is not used by this workflow.

This remains hourly, not instant monitoring. Its first natural updated heartbeat, actual chat submission and receipt readback must be verified separately from configuration. WEF Shows warning-log remains the authority for existing decoder schedule/tunnel notices; that workflow is unchanged. The task record retains pending scheduled proof rather than treating reviewed configuration as working delivery.

## Show-times — daily observation with optional exact-date link

**wef-decoder remains responsible for show-times.** The owner directed: create a time for every day and link only when the matching show-day is available.

The producer now saves a valid daily clock observation whether or not a canonical show-day exists. It links only the exact show plus source-calendar date. When there is no match, the show-day link is empty and the observation remains saved. There is no selected-day substitution, artificial canonical day, future timestamp or historical clock backfill. The existing source request, clock key/time, cadence, health behavior and slot dedup remain unchanged.

The November 10 row `rec70KnNzBms3sj1M`, key `15|200000078|2026-11-10`, previously linked November 11. The owner cleared that link. Native readback verified it empty while the source timestamp and show link remained. Codex did not edit that business record; no November 10 show-day was created.

The narrow producer correction and tests passed independent Code Review and 15 offline tests. Deployment: November 12 at 12:18:12 Eastern. The normal 12:21 scheduled check returned Recorded, clock true, zero errors and exit 0. Native record `recahnH3v9D9IG3t8` has today's key, show and exact November 12 day `recheWy529gKKegCk`; checked-at advanced to 12:21:06 Eastern. The unmatched-day case was verified offline, not by manufacturing a live source day.

Evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-show-times-exact-date-20261112/`, including `scheduled-readback.json` and `independent-review/`. No Fast collector file, credential, task configuration, source URL or pause changed for this correction.

## Verification and evidence

The scheduled November 12 11:45 Eastern pass was verified through separate saving, linking and logging. Day 1 collected 51 responses, acknowledged 2,432 business writes and had zero new saving errors. All four in-range dates had successful native Data run records; total acknowledged business writes were 2,952. Final linking completed at 11:53:28 Eastern. Same-date waiting and independence between dates were observed through subsequent natural starts. Day 4 was outside the selected show date range.

Day 0 used confirmed Live coverage. Days 2–3 retained earlier failed-class exclusions; this does not claim those endpoints recovered or that all historical class details are populated. Counts are writes, including updates, not unique/new record counts.

Evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-fast-md-exact-20261112/per-date/utf8/`, especially `scheduled-verification.json`, `native-readback.json` and `independent-review/`. Earlier failed releases and owner approvals remain in their dated packets. The current lock is a snapshot; compare actual hashes before any approved change or acceptance claim.

## Additional responsibilities returned by Fast

Fast identified three relevant outstanding checks: failure-alert delivery; activation/recipient of the existing periodic Data code review; and propagation from saved Data to the shared schedule app. These are not established by the collector’s native saving/linking proof. Keep them as open checks rather than silently claiming completion or starting unrelated work. The deprecation notice authorized an inventory and acknowledgment, not additional implementation.
