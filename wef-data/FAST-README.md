# Fast operating README

Date: **November 12, 2026 — America/New_York**  
Document/lock revision: **2026.11.12.7**; frozen Fast collector baseline: **2026.11.12.7**  
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
| Full Day 0 collection | `wef-data-day-0` | `fast-hidden.vbs /day:0` | Every 3 minutes 06:00–18:00; every 3 hours 18:00–06:00, Eastern |
| Full Days 1–4 collection | `wef-data-collector` | `fast-hidden.vbs /day:1` through `/day:4`, sequential actions | Existing three-minute wake unchanged |
| Saving | `wef-data-save` | `fast-hidden.vbs /saving` → `fast-entry.js` → `fast-day.py --saving` | Completed collection requests the existing task; busy requests queue |
| Linking | `wef-data-linking` | `fast-hidden.vbs /linking` → `fast/linker.js` → `fast_linker.py` | Each date’s completed saving requests the existing task; busy requests queue |
| Cleanup | `wef-data-cleanup` | `cleanup-hidden.vbs` → `cleanup-run.js` → `cleanup-lane.js` | Daily 7 p.m. Eastern plus the existing genuine Records-event dispatch |
| Show-times clock | `wef-decoder-show-time-health` | `workers/wef-schedules-decoded/run-show-time-health.ps1` | One-minute wake; source checks in the existing daytime three-minute/overnight three-hour slots |

These are observed existing configurations, not new schedules created by this documentation.

### Collection

1. Read the selected show and actual date from synced Shows data. The same code handles Days 0–4 within the show date range.
2. Another pass for that actual show/date waits until its previous independent saving **and** linking have completed. Other dates continue.
3. Preparation confirms schedule and `getLiveClassData` access. Check the saved working schedule lane first; use the existing recovery path only after a failed preparation check. Retain successful preparation responses.
4. Schedule requests use the decoder. Repeat comparison uses `source-position` and decoded `source-json`, excluding encrypted `_`. Capture field differences as **old → new** and save only new/changed schedule rows.
5. Keep records but exclude requests for No entries/zero trips, Flat/Under Saddle, Model, breaks and priority 999. Keep First trip → Second trip → Classic → other order. Day 0 ignores saved failure checkboxes when selecting class details; error history stays intact. Days 1–4 retain the existing failed-class exclusion.
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

## Approved token/error correction — November 12, 3:35 p.m. Eastern

Only `fast-source.js` and `fast_workflow.py` changed in the frozen Fast core: failed requests now retain redacted decoder diagnostics/allowlisted headers/nested cause, and failed preparation records retain the capture pointer in existing health metadata. Collection rules, cadence, independent saving/linking and all other core code are unchanged. The independent decoder token fix and exact installed-runner error responses were reviewed and preview-tested; normal scheduled collection, saving, linking and logging completed for November12–15 with zero new source/save errors. November14 requested zero classes, so its class coverage is not verified; see [token recovery and coverage benchmarks](../workers/wef-schedules-decoded/TOKEN-RECOVERY.md). Evidence is `C:/Users/gombc/Documents/Codex/outputs/wef-token-acquisition-error-capture-20261112/`.


## Git latest schedule/classes — November 12, 5:38 p.m. Eastern

The independent saver publishes latest successful JSON to existing `ringstatus-data`, branch `codex/fast-snapshots`, in `schedules/{customer}/{show}/{date}.json` and `classes/{customer}/{show}/{date}/{ring}-{class}.json`. Git compares with the last successfully saved same-date revision; Airtable consumes changed payloads from that Git commit and skips unchanged mapped records. Failed responses retain error history and cannot replace good payloads. Full local captures, existing keys/links, source URLs, pause, rules and independent scheduled stages remain unchanged. New manifests finish collection before this separate saving stage starts. The existing same-date save/link wait remains.

Receipts retain Git commit/changed paths and a `.diff` pointer. No new jobs, source retries, credentials or manual collectors. Source requests still occur to discover changes. Eight offline tests and single-agent Code Review passed before installation; natural scheduled proof is pending. Exact workflow/evidence: [fast-test.md](fast-test.md), `C:/Users/gombc/Documents/Codex/outputs/wef-git-latest-20261112/`.


Scheduled Git verification: the 17:39 collection published November12/13/14 schedule payloads and November13/14 class payloads to `codex/fast-snapshots`. November13 saved 56 changed native records; November14 saved 110; both first passes finished linking without new saving/linking errors. The 17:45 November13 repeat fetched the same 48 class payloads; its scheduled saver found zero changed Git files and made zero business writes. Native Data run readback confirms Succeeded, records 0, errors 0. Operational run/log writes remain separate from business-data writes. November15's source preparation failed because config omitted `_t`; it saved no false payload and is not claimed recovered. No manual collectors ran. Full receipts/remote/native readbacks: `C:/Users/gombc/Documents/Codex/outputs/wef-git-latest-20261112/scheduled-verification.json`.


## Latest-version Git index and access-only alerts — November 12, 2026

Owner approved using existing ringstatus-data with a latest index for the Airtable consumer and prioritizing speed. The sole existing independent saver Git writer publishes root `index.json` on `codex/fast-snapshots` before consuming data. The index is keyed by customer/show/date and records the latest successful schedule/class file paths with original observed-at timestamps, current pass/run/time and redacted errors. Failed or omitted payloads retain last good paths/timestamps. Git commits data and index together; Airtable mapping reads paths/payloads/times from that exact commit and skips unchanged data. It no longer reopens local class capture bodies when indexed data/time is supplied. Full local captures/history remain.

No second Git writer or new task, timer, gate, source retry or concurrency setting. Source requests, URLs, pause, native keys/results/links, independent collection/saving/linking and same-date sequencing are unchanged. This adds the explicit latest-index contract; collection speed must still be measured from scheduled receipts rather than assumed. Legacy in-flight capture consumption remains compatible.

The existing hourly alert scope now documents individual class errors without class-level failure/recovery notices. Whole schedule/class-lane access failures and actual saving/linking/logging/cleanup/observation failures remain reportable. No error history or failure checkbox was cleared. Cadence/single health request is unchanged and the automation update was read back.

Eleven offline producer/consumer/Git tests and syntax passed; single-agent Code Review COMPLETE/PASS before installation. Natural scheduled acceptance is pending. Evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-git-index-20261112/`.


Scheduled index acceptance: November14 attempt `25e171e9-40ba-432f-9118-e4f0c32ccb38` collected 94 classes in40.484seconds; independent saving took6.955seconds and made0 business writes because payloads were unchanged. November15 attempt `d2186a6b-d3c3-4784-9e7c-22595e07f82d` collected4 in8.234seconds, saved in5.579seconds with0 business writes. Native run readback: both Succeeded/0errors. All98 class bodies and original observation times matched their exact indexed Git commit. Both passes linked by23:17:26.805Z; saving/linking log delivery succeeded. Full November14 collection-to-link time was308.052seconds (5m8s), including existing scheduled stage waits and linker reads. The index is working; this is not a claim that the complete workflow finishes in47seconds. Source loop/task cadences were preserved. Evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-git-index-20261112/scheduled-verification.json` and `native-readback.json`.


## Approved ready-input triggering — November 12, 2026, 9 p.m. Eastern

The existing collector now requests existing `wef-data-save` after a completed capture, including captured partial results. It does not wait for saving. Each actual date's completed saving receipt immediately requests existing `wef-data-linking` before the saver processes another date or delivers logs. It does not wait for linking. No source invocation or data retries are added. Skipped/Deferred source/no finished save requests no new stage.

Both existing stage tasks use Windows' built-in Queue instance policy so busy-stage wake-ups are retained without overlapping copies. Their old periodic triggers were disabled after natural readiness proof; tasks remain enabled on demand. Collector task XML/cadence is unchanged. Stage task actions, principals, credentials and every other setting match the original exported XML; comparison passed. No new task, custom queue, service or runtime data gate. The same actual date still waits until its prior saving and linking are complete. A task-start failure is retained as an actual trigger error with no self-retry.

Scheduled proof: November14 attempt `1e669843-19c4-4c7e-a6ed-681ff5fca541` collected94 valid classes, saved79 acknowledged business writes and linked in101.574seconds (1m42s), compared with the earlier same-date94-class308.052seconds (5m8s). Source completed02:00:53.977Z; saver accepted the ready request02:00:54.078Z, began02:00:55.490Z, saved02:01:13.081Z, requested linker02:01:13.400Z; linker began02:01:13.685Z and completed02:01:59.103Z. The latter starts before the old periodic linker boundary.

After timer removal, selected Day0/November13 attempt `4744b050-2e37-445a-b839-e7cc044dc9bc` collected48 classes, saved3 acknowledged writes and linked in91.294seconds (1m31s). Native runs for both are Succeeded/0errors. Subsequent same-date collection began02:06:15.359Z, after prior link completion02:04:46.734Z. Saver/linker log delivery succeeded; no trigger errors. A later November14 pass had individual source class failures, and November15/next November13 preparation had the existing missing-_t error; these were retained and are not claimed fixed by trigger changes. Individual class failures remain documentation-only. No failed marks were cleared, no manual source or collector runs occurred.

Review: fifteen offline Node/Python tests, syntax and effective task-definition comparison PASS; single-agent Code Review COMPLETE/PASS before installation. Evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-ready-triggers-20261112/`, including `scheduled-verification.json`, `native-readback.json`, exported final task XML and `review/`. Unchanged linker reads still take time; this change removes timer waits, not its mapping/read implementation.
