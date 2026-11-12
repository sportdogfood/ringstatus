# Fast operating README

Date: **November 12, 2026 — America/New_York**  
Document/release version: **2026.11.12.1**  
Operational owner: **wef-decoder**, chat `01a1a753-dce7-7951-a147-1b096e1538ba`  
Collector lock: [FAST-LOCK.json](FAST-LOCK.json)  
Exact accepted workflow and dated results: [fast-test.md](fast-test.md)

## Ownership

The owner deprecated **wef-fast as the operational owner** and instructed wef-decoder to retain this workflow, own wef-data cleanup and remain responsible for show-times. This does not disable the working Windows collector or its separate saving/linking tasks. Windows tasks execute the business workflow; Codex inspects, makes explicitly approved changes, reviews and verifies.

On November 12, owner-directed notices were submitted to `wef-fast` (`01a19273-bdf8-71b2-8503-cc8079efc129`) and `wef-records-coordinator` (`01a169bc-e58a-7de0-94c6-5d3931fcf745`). Fast was asked to identify every remaining responsibility not already covered. Records was asked to identify cleanup/alert routing still directed to deprecated Fast. Both recipient chats acknowledged the change. Records confirmed that its existing cleanup reminder still targets deprecated Fast; redirecting future notices is pending and was not performed by these messages. No additional chats were notified.

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

**Confirmed gap:** `cleanup-lane.js` scans local `runtime/logging/wef-data` and `runtime/logging/wef-data-linking`. The active Fast outboxes are under `runtime/fast/day*/logging`, `runtime/fast/fast-save/logging` and `runtime/fast/fast-linker/logging`. Those new local paths are not covered by that scanner, including its protection of pending delivery. Capture/save/link receipt retention is not established by the existing scanner either.

The independent cleanup consumer can be adapted to those actual paths without editing the frozen collector. That adaptation is not implemented or authorized by this documentation. Records' existing cleanup routing to deprecated Fast must also be reconciled with the new owner before claiming the event handoff works end to end.

## Failure logging and owner alerts

Collection, saving and linking retain actual redacted errors and deliver their own results through the existing Data `run-logs`, `err-logs` and `heartbeat` tables. A source failure, saving failure and linking failure are distinct outcomes. The scheduled UTF-8 saving correction was verified using the actual Windows Python and native Airtable readback.

**Confirmed gap:** the legacy desktop/Records notifier `collection-alerts.js` is called by old `entry.js`; the current `fast-entry.js` does not call it. It expects the old report shape. Error records and successful log delivery do not prove that an owner failure notice was sent.

A separate consumer of the existing native run/error records can provide owner alerts without changing collection. Its deduplication, delivery and recovery behavior still need an explicitly approved implementation and verification; no such consumer was created here.

The existing hourly `check-wellington-decoder` automation covers decoder schedule health and passive tunnel connection transitions. WEF Shows `warning-log` remains its notification authority, mirrored to `decoder-fail-alerts`; submitted notices are not owner-read acknowledgments. That monitor does not establish Fast class/saving/linking alert coverage.

## Show-times — owned here, known wrong-date link

**wef-decoder remains responsible for show-times.** The owner's reported row is still incorrect:

- Table: WEF Shows `show-times`, `tblBJ8DwRklrmDNb5`.
- Record: `rec70KnNzBms3sj1M`.
- Key: `15|200000078|2026-11-10`.
- Actual linked day: November 11, `recSDxcUJg9vRSoW0`.
- Exact November 10 show-day lookup for this show returned no record.

The cause is explicit in `workers/wef-schedules-decoded/show-time-health.mjs`: the producer falls back to the selected day when a matching day is absent, then uses that fallback for the source-date observation. The row notes confirm that substitution. Its source timestamp and November 10 key were preserved; the linked date was not.

This is an **open defect**, not a verified link correction. Exact-date matching and the disposition of the already incorrect historical link need an approved correction; do not manufacture a canonical show-day or silently repair the business row. Show-times is separate from the frozen Fast collector and is not represented as a verified component of this lock.

## Verification and evidence

The scheduled November 12 11:45 Eastern pass was verified through separate saving, linking and logging. Day 1 collected 51 responses, acknowledged 2,432 business writes and had zero new saving errors. All four in-range dates had successful native Data run records; total acknowledged business writes were 2,952. Final linking completed at 11:53:28 Eastern. Same-date waiting and independence between dates were observed through subsequent natural starts. Day 4 was outside the selected show date range.

Day 0 used confirmed Live coverage. Days 2–3 retained earlier failed-class exclusions; this does not claim those endpoints recovered or that all historical class details are populated. Counts are writes, including updates, not unique/new record counts.

Evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-fast-md-exact-20261112/per-date/utf8/`, especially `scheduled-verification.json`, `native-readback.json` and `independent-review/`. Earlier failed releases and owner approvals remain in their dated packets. The current lock is a snapshot; compare actual hashes before any approved change or acceptance claim.

## Additional responsibilities returned by Fast

Fast identified three relevant outstanding checks: failure-alert delivery; activation/recipient of the existing periodic Data code review; and propagation from saved Data to the shared schedule app. These are not established by the collector’s native saving/linking proof. Keep them as open checks rather than silently claiming completion or starting unrelated work. The deprecation notice authorized an inventory and acknowledgment, not additional implementation.
