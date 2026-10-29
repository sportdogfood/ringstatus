# Attempt/error logging release - 29 October 2026

Result: PASS for the approved schedules logging delta.

## Delivered

- Separate error-log table tblntJGnYFpLxjFSr in schedules base appUq5Qz9kARqFija, with nine checked fields: error-id/run-id/stage/recorded-at/started/date/show-id/message/run-log-url. Schema saved/read back; no business table fields or unrelated schemas changed. run-logs description now accurately explains launched-attempt coverage.
- Stable wrapper attempt ID/start receipt precedes environment and Node checks. New logging entry creates the same remote run-log before product imports or collector lock checks; existing business source/normalization/store/reference work is unchanged.
- New early failure evidence remains in small local per-attempt receipts if remote/startup dependencies are unavailable. Later NORMAL runs publish only marked new-attempt metadata, never rerun source collection or repair historical old logs. Exact-evidence ACK, original timestamps, maximum ten pending attempts per invocation, error replay independent of terminal run status and all retained receipt files prevent loss/duplication/starvation.
- Separate error rows retain context, normalized timestamps, secret redaction and current-slot run-log URL. Deterministic keys reconcile uncertain POST outcomes without repeating POST. Bounded error slots archive/read back exact old records before reuse.
- Logging mutations serialize separately from the unchanged business lock. Invalid/dead logging locks fail closed for inspection, not automatic clearing. No new tasks, task/history settings or cadence changes.

## Verification

165 full offline tests PASS, including actual logging-entry inert adapters, copied PowerShell missing-env/native launch failures, success, lock failure, dated failure, lost response, timestamp normalization, long-outage/error recovery, exact null identity, three independent logging processes, unknown receipts/ACKs, and archive-before-removal. Independent reviewer found no remaining Critical/Important issues; review.json pins the seven reviewed product files and 31 independently run fixture checks.

| Normal start Eastern | Attempt ID | Airtable terminal completion Eastern | Result |
| --- | --- | --- | --- |
| 11:20:02.703 | bb74c7e7-93f4-43b7-8a7c-359841e890e0 | 11:20:57.900 | Success |
| 11:25:02.424 | a1c743ee-127a-437c-9409-718742f9b9b6 | 11:25:49.008 | Success |

Both actual wrapper receipts completed exit0 and match exactly one Airtable run-log by run ID/normalized original start. Latest heartbeat matches. The second normal process ACKed the first receipt without rerunning collection. Both collected all four dates and504 active records with zero failed dates. Schedule stage keys/class IDs/native show/ring links and all504 schedule,56 ring,3 local show and14 helper IDs/static fields were preserved. Eleven logged archives were decompressed and matched their SHA256. Before/after/final task XML is identical. Protected collect.js, completion-guard.js, store.js and settings.js match baseline; no business collector/normalizer/current-store/reference logic edits.

Error-log is empty in both observed runs because they were healthy. Failure behavior is offline fixture-proven only; no production failure/lock collision/outage was manufactured and no manual collector or source run occurred. No historical record repairs. Actual error posting under a future natural failure is not claimed already observed.

## Limits retained honestly

- Airtable cannot receive logs while access/credentials/network are unavailable. Local receipts preserve new launched attempts for later normal publication; local storage failure cannot guarantee even a local write.
- Unlaunched/suppressed Windows triggers and an unavailable/sleeping PC are not fabricated collection attempts. Unclosed outcomes stay unknown; no forced successful completion.
- A dead/invalid logging lock requires authorized inspection. The implementation never guesses an expiry or clears it automatically.
- Direct run-log URLs point to current Airtable slots. Archived older runs remain identified by stable run-id and verified history after slot reuse.
- Existing show/source/archive/local-host dependencies and independently documented CWF task-lifecycle issue are not changed by this schedules release.

## Locations

Editable source: C:/Users/gombc/.codex/worktrees/wef-schedule-collector/ringstatus/wef-schedules

Deployment: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-schedules

Evidence in this directory: baseline.json/before/, plan.md, schema.json, review.json, offline-tests-final.txt, deployment.json, task-before.xml/task-after.xml/task-final.xml, business-before.json/business-latest.json, normal-run-1.json/normal-run-2.json and archives-verified.json. verify-normal.cjs is GET-only acceptance inspection, never collection. Private runtime credentials were not copied or printed.
