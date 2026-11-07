# Cleanup workflow and remaining controls — deployed-code inspection

Scope: read-only explanation requested by owner. No implementation changes made for this inventory. Source: deployed cleanup.js, cleanup-airtable.js, cleanup-fields.js, cleanup-local-logs.js, cleanup-run.js, registered task settings, and actual Records automation prompt. This lists controls present in the Data implementation, including retained controls from earlier versions; it does not label every control newly added by the latest diagnostic change.

## Workflow as instructed

- Records alerts invoke the existing independent cleanup task.
- A new calendar day clears logging records; a new show clears logging and show records.
- The existing three-hour logging check applies the requested 1,000/oldest500 rule to logging only.
- cleanupV2 supplies selected table names and receives latest outcomes. cleanup and cleanup-logs are no longer runtime control/outcome destinations.
- Airtable records and confirmed-delivered local log files are deleted. No record backups, snapshots or archives are created. Pending undelivered logs remain excluded under the owner's explicit approval.

## Actual remaining controls

| Control | Actual behavior / consequence | Source |
|---|---|---|
| Records producer identity/alert shape | Requires existing producer ID, supported event type, positive show ID, calendar-date text, nonempty event ID and parseable issue time whose Eastern date matches the calendar day. Invalid input ends that invocation. | cleanup-run.js alertShow; cleanup.js cleanup entry |
| Records source/release readiness | Live prompt requires matching deployment/review references, eight installed dependency hashes, schema binding projection, V2 native binding and task binding. A mismatch can prevent an alert. This is an additional dispatch barrier, separate from deletion. | actual Records automation prompt / consumer-readiness.json |
| Records prior state rules | Live prompt snapshot still included skipping active/stopped/uncertain/handled/requested events, preserving failed stops, and requiring failed-stop release authority. Data's stop-file requirement has been removed; removal of these corresponding producer instructions was not yet verified at this inspection. | actual Records automation prompt |
| Records check duration | Existing reminder prompt limits the check to two minutes. This is not a source-collection timeout, but can leave an alert unevaluated. | actual Records automation prompt |
| Canonical show | Exactly one local w-shows default for customer15 with a positive valid show ID; it must match the alert. Read again before table deletion and every ten-record delete batch; a changed show ends that table action. | cleanup.js canonical/execute; cleanup-airtable.js retireCleanup |
| Table selection plus hardcoded allowlist | A table must both be checked in cleanupV2 and occur in the code's TABLES list. Unknown/unlisted tables are ignored even if checked. API reads/writes have another allowed-target list; summary writes permit only cleanupV2. | cleanup.js run.selections/selected; adapter readable/writable |
| Event/due deduplication | A successfully handled event is skipped. New-show must differ from saved show; calendar day must advance; log-check issue time must advance. A non-show event also requires the same saved/current show. Success checkpoint is written only when the combined result is Success. | cleanup.js due / handled branches / cleanup-state |
| 1,000/500 logging rule (requested) | Total table size must reach1,000 and there must be at least500 eligible rows with valid creation timestamps. Exactly500 are selected oldest-first; adapter rejects a quick-cleanup request of another size. | cleanup.js quickRows; retireCleanup |
| Completed-record eligibility | run-logs/runs must have a terminal status and parseable completed time; err-logs must have parseable occurred-at. Heartbeat rows are always ineligible in completed(). | cleanup-fields.js completed |
| Current/native-record protection | Protects heartbeat rows, heartbeat last-run links, latest run per workflow, unfinished runs and their linked errors, explicitly protected IDs, errors without known native parents, and new logging rows absent from the prepare snapshot. | cleanup-fields.js protectedRows; cleanup.js protect; adapter prepare |
| Pending-family protection (requested) | Exact local current run/error payload hashes are compared with delivery acknowledgments. A pending family protects its matching Airtable parent and errors, not just its local files. | helper pending; adapter prepare |
| Pre-delete record comparison | Before each batch, expected/current counts and IDs, normalized field values and current eligibility must match. Any mismatch rejects that batch and ends that table action. This check remains; it produced the historical generic error. New diagnostics identify reasons, IDs and field names. Other table actions can continue. | adapter retireCleanup |
| Delete-response and absence confirmation | DELETE must confirm the exact expected IDs and deleted flags; subsequent GET must show them absent. A mismatch/error ends that table action after any already-confirmed deletions. | adapter retireCleanup |
| API/time/error checks | Existing HTTP calls have30-second timeouts and require successful HTTP/JSON responses. Summary PATCH must return the expected control-row ID. Generic adapter upsert helpers also check unique identities, reconciliation and readback. HTTP/read errors can end preparation; per-table/summary failures are logged with overall Error. | cleanup-airtable.js call/upsert/verify; cleanup.js report/execute/catch |
| Local pair-selection condition | Local family cleanup is enabled only when both run-logs and err-logs are selected. Selecting only one disables local family deletion. | helper request.enabled |
| Local workflow/file checks | Only existing Data/Data-linking roots in the request are serviced; base/workflow must match, IDs must pass path validation, and JSON must be readable. Failures are logged; mismatches cause files to be retained. | helper request/prune/pruneDelivered/location/rows |
| Extra local retained history | Local newest attempt, latest successful attempt and newest error's parent are retained. A completed family is also retained if an owner/child PID is alive or completion/status is unsuitable. These are additional exclusions beyond merely requiring delivery. | helper pruneDelivered keep / alive |
| Local delivery proof (requested) | Every run and every linked error in a family must have an acknowledgment matching its exact current outgoing payload; missing/malformed/mismatched acknowledgments retain the whole family. | helper pruneDelivered / ack / runFields / errorFields |
| Local same-request markers | Existing root receipt or service status Started/Success/Error for the same request prevents another local service attempt for that request. These bookkeeping markers remain; they are not the removed global cleanup-stop file. | helper prune |
| Combined completion waits for local receipts | Cleanup waits until every requested workflow service reports Success/Error. A different request ID is an error; a registered service process ending without a terminal receipt gives Error with unknown counts. A Pending service or still-running process can keep the cleanup invocation waiting. | helper wait; cleanup.js local.wait |
| Existing task overlap rule | Windows task IgnoreNew does not launch a second cleanup instance while one is running. Existing collector/linker IgnoreNew likewise serializes their local-file service with delivery. No replacement lock was added. | existing registered tasks / register-cleanup-task.ps1 |

## Removed and not present

- cleanup.js no longer reads, creates, writes or deletes cleanup-stop.json.
- No new wall-clock local-cleanup cutoff was installed.
- No new schedules, record backups or archives were introduced.

## Consequences

Removing the global stop file did not remove the pre-delete record comparison, Records readiness barriers or local receipt barrier. Pending preservation and the1,000/500 rule were explicitly requested; they should not be represented as unrequested extras. Other listed restrictions must be shown openly rather than described as a gate-free implementation.

The current resumed invocation was still running at inspection; the old last-run receipt was not proof of its outcome. The linker local receipt already showed7run files/10error files deleted successfully; the collector local service was Pending at that moment.

Subsequent outcome: run1794059296142 completed Success at2026-11-07T13:56:46.115Z, task exit0. Fresh V2 readback1036run logs/190errors deleted; localData7runs2errors, localLinker7runs10errors, allSuccess. This closes the pending execution observation, not the gate-removal concern. No broader controls were removed while preparing this inventory.
