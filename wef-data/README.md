# wef-data — current scheduled collector

**README version:** `2026.11.01.2` (documentation revision).

**Updated:** November 1, 2026 (America/New_York).

This README describes the deployed workflow, superseding the older multiday/recovery descriptions. Production directory: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data`. Airtable: [wef-data](https://airtable.com/appdl16HJ5ETm8N69).

## Results and independent linking deployed - November 1

Current release deployed at **8:40:56 a.m. Eastern**. Working fast `scheduled.py` startup bytes, source error handler, source endpoints, four class workers, keys, existing data and collection cadences were preserved. The existing linking task was enabled after the first normal fast run passed; its task definition, interval and offset were unchanged. No manual collection, task start, stop clear, new adapter, quality gate, extra request delay or historical run scan was added.

Fast collection now saves the existing result-field mapping from the same class responses: result-key equals trip-key; class status, placing/position, scores, times/faults, points/prize money, gone-in and scratch retain established zero/null handling and source observation time. It continues existing ring/group/class/entry/trip/schedule scalar updates. It does not refresh horse/rider/trainer/owner names or add entity endpoints. Native relationship writes are separate.

The normal **8:42 a.m.** and **8:45 a.m.** fast runs each saved **579 records with zero errors**, including 160 results. Collection/save elapsed **53.243** and **62.084 seconds**; results saving was **14.828** and **12.669 seconds** respectively. These are measured runs, not a guarantee of future latency or zero overhead.

The normal **8:43 a.m.** and **8:46 a.m.** linking runs succeeded with **zero unresolved targets**: 1,096 relationship updates in 59.257 seconds, then 435 in 50.549 seconds. The second pass updated newer collection-run relationships. Linking reads saved eligible Data facts and existing native keys, including the synced Shows/day and w-* master links, with its own Python/Node locks, report and operational outbox. Missing native entity keys can be reserved using the existing key-only upsert; existing names and scalar fields remain unchanged. Synced/master tables are read-only and Airtable maintains reciprocal links.

The existing per-day cadence metadata additionally stores the already-generated native run-key. Linking reads those at-most-four keys so later day-0 wakes do not erase day 1-3 provenance. No new state file or collection checkpoint was introduced. Relationships, including last-run and schedules runs/run-logs, are updated when the separate linking lane runs and can briefly lag scalar saving. Source timestamps are never renewed by linking.

Independent review passed 47 tests, both fresh private-Python startup imports and the frozen scope/schema checks. Native verification matched all **160 refreshed results** against their stored trip responses and verified class/entry/entity/results/master relationships. Before/after readback preserved **6,231 existing native IDs/keys and 32,089 nonmetadata relationship IDs**. Both operational heartbeats and all four release-proof run-logs reported Succeeded. Only November 1 is currently eligible as day 0; days 1-3 retain existing cadence behavior and offline coverage, with populated live proof unavailable for this short show.

Evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-data-results-linking-20261101` (`ACCEPTANCE.md`, independent report/log/receipt/verifier, deployment/task XML, scheduled fast/link first/repeat reports, native-verification.json, native-preservation.json and operational-native.json). The earlier failed release/restoration evidence below remains historical; this release supersedes the restored collector's missing results/separate-linking limitation.

## Working collector restored - November 1

Current status: **collection restored and verified through the normal schedule**. At 8:17:47 a.m. Eastern, the eight runtime files were restored to their exact working pre-change bytes. Only the added `wef-data-linking` task was disabled, before restoring the launcher. The existing `wef-data-collector` task XML and three-minute cadence were unchanged. Days 0-3 retain their existing 3-minute/hourly/2-hour/3-hour refresh rules. Source error detail handling, saved runtime progress, data, keys and links were preserved; no manual collection, task start or stop clearing was used.

The normal **8:18 a.m.** run saved **419 records, zero errors**, in **41.554 seconds** of collection/save time. Native Airtable readback verified fresh data for 8 rings, 52 classes, 160 trips and 54 schedule occurrences, including current-run links on rings/classes/trips. The native run-log and heartbeat both report Succeeded and are linked. Retained rows absent from this response remain stored; these counts do not claim every retained row was refreshed.

The failed October 31 field/link split was withdrawn from the active runner. This restoration returns the preceding collector; it does not deliver the separate linking/results expansion. Its inactive new modules remain on disk and the added task remains disabled. Do not rerun the registration helper to restore the existing task: the actual registered task remains PT3M. All failed release and independent-review evidence is preserved in the parent evidence directory.

Restoration work began **8:15:13 a.m.**, with native collection verification at **8:19:46 a.m. Eastern**: **272.855 seconds**, rounded to **4 minutes 33 seconds** in the complaint. The estimate was 10 minutes. Outage tracking starts at the first failed attempt, October 31 at 10:33:15 p.m. EDT; verified restoration was November 1 at 8:19:46 a.m. EST, including the daylight-saving clock change. The complaint's restored-at, actual-restoration-time, estimate and computed downtime were saved and read back. Complaint remains Open; service-restoration remediation is Verified.

Evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-data-field-linking-20261031/restoration-20261101` (`INDEPENDENT-REVIEW.md`, independent receipt/log/verifier, deployment.json, task-after.json, scheduled-report.json, scheduled-attempt.json, native-verification.json, operational-native.json and ACCEPTANCE.md). [Outage complaint](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/rec2OrJVMnPWVxwVB).

## Days 0–3 completion — October 31

Pre-change limitation: **Days 1, 2 and 3 are not being collected by the current workflow.** This was the state before the days 0–3 correction; the prior release below proved day 0 only.

The approved correction retains the existing three-minute scheduler and single-day collection/save behavior. Each scheduled invocation reads synced Shows/show-days and processes eligible priorities in ascending order. Initial discovery collects and saves day 0, then 1, then 2, then 3, where those days exist in the show. Every day finishes its collection/save attempt before the next starts. Days beyond the show's synced dates are not invented.

Day 0 runs on every normal three-minute wake. Day 1 is due one hour after its previous attempt starts; day 2 after two hours; day 3 after three hours. Future days run on the first existing scheduled wake at or after they become due. A long multi-day invocation may span a scheduled wake; the existing no-overlap behavior remains in effect. No additional tasks are introduced.

`runtime/day-cadence.json` stores only per-show/date attempt timestamps, outcomes and completion times. It is updated after each day returns, including a reported Partial attempt. Successful earlier days retain their cadence after interruption; a day interrupted before returning remains due. No class checkpoints, response replay, work queue, quality gate or old stop handling are added. The next collection of an eligible day uses the existing fresh single-day algorithm. Negative priorities are skipped on every wake, including after interruption.

Each day has its own existing `runs` record: day 0 retains `collection-<attempt ID>`; future days append `|<date>` so summaries and business last-run links remain distinct. The normal operational run-log/heartbeat describes the complete scheduled invocation. Its `counts-json.days` contains each attempted day's priority, date, start/completion, outcome, run key, saved count, errors and stage timings. Errors still flow into the existing err-logs and unaffected days continue.

At the initial October 31 discovery verification, the show ended November 1 and only priorities 0 and 1 were eligible. The later 8:45 p.m. scheduled proof selected November 1 as priority 0 from the synced show-days. Priorities 2 and 3 require an eligible longer show for populated live proof; four-day selection/cadence behavior was checked offline without creating show-days or manually collecting.

Candidate/review/deployment/normal scheduled acceptance evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-data-days-20261031`. Do not treat offline checks as live cadence proof. See `ACCEPTANCE.md` there for the actual completed and remaining checks.

## Previous verified day-0 deployment

Deployed October 31 at **2:11:15 p.m. Eastern**. Only `data_runner.py` and `entry.js` changed. The existing scheduler, launcher, schema file, source adapter and logging transport were preserved.

| File | SHA-256 |
| --- | --- |
| data_runner.py | bb91828159be6997accbfe417fccabc30a0a05fe7f0b9134b51409fe1c8f40b2 |
| entry.js | 7e27ac7462019006dd0456ac198a8457ea68ce1bc44e5277368bad79bb9c4d16 |

The approved correction restored existing schedule occurrence writes, ring freshness timestamps, class status/count fields and current-run links for rings/classes/trips. Each invocation creates one simple record in the existing `runs` table. This does not restore the old checkpoints, queues or completed-revision machinery.

Two normal scheduled runs were verified directly against Airtable:

| Start, Eastern | Collection/save | Full PowerShell runner including logging | Business records | Errors |
| --- | ---: | ---: | ---: | ---: |
| 2:12:14 p.m. | 51.935 seconds | 57.143 seconds | 642 | 0 |
| 2:15:14 p.m. | 56.551 seconds | 62.811 seconds | 642 | 0 |

Each observed run saved 8 rings, 42 groups, 70 classes, 165 entries, 284 trips and 73 schedule occurrences. These are observed totals, not fixed expectations. The simple `runs` record is metadata and is excluded from the business-record count. Existing record IDs, keys, protected fields, unrelated links and original saved response/control files were preserved.

Independent review passed 11 Python and 12 Node/source checks plus a saved-source fixture comparison with the established schedule mapper. Offline checks, deployment and actual scheduled proof are separate evidence.

- [Full deployment and native acceptance](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/fast-collector-20261031-candidate/FIELD-CORRECTION-LIVE-ACCEPTANCE.md>)
- [Exact deployment receipt](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/fast-collector-20261031-candidate/field-correction-deployment.json>)
- [Independent review, execution log, receipt and verifier](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/fast-collector-20261031-candidate/FIELD-CORRECTION-INDEPENDENT-REVIEW.md>)
- [Combined scheduled/native proof](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/fast-collector-20261031-candidate/field-correction-scheduled-acceptance.json>)

## Existing runner and cadence

Windows task `wef-data-collector` repeats every **three minutes**, with no overlapping instance. Its existing action remains `wscript.exe` → `run-hidden.vbs` → `run.ps1` → `entry.js` → `scheduled.py` → `data_runner.run()`.

`run.ps1` supplies a fresh GUID attempt ID on every invocation. `runs.run-key` is `collection-<attempt ID>`; operational `run-logs` stores that same run key. The supported business path is this existing schedule. Codex does not manually invoke collection, start the task or repair native records as workflow proof.

## Collection steps

1. Read the default show from synced **wef-shows.shows inside wef-data**. Read its linked **wef-shows.show-days inside wef-data** and select **day-priorities 0–3**, in order. Apply the per-day cadence described above; skip negative priorities.
2. Fetch that date from the existing decoded `/schedule` endpoint. Save rings, groups, the class list and every schedule occurrence, including breaks.
3. Fetch **every current class**, including classes with zero entries, from the existing decoded `/classes/{id}` endpoint, using the existing maximum of four concurrent requests. Entries and trips arrive together in these class responses; no entry-directory request is made.
4. Save complete class/trip source JSON and the authored scalar/link fields in batches of up to ten records. Existing stable keys match existing native IDs; only missing keys are created. Source-owned fields sent in the update are replaced; unrelated fields/links are omitted and preserved.
5. Finish each day’s simple `runs` record before the next eligible day. Finish operational logging for the whole invocation. Each selected day uses fresh collection; the cadence file only determines when future days are due. No deferred queue, quality comparison or global stop controls this active path.

Source base remains `https://wef-schedules-decoded.gombcg.workers.dev`. No new adapter or endpoint was added.

## Authored data and keys

| Table | Stable key | Current authored behavior |
| --- | --- | --- |
| rings | show key + date + ring number | ID/number/name, schedule observation time as last-success, current last-run |
| groups | ring key + group ID | ID/name/ring link, existing subset of schedule source JSON, last-success |
| classes | ring key + class ID | ID/number/name/ring/group links, full class JSON, status, total_entry_trips as entry-count, last-success, current last-run |
| entries | source show ID + entry ID | Entry ID/number and last-success, derived from class trips |
| trips | class key + entry ID + source UUID or trip ID | Existing trip scalar values, full trip JSON, class/entry links, last-success, active status, current last-run |
| schedules | ring key + class_groupxclasses_id, otherwise position-N | Every ordered class/break occurrence, show/day/ring/group/class links, position, source ID, name, start/end times, full occurrence JSON and last-success |
| results | Same existing trip key | Result values, class status, result JSON and source last-success in fast; trip/class/entry/last-run relationships in linking |
| runs | collection-attempt ID | One simple invocation record with started/completed/status, acknowledged business-record count, error count and elapsed time |

Schedule identity and mapping reuse `wef-schedules/tests/build-wef-data-graph.py:inventory_graph` (established commit `873b36e80`). The collector uses that mapping without invoking its old validation/work-item wrapper. A unique class can appear in more than one schedule occurrence. Retained rows absent from the latest response are not deleted or automatically treated as current membership.

Business `last-run` fields on rings/classes/trips/results link to **runs**, populated by the independent linking lane. Heartbeat's `last-run` links to **run-logs**. They are different tables; both now identify the same invocation through its run key.

## Schedule columns exposed in Airtable

The owner requested these five existing source values as visible columns on `schedules`: `class_type`, `completed_trips`, `results_verified`, `schedule_sequencetype`, `total_trips`.

They are Airtable formula fields reading each row's existing `source-json`. Text remains text; counts and the source verification flag remain numbers, including zero. A missing value remains blank. No collection filter, skip policy, additional source request, source rewrite or new runner deployment is associated with these columns. The existing show/day/ring/group/class link fields are retained alongside them.

| Column | Airtable field ID | Result |
| --- | --- | --- |
| class_type | fldmSqiJ2yhFBxIFE | Text |
| completed_trips | fldFAkxRMNUXbWzbt | Integer |
| results_verified | fldCYzMbV7UJ2UEpw | Source integer flag |
| schedule_sequencetype | fld5uxF2I12jXiGea | Text |
| total_trips | fldrcMRlyzeR5me1Q | Integer |

[Field definitions and live verification](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/fast-collector-20261031-candidate/schedule-field-exposure-proof.json>): all 2,655 field values across 531 stored schedule rows matched their raw JSON, including zeros and absent values. Existing IDs, keys and the five parent links remained unchanged. This is column exposure, not a speed-policy change.

## Class array columns exposed in Airtable

The owner selected only the 12 array columns from the supplied class JSON. These formula columns read complete arrays from the existing compact, sorted class source-json; no additional scalar columns or collection rules were added:

`cdm_data`, `class_group_order_of_go.entries`, `class_related_data.class_prizes`, `class_related_data.class_split_codes`, `class_related_data.judges`, `class_related_data.score_fields`, `my_entry_ids`, `phase_columns`, `phase_order_of_go`, `phase_rows`, `team_scoring_result`, `trips_json`.

The original trips linked-record field is unchanged. Empty arrays remain `[]`, and all array elements stay together in one JSON text column. Formulas use the established JSON sibling/end-of-parent boundaries; source-format changes require checking those mappings. [Field definitions and native verification](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/fast-collector-20261031-candidate/class-array-exposure-proof.json>): all 4,872 array values across 406 saved class records matched the source arrays. IDs, keys and checked ring/group/trip links stayed unchanged. Collector runtime hashes and scheduling are unchanged.

## Operational logs

| Table | Contents |
| --- | --- |
| [run-logs](https://airtable.com/appdl16HJ5ETm8N69/tblKMNAtvvIFSvgLh) | Invocation, status, start/completion, elapsed time, business run key, saved count and per-stage seconds in counts-json |
| [err-logs](https://airtable.com/appdl16HJ5ETm8N69/tblhPcSD6UPp5jkCa) | Errors linked to the operational attempt |
| [heartbeat](https://airtable.com/appdl16HJ5ETm8N69/tbl2r6hhTnlZh3leX) | Latest observed status/attempt/success/error and operational run-log link |

Local runner output is in `runtime/collector-YYYY-MM-DD.log`; the latest report is `runtime/last-run.json`. Stage measurements use a monotonic clock. Collection/save duration excludes process startup and final log delivery; the PowerShell START/END window includes them.

## Errors and preserved boundaries

Successful work is saved. Item/batch errors are logged; unaffected collection/saving continues and incomplete attempts report Partial. Failure to record run metadata does not block other collection or clear prior run links. Missing show/day selection or an unreadable schedule cannot produce dependent work and is reported as incomplete. No control file is created to prevent subsequent normal scheduled attempts.

Fast authors the existing ring/group/class/entry/trip/schedule fields, results and runs metadata. Independent linking maintains the requested existing native relationships and can reserve missing native entity keys; entity names/scalars are not refreshed. Synced Shows/day and w-* masters inside Data are read-only. Other collectors, Season transfers, retention, archiving and historical cleanup remain excluded.

Show/day and ring aggregate status/count columns, groups.sequence, class fingerprint/last-change and other enrichment fields are not refreshed by this correction. Existing values remain retained; a successful run log does not establish freshness of every field in every base table. Rendered UI behavior was not part of the live acceptance. Future show/day native-parent creation is outside this delta; schedules currently link to the existing parents.

The narrower payload proposal and synced Live matching remain on hold. The days 0–3, duplicate-selection/timing and error-detail corrections are deployed; their verification limits are recorded below. The failure record does not open a new implementation task.

## October 31 agent failure record

Owner-requested record for Data chat `01a19273-bdf8-71b2-8503-cc8079efc129`. These are failures evidenced in this chat and its saved release/review artifacts, including owner corrections. They are not authority for further edits, cleanup, retries or changes to another collector.

| Failure from today | Exact reminder to apply |
| --- | --- |
| Treated the day-0 implementation as the finished workflow and omitted discovery/refresh for days 1–3 until the owner raised it again. | **Reconcile the entire accepted goal with later corrections; never call a completed substep the completed task.** |
| Retained an unused optional day-0 selection path after adding selection in the scheduled wrapper. | **Keep one selection path; preserve working behavior without adding duplicate fallback implementations.** |
| Moved existing top-level stage timings into per-day reports without preserving the original log format. | **Preserve existing consumers and log contracts. Additional day detail is not permission to move existing fields.** |
| Logged only a generic transport label and exception type. The eight 7:03 p.m. source failures consequently lost the original message/cause, preventing a supported root-cause explanation. | **Preserve actual error messages, nested causes and connection codes in the existing logs, with credential redaction. Do not claim a cause the saved evidence cannot establish.** |
| Claimed an unverified downstream `wef-shows` update responsibility when the owner had only required reading the synced tables inside Data. This was a false scope statement; no such writes were added. | **Read synced Shows/show-days inside Data only. Do not invent Shows writes, count publication duties or another owner's integration work.** |
| Kept the chat open with repeated waits/polls to observe the first hourly refresh after deployment and initial scheduled proof, until the owner objected to time/token use. | **Use the next necessary normal scheduled outcome and relevant offline checks; state remaining live-proof limits. Do not self-impose a full-hour wait. Label agent work time and collector runtime separately.** |
| The field-comparison discussion produced a 54-row mapping inventory that did not meet the owner's comparison need; the owner supplied direct synced Live tables and then put matching on hold. | **Use actual available tables and identities for the requested comparison. Do not multiply representations or turn discussion annotations into runtime rules. Keep held work held.** |
| Initially confused nonzero array instances with nonzero field values when applying the owner's ignore request. | **Distinguish `[0]`/`[8]` array positions from values. Use `[*]` as the field template; match trips by identity, never equal array positions.** |
| The first error-detail candidate's compact JSON was damaged by a second pass of existing credential redaction; an editing pass also corrupted an existing UTF-8 test literal. Independent review caught both before deployment. | **Test the complete existing source-to-log path, including redaction, and compare the final diff with the baseline. Preserve original test fixtures and use explicit UTF-8. A passing isolated formatter test is insufficient.** |

The duplicate-selection and timing-format correction is deployed in `data_runner.py` SHA-256 `80191995a0d382c5c48de7c9a86a7a6ada3fc3602cc1068b324b6993ca931a99`. The error-detail correction is deployed in `source-reader.js` SHA-256 `b2a9872b28b186a05987323d476797bb2ad543954d1903e9e2ebd1ab19545ae6`. These replace the earlier limitations; do not reimplement them from this failure record.

Evidence: [timing correction and Partial collection outcome](C:/Users/gombc/Documents/Codex/outputs/wef-data-timing-20261031/ACCEPTANCE.md), [independent error-detail review](C:/Users/gombc/Documents/Codex/outputs/wef-data-error-details-20261031/INDEPENDENT-REVIEW.md), and [error-detail deployment and scheduled acceptance](C:/Users/gombc/Documents/Codex/outputs/wef-data-error-details-20261031/ACCEPTANCE.md). Error-detail propagation passed independent end-to-end offline checks; the 8:45 p.m. scheduled run saved 484 records without errors, so that live run did not exercise a natural failure. Historical discarded causes cannot be reconstructed. Existing error-message length limits remain; do not promise unlimited traces.

Apply the reminders at the [frequency required in this agent's instructions](AGENTS.md#this-data-agents-failures-and-required-reminders--october-31-2026): startup/resumption, each edit/review/deployment/delivery boundary, every correction/failure, and each 10 minutes of active work. These are agent execution checks, not scheduled reminder jobs, owner babysitting or collection gates.


## Complaint records and document revision

README revision `2026.11.01.2` records the current deployed corrections, this agent's dated failure account and the required scope-reminder frequency in [AGENTS.md](AGENTS.md#this-data-agents-failures-and-required-reminders--october-31-2026). This is a documentation version, not a collector, dependency or plugin version change.

Ten individual failures were added one record at a time to [agents → complaints](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U). Each was read back and verified, remains Open, and includes the complaint, corrective reminder, source evidence and actual remediation state. The final local failure-table entry contains two distinct defects (redaction/cause loss and UTF-8 fixture corruption); these have separate Airtable records. Documentation and recorded remedies do not establish prevention of recurrence.

Latest deployed source-error correction: October 31, 8:44 p.m. Eastern. Its normal 8:45 p.m. run saved 484 records with zero errors; actual error-message/cause delivery was independently verified end to end offline, not exercised by a natural failure in that live run. See the linked release evidence in the failure record. That earlier documentation-only revision changed no runtime; the subsequent failed field/link release and November 1 restoration are documented above.
