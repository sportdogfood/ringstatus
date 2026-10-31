# wef-data — current scheduled collector

Updated October 31, 2026. This README describes the deployed workflow, superseding the older multiday/recovery descriptions. Production directory: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data`. Airtable: [wef-data](https://airtable.com/appdl16HJ5ETm8N69).

## Latest verified deployment

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

1. Read the default show from synced **wef-shows.shows inside wef-data**. Read its linked **wef-shows.show-days inside wef-data** and select **day-priority 0 only**.
2. Fetch that date from the existing decoded `/schedule` endpoint. Save rings, groups, the class list and every schedule occurrence, including breaks.
3. Fetch **every current class**, including classes with zero entries, from the existing decoded `/classes/{id}` endpoint, using the existing maximum of four concurrent requests. Entries and trips arrive together in these class responses; no entry-directory request is made.
4. Save complete class/trip source JSON and the authored scalar/link fields in batches of up to ten records. Existing stable keys match existing native IDs; only missing keys are created. Source-owned fields sent in the update are replaced; unrelated fields/links are omitted and preserved.
5. Finish the simple `runs` record and operational logging. Every next scheduled invocation begins fresh. No saved-work pickup, deferred queue, quality comparison, success-based skip or global stop controls this active path.

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
| runs | collection-attempt ID | One simple invocation record with started/completed/status, acknowledged business-record count, error count and elapsed time |

Schedule identity and mapping reuse `wef-schedules/tests/build-wef-data-graph.py:inventory_graph` (established commit `873b36e80`). The collector uses that mapping without invoking its old validation/work-item wrapper. A unique class can appear in more than one schedule occurrence. Retained rows absent from the latest response are not deleted or automatically treated as current membership.

Business `last-run` fields on rings/classes/trips link to **runs**. Heartbeat's `last-run` links to **run-logs**. They are different tables; both now identify the same invocation through its run key.

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

Only the five original business tables, restored schedules, simple runs metadata and three existing operational logging tables are authored. Synced show/day inputs are read-only. No horses/riders/trainers/owners/results processing, other collector changes, Season transfer, retention, archiving or historical cleanup is included.

Show/day and ring aggregate status/count columns, groups.sequence, class fingerprint/last-change and enrichment links are not refreshed by this correction. Existing values remain retained; a successful run log does not establish freshness of every field in every base table. Rendered UI behavior was not part of the live acceptance. Future show/day native-parent creation is outside this delta; schedules currently link to the existing parents.

There are no pending speed changes. The owner will provide the next instruction.
