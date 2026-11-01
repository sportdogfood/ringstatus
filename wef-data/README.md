# wef-data — current scheduled collector

**README version:** `2026.11.01.5` (documentation revision).

**Updated:** November 1, 2026 (America/New_York).

This README describes the deployed workflow, superseding the older multiday/recovery descriptions. Production directory: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data`. Airtable: [wef-data](https://airtable.com/appdl16HJ5ETm8N69).

## Current delivery status and accepted membership states

**Collection is working; the current membership states are accepted.** On November 1 the owner explicitly decided to keep **Active**, **Active — no entries**, **Absent** and **Dropped**, with existing `day-priority` separate. **No separate day0/day-* state is required.** Do not create one or reopen that superseded proposal from earlier notes.

The membership release was deployed at **12:07:28 p.m. Eastern** and verified through the normal **12:09** and **12:12** runs. A later read-only check confirmed the normal **12:21** and **12:24** runs also succeeded with zero errors, refreshing 52 classes and 54 schedule items. Class competition status, such as Completed, remains separate from membership and day-priority.

The earlier complaint about an expected day-* state is retained as historical evidence, with the owner's later decision recorded in the same [complaint](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recWnPEXguBWv99CF). Its former implementation proposal is withdrawn. No collector correction, new field or deployment is needed for that proposal. Complaint disposition concerns this specific decision and accepted existing behavior; it does not erase the historical communication issue or certify unrelated complaints.

| Verification scope | Actual result |
| --- | --- |
| Collection and saving | Normal day-0 scheduled runs verified; zero errors in the named runs |
| Membership labels | Active, Active — no entries, Absent and Dropped verified in classes/schedules and accepted by owner |
| Separate day0/day-* state | **Not required; withdrawn by owner decision** |
| Day-priority | Existing field remains separate and unchanged |
| Days 1–3 | Shared implementation and all-state/cadence checks passed offline; no eligible future-day rows for populated live proof |
| Preservation | 3,945 existing native IDs/keys and 24,318 nonmetadata links preserved; 1,313 past-day rows unchanged |
| This update | README and the named complaint only; no collector code, business records, schema, tasks, cadences, stops or deployment changes |

## Schedule membership states — November 1

For every eligible collected day0–3, `classes.record-status` and `schedules.record-status` describe membership in that day's successful schedule: **Active**, **Absent** after one complete missing observation, **Dropped** after the next, and **Active** again on reappearance. A current class response with a numeric zero `total_entry_trips` labels its scheduled class/occurrences **Active — no entries**. Missing/failed/invalid entry counts are not verified zero. Class competition `status`, such as Completed, remains separate.

These labels never gate source collection or delete records. All current schedule classes are fetched, including zero-entry classes. Absent/Dropped patches preserve saved facts, source success clocks, keys/native IDs and links. Failed/incomplete source responses do not advance absence. Retained rows remain available. Live is comparison evidence only and does not select membership. Existing trips.record-status uses active/absent/dropped from two complete class-response misses; a missing parent does not fabricate a trip response. Negative-priority days remain untouched, with original0/1/2/3 cadences preserved.

This release changes only the collector's status handling, the two necessary native text field mappings and focused tests; adapters, endpoints, four class workers, result mappings, independent linking, tasks, locks, stops and other collectors remain unchanged. Review/deployment/normal-schedule acceptance evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-data-states-20261101/ACCEPTANCE.md`. No manual collection or record repair is part of this release.

Native text fields are `classes.record-status` (`fldoCZT91yybRjlQy`) and `schedules.record-status` (`fldXSh9Qj0ilXMOG4`). Class `status` remains the source competition status. These membership labels contain no day designation. Trips retain the existing lowercase `active`, `absent`, `dropped` values.

| Normal run start, Eastern | Collection/save seconds | Errors | Retained classes | Retained schedule items |
| --- | ---: | ---: | --- | --- |
| 12:09:17 p.m. | 52.732 | 0 | 66 Absent | 85 Absent |
| 12:12:17 p.m. | 51.316 | 0 | 66 Dropped | 85 Dropped |

Both readbacks showed 52 current classes: **37 Active** and **15 Active — no entries**. The 54 current schedule items comprised **38 Active** and **16 Active — no entries**. Those counts are observations, not hardcoded limits. Every zero-entry class was still fetched. The 66 retained classes and 85 retained schedule items kept their saved source JSON, source-success timestamps and links. Dropped is a label; it does not prevent collection if the source returns the record again. Reappearance was independently tested offline and was not manufactured in production.

Independent review passed **38 focused checks**, with actual native field bindings and the four reviewed release files. The deployed runtime is `data_runner.py` SHA-256 `ab243e946be911adb0ebff0a2da5b08bba2bd6fba7077bdb3def3c69b2cd19db`. Source adapters, result mappings, linking and cadences stayed unchanged. [Native release review](https://airtable.com/appdl16HJ5ETm8N69/tblrpt7ZqoUGr2VFj/recGTvVuohQUSANva) and the evidence packet prove the named membership checks. The owner subsequently accepted these existing states and withdrew the separate day-* proposal. Historical concern and later decision are retained in the same complaint; no new runtime fix is claimed.

## Previous release: results and independent linking — November 1

The results/linking release deployed at **8:40:56 a.m. Eastern**, before the membership release above. Its functionality remains in use. Working fast `scheduled.py` startup bytes, source error handler, source endpoints, four class workers, keys, existing data and collection cadences were preserved. The existing linking task was enabled after the first normal fast run passed; its task definition, interval and offset were unchanged. No manual collection, task start, stop clear, new adapter, quality gate, extra request delay or historical run scan was added.

Fast collection now saves the existing result-field mapping from the same class responses: result-key equals trip-key; class status, placing/position, scores, times/faults, points/prize money, gone-in and scratch retain established zero/null handling and source observation time. It continues existing ring/group/class/entry/trip/schedule scalar updates. It does not refresh horse/rider/trainer/owner names or add entity endpoints. Native relationship writes are separate.

The normal **8:42 a.m.** and **8:45 a.m.** fast runs each saved **579 records with zero errors**, including 160 results. Collection/save elapsed **53.243** and **62.084 seconds**; results saving was **14.828** and **12.669 seconds** respectively. These are measured runs, not a guarantee of future latency or zero overhead.

The normal **8:43 a.m.** and **8:46 a.m.** linking runs succeeded with **zero unresolved targets**: 1,096 relationship updates in 59.257 seconds, then 435 in 50.549 seconds. The second pass updated newer collection-run relationships. Linking reads saved eligible Data facts and existing native keys, including the synced Shows/day and w-* master links, with its own Python/Node locks, report and operational outbox. Missing native entity keys can be reserved using the existing key-only upsert; existing names and scalar fields remain unchanged. Synced/master tables are read-only and Airtable maintains reciprocal links.

The existing per-day cadence metadata additionally stores the already-generated native run-key. Linking reads those at-most-four keys so later day-0 wakes do not erase day 1-3 provenance. No new state file or collection checkpoint was introduced. Relationships, including last-run and schedules runs/run-logs, are updated when the separate linking lane runs and can briefly lag scalar saving. Source timestamps are never renewed by linking.

Independent review passed 47 tests, both fresh private-Python startup imports and the frozen scope/schema checks. Native verification matched all **160 refreshed results** against their stored trip responses and verified class/entry/entity/results/master relationships. Before/after readback preserved **6,231 existing native IDs/keys and 32,089 nonmetadata relationship IDs**. Both operational heartbeats and all four release-proof run-logs reported Succeeded. Only November 1 is currently eligible as day 0; days 1-3 retain existing cadence behavior and offline coverage, with populated live proof unavailable for this short show.

Evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-data-results-linking-20261101` (`ACCEPTANCE.md`, independent report/log/receipt/verifier, deployment/task XML, scheduled fast/link first/repeat reports, native-verification.json, native-preservation.json and operational-native.json). The earlier failed release/restoration evidence below remains historical; this release supersedes the restored collector's missing results/separate-linking limitation.

## Historical restoration — November 1, before results/linking deployment

Status at that restoration checkpoint: **collection restored and verified through the normal schedule**. At 8:17:47 a.m. Eastern, the eight runtime files were restored to their exact working pre-change bytes. Only the added `wef-data-linking` task was disabled then, before restoring the launcher; the later reviewed results/linking release re-enabled it. The existing `wef-data-collector` task XML and three-minute cadence were unchanged. Days 0-3 retained their existing 3-minute/hourly/2-hour/3-hour refresh rules. Source error detail handling, saved runtime progress, data, keys and links were preserved; no manual collection, task start or stop clearing was used.

The normal **8:18 a.m.** run saved **419 records, zero errors**, in **41.554 seconds** of collection/save time. Native Airtable readback verified fresh data for 8 rings, 52 classes, 160 trips and 54 schedule occurrences, including current-run links on rings/classes/trips. The native run-log and heartbeat both report Succeeded and are linked. Retained rows absent from this response remain stored; these counts do not claim every retained row was refreshed.

The failed October 31 field/link split was withdrawn from the active runner. That restoration returned the preceding collector and did not itself deliver the separate linking/results expansion. Linking/results were subsequently delivered by the 8:40:56 a.m. release documented above; the linking task is now enabled. Do not rerun the registration helper to restore the existing task: the actual registered task remains PT3M. All failed release and independent-review evidence is preserved in the parent evidence directory.

Restoration work began **8:15:13 a.m.**, with native collection verification at **8:19:46 a.m. Eastern**: **272.855 seconds**, rounded to **4 minutes 33 seconds** in the complaint. The estimate was 10 minutes. Outage tracking starts at the first failed attempt, October 31 at 10:33:15 p.m. EDT; verified restoration was November 1 at 8:19:46 a.m. EST, including the daylight-saving clock change. The complaint's restored-at, actual-restoration-time, estimate and computed downtime were saved and read back. Complaint remains Open; service-restoration remediation is Verified.

Evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-data-field-linking-20261031/restoration-20261101` (`INDEPENDENT-REVIEW.md`, independent receipt/log/verifier, deployment.json, task-after.json, scheduled-report.json, scheduled-attempt.json, native-verification.json, operational-native.json and ACCEPTANCE.md). [Outage complaint](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/rec2OrJVMnPWVxwVB).

## Days 0–3 completion — October 31

Pre-change limitation: **Days 1, 2 and 3 are not being collected by the current workflow.** This was the state before the days 0–3 correction; the prior release below proved day 0 only.

The approved correction retains the existing three-minute scheduler and single-day collection/save behavior. Each scheduled invocation reads synced Shows/show-days and processes eligible priorities in ascending order. Initial discovery collects and saves day 0, then 1, then 2, then 3, where those days exist in the show. Every day finishes its collection/save attempt before the next starts. Days beyond the show's synced dates are not invented.

Day 0 runs on every normal three-minute wake. Day 1 is due one hour after its previous attempt starts; day 2 after two hours; day 3 after three hours. Future days run on the first existing scheduled wake at or after they become due. A long multi-day invocation may span a scheduled wake; the existing no-overlap behavior remains in effect. No additional tasks are introduced.

`runtime/day-cadence.json` stores per-show/date attempt timestamps, outcomes, completion times and the existing native run-key used by independent linking. It is updated after each day returns, including a reported Partial attempt. Successful earlier days retain their cadence after interruption; a day interrupted before returning remains due. No class checkpoints, response replay, work queue, quality gate or old stop handling are added. The next collection of an eligible day uses the existing fresh single-day algorithm. Negative priorities are skipped on every wake, including after interruption.

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
2. Fetch that date from the existing decoded `/schedule` endpoint. Its returned items define current membership for that day. Save ring/group/class inventory fields; keep every schedule occurrence, including breaks and repeated class occurrences.
3. Fetch **every current class**, including classes with zero entries, from the existing decoded `/classes/{id}` endpoint, using the existing maximum of four concurrent requests. Entries and trips arrive together in these class responses; no entry-directory request is made.
4. Apply the membership labels and save schedule occurrences once after gathering class responses, so Active — no entries uses a current class count. Save complete class/trip source JSON, scalar fields, results and status-only absence changes in batches of up to ten records. Existing stable keys match existing native IDs; only missing keys are created. Source-owned fields sent in the update are replaced; unrelated fields/links are omitted and preserved. Native relationship updates remain in the independent linking stage.
5. Finish each day’s simple `runs` record before the next eligible day. Finish operational logging for the whole invocation. Each selected day uses fresh collection; the cadence file only determines when future days are due. No deferred queue, quality comparison or global stop controls this active path.

Source base remains `https://wef-schedules-decoded.gombcg.workers.dev`. No new adapter or endpoint was added.

## Source requests, payloads and Live comparison

Each due collection makes a new GET to the decoded `/schedule` endpoint for its show/date, followed by GETs to `/classes/{id}` for the current schedule's class keys. Requests have no JSON body. Retained Airtable class rows are not the source request list. For the inspected November 1 day-0 snapshot, 54 schedule occurrences represented 52 unique classes; the other 66 date-linked class records were retained records. These are dated observations, not fixed limits or a rule to exclude a returning source class.

The deployed `wef-schedules-decoded` Worker was inspected through Cloudflare's management API on November 1. Its active version `7fe2e7e5-af4c-42e5-8747-afca8925a59b` served 100% of traffic, with upstream no-cache headers, `cf.cacheTtl: 0`, `cacheEverything: false`, response `Cache-Control: no-store`, and no response-cache code/storage bindings. That establishes the inspected proxy configuration, not proof that Wellington's own backend never caches data. Saved Data response metrics do not include cache-age/hit headers.

Current `classes.source-json` retains complete saved class responses, including arrays and trips. `schedules.source-json` retains each occurrence item; the complete original schedule response envelope is **not retained by this fast path**. Older files under `runtime/source-responses` are historical captures and must not be presented as the latest run. The owner-requested latest-run export used actual saved native source JSON: `C:/Users/gombc/Documents/Codex/outputs/wef-data-day0-latest-scheduled-20261101`. Its 52 full class responses and 54 schedule items belong to the 10:48–10:50 a.m. Eastern run; the schedule-items file is explicitly not the complete original schedule response.

Live is separate observation evidence. A read-only comparison of the 11:51 a.m. Data schedule and 11:54 a.m. saved Live feed matched 41 of 52 unique classes. The 11 unmatched were **four Under Saddle/Flat classes and seven Over Fences classes**, not eleven under-saddle classes as this agent initially assumed. The seven Over Fences classes had zero entries in their inspected class responses. All eleven were still on the schedule. Absence from Live alone never establishes Absent/Dropped membership, changes collection eligibility or permits deletion. Compare actual latest feed membership, source IDs and observation times; retained Live table rows alone can overstate the current feed.

## Authored data and keys

| Table | Stable key | Current authored behavior |
| --- | --- | --- |
| rings | show key + date + ring number | ID/number/name, schedule observation time as last-success, current last-run |
| groups | ring key + group ID | ID/name/ring link, existing subset of schedule source JSON, last-success |
| classes | ring key + class ID | ID/number/name, full class JSON, source competition status, total_entry_trips as entry-count, membership record-status and last-success; existing ring/group/last-run links maintained separately |
| entries | source show ID + entry ID | Entry ID/number and last-success, derived from class trips |
| trips | class key + entry ID + source UUID or trip ID | Existing trip scalar values, full trip JSON, last-success and active/absent/dropped labels; existing class/entry/last-run links maintained separately |
| schedules | ring key + class_groupxclasses_id, otherwise position-N | Every ordered class/break occurrence, position, source ID, name, start/end times, full occurrence JSON, membership record-status and last-success; existing show/day/ring/group/class/run links maintained separately |
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

The original trips linked-record field is unchanged. Empty arrays remain `[]`, and all array elements stay together in one JSON text column. Formulas use the established JSON sibling/end-of-parent boundaries; source-format changes require checking those mappings. [Field definitions and native verification](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/fast-collector-20261031-candidate/class-array-exposure-proof.json>): all 4,872 array values across 406 saved class records matched the source arrays at that check. IDs, keys and checked ring/group/trip links stayed unchanged. That column-exposure change did not alter runtime or scheduling; later runtime releases are documented separately above.

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

The narrower payload reduction and automated synced-Live matching remain on hold. The owner's requested read-only Live/schedule comparison was performed; it did not authorize a matching writer or collection gate. The days 0–3, duplicate-selection/timing, error-detail, results/linking and membership corrections are deployed with their dated verification limits. The separate day-* proposal is withdrawn; keep the current membership labels and existing day-priority. This update changes the README and named complaint only; it does not change the collector.

## November 1 day-state complaint and later owner decision

Historical concern: this agent delivered membership labels with day-priority separate and reported completion before the owner questioned the absent day0/day-* stage. The original complaint and prior proposed correction remain historical evidence. They are not current implementation instructions.

**Later owner decision:** "we will not use the missing day-* state using day-priority. we will keep your current membership state". The owner then requested updating this README and the complaint to reflect that no separate day-* state is needed. This later decision supersedes the former day-state implementation proposal.

- **Complaint:** [recWnPEXguBWv99CF](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recWnPEXguBWv99CF). The same record now contains the owner decision and retains its original incident history. Remediation is **Verified** for the amended agreed outcome; the updated fields were read back. Administrative complaint-status remains **Open**, the only configured choice; no Closed option was invented.
- **Current disposition:** existing membership behavior accepted; no separate day-* implementation remains. Verification concerns the owner decision, documentation/readback and previously observed working membership states; no new code correction or general prevention claim.
- **Required reminder:** follow the complete human instruction history, including later withdrawals and accepted behavior. Do not continue implementing a superseded proposal or silently revive it from a complaint or old README.
- **Protected behavior:** the schedule remains membership authority for every eligible day; collect zero-entry classes; preserve retained records, keys, links and source clocks. Keep day-priority and competition status separate. No gates, deletion, manual collection or additional systems.

## October 31 agent failure record

Owner-requested record for Data chat `01a19273-bdf8-71b2-8503-cc8079efc129`. These are failures evidenced in this chat and its saved release/review artifacts, including owner corrections. They are not authority for further edits, cleanup, retries or changes to another collector.

| Historical October 31 failure | Exact reminder to apply |
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

The historical duplicate-selection/timing correction used `data_runner.py` SHA-256 `80191995a0d382c5c48de7c9a86a7a6ada3fc3602cc1068b324b6993ca931a99`; the current runtime hash is in the membership release section above. The unchanged source-error reader is `source-reader.js` SHA-256 `b2a9872b28b186a05987323d476797bb2ad543954d1903e9e2ebd1ab19545ae6`. Later releases preserve those corrections. Historical hashes are evidence, not rollback instructions.

Evidence: [timing correction and Partial collection outcome](C:/Users/gombc/Documents/Codex/outputs/wef-data-timing-20261031/ACCEPTANCE.md), [independent error-detail review](C:/Users/gombc/Documents/Codex/outputs/wef-data-error-details-20261031/INDEPENDENT-REVIEW.md), and [error-detail deployment and scheduled acceptance](C:/Users/gombc/Documents/Codex/outputs/wef-data-error-details-20261031/ACCEPTANCE.md). Error-detail propagation passed independent end-to-end offline checks; the 8:45 p.m. scheduled run saved 484 records without errors, so that live run did not exercise a natural failure. Historical discarded causes cannot be reconstructed. Existing error-message length limits remain; do not promise unlimited traces.

Apply the reminders at the [frequency required in this agent's instructions](AGENTS.md#this-data-agents-failures-and-required-reminders--october-31-2026): startup/resumption, each edit/review/deployment/delivery boundary, every correction/failure, and each 10 minutes of active work. These are agent execution checks, not scheduled reminder jobs, owner babysitting or collection gates.


## Complaint records and document revision

README revision `2026.11.01.5`, dated November 1, 2026, records the deployed membership release, its scheduled proof and the owner's decision to keep current membership states without a separate day-* state. The named complaint retains the historical concern and the superseding owner decision. It also reconciles historical restoration/linking statements, actual source/save behavior, payload limitations and the dated read-only Live comparison across this document. The reminder frequency remains in [AGENTS.md](AGENTS.md#this-data-agents-failures-and-required-reminders--october-31-2026). This update changes the README and the named administrative complaint only; no collector code, business data, schema, task or cadence change follows.

The original ten October 31 failures were added one record at a time to [agents → complaints](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U). That historical count is not a current total: later complaints include the November 1 outage/restoration and the day-state complaint with its later owner decision linked above. The original records were read back and remained Open at their recorded checks; do not infer current status for every historical complaint without re-reading it. The final October 31 failure-table entry contains two distinct defects (redaction/cause loss and UTF-8 fixture corruption), with separate Airtable records. Documentation and recorded remedies do not establish prevention of recurrence.

Latest deployed source-error correction: October 31, 8:44 p.m. Eastern. Its normal 8:45 p.m. run saved 484 records with zero errors; actual error-message/cause delivery was independently verified end to end offline, not exercised by a natural failure in that live run. See the linked release evidence in the failure record. That earlier documentation-only revision changed no runtime; the subsequent failed field/link release and November 1 restoration are documented above.
