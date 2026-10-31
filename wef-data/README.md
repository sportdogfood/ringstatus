# wef-data — fast current-show collector

Updated October 31, 2026. This is the current operating README. Earlier release narratives are preserved in the [previous README](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/wef-data-readme-before-20261031.md>). This documentation does not authorize code changes, stop clearing, collection, archival, cleanup or record repair.

## Purpose and ownership

Collect the qualifying days of the current show, save each day to Airtable, then move to the next qualifying day. Keep the original business keys, source identities, native record IDs, links and source observation times.

- **Shows** owns the current show, dates, selected planning date and `day-priority` labels. Data reads them; it does not discover or predict the next show.
- **Data** owns source acquisition and verified native saving. It produces completed-day evidence for Shows' separately owned counts reader.
- **Season** is a separate retention workflow. It is not a gate or intermediary for this fast collector and is not changed by this release.
- **Live** independently observes actual calendar TODAY. Its approved Data overlays are restricted to canonical priority 0.
- The separate **wef-entries** and **wef-schedules** workflows remain separately owned. No changes to them are authorized here. Data's own `entries` table is a linking table; existing trips supply single horse/rider/trainer assignments. Ambiguous parent-entry assignments are withheld.

Production module: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data`.
Airtable base: [`wef-data`, `appdl16HJ5ETm8N69`](https://airtable.com/appdl16HJ5ETm8N69).

## Current deployed version and proof

`data_runner.py` SHA-256: `c252a1a2346d568648f0ee54e1c58d28dd185c943effd7e13434dea5e6d56524`.

This is the existing runner with its original complete linked input restored for the proven class projector. Only the output day/ring aggregates are restricted to the captured day. There is no new adapter. Reservation, capture, native save/readback and saved-response recovery remain the existing implementations.

- [Independent source review and 10 targeted checks](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/minimal-collector-input-repair-review-20261030.md>): PASS, separate from operational proof.
- [One-file deployment receipt](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/minimal-collector-input-repair-deployment-20261030.json>): October 30, 11:53 p.m. Eastern. Existing tasks unchanged; no manual collectors.
- Same saved 70 October 31 responses recovered through the existing task, completed October 30 at **11:55:24 p.m. Eastern**. Original source/reservation evidence remained available.
- A subsequent **fresh** October 31 pass completed October 30 at **11:59:59 p.m. Eastern**, exit 0: 71 source requests (one inventory, 70 classes), 23 inventory and 144 class write/readback blocks. [Scheduled log](runtime/collector-2026-10-30.log), [immutable completed-day receipt](runtime/completed-days/52bb634110148ac23fd6435b4b502d250d35843ec6239250a70a9aa0d5fd9bb6.json).
- Later scheduled priority-0 and priority-1 saves are present in the current dated receipt table below. Priorities 2 and 3 have no qualifying day in this show; production execution of those priorities remains unobserved.

A successful offline gate, recovery, day save, Shows count publication and Season transfer are different claims. One does not prove the others.

## Canonical input and reusable day rules

Read `wef-shows` base `appCaSmL819KK7UFJ`:

| Source | Table | Contract |
| --- | --- | --- |
| Current show | `shows`, `tblJA5CUon5s1sWib` | Exactly one valid default for customer 15; existing `customer_id|show_id` identity and validated authority/date context |
| Qualifying days | `show-days`, `tbl14MaCCDw6dAywH` | Native show-day key, date and show link must agree; existing `day-priority` field `fldbK4VEp3aNOdJlB` supplies the label |

`15|200000076` is a dated example, not a hard-coded next-show rule. A changed source show ID identifies a different show. A different selected day does not.

Priority 0 follows **Shows' selected planning date**, which can differ from calendar today. Data validates labels against that canonical context; it does not replace missing labels with locally invented ones. Missing/incoherent day publication defers acquisition. Original authority and source freshness guards remain in place.

| Priority | Order | Refresh eligibility |
| --- | --- | --- |
| −3, −2, −1 | Never acquire or replay excluded day work | None; preserve existing records and saved work |
| 0 | Collect/save first | Every 180 seconds / 3 minutes |
| 1 | Collect/save after 0 | Every 3,600 seconds / 1 hour |
| 2 | Collect/save after 1 | Every 7,200 seconds / 2 hours |
| 3 | Collect/save after 2 | Every 10,800 seconds / 3 hours |

Absent priorities are skipped. First collections precede completed repeats; completed day selection advances the existing next-priority cursor. A day saves before another day is admitted. Calendar-past days are also excluded. For **actual New York calendar today**, an explicit numeric `entry-count = 0` skips that class's detail fetch; missing, partial, failed or unknown counts do not mean zero. Inventory still establishes the dated scope.

These are due intervals, not four task timers or guaranteed finish-to-finish intervals. The saved pass clock drives eligibility; a one-minute wake, an occupied worker, an admitted save and source/Airtable delays can affect actual start/completion. Preserve original source time on recovery.

## Workflow executed by the existing task

1. The existing Windows task starts the existing Node/Python process and checks locks/stops.
2. Read and validate current Shows authority and native day priorities. Resume eligible saved work first; preserve excluded work without replaying past days.
3. Select the next due qualifying day under the order above.
4. Fetch that day's decoded schedule, validate its scope and save the dated inventory using existing keys and native links.
5. Fetch eligible class detail with at most four concurrent reads. Save successful response bytes and hashes; retain missing/failed contexts separately.
6. Use the unchanged projector's **complete linked show input** to preserve its integrity checks. Stage only the selected day's aggregate day/ring output, plus its actual class/trip/entity/result changes. Reading existing dependencies is not re-collecting past days.
7. Use existing key reservations, bounded transport batches and native readback. Do not advance the completed-day pointer until that day's work is saved and verified.
8. Save the local completed-day receipt and day head. A later scheduled wake selects the next due day. Attempt/error logging remains separate from business success.

### Decode endpoints

Base: `https://wef-schedules-decoded.gombcg.workers.dev`.

| Endpoint | Data use |
| --- | --- |
| `GET /schedule?date={canonical-day}&show_id={canonical-show-id}&customer_id={canonical-customer-id}` | One dated inventory; source dates/context and native identities must validate |
| `GET /classes/{class_id}?show_id={canonical-show-id}&customer_id={canonical-customer-id}` | Detail for eligible source classes in that inventory |
| `GET /entries` | Existing historical directory acquisition/pagination support; **not dispatched by the current fast dated-day loop**. Do not equate class coverage with a complete entry directory. Existing directory evidence is retained. |

Default `/ring?customer_id=15` and sentinel-date discovery requests belong to Shows, not this collector. This README executes no endpoint request. `is_under_saddle_class` is source context, not a blanket explanation for an HTTP error or permission to fabricate an empty class.

### What is excluded from collection

No Git push, outgoing archive, historical cleanup, predecessor-history publication or Season handoff is performed by the current fast `tick`/`finish` path. Historical archive/handoff modules and retained data still exist; their presence does not mean the current day save invokes them. No new archive worker, automatic purge or replacement handoff was created by this simplification. Do not delete those files, histories or rows from this document.

## Where records and saved progress live

The 19 existing business/control tables and keys are unchanged. [schema.json](schema.json) pins table/field IDs; [authored-fields.json](authored-fields.json) pins writable fields. Those files, rather than names or new proposed keys, define the existing writer contract.

| Table | Existing table ID | Primary field | Owned meaning |
| --- | --- | --- | --- |
| runs | `tblSxcmUcmB0W9Vhu` | `run-key` | Run journal, accepted revision/checkpoint and completion evidence |
| shows | `tbl7HtrFIRUlJpVsc` | `show-key` | Mirror of canonical show context, not discovery authority |
| days | `tbl7WyLyshiM4KNjh` | `day-key` | Dated show contexts |
| rings | `tblhKkfYXppW3TbTq` | `ring-key` | Existing dated source ring identities |
| groups | `tblQObOvktufkyq54` | `group-key` | Source class-group identities |
| classes | `tbl0IFf067w4wUBj5` | `class-key` | Dated ring/class identity and retained detail |
| schedules | `tbl6KKlYuMIPHEJWe` | `schedule-key` | Schedule occurrences/breaks; cumulative retention is not current eligibility |
| horses | `tblkGRRMJxnH55hi8` | `horse-key` | Existing source horse identities |
| entries | `tblcKPbT4DPbFPu5b` | `entry-key` | Show entry registrations and entity links; not a rider assignment across all trips |
| riders | `tblGEzjOJzpn3vxX5` | `rider-key` | Existing source rider identities |
| trainers | `tbl9ROAViHtbKqpRd` | `trainer-key` | Existing source trainer identities |
| owners | `tblvEa5ZqW6W2aL7N` | `owner-key` | Retained source owner identities |
| trips | `tblt0GvQ39bHIu930` | `trip-key` | Single dated horse/rider/trainer facts and scope |
| lanes | `tblLIyT2lFsqLfywz` | `lane-key` | Stored acquisition policies and interval bounds |
| work-items | `tblqmUtXzVuiBnNQy` | `work-key` | Per-class due/attempt/checkpoint state |
| slots | `tblQQSMCD9NpZsBOK` | `slot-key` | Bounded source dispatch batch evidence |
| collections | `tbl8fQXTGJmv5esNj` | `collection-key` | Preserved legacy observation rows; the fast day loop does not append observation rows here |
| requests | `tblkESaEApR2SRBEu` | `request-key` | Preserved historical request provenance; new fast-loop request metrics stay with local evidence |
| results | `tblaCoQC0gQcaOUpl` | `result-key` | Source-backed trip/result facts; existence is not completed-result proof |


`results` can retain observations of an unfinished trip; a result row alone does not prove final placing/completion. Retained schedules/classes are historical rows, not automatic proof of current inventory membership. Entry roles are not a union of every rider observed across trips.

Separate existing operational tables:

| Table | ID | Purpose |
| --- | --- | --- |
| `heartbeat` | `tbl2r6hhTnlZh3leX` | Last workflow outcome/health |
| `run-logs` | `tblKMNAtvvIFSvgLh` | Each attempt and terminal result |
| `err-logs` | `tblhPcSD6UPp5jkCa` | Separate source, transport, integrity, process and logging errors |
| `code-reviews` | `tblrpt7ZqoUGr2VFj` | Administrative review evidence, not collection success |

Local paths under this module:

| Path | Meaning |
| --- | --- |
| `runtime/last-run.json` | Latest scheduled child result; can still name a previous result while a new child is running |
| `runtime/active.json` | Current reserved run; intentionally removed after successful completion |
| `runtime/pending.json` and per-run `day-work/` | Exact pending native writes, original keys/native ID pins, plans and readback progress |
| `runtime/adaptive-*/class-batch-*/` | Saved reservations, successful responses, metrics and projection evidence |
| `runtime/completed-days/{hash}.json` | Immutable local completed-day state; preserves original source timestamps |
| `runtime/day-heads/{customer}-{show}/{date}.json` | Hash-pinned pointer to the latest saved day, read by existing count integration |
| `runtime/stopped.json` | Durable failure stop; later scheduled wakes must not collect through it |
| `runtime/resume-approved.json` | Existing exact-run recovery authorization, only after applicable owner approval |
| `runtime/source-errors/`, deferred controls | Saved source errors and bounded scheduled backoff |
| `runtime/collector-YYYY-MM-DD.log` | Scheduled starts, child results, process exit times |
| `runtime/logging/wef-data/{run-logs,err-logs,acks}` | Local attempt/error delivery queue; retained when Airtable logging is unavailable |
| `runtime/retained-evidence/`, `handoff/`, `archive-repository/`, outgoing/retirement receipts | Preserved historical storage from the older workflow; not a current fast-collection prerequisite |

`runtime/collector.env` contains host-managed configuration; never print, commit or copy credentials into evidence. There is no public-export guarantee in a local receipt. Shows owns publication of its counts. This release's local day head is not the older Git/Season accepted-revision head.

## Actual schedule and recovery behavior

`wef-data-collector` repeats every **one minute**, `IgnoreNew`, `PT0S` execution limit. Existing route: `wscript.exe` → `run-hidden.vbs` → `run.ps1` → Node `entry.js` → private Python `scheduled.py` → `data_runner.py`. Node/Python collect; the agent does not. The registered interactive host must be available. Existing collector PID and Python worker locks prevent overlapping workers.

A stopped wake is a skipped attempt, not a new failed acquisition. A Deferred response can mean saved work or a bounded transport wait, not data loss. Successful saved responses are reused under original scope/hash checks. Unknown Airtable write outcomes reconcile the exact reserved keys instead of blind duplicate creates. Existing explicit HTTP 502/503/504/read-timeout handling permits bounded scheduled retries; exhausted retries or integrity errors stop. Stop clearing and failed-checkpoint recovery require the exact owner authorization. Never start a manual collector, call substitute source endpoints or repair records to obtain a green report.

Existing periodic administrative review: `wef-data-monday-code-review`, Monday 09:00 America/New_York, routine record `rec3DITcZqf1uAP1L`; first scheduled November 2 review remains unverified here. It is separate from business collection. This README does not change that routine or its write scope.

## Errors encountered: current interpretation and historical disposition

This register includes documented runtime failures, review findings, test/setup failures and scope mistakes. **Historical resolution is not a promise that an external error can never recur.** Removed collection-path work is not being retested or re-enabled here. The exact locally observed error signatures are listed in the appendix; repeated stop-guard messages are repeated blocked wakes, not hundreds of independent root failures.

| Error / incident | Cause and impact | Evidence / disposition |
| --- | --- | --- |
| `Historical shared timestamp regressed` | Airtable millisecond formatting compared against original microseconds looked like regression. | Existing date-equivalence correction; [original readback diagnosis](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/wef-data-scheduled-failure-readback-20261029.json>). Preserve source time; do not replace business keys. |
| Source HTTP 403 | A retained source request was forbidden; not a valid empty result. | `runtime/stopped-20261029T120729-source403-failure.json`; fatal classification retained. No claim that every future 403 is resolved. |
| `Directory total/size/order drift` | Pagination changed while collecting; unsafe to declare a complete directory. | `runtime/stopped-20261029T123510-directory-drift.json`; historical consistency/reconciliation work. Directory is not currently dispatched. |
| Source HTTP 502, 503, 504 or timeout | Transient decoded/upstream failure; saved successful parallel responses must survive. The observed `/classes/200030704` 502 was not explained by under-saddle status. | [Bounded recovery and scheduled proof](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/wef-bounded-source-recovery-acceptance-20261029.md>). First observed 502 and exact stage: [saved-write/source failure report](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/wef-saved-write-and-cadence-20261029.md>). External recurrence remains possible. |
| `WinError 10054`, remote connection forcibly closed | Transport failed, not verified empty or completed work. | Exact logged signature in appendix. Preserve checkpoint; do not substitute manual collection. |
| Saved-write blockage / uncertain Airtable POST or PATCH | Native writes can be partially accepted or lose their response. Missing/reserved-row handling previously blocked progress. | [Saved-write acceptance](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/wef-saved-write-and-cadence-20261029.md>); current key reservation/readback remains. Deferred messages retain pending work; no blind duplicate POST. |
| Entry horse identity conflict | Source changed the horse ID for existing entry `200000076|200244210`, from `200005163` to `200005221`. | Historical conflict isolated; observations/keys retained and unsafe relinking withheld. Current directory loop is excluded, not proof the source conflict vanished. |
| Multiple riders/trainers on one entry | Parent links accumulated trip/observation roles; they were not a single assignment. | [Scope/unwind decision](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/scope-unwind-review-20261029.md>); [trip-based scheduled acceptance](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/trip-assignment-scheduled-acceptance.json>). Existing trips own assignment; ambiguous parent entry withheld. |
| `Class moved; inventory required` | Saved occurrence no longer matched fresh inventory. Replaying it as unchanged would corrupt scope. | Existing recovery validates context, retains held occurrences and reuses eligible saved responses. [Saved-checkpoint verification](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/saved-checkpoint-recovery-final-verification-20261030.json>). Not permission to change keys. |
| `Immutable evidence collision` | Distinct serialized bytes targeted one immutable identity, including equivalent timestamp spellings; bytes must not be silently overwritten. | [Collision review](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/immutable-collision-independent-review-20261030.md>); existing original-time/equivalence and immutable-byte protections. Historical publication is outside current fast loop. |
| `Immutable retained cache collision` | Separate existing collection records with identical payloads shared a local cache filename. | Approved Data/Season cache correction preserves each record ID plus payload hash; historical cache evidence retained. No Season changes in current release. |
| Git merge/push/divergence and archive retry exhaustion | Old publication path depended on a divergent checkout/concurrent remote history and could block collection. | [Archive correction review](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/archive-fixes-code-review.md>), [outgoing retry/race review](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/outgoing-race-independent-review-final-20261030.md>). Isolated checkout, frozen retry snapshot and prefix/history guards are historical fixes. Git work is excluded from current fast collection. |
| Unfrozen outgoing cutoff / foreign ahead-only commits | Retry could recapture changed rows or accept unpublished commits outside approved archive scope. | [Outgoing final review](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/outgoing-race-independent-review-final-20261030.md>). Corrected historically; natural different-show rollover is not newly proven here. |
| `KeyError: retained-json` | Raw archived metadata rows were incorrectly required to contain accepted observation payloads. | [Exact failed recovery](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/history-cleanup-scheduled-failure-20261030.md>), [later preservation verification](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/raw-preservation-final-operational-verification-20261030.json>). Preserve raw metadata separately from payload validation. |
| `Run journal exceeds accepted cell budget` | Observation references and active-class/directory controls exceeded the 60,000-byte journal guard. | [Byte-level diagnosis](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/journal-size-root-cause-20261030.md>). Approved lossless control compaction/part sizing; guard retained. Fast loop also excludes historical publication overhead. |
| Collections/runs growth and long pre-collection history work | Legacy observation parts/predecessor publication and retention processing expanded Airtable work and latency. | Owner reduced scope. Current collection-only repository excludes legacy collections/requests loads and does not create those observation rows. Old data is preserved; no blanket purge is implied. |
| Freshness over 300 seconds / completed-day publication behind future work | Old collection plus persistence/publication exceeded freshness, and fairness counters could reset across runs. | Historical freshness failures remain dated failures. Current one-day save boundary removes publication/retention gating; no universal source-to-consumer freshness promise. |
| Past-day replay, fixed-date assumptions and starvation risk | Saved recovery could bypass selected/current-day admission; repeated current-day work could delay first future-day collection. | [Selected-window review](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/selected-window-final-independent-review-20261030.md>), current canonical priority order. Dates are reusable; no hard-coded October-only rule. |
| `Active trip count must equal latest class counts` | Added day-truncated projector input imported cross-day classes via legacy links without all their trips. First minimal-release run stopped after capturing 70 responses. | [Exact repair review](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/minimal-collector-input-repair-review-20261030.md>): original complete input restored; only output day/ring aggregates filtered. Approved one-file deployment and scheduled recovery succeeded. Shared projector unchanged. |
| Priority-0 success with zero requests | A run finalized already-recovered detail and an unfinished day pass; it did not perform a fresh acquisition. Idle/no-due success can also make zero requests. | October 30 11:56:49 saved result had zero observations/requests. The next fresh pass made 71 requests and completed 11:59:59. Never label a zero-request receipt a fresh collection without its source evidence. |
| Final audit `active.json` missing | Documentation/audit checker tried to read the active checkpoint after successful completion correctly removed it; a slash-direction filter failed to exclude it. | Audit failed; collector succeeded. No production repair followed and no complete final audit claim is made from that failed script. |
| Logging timeout / delivery budget reached | Attempt/error delivery could not finish within its budget. | Local queue retains delivery independently. A logging success/failure is not collection success/failure. See appendix examples. |
| `Schedule collector already running` | Shared lock rejection encountered in Data's process log. | Exact signature in appendix; no alternate runner or overlapping invocation authorized. |
| Source/setup environment failures | Missing timezone data in PATH Python, Git clone exit 128 in temporary tests, missing shared fixtures/schema/env and an outdated fake API without show-days. | Historical offline/test setup failures, not proof of production failure. Use the established interpreter and existing verified fixtures; do not install/rebuild production to satisfy a test copy. |
| Fixture/assertion errors | Same-clock receipt names, reversed observation chronology, legacy count expectations, native-ID reservation assertions, retry test excluding the existing schedule recheck, Windows path separators and JSON property ordering. | Corrected only under their separate approvals. Passing offline checks do not replace scheduled proof. UTF-8 audit/console failures also occurred; use explicit encoding and do not describe them as data corruption. |
| Scope drift / discarded adapter | Unnecessary adapter and test-copy work replaced focus on the proven runner. Separate wef-entries changes were outside this coordinator's ownership. | Adapter discarded; current deployed route uses the existing runner. No rollback/edits to sibling modules from this README. Historical scope/unwind report records the boundary. |

## Open limitations and operating checks

- Priorities 0 and 1 have actual saved scheduled receipts. Priorities 2/3 and a natural next-show first-load/rollover are not established by this two-day show's proof.
- Current fast schedule/class collection does not establish full `/entries` directory coverage. Do not call it complete directory acquisition.
- Historical rows can remain in Data. Removing non-current records, archival retention and Season work are separate from the fast day loop; this README neither executes nor restores those paths.
- Shows' separate historical count-publication stop is not cleared by a Data save. Inspect its owning evidence before claiming its counts are updated. No sibling repairs here.
- An active run can coexist with an older successful `last-run.json`; correlate the exact run key, completed-day head and terminal scheduled log.
- `completion-at`/process completion is not a new source observation time. Zero requests can mean recovery finalization or no due work, not fresh success.
- Use actual captured request metrics for collection time. Separate native write/readback, preparation and total scheduled duration; do not present inherited timing fields as measurements of the newest pass.
- The October 30 11:57–11:59 fresh pass measured about **7.5 seconds source-request windows**, **1m52s inventory/class save-and-readback windows**, **2m43s total task time**. Save windows are plan-to-last-readback intervals, not isolated Airtable server latency. Those numbers are one measured run, not a service guarantee.

The only next actions implied by this document are reading scheduled evidence and operating under existing owner approvals. A new failure does not authorize automatic code repair, task changes, stop clearing, manual runs or data deletion.

## Dated scheduled-save snapshot

Snapshot from existing scheduled outputs; point-in-time evidence, not a claim about future runs. Times below are UTC in run keys and Eastern in completion labels.

| Priority | Day | Saved scheduled run | Completion (Eastern) | Requests |
| --- | --- | --- | --- | ---: |
| 0 | 2026-10-31 | [adaptive-2026-10-31T12:04:52.518374+00:00](runtime/completed-days/87002a28d11eafcc5be3457c43a4fe120984620090ae111ee9023137632c7b2c.json) | 2026-10-31T08:07:13.213051-04:00 | 63 |
| 1 | 2026-11-01 | [adaptive-2026-10-31T11:40:43.912131+00:00](runtime/completed-days/96af376aff1f874b9396aff04da8c705c9dcd5fb4276d8b55cea9e1cdd9dbe75.json) | 2026-10-31T07:43:19.477721-04:00 | 119 |

## Exact observed error signatures

Generated from existing local `runtime/logging/wef-data/err-logs` on October 31. Category/message-prefix grouping prevents variable run/source IDs from becoming separate root causes. Counts include repeated skipped/deferred wakes. The [full inventory](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/wef-data-readme-source-inventory-20261031.json>) preserves the representative original message and source file for every group. No provider or Airtable calls were made to construct this list.

| Category / operation | Recorded message prefix | Logged records | Representative local evidence |
| --- | --- | ---: | --- |
| Collection / — | `Archive recovery exhausted after three scheduled retries: Archive Git merge did not complete; saved evidence retained` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/9e61229c-c07c-4e80-b222-e14f0ec2ba80-0001.json>) |
| Collection / — | `CalledProcessError: Command '['C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\wef-data\\.venv\\Scripts\\python.exe', '-B', 'C` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/aae29210-5058-4ccb-8ff2-95c903488709-0001.json>) |
| Collection / — | `CalledProcessError: Command '['git', '-C', 'C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus-data', 'merge', '--ff-only', 'origi` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/247f1d62-4bc2-40e7-91e5-a03a229dda94-0001.json>) |
| Collection / — | `KeyError: 'retained-json'` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/057c79e3-6818-4be4-824c-494894740166-0001.json>) |
| Collection / — | `Retained checkpoint awaiting its bounded recovery time` | 48 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/0fe38313-042d-484b-a9dc-009ac7098eb4-0001.json>) |
| Collection / — | `Saved archive publication awaiting bounded recovery time` | 8 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/0bb006b8-055a-4b58-b064-61ed9f694219-0001.json>) |
| Collection / — | `Source checkpoint awaiting bounded recovery time` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/0aa4f119-961a-4d85-9b5b-d69753eaa612-0001.json>) |
| Collection / — | `SourceFailure: Source HTTP 502` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/bdfca812-705f-46ee-87f1-b077c746d7a5-0001.json>) |
| Collection / — | `ValueError: Class moved; inventory required` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/f35588ff-8406-44a6-8360-822fbfb3b064-0001.json>) |
| Collection / — | `ValueError: Entry horse identity conflict` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/a2468660-5975-4959-bc7b-8d7d19200c5e-0001.json>) |
| Collection / — | `ValueError: Immutable evidence collision` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/4fb5434e-5a9c-4c41-b2c5-d9c92a20aaf1-0001.json>) |
| Collection / — | `ValueError: Run journal exceeds accepted cell budget` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/22bda407-024e-4b58-9dee-bb9a566c952a-0001.json>) |
| Collection / Archive | `Archive Git merge did not complete; saved evidence retained` | 2 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/a4ec6c40-da7a-459c-afc5-67d1a9638381-0001.json>) |
| Collection / Archive | `Archive Git push did not complete; saved evidence retained` | 3 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/1a5be277-3a0b-4ab0-aaec-5ff191890dbb-0001.json>) |
| Collection / Continue | `Verified work saved; exact remaining blocks continue at the existing scheduled wake` | 11 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/1d256a8c-c4b0-4783-ae20-59be3ef31b37-0001.json>) |
| Collection / GET | `Source HTTP 502` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/dfb81940-10d2-4648-b03f-436595e1ca01-0001.json>) |
| Collection / PATCH | `Airtable transport deferred: PATCH tbl9ROAViHtbKqpRd` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/4eede8f8-5c8f-49d1-96f7-bfab740c7774-0001.json>) |
| Collection / PATCH | `Airtable transport deferred: PATCH tblGEzjOJzpn3vxX5` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/2291c923-ec0e-46e3-ac4f-28bf164938ed-0001.json>) |
| Collection / PATCH | `Airtable transport deferred: PATCH tblSxcmUcmB0W9Vhu` | 3 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/0baffae9-f061-43b3-aac3-b589dd041a61-0001.json>) |
| Collection / PATCH | `Airtable transport deferred: PATCH tblaCoQC0gQcaOUpl` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/3db47de4-4d0e-457d-8c01-8386f1065865-0001.json>) |
| Collection / POST | `Airtable transport deferred: POST tblt0GvQ39bHIu930` | 11 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/0bb9db0a-3009-4c00-8260-bf618456d406-0001.json>) |
| ExistingStop / Collect | `Attempt blocked by retained stop: Archive recovery exhausted after three scheduled retries: Archive Git merge did not complete; saved evidence retaine` | 596 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/0035e5e9-e80a-4299-bc8c-01f37fe75abb-0001.json>) |
| ExistingStop / Collect | `Attempt blocked by retained stop: CalledProcessError: Command '['C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus\\wef-data\\.ve` | 11 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/14c33f32-ce87-4c40-a6e7-14d09505ceb8-0001.json>) |
| ExistingStop / Collect | `Attempt blocked by retained stop: CalledProcessError: Command '['git', '-C', 'C:\\Users\\gombc\\OneDrive - Sport Dog Food\\github\\repos\\ringstatus-d` | 8 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/04ea7b79-28ed-47d5-94de-30c25a670bd5-0001.json>) |
| ExistingStop / Collect | `Attempt blocked by retained stop: KeyError: 'retained-json'` | 13 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/0981038a-94b8-45a2-8d59-a481ae169968-0001.json>) |
| ExistingStop / Collect | `Attempt blocked by retained stop: SourceFailure: Source HTTP 502` | 16 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/1c5b232e-3c77-4034-ac51-308771639963-0001.json>) |
| ExistingStop / Collect | `Attempt blocked by retained stop: URLError: <urlopen error [WinError 10054] An existing connection was forcibly closed by the remote host>` | 111 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/015123ef-fdb9-4b82-b9fc-d51fba4d778b-0001.json>) |
| ExistingStop / Collect | `Attempt blocked by retained stop: ValueError: Class moved; inventory required` | 126 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/05f1e972-1515-49b8-b8aa-a3263b9a2d47-0001.json>) |
| ExistingStop / Collect | `Attempt blocked by retained stop: ValueError: Entry horse identity conflict` | 10 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/0005a681-2c18-4234-bf01-6bae33c3e8ea-0001.json>) |
| ExistingStop / Collect | `Attempt blocked by retained stop: ValueError: Immutable evidence collision` | 60 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/0166c08e-95e5-4347-baf2-3a0deb4ea7bb-0001.json>) |
| ExistingStop / Collect | `Attempt blocked by retained stop: ValueError: Run journal exceeds accepted cell budget` | 14 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/10fa9d79-dc35-4f81-a017-b439e14936cb-0001.json>) |
| Handoff / Publish | `ValueError: Immutable retained cache collision` | 1 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/78c19dd7-535e-489c-95b7-f04cf2077415-0001.json>) |
| IdentityConflict / Reconcile | `Held conflicting entries: SOURCE_ID&#124;SOURCE_ID; exact prior/source evidence retained in the directory observation.` | 56 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/0217f4c1-e691-4455-ac21-d10767b6c03d-0001.json>) |
| Logging / — | `Logging delivery budget reached` | 8 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/0f92769a-82fc-45bc-aa8d-cf904893830a-0002.json>) |
| Logging / — | `The operation was aborted due to timeout` | 14 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/2273dd90-729a-47bf-ace2-54c23887fcb4-0002.json>) |
| Process / — | `Schedule collector already running` | 4 | [record](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/logging/wef-data/err-logs/0f92769a-82fc-45bc-aa8d-cf904893830a-0001.json>) |

## Instructions and safe delivery

Apply [AGENTS.md](AGENTS.md), the [owner operating block](../docs/coordination/wef-owner-operating-instructions.md) and [common WEF review standard](../docs/coordination/wef-code-review-standard.md). Preserve all established keys/records and original evidence. Owner approves; Records coordinates; Shows is canonical. Documentation is not deployment or recovery authorization.

Use the existing guarded deployment route, exact reviewed source hashes and backups, existing locks, and separately authorized normal scheduled verification. Codex is not the runner. Never edit a failed production path or resume it without the applicable fresh approval. Preserve failure evidence and continue only independent authorized work. Never commit runtime credentials or private raw chat history.
