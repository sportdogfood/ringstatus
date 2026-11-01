# wef-season — retained entities and show/show-day history

## Horse associations — version1.1.0, November1,2026,08:37America/New_York

Owner-approved addition to the existing hourly task: horses now retain rider-associations and trainer-associations as additive JSON maps using the unchanged rider/trainer keys, with existing shows,show-days and entry/trip source keys. Different riders or trainers on different trips/shows are valid. No bindings table, copied profiles, new identity convention, Data changes or new gating was introduced. A changed/ambiguous role on the same source is retained in the new conflicts table; only that assignment is withheld and other associations continue. Missing roles are not guessed. Existing entity IDs, source clocks and historical memberships remain protected.

Two fields were added to horses: rider-associations fldtUh5WtRHYsDjq6 and trainer-associations fldSo6L3GZj5joi40. The approved conflicts table is tblZlDewVNlXmPRqS in the same horse base. It uses existing source-key,source-type,horse-key,details,status fields; repeat issues upsert without duplicate conflict rows. No rider/trainer profile data is copied into the horse base.

Runtime changes are Season-owned season.py,horse_associations.py and the horses-only portion of schema.json. Existing source fields are read; no source acquisition or Data mutation. Independent21targeted checks and scoped source review passed; delivery completed08:37Eastern. The normal08:51run succeeded in all4bases and completed08:53:05Eastern:897horse association fields updated;295horses linked to312distinct rider associations,including17multi-rider horses;897trainer associations. Six ambiguous entry sources were saved to conflicts without blocking valid trip associations. Native auditPASS confirmed allprior records,keys,IDs,memberships,nicknames and valid associationreferences; no duplicate keys or evidence arrays. No collector was manually invoked. [Scheduled evidence](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/horse-associations-20261101/final-verification.json>).

## Current Season-only workflow — October 31, 17:44 Eastern

Season reads existing trainers, riders, horses and rings from wef-data and adds/upserts/deduplicates them in their four existing Season bases every hour. It copies existing keys/source IDs/source timestamps and unions supported show/show-day links. Directory associations supply show membership; single trip roles supply dated membership. Missing or ambiguous relationships do not prevent retaining the entity itself or manufacture an assignment. Existing records, history and owner ring nicknames are preserved.

The deployed one-file correction no longer reads completed-day receipts, sequences, journals, archive files or cached revisions, and does not require canonical freshness/fences or heartbeat checkpoints before entity updates. It reuses existing native SeasonStore writes and ring-helper maintenance. Temporary Airtable reads return Deferred; exact-key/native-ID/uncertain-write safety and existing task overlap protection remain. No Data files, schemas or task schedules changed and no collector was manually run. Code review and 13 targeted checks passed. The normal17:51 hourly run succeeded for all four bases, finishing17:52:32Eastern (about77seconds). It added1trainer,1rider and2horses, updated existing records and retained unchanged rows without duplicates. Rings and ring-helper updates succeeded with no new rows needed. Native GET audit confirmed every current Data entity key is present and every prior Season record/nativeID/key/sourceID/show-day link/ringnickname was preserved. Evidence: scheduled-result.json, scheduled-attempts.json and scheduled-native-audit.json in the packet below. Repeated invocation correctness was checked offline; the next natural hourly run is18:51. Prior input/ownership/status descriptions below are dated history, not current gates.

Evidence: [review/deployment packet](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/season-direct-upsert-20261031/task.md>). Data is independently owned by wef-data-fast-collector; this coordinator owns only the four Season bases. People remains cancelled.

## Current cadence — owner change, October 31, 2026

The existing Windows task `wef-season-collector` now repeats **every hour (`PT1H`)**, replacing five minutes. This applies to all four Season bases together. The original start boundary, action, account and `IgnoreNew` overlap setting remain in place. At readback, the next scheduled run was **October 31, 2026 at 09:51:14 America/New_York**. No manual task start was performed. Data's task and collection cadences were not changed. Five-minute descriptions below are historical.

To adjust later, edit the existing task in Windows Task Scheduler: **Task Scheduler Library → wef-season-collector → Properties → Triggers → Edit → Repeat task every**. One hour corresponds to `PT1H`. Preserve the existing start boundary, action, account, overlap setting and other task properties; do not create a second task. Read back the exported task and Next Run Time afterward. Changing cadence does not run collection or clear an entity stop.

Exact readback: [hourly-task-readback.json](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/season-completed-days-20261031/hourly-task-readback.json>). Before/after task exports are saved beside that receipt.

## Completed-day input — deployed October 31 at 09:00:57 Eastern

Season now reads Data's existing immutable `runtime/completed-days` receipts. The existing local accepted handoff supplies the initial complete facts once. Later completed-day changes are verified and applied in sequence. A cache in Season's own `runtime/day-reader-{customer}-{show}.json` retains resolved facts and cumulative four-entity projections, including memberships seen only in intermediate observations. Each base's existing heartbeat checkpoint advances only after its own writer/readback succeeds. An unchanged receipt skips entity writes; missing verified dependencies defer. New shows do not borrow another show's seed, and a selected-date change within the same show does not discard retained history.

Only `wef-season/season.py` was deployed. Existing projection/store/helper modules, Data files, keys, schema, native records, nicknames and entity stops were preserved. There is no new adapter, provider acquisition, Git operation or outgoing archive scan in this input path. Historical destination records stay intact.

Source SHA-256: `a3d1e682b2ab9687b723fc4c51719f432f0bd19ede60231403b144c7f26572b8`. Independent new checks: 9 PASS. Existing compatibility checks: 19 PASS and one obsolete Data-fixture error, preserved and not rerun or described as passing. The actual local-only input check validated 139 receipts and all four projections; it was not a collector or live Season write.

[Review](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/season-completed-days-20261031/independent-review.md>) and [deployment receipt](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/season-completed-days-20261031/deployment.json>). First post-deployment hourly scheduled acceptance is pending the existing 09:51:14 Eastern wake. No manual collector was run. Earlier handoff/archive and five-minute descriptions below are historical and do not describe this new input path.

## Current Data/Season status — 2026-10-30T17:02:31.972950+00:00

Outgoing archive correction deployed `dec332007a16fa31c06af39488a44a9678adff95` through existing guarded locks at12:55Eastern October30. Frozen retry snapshots/cutoffs, all unpublished-path validation and history-preserving publication independently reviewed PASS;163offline tests run160passed3skipped. Normal scheduled acceptance is pending; no manual runs, tasks/schema changes, record repairs, observers or Git history rewrite. Natural different-show rollover remains unobserved; offline race/actual-caller proof is separate. Evidence: [deployment](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/outgoing-deployment-20261030.json>), [review](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/outgoing-race-independent-review-final-20261030.md>), [scheduled proof](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/outgoing-scheduled-proof-20261030.json>).

Prior immutable timestamp-collision release886906ee2 remains preserved and accepted: two normal Data completions/eight Season acknowledgements, original keys/native IDs/historical links/evidence retained. Current code adds only separately approved outgoing corrections. Shows remains canonical; Entries linking-only; sibling collectors unchanged. Data1minute/Season5minute wakes unchanged. Four Season bases remain separate and People remains CANCELLED. No migration/exporter/backlog expansion is activated.

Separately owner-approved current-day freshness correction is private and NOTdeployed. Fresh fairness approval received; cumulative count-turn preserves cross-run fairness without changing slot keys. One current-day pass coalesces at most40due reads through the unchanged four-worker saved-response capture. Final independent reviewPASS;176full testsrun173passed3skipped,13focused independentPASS. Release9fe5330d2 is ready, NOTdeployed; normal timing/freshness remains unverified. Current-day source ages remain original; Shows five-minute guard stays locked. Previous four-class persistence measured377.955seconds for37scopes before publication. Coalescing changes that cost; only normal scheduled source-to-published timing can establish the300-second target. Exact stale input and references: [diagnosis](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/data-stale-count-diagnosis-20261030.json>); [review](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/current-day-refresh-independent-review-final-20261030.md>).

Next authorized action: finish outgoing normal scheduled proof independently; deploy reviewed freshness release9fe5330d2 under existing locks at natural idle, then verify two normal Data completions, original count ages, Shows consumption and four-base Season compatibility. Preserve the earlier failed review as dated evidence. No stopped task or production record was manually resumed/repaired. Existing Monday09:00Data and Monday10:30Season reviews remain separate, firstNov2Pending. No business observers.

## Workflow and authority

1. Read fresh canonical wef-shows context (`appCaSmL819KK7UFJ`, `tblJA5CUon5s1sWib`); never discover/select a show here.
2. Read Data's completed accepted head matching exact show/start/end/selected-date fence and immutable predecessor/hash/native map/link proof. Also consume verified outgoing archived histories not already checkpointed. A stale/missing/mismatched head is Deferred, not a fabricated empty result. No provider acquisition or Data-owned writes.
3. Copy existing source keys/IDs/names/source timestamps. Entries establish show-only registration. Verified single trip roles and linked class/day context establish dated horse/rider/trainer memberships; ambiguous parent assignments stay withheld. Missing trainers/day attendance are not guessed. Rings retain their existing dated ring keys/direct show-day links; they are not rekeyed into global physical rings.
4. Add missing entities, upsert existing keys, union historical show/day memberships and preserve destination-local native IDs/owner fields. Names never merge identities. Duplicate existing keys or conflicting source identities are errors, not permission to delete/repair. Older/unknown timed facts cannot replace newer facts; replay/acceptance time does not renew source freshness.
5. Read back required fields/native links, renew canonical fence, then advance each entity's independent per-show heartbeat checkpoint. Same revision skips; inconsistent equal sequence fails. One base's failure/stop does not become permission to modify another.

## Exact retained bases and checkpoints

Deployed [schema.json](schema.json) pins every table/field ID. Shared Data-owned [season_projection.py](../wef-data/season_projection.py), [season_store.py](../wef-data/season_store.py) and retained contracts implement the existing mapping/store.

### wef-season-trainers

Base **`appzZsd3UqMRR2CNc`**, owns trainers retention plus local shows/show-days. No competing Season identity is introduced.

| Table | Existing table ID | Primary field |
| --- | --- | --- |
| shows | `tblhwDJZDwrsFkpsL` | `show-key` |
| show-days | `tbl5L8plumfTMxGSq` | `day-key` |
| trainers | `tbl1vRzTPX4UOoW8u` | `trainer-key` |

Separate logs: heartbeat `tbl0KH3p6Xzo956aA`, run-logs `tbl3pnpgmhxYX1IHB`, err-logs `tbl7Q4ZpA0E7fcK11`. Authoritative per-show checkpoint JSON is heartbeat field `fldLuEFC9KGS55fJl` in `tbl0KH3p6Xzo956aA`. The checkpoint carries existing show-key, accepted revision/run/sequence or archive commit/path and acceptance time; it does not replace original source time.

Release review: [recyQXvJZbYFCwwsJ](<https://airtable.com/appzZsd3UqMRR2CNc/tbluWs084sVApiWyZ/recyQXvJZbYFCwwsJ>), **PASS within reviewed Season scope**, Records GET-readback confirmed. One independent shared Season implementation review (24offline tests) supports four base-specific records; it was not four independent test runs.

Latest saved local result in the `2026-10-30T13:17:47.127Z` receipt: status `Succeeded`, source revision `5854ddd65b6608416aa68875244c0ea9e6427f798682187032d7c2eeaeac3b7c`. This is the observed local completion/checkpoint-readback result, not a fresh direct Airtable checkpoint query. Last full native audit08:58Eastern pinned all four generation77 checkpoints and unchanged historical75: [archive-recovery-season-final-audit-summary.json](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/archive-recovery-season-final-audit-summary.json>).
### wef-season-riders

Base **`appkWkm9rFLrkUvaD`**, owns riders retention plus local shows/show-days. No competing Season identity is introduced.

| Table | Existing table ID | Primary field |
| --- | --- | --- |
| shows | `tblqMP5w5rZsLqBTS` | `show-key` |
| show-days | `tbl4njCgUqYc2t7g0` | `day-key` |
| riders | `tblQxTdBoM6hWEZzX` | `rider-key` |

Separate logs: heartbeat `tblAt5xXFQ7csoVG6`, run-logs `tblij7G8aBQBZz4OD`, err-logs `tblfdIdLsqIYDugAf`. Authoritative per-show checkpoint JSON is heartbeat field `fldnSWPquosIf9z0M` in `tblAt5xXFQ7csoVG6`. The checkpoint carries existing show-key, accepted revision/run/sequence or archive commit/path and acceptance time; it does not replace original source time.

Release review: [rececydkDBe3pq0nO](<https://airtable.com/appkWkm9rFLrkUvaD/tbl1yR7TBkb9qGKNs/rececydkDBe3pq0nO>), **PASS within reviewed Season scope**, Records GET-readback confirmed. One independent shared Season implementation review (24offline tests) supports four base-specific records; it was not four independent test runs.

Latest saved local result in the `2026-10-30T13:17:47.127Z` receipt: status `Succeeded`, source revision `5854ddd65b6608416aa68875244c0ea9e6427f798682187032d7c2eeaeac3b7c`. This is the observed local completion/checkpoint-readback result, not a fresh direct Airtable checkpoint query. Last full native audit08:58Eastern pinned all four generation77 checkpoints and unchanged historical75: [archive-recovery-season-final-audit-summary.json](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/archive-recovery-season-final-audit-summary.json>).
### wef-season-horses

Base **`appETrjio9Yj9Pv17`**, owns horses retention plus local shows/show-days. No competing Season identity is introduced.

| Table | Existing table ID | Primary field |
| --- | --- | --- |
| shows | `tblI7qSWXr7Twtwa7` | `show-key` |
| show-days | `tbl2qxWvX4unSHHch` | `day-key` |
| horses | `tbl8uYnqzysdiBqiu` | `horse-key` |

Separate logs: heartbeat `tblTAnUc3Ep3nD5nQ`, run-logs `tblSF8zFpykvRFdDT`, err-logs `tblc2X4rDk97qZBYj`. Authoritative per-show checkpoint JSON is heartbeat field `fld9z9AynzbrU0xZC` in `tblTAnUc3Ep3nD5nQ`. The checkpoint carries existing show-key, accepted revision/run/sequence or archive commit/path and acceptance time; it does not replace original source time.

Release review: [recjyJucOJKkhDKQv](<https://airtable.com/appETrjio9Yj9Pv17/tbl6q1qxN2KgdbIaE/recjyJucOJKkhDKQv>), **PASS within reviewed Season scope**, Records GET-readback confirmed. One independent shared Season implementation review (24offline tests) supports four base-specific records; it was not four independent test runs.

Latest saved local result in the `2026-10-30T13:17:47.127Z` receipt: status `Succeeded`, source revision `5854ddd65b6608416aa68875244c0ea9e6427f798682187032d7c2eeaeac3b7c`. This is the observed local completion/checkpoint-readback result, not a fresh direct Airtable checkpoint query. Last full native audit08:58Eastern pinned all four generation77 checkpoints and unchanged historical75: [archive-recovery-season-final-audit-summary.json](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/archive-recovery-season-final-audit-summary.json>).
### wef-season-rings

Base **`appxMk9wfbBHEvC5e`**, owns rings retention plus local shows/show-days. No competing Season identity is introduced.

| Table | Existing table ID | Primary field |
| --- | --- | --- |
| shows | `tbll9QcoUzENjI565` | `show-key` |
| show-days | `tblVaBn3Q3xjFjWy3` | `day-key` |
| rings | `tblzQ33F0whmvcPjp` | `ring-key` |
| rings-helper | `tblF2LM7ZSEdsCqsM` | Existing facility helper `ring-key` |

Owner-approved October30 addition: `rings-helper` exposes unique physical rings with `ring-key`, `ring-number`, `ring-id`, `name` and owner-maintained `ring-nickname`. Later owner-requested `ring-unq` (`fld1iCTlN08SpdAVN`) is an Airtable formula displaying customer + `|no:` + ring-number + `|id:` + ring-id, for example `15|no:8|id:53`; it automatically recalculates from the existing fields and remains blank if required values are missing. All14computed values and unchanged prior fields/native IDs were read back; evidence: [ring-unq-field-verification-20261030.json](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/ring-unq-field-verification-20261030.json>). It is a reference field, not a replacement upsert key; no collector code, configuration or task change was required. The existing five-minute Season workflow populates it from verified Data/history through [ring_helpers.py](../wef-data/ring_helpers.py). Fourteen existing helper identities seed key resolution; there are no ongoing Schedules/provider calls. Resolved keys are never replaced, names never establish identity, uncertain creates use the existing durable reservation/readback mechanism, and no nickname value is written by the collector. Dated `rings` history and its native show/day links remain unchanged. Helper synchronization also backfills equal checkpointed revisions, reads back every requested field, and defers without helper changes if any retained source is older than its accepted checkpoint. Data source timestamps remain in retained Data; the helper table is a reference list, not source freshness evidence. Release `e25e7cbda`: offline151run/148passed/3skipped and independent reviewPASS; deployed file hashes and unchanged task definitions verified. Normal scheduled run10:31Eastern added14helpers; the10:36wake completed10:37 with14unchanged/0added/0updated. Direct Airtable readback preserved all14helper native IDs/fields and all56dated ring records/keys/native links; both real run logs had no error links. No manual collector runs. Acceptance: [ring-helper-repeat-acceptance-20261030.json](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/ring-helper-repeat-acceptance-20261030.json>). [Review](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/ring-helper-independent-review-20261030.md>).

Separate logs: heartbeat `tblO9jrYgOlZRsHQj`, run-logs `tblXvV0cCJOWrDH3H`, err-logs `tblqOm2bonpArnu4T`. Authoritative per-show checkpoint JSON is heartbeat field `fldxudVk3Ayl8gyrp` in `tblO9jrYgOlZRsHQj`. The checkpoint carries existing show-key, accepted revision/run/sequence or archive commit/path and acceptance time; it does not replace original source time.

Release review: [rec3CSKz1au3Xs6XZ](<https://airtable.com/appxMk9wfbBHEvC5e/tblZa00dMCVYFo46K/rec3CSKz1au3Xs6XZ>), **PASS within reviewed Season scope**, Records GET-readback confirmed. One independent shared Season implementation review (24offline tests) supports four base-specific records; it was not four independent test runs.

Latest saved local result in the `2026-10-30T13:17:47.127Z` receipt: status `Succeeded`, source revision `5854ddd65b6608416aa68875244c0ea9e6427f798682187032d7c2eeaeac3b7c`. This is the observed local completion/checkpoint-readback result, not a fresh direct Airtable checkpoint query. Last full native audit08:58Eastern pinned all four generation77 checkpoints and unchanged historical75: [archive-recovery-season-final-audit-summary.json](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/archive-recovery-season-final-audit-summary.json>).


## Actual collection, retention and archive cadence

- Registered `wef-season-collector` wakes **every5minutes**, IgnoreNew, task limitPT0S; hidden VBS → PowerShell → Node `entry.js` → shared private Python `season.py`. Only normal scheduled triggers run business transfers. The host owner must be available/logged in. Locks and actual runtime mean a wake is not a promise of a completed transfer every5minutes.
- Each wake evaluates the then-available accepted Data head/outgoing histories. New shows and day/revision updates transfer on a subsequent normal wake **after verified Data exists**. Season does not start collection, control Data cadence or make itself a current acquisition readiness gate. Verified historical acknowledgement may separately be required before Data retires old-only rows.
- Season stores history **in its four Airtable bases**. It does not have a separate scheduled source/archive collector. Outgoing Data Git archives and immutable handoff objects remain Data-owned inputs; Season preserves show/week/day membership and checkpoints when consuming them. No Season entity history deletion or independent archive/export job is configured by this implementation. A lightweight public Season index/export remains a separately reviewed, undeployed deliverable, not an implied active job.
- Source scope controls meaning: directory-only registration is not daily attendance; missing/failed/partial input is not zero. A valid verified empty unit contributes no invented entities and cannot erase historical links. A successful transfer is not a declaration of complete current-source coverage or current schedule eligibility.

## Failure, logging, review and dated proof

Each destination owns heartbeat/run-logs/err-logs. Private `runtime/logging/wef-season-{entity}/{run-logs,err-logs,acks}` retains durable attempts, terminal success/Skipped/Deferred/failure and separate errors if Airtable is unavailable; later ordinary invocation delivers pending logs. Existing `runtime/{entity}/pending-create.json` reconciles lost create responses using original keys/native IDs; no blind duplicate create. `runtime/{entity}/stopped.json` isolates a declared failure until the applicable fresh owner authorization. Node overall receipt is `runtime/last-run.json`; no manual run, stop clear or record repair.

Two normal Data revisions/all-four Season consumption, same-revision skip, native baseline1967records/4149links and10506field checks: [archive-recovery-acceptance.md](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/archive-recovery-acceptance.md>). These are dated observed passes; different-show rollover was not forced/observed. Current reviewed Season code has no actionable direct finding in that scope; upstream full-outgoing Data P2 and shared dependency-baseline limits remain visible. Review index and full findings: [data-season-code-review-report-20261030.md](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/data-season-code-review-report-20261030.md>).

Use [wef-code-review-v1 procedure](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/wef-code-review-standard.md>) and the installed Code Review skill before separately approved material deployment and after observed behavior/verification failure. Reuse unchanged independent evidence only after exact hash/contract checks. Keep review, offline tests, deployment, normal scheduled acceptance and source freshness as separate gates. Do not review unchanged code per collector invocation. Review events do not start collectors or authorize fixes.

Records owns initial review-table/record writes for the present documentation request. Existing periodic automations have only their explicitly named review-record write permissions; they do not edit code, repair data or run business workflows. Leave their six fields and history intact. A date or README is not proof a review ran; host/app availability still matters. No new automation is created by this README update.


Periodic Season review is **ACTIVE and readback-confirmed by Records**: `wef-season-monday-code-review`, Mondays **10:30 America/New_York**, target this coordinator thread `01a169c8-a4cc-7f52-b9f9-ef0a769aab58`,20minute bound. Local config `C:/Users/gombc/.codex/automations/wef-season-monday-code-review/automation.toml` was independently read for ID/status/target/schedule. First review is November2,2026; all four routine records remain Pending, not completed future reviews.

| Routine scope | Existing periodic review record |
| --- | --- |
| trainers | `appzZsd3UqMRR2CNc/tbluWs084sVApiWyZ/recd56FCNbiq4Tumf` |
| riders | `appkWkm9rFLrkUvaD/tbl1yR7TBkb9qGKNs/recPrdL0LPFAcDTwa` |
| horses | `appETrjio9Yj9Pv17/tbl6q1qxN2KgdbIaE/recTSbpVGaYTTMII2` |
| rings | `appxMk9wfbBHEvC5e/tblZa00dMCVYFo46K/recl01Hjtwk9gilnP` |

This is distinct from Data Monday09:00 and from today's four release-review records above. Records created/verified the review setup; this module documentation created/edited no automation or Airtable row. Event-based reviews supplement periodic reviews and never change the5minute business cadence. Both schedules are best effort on the available host/app; Pending setup is not proof a Monday review executed.

## Instructions, hierarchy and skills

Read [module instructions](AGENTS.md), [canonical owner operating block](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/wef-owner-operating-instructions.md>) and [common review standard](<C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/wef-code-review-standard.md>) at startup/resume, changed direction and handoff. Applicable global/repository rules and actual owner approvals/stops remain controlling. A physical identical owner-block copy was read back at [coordinator owner-block copy](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/owner-operating-instructions.md>); a path alone does not transfer access.

Owner approves scope and changes; Records coordinates contracts/evidence. Canonical Shows → Data acquisition → Season retention. Live observes independently; Entries links only. Schedules still runs independently. Schedules retirement, reader switches and a future Data/Season ownership split are **planning only**. The temporary fork transfer was reversed. This coordinator retains Data and separately Season; it owns no sibling collector.

Current installed operating package manifest and applicable skill files were actually read; no installation/update occurred. rs-skills reported its package/remote verification; this owner independently verified its local manifest version and read the files below. Legacy `$rebalance` maps to packaged `rebalancer` as a name adaptation, not a new workflow.

- `instruction-loader`: [SKILL.md](<C:/Users/gombc/.codex/plugins/cache/created-by-me-remote/ringstatus-operating-skills/0.2.0/skills/instruction-loader/SKILL.md>) — READ in full, installed package **0.2.0**.
- `context-compact`: [SKILL.md](<C:/Users/gombc/.codex/plugins/cache/created-by-me-remote/ringstatus-operating-skills/0.2.0/skills/context-compact/SKILL.md>) — READ in full, installed package **0.2.0**.
- `code-review`: [SKILL.md](<C:/Users/gombc/.codex/plugins/cache/created-by-me-remote/ringstatus-operating-skills/0.2.0/skills/code-review/SKILL.md>) — READ in full, installed package **0.2.0**.
- `rebalancer`: [SKILL.md](<C:/Users/gombc/.codex/plugins/cache/created-by-me-remote/ringstatus-operating-skills/0.2.0/skills/rebalancer/SKILL.md>) — READ in full, installed package **0.2.0**.
- `handoff`: [SKILL.md](<C:/Users/gombc/.codex/plugins/cache/created-by-me-remote/ringstatus-operating-skills/0.2.0/skills/handoff/SKILL.md>) — READ in full, installed package **0.2.0**.

READ is not APPLIED or VERIFIED: Instruction Loader was applied to source/scope checks; Rebalancer to boundaries/current evidence; Code Review to the separately documented review; context-compact/handoff to continuity preservation. Verified documentation/source/guard receipts are linked below. No manual compaction is claimed. Loading skills creates no implementation approval or review schedule.


## Continuity, approval boundary and next action

Production/code baseline is the already deployed release, not this documentation update. Existing editable implementation: `C:\Users\gombc\Documents\Codex\2026-10-23\find-all-docs-discussing-wef-people-4\implementation`, branch `codex/wef-data-recurring`; production `C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus`. Runtime release `741ed4f5a916fac648e39dc5b028b48114be8a29`; prior archive baseline `952c92fa7e7f74f9dc041fe7bb867cceb97ff731`; assignment baseline `28b4cf0b920148866b3cffceef97280223572d47`. Exact owned/source/dependency hashes and limits: [module-code-review-baseline-20261030.json](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/module-code-review-baseline-20261030.json>). The pinned transitive `wef-schedules/collector-core.js` differs from this editable checkout; do not assert full shared-package equivalence or edit Schedules.

Established authorized deployment route: inspect/review the exact delta, pass applicable offline gates, then guarded file/hash deployment with backups under existing Data Python + Season locks during natural idle; verify **normal scheduled runs** and downstream readback. Never substitute a manual collector/task start, endpoints, reset/rebase/force push or record repair. No deployment is authorized by these documentation instructions.

Private resumable checkpoint: `C:\Users\gombc\.codex\context-archives\01a169c8-a4cc-7f52-b9f9-ef0a769aab58\resume.md` and `state.json`; thread `01a169c8-a4cc-7f52-b9f9-ef0a769aab58`. Source rollout is private; never put raw history, secrets, env/venv or session state in business archives/review notes. Read the latest checkpoint and relevant evidence, verify current source/runtime/approvals, preserve original source time, and continue only actually authorized work. No new chat, monitor or worker is implied.

Current authorized next action is the separately approved collision recovery and scheduled acceptance recorded above; documentation alone grants no business-change permission. The confirmed Data full-outgoing publication P2 needs **fresh explicit approval** before a fix, deployment or failed-path resumption; it is not fixed here. On an actual new failed verification, stop the affected execution path; continue unaffected approved work, preserve evidence, and obtain the required precise approval. Do not reinterpret old approvals or this README as permission to resume. Context pressure/compaction does not widen scope.

