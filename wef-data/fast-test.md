# Fast test log

Append future tests as separate dated entries. Preserve each test’s setup, actual results and evidence; do not overwrite earlier results.

## Implementation record — November 12, 2026

The owner subsequently approved implementation, debugging, Code Review, deployment and scheduled verification, with no manual collectors or additions. The reviewed implementation was deployed at approximately 10:24 a.m. Eastern to the existing collector/linker task actions. Original task triggers, principals and settings were preserved. The older running pass was left to finish. Natural scheduled acceptance is pending; the earlier discussion labels below describe their status when recorded, not the current deployment state.

Independent Code Review: COMPLETE/PASS. Mocked checks: 10 Python and 3 Node tests passed, including first/repeat passes, single-attempt class requests, failed-class exclusion, unchanged schedule rows, missing-ID trip retention, participant identity preservation, independent acquisition/saving and exact fresh Live scope. Actual schema bindings were independently retested. Evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-fast-workflow-20261112/independent-review/`.

Source-loop independence applies within each one-day invocation. A day finishes saving/logging before the next day starts sequentially. No parallel-day collection or additional service was introduced.

## Correction deployment — November 12, 2026, 11:09 a.m. Eastern

This record supersedes the earlier implementation status, not the workflow below. The owner explicitly reapproved editing to match this MD exactly. Collection now captures only; the existing separate saving task reads those captures and delivers collection logs; the existing linking task fills required empty links. The five reviewed runtime changes were deployed at 16:09:25Z. Existing task triggers/settings/principals were preserved apart from enabling the existing saving task. No manual collector or task start was performed.

Independent Code Review COMPLETE/PASS; 14 Python and 6 Node offline checks passed. The prior implementation's review remains FAIL in its historical packet. Natural scheduled end-to-end verification of this correction is UNVERIFIED. Current evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-fast-md-exact-20261112/`.

## Current approved workflow — Day 0 and Days 1–4

Owner-approved November 12 after the scheduled preflight failure. This is the current requirement and supersedes conflicting earlier discussion and implementation-status notes. The amendment and separately approved one-line UTF-8 correction are deployed. The normal scheduled collection, saving, linking, logging and same-date repeat boundary were verified below. Historical failures and their evidence remain retained.

### Implementation and delivery limits

- Keep the implementation simple. No added complexity, gates, optional features, enhancements or work outside this agreed workflow.
- Preserve the proven one-day implementation and implement only the explicitly approved differences. No patch-over-patch layering, speculative fixes, unrelated cleanup or reinvention.
- Codex owns quality control. The owner is not the tester or quality-control step.
- When implementation is authorized, complete the approved workflow end to end, including collection, separate saving, basic links, remaining empty-link completion and logging. Debug the actual approved behavior and verify downstream results; local tests alone do not establish scheduled end-to-end success.
- Run Code Review on the actual changes before delivery. Review is for finding defects against the agreed requirements, not adding enhancements or expanding scope.
- No final enhancement pass is authorized. Correct verified defects within the existing edit authority and rerun affected checks; do not use final review to add features, gates or speculative protection. Existing failed-runner approval boundaries remain in force.
- Do not call work complete until the agreed end-to-end result is verified. State any exact remaining blocker or missing proof without substituting the owner's review for Codex quality control.
- This documentation update authorizes no implementation, deployment, manual collector, credential change or schedule change.

| Stage | First pass | Recurring passes |
|---|---|---|
| Same-date completion | No previous pass to wait for. | Do not start another collection for this actual show/date until its previous saving and linking have completed. Other dates continue independently. |
| Health and lane | Confirm schedule and getLiveClassData access. Check the saved working lane first; use fallbacks here if needed. Retain the successful check responses. | Confirm/reuse both working lanes before collection, with recovery only in preparation if needed. |
| Schedule | Use the successful preparation response; capture it for separate saving. | Compare source-position and decoded source-json (excluding encrypted _) with saved schedules; save only new/changed rows. Show each changed field as old → new. Full diffs are in schedule-diffs in the existing local capture/report; the run log records their count and capture path. |
| Rules | Apply existing exclusions and priority order. | Same rules; also exclude classes marked failed. |
| Collect classes | Request each eligible class once using `getLiveClassData`. Capture responses locally. | Request remaining eligible classes once. No retries or fallbacks inside collection. |
| Save | A separate stage reads captured responses and saves records with their basic links. | Same. Changed-only saving for class data is not yet defined. |
| Link | `fast/linker.js` fills remaining required empty links as saved records become available. | Same; preserve existing links and leave ambiguous matches empty. |
| Errors | Record failures and indicate them on the schedule row. Do not retry. | Keep failed classes excluded. |

- **Day 0:** pause protected class-detail requests without removing the endpoint. Use `getLiveClassData` until a successful scheduled live run saves fresh data for the exact expected show, date and class. Then stop Fast's requests for that class. SGL live flags alone are insufficient.
- **Days 1–4:** use `getLiveClassData`, mapped to existing fields. Scores/results are not needed for the future-day preparation discussed; the owner's explicit earlier clarification named Days 1–3.
- **Existing rules:** retain records but skip requests for No entries/zero trips, Flat, Under Saddle/Flat, Model, breaks and priority 999. Request First trip → Second trip → Classic → other.
- **Basic links and remaining links:** saving still sets the basic links needed to identify each record. The exact basic-link field list is not yet specified. `fast/linker.js` covers the remaining required empty links from already saved records; it does not fetch source data or relink populated fields. Existing implementation must be verified through its scheduled path.
- **Independent progress:** collection, saving and linking run separately. Finishing collection does not wait for saving or linking, and the next date can collect. Only a repeat pass for the same actual show/date waits for that date’s prior saving AND linking to finish. Linking can fill empty links as records become available. It confirms completion only for saves finished before that linker pass began.
- **No additions:** the only repeat-start condition is the same-date save/link completion explicitly approved above. No global hold, extra retries, optional features, new tasks, credential changes or manual collectors. Existing URLs, pause, separate task triggers and native records/identities/links stay unchanged.

## Historical discussion — recurring passes for November 14 only

### Working lane before collection

Discussed only; not implemented or approved for code changes.

- Find a working lane once, then reuse it for November 14’s class requests without repeating full fallback discovery for each class.
- A successful schedule request confirms the schedule lane. A successful class request is needed to confirm the class lane.
- A later individual class failure alone does not prove the working lane has stopped working.
- The original November 14 test did not call `/health` before starting. It requested the schedule directly; runner/fallback selection happened inside the source requests.

### Saving after collection

Discussed only; the separate saving stage has not been built or tested.

- The test already saves complete successful class responses locally in `runtime/one-day-test-2026-11-14/responses/` (redacted captures).
- A later saving stage can read those saved responses and write class, entry, horse, rider, trainer, owner, trip and result records to Airtable without requesting the classes again.
- The tested script still performs those Airtable saves after each successful response, before requesting the next class. It does not yet separate collection from saving.

### Future-day class source and errors

Discussed requirements only; no code changes are authorized by this documentation update.

- Earlier discussion kept the protected class-detail endpoint for Day 0. The later Day 0 decision below supersedes that request behavior; the existing endpoint itself is not to be removed. The proposed source for Days 1–4 is iphonev2 `getLiveClassData`; its fields must be mapped before changing the workflow.
- The owner explicitly clarified that Days 1–3 do not need scores or results because they are not Day 0.
- iphonev2 `id` maps to protected `trip_id`. Verified example: class `200031435` has iphonev2 `id: "200402675"` and protected `trip_id: 200402675`. The trip ID is present under a different field name.
- Other discussed mappings: `ENo` = entry number (not entry ID), `Hor` = horse name, `Rid` = rider name, `Tra` = trainer name, `Own` = owner name, `OOG` = order-of-go (blank means none supplied).
- Record these future-day class-request errors, but do not retry them. Recurring passes must exclude the failed classes as described below; do not send these errors to a retry/trouble pass. This supersedes retrying these errors, not retaining their records or error evidence.

### Day 0 — revised decision and live confirmation

Discussed requirements only; not implemented.

- Pause Day 0’s protected class-detail requests and use iphonev2 `getLiveClassData` instead. Preserve the existing protected endpoint; do not remove it.
- Once our live collection is confirmed for a class, stop Day 0’s replacement requests for that class.
- `ListAjax` and `ClassStatus` can report `is_live: true`. These are SGL status signals, not proof that our live collector is running, saving successfully, or collecting the expected date.
- Before stopping Day 0 requests, confirm fresh saved live data from a successful scheduled live run for the exact expected show, date and class. Neither an endpoint’s live flag nor a successful run for another date meets this requirement.

### Recurring-pass steps

Not implemented. This documents the discussion, not approval to change code.

For each second and later pass of this same one-day script:

1. Fetch November 14’s schedule again.
2. Match rows by `schedule-key` against saved WEF Data `schedules` records (`tbl6KKlYuMIPHEJWe`). Compare `source-position` and all decoded values in `source-json`, excluding encrypted `_`. Do not treat `last-success` or logging links as schedule changes. Save only new or changed schedule rows.
3. Apply the same existing class-request rules: no detail requests for No entries/zero trips, Flat/Under Saddle/Flat, Model, breaks, priority 999, or Completed classes with verified results. Keep the schedule records. Eligible order remains First trip → Second trip → Classic → other.
4. The schedule row needs to show when its class-detail request failed, so recurring passes can exclude that class from detail requests. A successful schedule save does not mean class details succeeded. Keep the schedule row and its saved data. Failed classes remain excluded until the failure mark is cleared. The exact field/mapping has not been selected; this is not implemented, and the original test has no dedicated failure-table integration.
5. Request class details for the remaining eligible classes and save their returned data.

The schedule comparison does not handle failures or change the existing eligibility rules. This discussion adds no failure recovery procedure or changed-data-only saving for class details.

## 2026-11-12 — Original one-day test on November 14

### iphonev2 comparison supplied by the owner

For this specific November 14 comparison, `getLiveClassData` was the only iphonev2 endpoint that supplied useful trip/participant payloads for both classes:

- Class `200031435`: decoder returned HTTP 200; owner-supplied iphonev2 payload contained 4 trip rows.
- Class `200031425`: decoder returned HTTP 502; owner-supplied iphonev2 payload contained 2 trip rows.
- Both payloads identify ring 2, E.R. MISCHE GRAND HUNTER, and have the same structure. Both have blank order-of-go, scores and times, with Gone and Scr set to 0. Those blank fields do not explain the decoder failure.
- Route: `https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/getLiveClassData?show_id=200000078&cid={class-id}`.
- `ClassStatus` supplied live-status information, not equivalent trip detail. The supplied `ListAjax` payload listed November 12 classes and did not include these November 14 classes. This does not establish that ListAjax can only return Day 0.
- This finding applies to this comparison; it does not establish that every other iphonev2 endpoint is unhelpful, explain the decoder’s 502, or prove full equivalence to the protected class-detail endpoint.

### Authorized scope

Owner instruction: “use the one day test on the same day-2 Nov 14 do nothing else”. This was an explicitly requested manual diagnostic test, not proof of scheduled execution. No trouble pass, production code edit, task change, credential change or endpoint research was performed. The test wrote business records and operational logs through the original collector’s saving/logging behavior.

### Exact setup

- Target: customer `15`, show `200000078`, date `2026-11-14` (Day 2).
- Attempt ID: `4db6a77e-f934-4d8d-8400-6da7b047e228`.
- Baseline Python file: `C:/Users/gombc/Documents/Codex/outputs/wef-data-day1-new-20261112/day1.py`.
- Baseline SHA-256: `fa0e0a2f498353b3337f3a95ceb276983fc745c96b02b6a9cd80bdd4099de910`.
- The archived original Python implementation was executed in memory. Its day-selection predicate was changed from synced priority 1 to the exact date `2026-11-14`; its existing default-show selection remained.
- Test-only response/log output used `runtime/one-day-test-2026-11-14`. The existing run-mode label identified the November 14 test; its existing cadence metadata stored priority 2 rather than priority 1.
- Existing current Source transport, mappings, Airtable client and logging dependencies were reused. This was not a frozen replay of all original dependencies.
- Existing scheduled tasks were left unchanged. The test was not isolated from scheduled workload, so runtime is not a controlled throughput benchmark.
- Original behavior: schedule first; existing eligibility rules; serial class detail requests; business saves during collection; final error/log delivery.
- Decoder fast-mode was not selected: the shared source reader sends Accept and Cache-Control headers, without `X-RS-Recovery: defer`. A collector request may therefore invoke decoder fallback internally.

### Known behavior of this original test

- **Existing rules were used.** The test reused `select_class_details` and `schedule-priority.js`; no new eligibility rules were added. It skipped 67 classes under those existing rules.
- **Recurring normal passes are not changed-data-only passes.** The original test fetches the schedule, reapplies eligibility rules and requests each eligible class again. Its save function finds existing records by key but still submits PATCH writes for every supplied batch; it does not compare business fields and omit unchanged rows. Completed classes with verified results can be skipped by the existing rules. Calling this a first pass does not select a different algorithm for later normal passes.
- **No dedicated failure-table integration.** The original test does not create or update WEF Shows `decoder-request-failures`. Its 33 failures are saved in the test report and WEF Data `err-logs`, linked to its run log. The current sequential script has the additional failure-table integration.
- **Order-of-go is still returned within class detail.** The test requests `/classes/{class-id}`; it does not make a separate order-of-go request. It saves the complete decoded class response, including `class_group_order_of_go` when returned, into Airtable class `source-json`. It does not use the current script's grouped-block removal or Day 0 Git archive.
- **No trouble pass.** This test ends after its original collection, saving and logging steps; it does not retry its failures through the current sequential script's trouble pass.

### Actual result

| Measure | Observed |
|---|---:|
| Start, Eastern | Nov 12, 8:03:55 a.m. |
| Collector wrapper completed, Eastern | Nov 12, 8:23:01 a.m. |
| Collection and saving | 1144.945 seconds (19m 5s) |
| Wrapper through final report, before log delivery | 1145.529 seconds |
| Status | Partial — class requests failed |
| Rings returned | 9 |
| Class detail requests | 100 |
| Class detail successes | 67 |
| Class detail failures | 33 |
| Classes skipped by existing rules | 67 |
| Acknowledged business-record writes | 2787 |
| Operational log delivery | Succeeded; 105 operations |

Write counts include updates and repeated writes to the same record, not unique/new-record counts. The 2 run-record writes are separate from the 2,787 business writes. The measured collection time includes saving; full completion time including final log delivery was not separately recorded.

### Writes by table

| Table | Acknowledged writes |
|---|---:|
| runs | 2 |
| rings | 9 |
| groups | 97 |
| classes | 234 |
| schedules | 399 |
| entries | 306 |
| horses | 306 |
| riders | 274 |
| trainers | 260 |
| owners | 290 |
| trips | 306 |
| results | 306 |

### Actual errors

All 33 errors have exactly this saved message:

```text
SourceFailure: Source HTTP 502
Source fetch or protected response decoding failed
```

This message does not establish the precise upstream cause. No root-cause claim is made from the status code alone.

| Ring | Class ID |
|---:|---:|
| 2 | 200031425 |
| 2 | 200031420 |
| 2 | 200031458 |
| 4 | 200031646 |
| 4 | 200031651 |
| 4 | 200031726 |
| 4 | 200031617 |
| 4 | 200031657 |
| 5 | 200031632 |
| 5 | 200031675 |
| 5 | 200031644 |
| 6 | 200031587 |
| 7 | 200031388 |
| 7 | 200031393 |
| 7 | 200031345 |
| 7 | 200031322 |
| 7 | 200031340 |
| 7 | 200031440 |
| 8 | 200031601 |
| 8 | 200031659 |
| 2 | 200031436 |
| 2 | 200031426 |
| 6 | 200031476 |
| 6 | 200031479 |
| 6 | 200031484 |
| 6 | 200031514 |
| 6 | 200031523 |
| 6 | 200031502 |
| 7 | 200031323 |
| 7 | 200031341 |
| 7 | 200031441 |
| 3 | 200031683 |
| 3 | 200031682 |

### What this proves and does not prove

- CONFIRMED: the original one-day implementation selected November 14, returned nine rings, attempted 100 eligible class requests, saved valid returned data and delivered its operational logs.
- CONFIRMED: it did not reproduce the original Day 1 zero-error outcome; this test had 33 class-request failures.
- CONFIRMED: both the original one-day implementation and current sequential implementation use the existing Source reader without selecting decoder fast-mode.
- UNVERIFIED: why each upstream class response failed; the saved generic 502 message is insufficient.
- Not a scheduled-workflow proof, an isolated speed comparison, a fast-mode test or a recovery-pass test.
- The earlier completed scheduled Day 2 pass requested 102 classes and skipped 65; this later test requested 100 and skipped 67. Inputs and saved eligibility state were read live, so these are not identical fixed-fixture tests.

### Evidence

- Full report, counts and all errors: [result.json](runtime/one-day-test-2026-11-14/result.json).
- Private run receipt: [attempt receipt](runtime/one-day-test-2026-11-14/logging/run-logs/4db6a77e-f934-4d8d-8400-6da7b047e228.json).
- Redacted individual source captures: `runtime/one-day-test-2026-11-14/responses/`.
- [Saved Airtable run log](https://airtable.com/appdl16HJ5ETm8N69/tblKMNAtvvIFSvgLh/reci2NFOQXvtJ5NIQ).


## Scheduled verification — November 12, 2026, 11:53 a.m. Eastern

The 11:45 scheduled pass completed through separate saving, empty-only linking and native logging. Source/URL/pause/task settings and credentials stayed unchanged; no manual collectors or capture replay were run. The only later correction was the separately approved UTF-8 capture read, independently reviewed and tested using the actual scheduled Python.

| Day | Date | Source collection | Class requests | Acknowledged business writes | New source/save/link errors |
|---|---|---:|---:|---:|---:|
| 0 | Nov 12 | 5.05 sec | 0 (Live owns 7) | 39 | 0 |
| 1 | Nov 13 | 20.36 sec | 51 | 2,432 | 0 |
| 2 | Nov 14 | 6.11 sec | 0 (prior failed classes excluded) | 264 | 0 |
| 3 | Nov 15 | 5.39 sec | 0 (prior failed classes excluded) | 217 | 0 |
| 4 | — | Skipped: outside show range | 0 | 0 | 0 |

Counts are acknowledged writes, including updates; not new or unique record counts. All four native Data run records are Succeeded with zero errors and original show/day links. Native saving log records 2,952 business writes; final linking log records 3 empty-link updates and 3 completed passes. Logging delivery succeeded. Saving completed by 11:52:16; final linking completed at 11:53:28 Eastern. The three previously affected November 13 classes now have 77 freshly saved trip records (47 + 18 + 12), matching their current captures, with show/day/class links. Their earlier captures contained 76 rows; current collection added one trip.

Same-date sequencing was observed: Day0's next collection began 11:51:14 after its saving finished 11:48:27 and linking finished 11:50:27. That next-date-independent work proceeded before Day1 saving finished 11:51:33. Days1–3 next collected after their own linking finished 11:53:28. No global save/link hold was added.

Schedule diffs were captured as field/old/new. Example: Nov13 schedule key `15|200000078|2026-11-13|1|200043443`, `total_trips`: **46 → 47**. Nov12 had 5 changed rows; Nov13 had 13; Nov14/15 had none in this pass. Full differences remain in each existing local capture/report's `schedule-diffs`; run logs include their count and capture path.

This verification does not declare the historically failed class endpoints recovered: their existing failure marks remain and exclude their requests as agreed. No history, failed marks or business records were manually repaired.

Evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-fast-md-exact-20261112/per-date/utf8/scheduled-verification.json` and `native-readback.json`. Independent reviews are retained under the per-date and UTF-8 `independent-review/` directories; the earlier failing releases are historical evidence.


## Approved Git latest schedule/classes — November 12, 2026, 5:38 p.m. Eastern

The owner requested separate schedule/class folders, automatic Git diffs and Airtable consuming the latest data. The existing scheduled collection, saving and linking tasks remain separate; no source URL, pause, rule, credential, cadence or manual collector changes.

1. Collection retains complete responses/errors in the existing local captures, once per eligible request.
2. After that date finishes collection, the independent saver writes successful payloads into the existing `ringstatus-data` repository, isolated branch `codex/fast-snapshots`: `schedules/{customer}/{show}/{date}.json` and `classes/{customer}/{show}/{date}/{ring}-{class}.json`. A failed response never replaces a good class payload. Existing data-repository main and unrelated commits are not published.
3. Git compares stable payloads against the last successfully saved revision for that exact show/date. Random encrypted schedule `_` and capture/observation clocks are not included in comparison. Full public class payloads are retained.
4. The saver reads changed payloads from the exact new Git commit. Identical schedule/classes are skipped. Within a changed class, existing unchanged mapped records are skipped. Existing error-mark writes continue separately. An unsuccessful save does not advance the applied Git baseline.
5. The existing independent linker fills required empty native links. The same actual date still waits for its previous saving and linking to finish. Other dates continue.

Git publication/compare details and the commit are recorded in each `.saved` receipt; its `.diff` file retains Git's old/new changes. Source captures/error history remain unchanged. Git failure is reported through the existing saver error log with captures retained. No new task, timer, source retry or trouble pass is added. Git reduces repeated saving; fresh source requests are still required to discover changes.

Offline: eight disposable Git/saver tests PASS; syntax PASS; single-agent Code Review COMPLETE/PASS. Installed at 22:38:36 UTC. Scheduled proof is pending; installation alone is not acceptance. Evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-git-latest-20261112/`.


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


## November 13 — approved Day 0 failure-checkbox correction

Day 0 no longer excludes a class solely because a matching schedule row has `class-detail-failed` checked. This supersedes the failed-class exclusion above for Day 0 only. Keep the checkbox and error history. Normal exclusions, priority order, confirmed Live ownership, separate saving/linking and Days 1–4 behavior stay unchanged. Each eligible class is attempted once per scheduled pass; no manual collectors. Test/review and scheduled verification evidence: `C:/Users/gombc/Documents/Codex/outputs/wef-day0-failure-exclusion-20261113/`. Natural 9 p.m. Eastern verification: November14 selected;89 class requests,32 normal-rule skips,0 Live skips. All89 public responses were HTTP200 with `{}`;zero valid class payloads. Saving completed9:01:50p.m.;linking completed9:02:55p.m. The checkbox correction worked; source acquisition failed. No class freshness or new class saves are claimed. No further runtime patch or manual retry was made.
