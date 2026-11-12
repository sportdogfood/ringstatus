# Fast workflow handoff — November 12, 2026

## Current assignment and authority

Same-role continuation of the Fast workflow task. The required outcome is exactly the workflow in `fast-test.md`, copied verbatim below. This is not a saver-restoration assignment, a redesign, or permission to add requirements from an assistant's summary.

Latest owner approval after this handoff was created: “APPROVED TO EDIT. Make the script match fast-test.md exactly, nothing more. Keep collection, saving and linking separate. Debug and run Code Review before deploying, then verify the scheduled run. No manual collectors.” That exact correction was independently reviewed (COMPLETE/PASS, 20 offline checks) and deployed at 2026-11-12T16:09:25Z. The existing save task was enabled at its unchanged trigger. Current packet: `C:/Users/gombc/Documents/Codex/outputs/wef-fast-md-exact-20261112/`. Normal scheduled end-to-end acceptance remains UNVERIFIED; preserve any new failed-verification stop. No additions or manual collectors are authorized.

The source block's “discussed”, “not implemented” and approval labels are historical wording preserved verbatim. They do not describe current deployment state. Current state is recorded separately below. The source document's implementation header is not operational acceptance evidence.

## Target and source

- Workspace: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus`.
- Source chat: `01a1a753-dce7-7951-a147-1b096e1538ba`.
- Model/effort: not independently observable in this handoff.
- Source: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/fast-test.md`.
- Source SHA-256: `c6aa8b83c5152a45f10cb0de6c04666824d09d78892821b175a988673cdac04f`.
- Checkout at preparation: branch `main`, HEAD `0ff7641f71db0120d26dac7805fa73b58e42050d`. The deployed Fast changes are working-tree changes, not established by that commit alone.
- Read root/global instructions and `wef-data/AGENTS.md`; apply current owner corrections over stale historical notes. Read applicable Instruction Loader and Handoff procedures. The stale package 0.2.0 paths were unavailable; actual 0.2.10 procedures were read for this handoff. No package changes were made.

## Exact MD workflow — verbatim source excerpt

## Consolidated discussed workflow — Day 0 and Days 1–4

Not implemented. This section records the owner's workflow discussion; it does not authorize code changes. Historical test results below describe the script that actually ran, not this proposed behavior.

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
| Health and lane | Check schedule and class access. Run fallbacks here to find a working lane. | Confirm and reuse the working lane. |
| Schedule | Fetch and save the day's schedule. | Compare with saved schedule rows; save only new or changed rows. |
| Rules | Apply existing exclusions and priority order. | Same rules; also exclude classes marked failed. |
| Collect classes | Request each eligible class once using `getLiveClassData`. Capture responses locally. | Request remaining eligible classes once. No retries or fallbacks inside collection. |
| Save | A separate stage reads captured responses and saves records with their basic links. | Same. Changed-only saving for class data is not yet defined. |
| Link | Proposed `fast/linker.js` fills remaining required empty links as saved records become available. | Same; preserve existing links and leave ambiguous matches empty. |
| Errors | Record failures and indicate them on the schedule row. Do not retry. | Keep failed classes excluded. |

- **Day 0:** pause protected class-detail requests without removing the endpoint. Use `getLiveClassData` until a successful scheduled live run saves fresh data for the exact expected show, date and class. Then stop Fast's requests for that class. SGL live flags alone are insufficient.
- **Days 1–4:** use `getLiveClassData`, mapped to existing fields. Scores/results are not needed for the future-day preparation discussed; the owner's explicit earlier clarification named Days 1–3.
- **Existing rules:** retain records but skip requests for No entries/zero trips, Flat, Under Saddle/Flat, Model, breaks, priority 999, and Completed classes with verified results. Request First trip → Second trip → Classic → other.
- **Basic links and remaining links:** saving still sets the basic links needed to identify each record. The exact basic-link field list is not yet specified. The proposed `fast/linker.js` covers the remaining required empty links from already saved records; it does not fetch source data or relink populated fields. This describes intended behavior, not an existing verified file.
- **Independent progress:** collection does not wait for Airtable saving or linking. Linking can work as records become saved; it does not wait for the entire day to finish and does not block collection.
- **No additional mechanisms defined:** the health checks, lane reuse across passes, separate saving/linking execution and failure-field mapping still require concrete implementation and verification. This section does not introduce additional retries, recovery procedures or gates.

## Discussed recurring passes — November 14 only

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


## Historical failed deployment evidence — superseded by correction packet above

- CONFIRMED: the deployed implementation does not match the copied workflow. No code correction was made following the audit.
- Existing registered collector is `wef-data-collector`; its five actions call `fast-hidden.vbs` for Days 0–4 sequentially. Existing `wef-data-linking` calls the new empty-link implementation. Existing schedules continue; no task was manually started or stopped for this handoff. Do not change their status or cadence from this document.
- Actual implementation: `wef-data/fast_workflow.py`, `fast-source.js`, `fast-entry.js`, `fast-day.py`, `fast-hidden.vbs`, `fast_linker.py`, `fast/linker.js`, plus two schedule failure-field bindings in `schema.json`. Preserve unrelated/user changes.
- CONFIRMED departure: saving runs in a background thread inside each day, but the day joins all saving before returning. It is not independently completed outside that invocation; saving delays the next day's action.
- CONFIRMED departure: class fallback discovery is inside the first class iteration rather than a completed preparation stage. A discovery failure still allows subsequent requests without a confirmed class lane.
- CONFIRMED defect: the replacement saver writes per trip instead of retaining original per-class table batching. Independent fixture: 61 PATCH requests versus 7 for ten existing trips. This is failure evidence, not authorization or an assignment to restore the old combined collection/save workflow.
- Other audited differences requiring reconciliation against the actual MD and existing contracts: new upfront inventories, changed class source-json representation, new blank-entry trip-key handling, omitted result/Git publication paths, and incomplete existing link coverage. Do not convert this list into new requirements or silently repair it.
- Latest inspected Day 3 result (November 15): 79 class requests; 2,484 acknowledged business writes; 847.203 seconds total; source collection finished earlier. One actual saving error: horse-table PATCH, `TransportDeferred`, nested `ConnectionResetError`, Windows error 10054 (“An existing connection was forcibly closed by the remote host”). Log delivery reported ok. Write counts are not unique record counts. The saved error does not establish why the remote connection reset.
- Full scheduled Day 0–4 collection, saving, linking and logging acceptance remains UNVERIFIED. Successful schedule health is not proof of class saving or complete workflow acceptance.
- Current independent review acceptance is FAIL for the saving regression. The earlier PASS in fast-test.md/README is outdated; preserve the historical evidence, but do not claim current PASS.

Evidence packet: `C:/Users/gombc/Documents/Codex/outputs/wef-fast-workflow-20261112/`.
- `independent-review/review.md`, `review-receipt.json`, `execution-log.json` and `save_batch_regression.py` document the current finding.
- `deployment.json`, `candidate-hashes.json`, task XML exports and source proof files record what was deployed and checked. They are historical snapshots, not proof of current unchanged state.
- Saved scheduled outcome: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/runtime/fast/day3-last-run.json` (runtime files can be overwritten by later scheduled runs).
- Original one-day source for baseline comparison: `C:/Users/gombc/Documents/Codex/outputs/wef-data-day1-new-20261112/day1.py`, SHA-256 `fa0e0a2f498353b3337f3a95ceb276983fc745c96b02b6a9cd80bdd4099de910`. Inspect it to preserve working behavior; it is not the finished independent-saving workflow and is not rollback authorization.

## Receiver's first action

Read the exact copied workflow, actual source file and current failure evidence. Keep the owner's requirement intact: collection captures responses and continues without waiting for Airtable; a separate saving stage reads captures and saves basic links; the linker fills remaining required empty links. Do not replace this with “restore the batched saver”.

The correction is deployed; continue read-only verification of its natural scheduled collection, saving, linking and logging through the current packet. Do not redeploy or manually invoke it. New scheduled failures do not authorize another code correction or retry. The exact MD remains the requirement; the older defect list is historical evidence, not a competing repair assignment. No manual collector, added gate, optional enhancement, final enhancement pass, substitute workflow or patch-over-patch work. The agent owns quality control and verification; do not make the owner the tester.

This packet was prepared locally. No new chat was created, no handoff was dispatched, and no receiver acceptance or access has been verified. Existing review agents supply evidence only; no ongoing monitoring responsibility was transferred.
