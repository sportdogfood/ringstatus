# Publication fixes â€” 30 October 2026

Owner approval verified in Records human turn 01a18d75-27d3-7670-8c11-eaa4769c9dab: fix exactly three publication defects, test, independent review, deploy and verify two normal populated scheduled runs; no manual runs, tasks, repairs or observers.

Baseline: baseline.json and before/ snapshot current Live JS. Protected retention, class-error/audit behavior, source identity, freshness (six minutes; one-minute future tolerance), 45-second per-lane budget and single-field external PATCH unchanged. Existing guarded deployment target is this checkout used by wef-live-collector. Only publication.js and one summary line in collector.js are runtime deltas.

Prepared corrections: actual has_JSON spelling; planned/attempted/deferred/uncertain accounting with durable readback-before-retry; compare publication identity/matching/native links/overlay rather than unrelated acquisition metadata. Full rows retained in immutable audits. Uncertain count added to persisted run/heartbeat summary. Source evidence fixture derives from normal run 853114e5-fe5b-4c97-8999-ba0133a0a38c; no provider call.

Offline: 99 tests PASS. Independent review.md: no blocking finding; reviewer independently ran 26 publication and 45 collector tests and verified saved source provenance. Exact reviewed hashes there. First focused test caught dataset-id incorrectly treated as identity; corrected before review. First full stage suite needed its disposable runtime/history directory; no production issue or repair.

Deployed October 30 at 11:42:28 Eastern (15:42:28.363Z), reviewed hashes verified in deployed.json. Existing task was quiescent, no collector.lock and over 35 seconds to next normal trigger; no task changes/manual invocation. All 17 other Live JS files match baseline. Two normal populated runs inspected; see acceptance below. Do not substitute retention acceptance, empty runs or task exit codes for publication acceptance. Existing ambiguous source attribution and bounded lane coverage remain explicit, not forced joins. Same-date rollover remains separate. No observer.

## Deployed normal-run verification — October 30

Two normal populated runs inspected; no manual collector/provider call or record repair. Both collected 6 rings, 18 groups, 28 classes and 196 entries. Both retained Partial business status, not a full-coverage Success.

| Run ID | Eastern collection interval | Schedules verified / deferred / uncertain | Data verified / deferred / uncertain |
|---|---|---|---|
| 3d7a24cd-ed6d-4c5f-b0ba-e1ec503c0dbe | 11:44:18–11:45:47 | 35 / 0 / 0 | 37 / 230 / 1 |
| c8d1fe08-1c80-4097-a1f5-79120c1d258b | 11:47:13–11:48:40 | 35 / 0 / 0 | 38 / 230 / 0 |

145 immediate PATCH/readback pairs verified from hashed immutable audits and bounded destination GETs. All original non-overlay fields were unchanged in those before/readback pairs; identity and native links also matched current GET. Observation ages were 27.346–72.010 seconds at intent, retaining original timestamps and source archives. Single-field transport and original dispatch freshness guards remain unchanged/reviewed. Run/heartbeat summaries match separate deferred/uncertain totals, ten errs-log rows per run and zero logging errors. Second normal run reconciled the first uncertain Data intent as NotApplied before continuing; Data progressed to 38 distinct records after the prior37, with no overlap. Task completed normally (last result0); no collector lock at final verification snapshot. This does not promise no later source errors.

Evidence: normal-run-verification.json (exact receipts, audit paths/hashes, checked records/links, errors and counters), acceptance-summary.json, deployed.json and review.md. No business/Airtable writes by this verification; only GET and local evidence output.

**Gate results:** three approved corrections implemented, 99 offline tests PASS, independent review PASS, reviewed deployment PASS, two populated scheduled observations and protected-write/count/reconciliation checks PASS. **Full publication acceptance remains PARTIAL.** Data's 45-second lane budget deferred230 plans in each run; source class200030671 group attribution, ambiguous Data occurrences and absent exact trip200399568 match were withheld as intended. No relaxed joins, cadence, budgets or freshness.

The two selected Data rotations wrote trips, not group records: natural has_JSON field-publication proof remains pending although the actual saved-source casing and true/false/missing behavior are tested. No unrelated concurrent metadata change occurred in these audited write pairs: that branch is independently regression-tested but lacks positive natural-run exercise in this pair. These are explicit acceptance limits, not unimplemented code fixes. Do not call all publication complete or wait/poll indefinitely to manufacture proof. Inspect later saved evidence only within authorized work/on request, without observers. Same-date rollover and multi-group natural proof remain separate. No additional fixes authorized by this report.

Administrative review: existing Airtable release record rec5fLj3xEqkYBpxA remains the dated pre-fix review. Local review/deployment/verification receipt is ready; this task did not assume Records' administrative write authority or create/update Airtable trackers.
