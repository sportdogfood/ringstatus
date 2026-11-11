# Independent Class snapshot and cleanup review

Reviewer: `/root/class_status_review`, delegated from owner chat `01a16f18-25b9-7222-9b53-b64496ec84d9`.

**Review COMPLETE; candidate FAIL.** Independent saved-November-6 suite: 15 checks passed, 43 Active classes evaluated. Four independent correctness assertions failed. No candidate edits, fixes, failed-check replays, production runs, provider calls, deployment or task changes occurred.

## Confirmed findings

### P1 — Preserve unavailable predecessor finish when projecting the next class

File/symbol: `wef-timing/class-status.js`, `snapshots`, lines 86–106 (`projectedStart`, `finish`, `cursor`).

Trigger: the preceding group is running a jump-off with no usable phase total. Its finish is unavailable. The next class has a planned 09:30 start; evaluation is 10:00 Eastern.

Incorrect behavior: the missing finish leaves `cursor` null, so the next class receives an expected start of `2026-11-06T14:30:00.000Z` (09:30), despite the preceding work still running at 10:00. It also receives a finish derived from that unsupported start.

Consequence: riders and the trip calculator can receive an already-past, unsupported expected time. The planned time is valid schedule information, but is not a proven current projection.

Evidence: `counterexample-results.json`, first case; exact execution in `commands.json` command `independent-counterexamples` (exit 1).

Smallest correction: track an unavailable predecessor finish in ring sequencing and withhold dependent expected times with a reason until a usable finish or independent start is available. Preserve scheduled-start and unrelated class fields.

### P1 — Enforce retained-answer archive and minimum-age requirements before deletion

File/symbol: `wef-timing/class-status-cleanup.js`, `plan` line 35 and `cleanup` lines 43–50.

Trigger: a previous-show timing answer is in `unreferencedAnswerIds`, has a past date, but was created only 13 hours earlier and has no archive/hash/catalog/readback evidence.

Incorrect behavior: `plan` selects it. `cleanup` requires only unchanged readback plus intent callbacks before `api.remove`; it has no minimum-age validation or archive/catalog evidence check. The contract explicitly says historical archive-before-delete obligations on timing-answers remain. `complete:true` and an unreferenced ID establish neither archive verification nor age.

Consequence: binding this executor could delete a retained answer before the approved 24-hour minimum and without the required recoverable archive. This is a candidate defect, not a claim that any production deletion occurred.

Evidence: final case in `counterexample-results.json`; `CLASS-STATUS.md` cleanup contract; existing `baseline/ring-cleanup.js` is comparison evidence, not permission to import its whole implementation.

Smallest correction: exclude timing-answers from deletion until explicit verified age and full archive/hash/catalog/readback evidence are supplied and checked for the exact unchanged records. Keep the separate operational-log rules intact; do not add an archive requirement to the later-approved daily log clear by inference.

### P2 — Keep an invalid class clock from aborting all classes

File/symbol: `wef-timing/class-status.js`, `snapshots`, line 45 (and duplicate-key throw line 28 has the same batch-level disposition).

Trigger: one class has an invalid or future observation clock; another independent class is valid.

Incorrect behavior: the loop throws, returning no snapshots for either class.

Consequence: one bad record prevents the accepted evaluate-every-class snapshot cycle. This contradicts the approved plan to label unsupported values and continue other classes.

Evidence: second case in `counterexample-results.json`; exception `Missing or future original observation`.

Smallest correction: localize the invalid source disposition to that class, preserve its known fields with an unavailable reason, and continue independent classes. Do not guess a replacement clock.

### P2 — Missing answer date/identity must remain protected

File/symbol: `wef-timing/class-status-cleanup.js`, `plan`, lines 28–35.

Trigger: an unreferenced previous-show timing answer has no date or validated class identity.

Incorrect behavior: the current/future test runs only when `date` is a string. Missing date bypasses it; the unreferenced-ID check accepts the row.

Consequence: unknown-age/scope answers can be selected even though the cleanup contract says unknown identity/eligibility stays protected.

Evidence: third case in `counterexample-results.json` selects one record where zero is required.

Smallest correction: require a valid past date and validated owned answer/checkpoint identity before applying old-show eligibility; absent or malformed identity remains excluded.

## Verified scope and boundaries

- Read actual candidate source, contract, task scope, original supplied approval/corrections, offline tests, saved input/schema and comparison modules. Applied approval-boundary and recurring-delivery checks.
- All seven files in the writer's frozen baseline manifest still match their saved hashes. Existing Class code and the other owner's api/entry/cleanup changes were not modified by this candidate/review.
- Candidate is new pure calculation plus injected cleanup, not a replacement of the deployed Class implementation. Existing `localInstant` and `instant` are reused. No dependency/runtime/version upgrade, source-table repair or schedule change was found.
- All 35 candidate output names are represented in the saved created-table schema. This reviewer inspected the saved schema, not a fresh Airtable read. Parent independently performed live schema creation/readback.
- Exact candidate/dependency hashes are in `frozen-candidate.json`, `review-receipt.json` and `dependency-manifest.json`. The saved corpus is retrospective, captured after November 6; it is not an exact per-minute historical inventory.
- Independent execution of the writer suite used `suite-wrapper.js` solely to redirect its two output artifacts into this reviewer directory. The actual original `offline-test.js` was required unchanged. Its stdout/stderr/times/runtime are in `commands.json`.
- Saved November 6 output: 43 Active classes (42 Completed, 1 Upcoming), zero actual starts, one predicted end. This corpus alone does not test useful live Underway behavior. Explicit synthetic counterexamples cover an active jump-off, an invalid independent class clock and unsafe cleanup selections.
- `last-seen` is used by the existing class-live-source and synced-sample contracts as the collection observation time; `source-updated-at` is separately retained. The initial suspicion that last-seen alone violated the original-clock rule was not confirmed and is not a finding.

## Separate gates

| Gate | Result |
| --- | --- |
| Authorized scope / baseline preservation | PASS for inspected files; protected prior changes unchanged |
| Original offline suite | PASS — 15 checks, 43 saved November 6 classes |
| Independent counterexamples | FAIL — four confirmed correctness failures |
| Code acceptance | FAIL pending separately approved corrections |
| Table schema | Saved schema inspected; parent live readback is not independently re-executed here |
| Production deployment / recurring writer | Not deployed or bound by this candidate; unverified |
| Normal scheduled delivery / cleanup | Unverified; no manual substitute run |
| Current source freshness / exact historical inventory | Not proved by retrospective saved rows |
| Trip-calculator consumption | Unverified; no production consumer integration implemented |

Review findings do not authorize fixes, reruns, deployment or production deletion. Next action: owner approval for the four smallest in-scope corrections, followed by checks of the corrected candidate. No unrelated rebuild or integration expansion is needed to resolve these findings.
