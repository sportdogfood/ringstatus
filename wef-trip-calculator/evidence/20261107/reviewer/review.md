# Independent Trip status code review

Review disposition: PARTIAL because required calculator execution is stopped. Candidate: FAIL. Cleanup-only independent offline execution: PASS (13 checks). No calculator rerun, snapshots() invocation, production/manual business call, product fix, deployment or schema mutation by reviewer.

## Findings

### P1 — Current Ring/Live state can be ignored and an elapsed go time published
- Exact source: wef-trip-calculator/trip-status.js, snapshots(), lines94–103 (inspected-eta-branch.txt).
- Trigger: a pending trip has compatible Class/Ring records and known queue, Data class status remains Not Started, but newer Ring/current-class and Live observations identify that group/class as underway. Class start is earlier than the new observation. No other withholding reason applies.
- Incorrect behavior: the underway branch is selected only by `c.fields.status==='Underway'`. Otherwise the code uses `max(Class start, preceding end) + ahead * 2.5746`; Ring current state does not affect that decision. There is no check that the predicted pending trip time has already elapsed.
- Consequence: the first pending trip can receive the old Class start (e.g.08:00) at an08:12 observation instead of a supported remaining-work estimate or an explicit unavailable value. This is a confirmed source-branch defect by static inspection, not a claim of reproduced production behavior; the stopped calculator was not executed.
- Smallest supported correction: reconcile existing verified Ring/current-class and compatible Live state when selecting active-vs-upcoming timing, preserve source clocks, and explicitly handle an elapsed pending-trip prediction. Do not infer an actual start or introduce another collector. Add a narrow stale-Data/newer-Live counterexample after fresh edit/retest approval.

### P2 — Yesterday test assumes total rows equal eligible Active trips
- Exact source: wef-trip-calculator/test.js line10, first assertion. Inspected actual writer-command-log.json exit1; reviewer did not rerun it.
- Trigger: retained November6 fixture has656trip rows:592active and64dropped. Candidate selectsActive rows.
- Incorrect behavior: assertion expects656outputs rather than the eligible subset, so it stops all later calculation/repeat/cleanup cases.
- Consequence: requested yesterday validation and later synthetic calculation cases remain unexecuted/unverified. This is a test-contract defect, not evidence Active filtering or Data collection failed.
- Smallest supported correction: compare output identities/count with the actual eligibleActive scoped source rows. Preserve original fixture/statuses and production eligibility; do not simply manufacture passing output. Separate APPROVED TO EDIT is required under current stop.

## Inspected baseline, scope and integration

Owner authority was inspected from the inherited current human request and cleanup amendments, recorded as selected requirements in authority.md. Task, STOP, schema, README, frozen manifests, actual failed writer log and selected source fixture were inspected. Legacy trips_calculator.js lines1301–1316 uses start+(effectiveOrder-1)*pace; Entry orderValue/providerTime excludes100000/midnight; existing Class snapshot and cleanup code are reused patterns, not proof of Trip deployment. Frozen baseline commit73699ab4cd2801b8642cc1ff25b8f4290e0b4330 and exact hashes are in baseline.json. Existing implementation/helper files were preserved.

Approved changes are confined to the new Trip module, tests/evidence/docs and explicitly created trip-status table. No new dependency, package/runtime upgrade, scheduler, Records-alert integration, source collector or old Entry/Ring/Class modification was found in this candidate. Existing other-owner dirty edits are outside this candidate. New table schema has31names/types in schema.json and parent table-readback.json; reviewer inspected that readback rather than making another Airtable mutation. No direct field/schema expansion beyond the new approved table was observed. No claim of historical legacy equivalence beyond the inspected arithmetic/helper reuse is made.

Real path: synced wef-data.trips/classes/schedules plus wef-live entries/classes/groups and existing Ring/Class answer payloads -> pure snapshots() -> returned Trip field objects. There is no deployed writer consuming this output. The actual November6fixture contains no Class/Ring answers; Class start/preceding-end consumption and useful ETA output are therefore absent from that real corpus. Existing class-status sync has legacy payload-json fields, not the new flat Class snapshot schema. Integrating a different producer/sync or registering a production route remains unverified and outside this review's mutation authority.

Source timestamps are preserved rather than retimed. Complete roster identity, unknown order, shared-group/phase/appearance guards and provider estimate separation were statically inspected. The test file contains synthetic positive/negative contract cases, explicitly not real November6timing proof; they were not reached after the writer failure. The fixture is selected saved rows inspected later, not a reconstructed sequence of historical inventories. Actual completed/progress recovery, live save/readback and repeated normal scheduled predictions remain unverified.

## Independent cleanup verification

Executed only cleanup-only-checks.js with Nodev22.22.2;13checks passed. Coverage includes winter/summer19:00Eastern boundary, independent three-hour due time,999/1000threshold and oldest500, daily all eligible completed rows, active/protected/pending selections, synced/control isolation, old-show unreferenced snapshots, current/future protection, unresolved/resolved errors, intent-before-delete, changed-target prevention, absence verification, unfinished intent on uncertainty and base isolation. API is a local fake transport; no real deletion/runner call occurred. Exact command/CWD/runtime/UTC/exit/stdout/stderr is command-log.json (tool output faa95e).

There is no separate runs table in the inspected Trip schema; run-logs is the existing invocation-run table. Cleanup requires an existing caller to persist show/dailyDay/quickAt and supply complete protection inventory and a durable intent transport. The candidate has no standalone timer or Records alerts, as requested. Offline checks establish only selection/transport-interface behavior; no deployed cleanup or normal cadence proof is claimed.

## Integrity and remaining acceptance

All12frozen candidate/baseline hashes matched before and after review (before-hashes.json, after-hashes.json). Calculator source was read only. Separate cleanup checks and reviewer evidence did not change the candidate. Required calculator execution is BLOCKED by actual first-check failure/current approval boundary. Review staysPARTIAL and candidateFAIL; packet-verifierPASS would establish artifact integrity only.

Exact next action: owner approval for the narrowly identified test correction and current-state/elapsed-ETA correction, then authorized meaningful calculator tests and fresh independent review. Do not rerun/patch/deploy from these findings. Missing yesterday Class/Ring evidence and production writer acceptance remain visible; do not label them a proven upstream outage. Preserve all current product stops.
