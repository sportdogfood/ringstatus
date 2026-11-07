# Independent schedule-stage review

Reviewer /root/schedule_stage_review. Reviewed UTC 2026-11-07T02:29:35.054854+00:00.

## Result

Code and independent offline verification PASS for the current frozen candidate. No remaining actionable candidate defect found within the approved scope. Deployment and normal scheduled collection remain UNVERIFIED by this reviewer.

Initial inspection found an integration mismatch: the Airtable evaluator included Absent/Dropped rows while the new collector evaluated current schedule rows only. A missing lower-numbered class could renumber an Active class after its immediate save. The writer amended the wrapper to evaluate current Active / Active — no entries rows. Independent execution of the actual generated Airtable wrapper then returned zero updates to the correctly numbered Active row. The amendment was already installed in the candidate before the first reviewer execution; no failed execution is claimed.

## Approved scope and comparison

Owner explicitly approved implementing the agreed schedule stage, total-trips, immediate record-status / rs-lever / rs-flag / rs-priority updates, retaining class detail as the next stage, preserving cadences, independent review, deployment and proof through the existing schedule, with no extras. Previous owner correction selected schedule total_trips for the zero-count decisions. The existing decoder timeout change is protected.

Compared the frozen baseline and candidate fast_collector.py, schedule-priority.js and schema.json. The candidate adds numeric total-trips and mappings for the three existing priority fields, moves schedule persistence ahead of class requests, evaluates priorities from schedule data, and removes the later class-detail overwrite of schedule membership. The evaluator uses the existing category/Flat9/full-name/numeric-order rules and shared source for collector and Airtable. It derives ring/group grouping from the schedule identity/source to avoid depending on delayed native links. This is required for immediate classification of newly created schedules, within the approved delta.

No cadence, source request, retry or timeout changes. No class-detail request/preparation changes. These were confirmed by independently executed AST/text equality checks. No dependencies, runtime versions, external source routes, linked-record writers, cleanup or other collectors were changed by this candidate. Only the four schedule field mappings differ in schema.json. Existing source JSON, keys, upsert path, per-batch error continuation and native-link exclusion remain.

## Executed verification

Reviewer independently ran test_schedule_stage.py under the existing wef-data private Python interpreter. Four tests passed: schedule/status/priorities exist before class requests, detail failure preserves saved schedule, successive membership/reappearance and failed schedule handling preserve records, and rejected schedule writes still allow class/trip collection.

Reviewer authored and executed evaluator-check.js. Confirmed independent 1/2 numbering for 111/112, 177/178, 179/180 by full name; Flat9; zero-count flags; missing count is not zero; all existing named categories and Model; and execution of the actual Airtable wrapper against Active plus Absent records, preserving already correct current priorities.

Reviewer authored and executed scope-check.py. Confirmed unchanged AST for run, Source and schedule_payload, unchanged class-detail request/preparation section, and exact four-field-only schema extension.

Tests use fake source/Airtable and temporary test state. No source requests, real Airtable reads/writes, collectors or task invocations were performed by this reviewer.

## Integration and remaining operational proof

Inspected run.ps1 -> entry.js -> scheduled.py -> fast_collector.py route, DataStore inventory/reserve/fields_for/save behavior, source timestamps and log reporting. Python invokes the shared priority script after successful schedule saving, catches errors and proceeds into the existing class-detail code. The Airtable deployment must use the candidate script prefix before its CommonJS branch followed by `await evaluateAirtable();`; the currently old automation must not remain active with linked class-count rules. The writer owns that deployment and readback.

The actual scheduled release, Airtable field type/readback, automation publication, schedule-before-class runtime timing and final saved results require the owner's authorized normal scheduled verification. Passing offline checks establishes no claim of those live outcomes.
