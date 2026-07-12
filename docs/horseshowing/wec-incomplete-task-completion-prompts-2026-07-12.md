# WEC Incomplete Task Completion Prompts - 2026-07-12

Use the shared preamble and only one task block in each Codex task.

Do not place Tasks 05, 06, and 07 in one implementation task.

## Shared Preamble

```text
RINGSTATUS BOUNDED COMPLETION TASK

Before using tools, read these files completely:

1. C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\AGENTS.md
2. C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\horseshowing\chatgpt-codex-operating-package-2026-07-12.md
3. C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\horseshowing\wec-stack-and-preflight-handoff-2026-07-12.md

The handoff proves the July 12 Core preflight reached runtime_ready with:

- 9 rings;
- 114 schedule rows;
- 85 eligible classes;
- 24 parsed raw documents;
- 37 tracked entries;
- projected runtime of 9 rings, 85 classes, and 37 entries;
- zero Step 4 blockers;
- zero production writes from the preflight.

Core is outside this task. Do not reopen, rerun, patch, or redesign Core.

This is an implementation task, not an architecture discussion. Do not infer
new stages, ownership, triggers, fields, fallbacks, or workflow edges.

Do not edit Airtable hs_endpoints. The known Airtable presentation corrections
require separate approval.

Codex is not the runner. Manual or direct endpoints are diagnostic only and do
not count as scheduled proof. Do not repair production records.

START WITH A READBACK

Return:

CONTRACT_READ
task:
mode:
authority_files:
current_verified_state:
target:
owned_components:
allowed_files_to_inspect:
proposed_allowed_files_to_edit:
must_not_change:
unknowns:
scheduled_pass_gate:
next_allowed_action:

Perform the readback at the start of the same response. If it matches the
selected task and no authority conflicts exist, continue immediately with the
bounded read-only inspection. Do not wait for a separate readback approval.

If the authorities conflict or the requested task cannot be isolated, return
CONTEXT_FAIL and identify the exact conflict. Do not resolve it by assumption.
Do not inspect further after CONTEXT_FAIL.

After a successful readback, in the same task and turn:

1. Inspect only the selected task's current implementation, schema, deployed
   configuration, and scheduler evidence.
2. Separate CURRENT from TARGET.
3. Report every proven blocker before editing.
4. Propose one bounded repair batch with exact files and regression gates.
5. Stop before edits and request approval for the exact repair batch.
6. After approval, apply the smallest diff, test, deploy, and wait for the
   scheduled PASS gate.
7. Stop at PASS or the earliest exact failure.
8. Do not begin another task in this Codex task.
```

## Task 05 Prompt - Live Current State

Append this block to the shared preamble in a new Codex task:

```text
TASK
  05 - Stack 5 Live Current State

OBJECTIVE
  Complete the Live work that was discussed but not completed.

OWNED FLOW
  get_rings
    -> hs_ring_status
    -> hs_class_start_times
    -> hs_entry_go_times

REQUIRED OUTCOMES
  entry_count_now
  n_gone_now
  n_to_go_now
  reliable is_live
  observed and frozen class start
  snapshot-delta pace accepted only from 105 to 285 seconds
  current entry position
  current go-time estimates
  ring now, next, end, and lateness after schedule slack

REQUIRED BEHAVIOR
  Continue when the source reports live or not-live.
  Use get_rings as the live source.
  Keep get_orders deprecated from the hot Live lane.
  Update current Catalyst runtime projections without rebuilding Core.

MUST NOT CHANGE
  Core Stacks 1-4
  Time Engine trigger policy
  statewise_now producer
  Results
  outputs
  schedules
  Airtable contracts

SCHEDULED PASS
  Two consecutive scheduled Live cycles return HTTP 200.
  Approved fields are populated from current source values.
  The second unchanged cycle creates no duplicate history events.
  Core rows and keys remain unchanged.
```

## Task 06 Prompt - Time Engine And Statewise

Use only after Task 05 reaches scheduled acceptance. Append this block to the
shared preamble in a separate new Codex task:

```text
TASK
  06 - Stack 6 Time Engine And Statewise

OBJECTIVE
  Complete the Time Engine trigger policy and statewise producer that were
  discussed but not completed.

OWNED DELIVERABLES
  Time Engine calculations
  time_engine_triggers
  statewise_now producer

TRACKED SCOPE
  Use unique classes represented in hs_entry_go_times for tracked class and
  result readiness events.

REQUIRED TRIGGERS
  class_start_60
  class_start_30
  class_live
  entry_go_40
  entry_go_20
  entry_class_10_gone
  entry_10_away
  ring_late_15
  ring_late_30

INTERNAL EVENTS
  ring_class_change
  result_ready
  statewise_snapshot_due

STATEWISE REQUIREMENT
  Produce statewise_now on the approved twelve-minute interval.

REMOVE FROM CUSTOMER TRIGGERS
  ring_live
  inferred ring_gate

DEDUPLICATION
  An unchanged cadence must not append the same trigger event again.
  Legitimate later transitions must remain possible.

MUST NOT CHANGE
  Core
  Live source acquisition
  Results polling
  output design
  schedules
  Airtable contracts

SCHEDULED PASS
  Two consecutive scheduled Time Engine cycles return HTTP 200.
  One scheduled statewise interval produces valid statewise_now rows.
  Required trigger types and tracked scope are correct.
  Unchanged state creates zero duplicate trigger events.
```

## Task 07 Prompt - Rider Results

Use only after Task 06 reaches scheduled acceptance. Append this block to the
shared preamble in a separate new Codex task:

```text
TASK
  07 - Stack 7 Rider Results

OBJECTIVE
  Complete the narrow rider-result producer that was discussed but not
  completed.

OWNED FLOW
  tracked result_ready class
    -> results polling
    -> class_no and entry_no match
    -> placed or terminal no_place
    -> hs_rider_results
    -> stop polling completed class

REQUIRED SCOPE
  Poll only tracked classes and entries represented by the approved runtime
  scope. Do not widen to every scheduled class.

REQUIRED RESULT
  Preserve place, score, and finished time when supplied by the source.
  Record a terminal no_place outcome when the tracked entry has no placing.
  Do not repeatedly poll a completed class.

MUST NOT CHANGE
  Core
  Live
  Time Engine calculations
  existing legacy result outputs
  schedules
  Airtable contracts

SCHEDULED PASS
  Scheduled Results returns HTTP 200.
  Live hs_rider_results records are created for tracked entries.
  Terminal outcomes stop repeated polling.
  Existing legacy result outputs remain unchanged.
```

## Required Order

```text
Task 05
  -> scheduled acceptance
Task 06
  -> scheduled acceptance
Task 07
  -> scheduled acceptance
global scheduled review
cleanup only afterward
```
