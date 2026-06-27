# Barn White Board Audit Concept

## Status

Status: concept only.

`barn-white-board-audit` / `white-board-audit` is not currently an implemented audit lane, runner, endpoint, workflow stage, or proof gate.

This is not a workflow QA audit, not a print layout audit, not a cadence proof, and not a broad dashboard. It is a barn white-board input/audit concept:

```text
partial inputs -> narrow to class -> select horse(s) -> save/submit normalized board line
```

This concept document must not trigger workflow, sync, helper repair, raw fetch, output publish, deploy, Airtable updates, or runner creation.

## Purpose

Help a barn user quickly build or audit a white-board line by drilling down from partial board hints to the correct class and horse or horses.

The user should not need to search every current focus-day record directly. The experience should start from the full current focus-day schedule, then narrow quickly:

1. Select one ring from the current focus-day rings.
2. Add an approximate time.
3. Type the first letter or first few letters of the class name.
4. Select the correct class.
5. Select one or more horses.
6. Save or submit the normalized board line.

The audit target is not merely whether a schedule row exists. The audit target is whether a barn board line resolves to the correct `class_oog` class and the correct `entry_go_times` horse/entry record or records.

For the current WEC example:

- `update_schedule_staging` may contain about 90 schedule rows.
- `class_oog` / `class_oog_staging` may contain about 28 class/order rows.
- `entry_go_times` may contain about 26 horse/entry rows.

## Relationship To `barn_board_hot_patches`

`barn_board_hot_patches` is related existing implementation/data, but it is not proof that the full `barn-white-board-audit` concept exists.

Current `barn_board_hot_patches` behavior:

- uses Airtable table `barn_board_hot_patches`
- stores board-style hints such as `board_line`, `ring_hint`, `time_hint`, `class_name_hint`, `horses`, `entry_go_times`, `match_status`, `hot_patch_active`, `release_status`, and `focus_day`
- can match active `entry_go_times`
- can produce active hot patch rows for rendered schedule output
- has a `sync-barn-board-hot-patches` action

`barn_board_hot_patches` is an operational hot patch overlay/sync surface.

`barn-white-board-audit` is the user-facing concept for resolving partial white-board inputs before review, save, or submit. It should not be treated as implemented until a separate approved design and implementation gate exists.

## Core Flow

1. User gives partial hints.
2. System narrows matching current focus-day schedule rows.
3. User drills to the correct class.
4. User selects one or more horses.
5. User can add another board line.
6. User can save or submit the normalized board line.

## Inputs

Expected user or upload hints:

- `ring_hint`
- `time_hint`
- `class_hint` / `class_name_hint`
- `horse_hint` / `horses_hint`

These hints are not the final audit result. They are inputs to a resolver that must bind the line to current focus-day class/order and horse/entry records.

## Smart Dynamic Form

The dynamic form should not only filter the full schedule list. It must also use the current `class_oog` / `class_oog_staging` and `entry_go_times` scope because those are the actual records being audited.

Smart form flow:

1. Start with current focus-day schedule rows.
2. Select ring.
3. Narrow to `class_oog` / `class_oog_staging` classes for that ring.
4. Select or type approximate time.
5. Type first letter or partial class name.
6. Select the correct class.
7. Show matching `entry_go_times` horses for that class.
8. Select one or more horses.
9. Save normalized board-line record.

Source responsibilities:

- `update_schedule_staging` provides broad ring/class schedule context.
- `class_oog` / `class_oog_staging` provides the auditable class/order scope.
- `entry_go_times` provides horse-level selection and final matching.

## Bulk CSV Upload

A second intake path can accept quick board lines from CSV.

CSV format:

```text
ring | time | class | horse
```

Example:

```text
Indoor 2 | 10:30 | Low Adult Jumper | Blue Moon
```

The CSV upload should:

- resolve each row against current focus-day schedule/class/entry data
- use the same resolver as the smart dynamic form
- flag unresolved rows
- flag ambiguous rows
- flag wrong-class risk
- produce the same normalized board-line records as the smart form

## Shared Resolver

Both intake paths must feed the same resolver:

```text
Smart form
CSV upload
-> same resolver
-> same normalized board-line record
-> later audit/review
```

The resolver should preserve partition and scope keys:

- `show_no`
- `focus_day`
- `ring_no`
- `ring_day_no`
- `class_no`
- `entry_no`, where applicable
- linked `class_oog` / `class_oog_staging` record, where applicable
- linked `entry_go_times` record or records, where applicable

## Audit Risk Cases

### Missing Match

A submitted or uploaded board line may not match any current `class_oog` or `entry_go_times` record.

Missing matches must be flagged for review, not silently accepted.

### Wrong Class Binding

A submitted or uploaded board line may match a horse, but bind that horse to the wrong class.

The audit target is not only whether the horse exists. The audit must prove the board line resolves to the correct class and correct horse/entry.

Ambiguous matches require user confirmation. Missing matches and wrong-class matches must be flagged for review, not silently accepted.

## Output Target

Both smart form and quick/CSV intake should produce the same normalized board-line record.

Likely conceptual output fields:

- `board_line`
- `focus_day`
- `ring`
- `ring_hint`
- `time_hint`
- `class_hint`
- `class_name_hint`
- `horse_hint`
- `resolved_class`
- `resolved_class_oog`
- `resolved_entry_go_times`
- `resolved_horse`
- `match_status`
- `confidence`
- `release_status`
- `needs_review`

## PASS/FAIL Conditions

PASS should require:

- approved current focus-day schedule scope is available
- `class_oog` / `class_oog_staging` class/order scope is available
- `entry_go_times` horse/entry scope is available
- every submitted board line resolves to one correct class and one or more correct horse/entry records, or is explicitly marked for review
- ambiguous matches are not silently accepted
- wrong-class risks are flagged
- missing matches are flagged
- smart form and CSV intake produce the same normalized record shape through the same resolver

FAIL should occur if:

- the resolver only proves a broad schedule row exists
- the resolver does not bind to `class_oog` / `class_oog_staging`
- the resolver does not bind selected horses to `entry_go_times`
- a horse match can bind to the wrong class without review
- ambiguous rows are silently accepted
- missing rows are silently accepted
- the concept is treated as an implemented workflow lane without approval
- the audit would require workflow runs, syncs, helper repair, raw fetches, deploy, publish, or Airtable updates without explicit approval

## Non-Goals

This concept is not:

- workflow QA audit
- print layout audit
- broad dashboard
- cadence proof
- raw fetch
- helper repair
- sync runner
- replacement for `entry_go_times`
- replacement for `barn_board_hot_patches`
- proof that `barn-white-board-audit` is implemented

This concept does not:

- create a runner
- create an endpoint
- create or modify Airtable records
- trigger schedule staging
- trigger class OOG, class start time, or entry go time sync
- publish Webflow
- deploy Catalyst

## Future Implementation Gates

Before implementation, the following gates must be approved and verified:

1. Source of truth: identify the approved barn white-board source or intake path.
2. Field contract: freeze required hints, normalized output fields, and record links.
3. Scope: confirm whether the first version is smart form, CSV upload, or both.
4. Resolver contract: define row keys, matching rules, confidence rules, tie-breakers, and review states.
5. Data contract: prove current focus-day `update_schedule_staging`, `class_oog` / `class_oog_staging`, and `entry_go_times` inputs are available.
6. Safety: confirm the concept cannot write Airtable, Catalyst, Webflow, or generated outputs unless a separate approved save/submit action is created.
7. Output contract: define normalized board-line fields, PASS/FAIL summary, and blocker format.
8. Verification: run against one approved focus day and prove no workflow/sync/raw/helper lane was triggered.
