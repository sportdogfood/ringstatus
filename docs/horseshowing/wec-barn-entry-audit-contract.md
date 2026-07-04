# WEC Barn Entry Audit Contract

## Purpose

`barn_entry_audit`, also called `expected_entry_reconciliation`, is a separate operator-facing reconciliation lane.

Its purpose is to compare what the barn or user expects to see against what the current WEC mapping produced, then surface differences without breaking the workflow.

This is not `workflow_log`.

This is not internal parent/child count auditing.

This lane exists because current WEC mapping is inferred from available data. It is not guaranteed complete because the workflow does not start from a guaranteed barn-submitted roster.

## Core Context

The current mapping is inferred from available sources:

- schedule data
- `hs_class_oog`
- `hs_entry_go_times`
- `hs_class_start_times`
- helper tables for horses, riders, and trainers
- active trainer scope

Absence from the mapped schedule does not automatically prove that a horse is absent from the show.

The audit must help discover misses. It must not declare public workflow failure to the user.

## Real Scenario

Last night, 2 horses were missed.

This lane is designed to catch that scenario early and give operators a controlled path to add those entries hot without destabilizing the stack.

## Audit Inputs

### Current System Mapping

Use current mapped runtime/source tables:

- `hs_class_oog`
- `hs_entry_go_times`
- `hs_class_start_times`
- helper table: `horses`
- helper table: `riders`
- helper table: `trainers`
- active trainer scope

### Independent Active-Trainer Entry Scan

Add an independent discovery lane from `trainers.trainer.active`.

This scan should produce entries associated with active trainers and use them as another discovery source.

This source is not perfect truth. It is a second lens for finding likely misses.

### User/Barn Submitted Form

The form should capture:

- `horse`
- `rider`
- `trainer`
- `expected_class`
- `expected_ring`
- `expected_day`
- `notes`
- `submitted_by`
- `submitted_at`

The expected class, ring, and day are optional when the user does not know them.

## Audit Behavior

The lane compares user-submitted expected horses or entries against current WEC mapping.

If a submitted horse or entry is missing or mismatched, create an audit/review item.

Rules:

- Do not fail the public workflow.
- Do not show internal failure language to the user.
- Do not attempt broad troubleshooting inside the user flow.
- Do not automatically rewrite runtime tables from an unconfirmed form submission.
- If the user/operator confirms the mismatch, hot-add or force-isolate the item so operations can continue.
- Keep forced items isolated so mapping logic can be fixed cleanly later.

## Audit Output Table

Proposed table:

```text
hs_barn_entry_audit
```

The table should be an operator review table, not a public error table.

Suggested fields:

- `barn_entry_audit_key`
- `show_no`
- `focus_day`
- `focus_day_key`
- `focus_show`
- `horse`
- `rider`
- `trainer`
- `expected_class`
- `expected_ring`
- `expected_day`
- `notes`
- `submitted_by`
- `submitted_at`
- `matched_horse`
- `matched_rider`
- `matched_trainer`
- `matched_class_visual_key`
- `matched_entry_visual_key`
- `current_mapping_source`
- `active_trainer_scan_match`
- `status`
- `review_notes`
- `confirmed_by`
- `confirmed_at`
- `hot_add_key`
- `isolated_for_fix`
- `created_at`
- `updated_at`

## Status Values

Use these statuses:

- `pending_review`
- `matched`
- `missing_from_mapping`
- `confirmed_hot_add`
- `isolated_for_fix`
- `ignored`

Status meanings:

- `pending_review`: submitted or discovered, not yet reconciled.
- `matched`: current mapping already contains the expected horse/entry.
- `missing_from_mapping`: current mapping does not show the expected horse/entry.
- `confirmed_hot_add`: operator confirmed the mismatch and approved a hot add.
- `isolated_for_fix`: the mismatch is preserved for later mapping repair.
- `ignored`: operator reviewed and decided no action is needed.

## Hot Add / Force Isolate Rule

When a mismatch is confirmed:

1. Create or mark the audit row as `confirmed_hot_add`.
2. Add the operational item through an approved hot-add lane.
3. Mark the item as isolated from normal mapping assumptions.
4. Preserve enough context to repair the underlying mapping later.

The goal is to keep operations moving without hiding the fact that the normal mapping missed something.

## Exclusions

This contract does not approve:

- workflow failure
- automatic broad repair
- public error state
- alert/send logic
- Step 1-6 changes
- mobile/print/PDF changes
- Production deploy
- Webflow publish

## Relationship To workflow_log

`workflow_log` audits workflow execution and handoff health.

`barn_entry_audit` audits expected barn/user entries against current mapping.

They are related, but separate.

`workflow_log` should not become the barn/user reconciliation table.

`barn_entry_audit` should not become the system execution log.
