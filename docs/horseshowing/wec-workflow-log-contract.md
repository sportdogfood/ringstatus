# WEC workflow_log Contract

## Purpose

`workflow_log` is the WEC system execution audit lane.

It exists to record system execution, stage outcomes, handoff proof, gate state, and stop reasons.

`workflow_log` is not barn/user expected entry reconciliation.

`workflow_log` is not a message/alert queue.

`workflow_log` supplements `hs_heartbeat`. It does not replace `hs_heartbeat` unless that migration is separately approved.

The contract is one `workflow_log` record per step or stage result.

Catalyst should be canonical. Airtable mirrors are for visibility and review only.

## Required Fields

Each `workflow_log` row should support:

- `workflow_log_key`
- `run_id`
- `cron_name`
- `trigger_source`
- `action`
- `stage_no`
- `stage_name`
- `step_name`
- `show_no`
- `focus_day`
- `focus_day_key`
- `heartbeat_id`
- `focus_show_record_id`
- `started_at`
- `finished_at`
- `duration_ms`
- `status`
- `gate_passed`
- `source_table`
- `source_count`
- `destination_table`
- `destination_count`
- `expected_handoff`
- `expected_handoff_count`
- `actual_handoff_count`
- `required_fields`
- `required_fields_missing_count`
- `required_helpers`
- `required_helpers_missing_count`
- `optional_fields`
- `optional_fields_missing_count`
- `optional_helpers`
- `optional_helpers_missing_count`
- `link_backs_required`
- `link_back_missing_count`
- `helpers_related`
- `warning_count`
- `warning_summary`
- `next_stage`
- `stop_reason`
- `error_message`
- `payload_json`

## Gate Statuses

Use these statuses:

- `PASS`
- `PASS_WITH_WARNINGS`
- `FAIL`
- `SKIPPED`

## Status Meaning

`PASS` means the stage ran and required handoff checks passed.

`PASS_WITH_WARNINGS` means the stage ran, required handoff checks passed, and optional/helper/display issues were captured as warnings.

`FAIL` means the stage ran or attempted to run and a required identity, handoff, link-back, source, destination, or runtime requirement failed.

`SKIPPED` means the stage did not run because its gate was false or upstream state did not approve the stage.

## Expected Handoff Examples

| Stage | Expected Handoff |
|---|---|
| Step 1 | `hs_get_ring_days` current-day rows > 0 |
| Step 2 | `hs_update_schedule` non-preflight rows > 0 |
| Step 3 | `hs_class_oog` checkpoint complete or partial checkpoint saved |
| Step 4 | `hs_ring_status`, `hs_class_start_times`, and `hs_entry_go_times` populated |
| Step 5 | skipped unless `live_enrichment=true`; otherwise live fields updated |
| Step 6 | skipped unless `results_enabled=true`; otherwise result queue checked / pending / completed / exhausted |
| future message lane | skipped unless `message_lane_enabled=true` |

## Drift Rules

- No manual endpoint proof as cron proof unless explicitly approved.
- Counts alone do not prove handoff.
- Required identity fields can fail a gate.
- Required handoff fields can fail a gate.
- Required link-back misses can fail a gate.
- Optional helper/display misses warn only.
- `workflow_log` rows must identify the stage that stopped.
- `workflow_log` must preserve the stop reason and error message when a stage fails.
- `workflow_log` must preserve skipped stage reason when a gate is false.

## Separation From Other Audit Lanes

`workflow_log` audits the system workflow execution.

`barn_entry_audit` reconciles barn/user expected entries against current mapping.

`hs_message_queue` or `wec-alerts` handles message/alert events.

These lanes may reference the same `focus_show`, visual keys, and runtime rows, but they are not interchangeable.
