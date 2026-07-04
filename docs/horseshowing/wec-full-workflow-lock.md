# WEC Full Workflow Lock

## 1. Purpose

This document is the operating workflow lock for the WEC Catalyst-first workflow.

It exists to prevent:

- stage drift
- table handoff drift
- cron drift
- alert/message drift

This document does not implement anything. It is the reference contract for what should run, what each stage should prove, which tables must reconcile, and which lanes remain work in progress.

Supporting WEC workflow contracts are indexed here:

```text
docs/horseshowing/wec-workflow-contract-index.md
```

## 2. Data Contract

| Table | Links Back To | Feeds / Proves | Required Fields | Helpers | Status |
|---|---|---|---|---|---|
| `hs_heartbeat` | `focus_show` | cadence context, latest run state | `heartbeat_id`, `show_no`, `focus_day`, `focus_day_key`, `focus_show`, `status`, `action`, `created_at` | `focus_show` | Active |
| `focus_show` | latest `hs_heartbeat` | active show/day control, counts all active WEC tables | `show_no`, `focus_day`, `focus_day_key`, `show_focus_key`, `active`, `is_pause`, `live_enrichment`, `results_enabled`, `last_heartbeat` | `shows`, `hs_heartbeat` | Active |
| `hs_get_ring_days` | `focus_show` | `hs_update_schedule`, `hs_ring_status` | `show_no`, `focus_day`, `iso_date`, `focus_day_key`, `show_focus_key`, `focus_show`, `ring_day_no`, `ring_no`, `ring_name`, `ring_name_normalized`, `ring_name_prioritized`, `ring_name_slugified`, `ring_visual_key`, `is_active_focus_day` | `shows`, `focus_show`, `ring_days`, `rings`, `ring_names` | Active |
| `hs_update_schedule` | `focus_show`, `hs_get_ring_days` | `hs_class_oog`, `hs_class_start_times`, `hs_entry_go_times`, outputs | `show_no`, `focus_day`, `iso_date`, `focus_day_key`, `show_focus_key`, `focus_show`, `ring_day_no`, `ring_no`, `class_no`, `class_name`, `time_text`, `event_type`, `is_preflight`, `preflight_reason`, `ring_name`, `ring_name_normalized`, `ring_name_prioritized`, `ring_name_slugified`, `ring_visual_key`, `class_visual_key`, `is_active_focus_day` | `shows`, `focus_show`, `ring_days`, `rings`, `ring_names`, `classes` | Active |
| `hs_class_oog` | `focus_show`, `hs_update_schedule` | `hs_entry_go_times`, results scope, rollups | `show_no`, `focus_day`, `iso_date`, `focus_day_key`, `show_focus_key`, `focus_show`, `ring_day_no`, `ring_no`, `class_no`, `entry_no`, `entry_order`, `horse`, `rider`, `trainer`, `ring_name_normalized`, `ring_name_slugified`, `ring_visual_key`, `class_visual_key`, `entry_visual_key`, `is_active_focus_day` | `shows`, `focus_show`, `ring_days`, `rings`, `classes`, `entries`, `horses`, `riders`, `trainers` | Active |
| `hs_ring_status` | `focus_show`, `hs_get_ring_days` | ring now/status, mobile/print, alerts/messages | `ring_status_key`, `show_no`, `focus_day`, `focus_day_key`, `focus_show`, `ring_day_no`, `ring_no`, `ring_name`, `ring_name_normalized`, `ring_name_slugified`, `ring_visual_key`, `status`, `is_active_focus_day` | `focus_show`, `ring_days`, `rings`, `ring_names` | Active |
| `hs_class_start_times` | `focus_show`, `hs_update_schedule`, `hs_class_oog` | class status, live enrichment, results eligibility, class alerts/messages | `class_start_key`, `show_no`, `focus_day`, `focus_day_key`, `focus_show`, `ring_day_no`, `ring_no`, `class_no`, `class_name`, `class_start_time`, `entry_count`, `ring_name_normalized`, `ring_name_slugified`, `ring_visual_key`, `class_visual_key`, `class_status`, `is_active_focus_day` | `focus_show`, `ring_days`, `rings`, `classes`, `hs_update_schedule`, `hs_class_oog` | Active |
| `hs_entry_go_times` | `focus_show`, `hs_update_schedule`, `hs_class_oog`, `hs_class_start_times` | entry go alerts/messages, rollups, rider now | `entry_go_key`, `show_no`, `focus_day`, `focus_day_key`, `focus_show`, `class_no`, `entry_no`, `entry_order`, `horse`, `rider`, `trainer`, `ring_name_normalized`, `ring_name_slugified`, `ring_visual_key`, `class_visual_key`, `entry_visual_key`, `is_active_focus_day` | `focus_show`, `classes`, `entries`, `horses`, `riders`, `trainers`, `hs_update_schedule`, `hs_class_oog`, `hs_class_start_times` | Active |
| `hs_get_rings` | `focus_show`, `hs_ring_status`, `hs_class_start_times` | live ring/class enrichment | `show_no`, `focus_day`, `focus_day_key`, `focus_show`, `ring_day_no`, `ring_no`, `ring_name_normalized`, `ring_name_slugified`, `ring_visual_key`, `current_class_no`, `n_gone`, `n_to_go`, `elapsed_seconds`, `is_active_focus_day` | `focus_show`, `ring_days`, `rings`, `classes`, `hs_ring_status`, `hs_class_start_times` | Active / live |
| `hs_get_orders` | `focus_show`, `hs_class_start_times`, `hs_entry_go_times` | live pace/go-time enrichment | `show_no`, `focus_day`, `focus_day_key`, `focus_show`, `ring_day_no`, `ring_no`, `class_no`, `entry_no`, `ring_name_normalized`, `ring_name_slugified`, `ring_visual_key`, `class_visual_key`, `entry_visual_key`, `n_gone`, `n_to_go`, `elapsed_seconds`, `is_active_focus_day` | `focus_show`, `ring_days`, `rings`, `classes`, `entries`, `horses`, `hs_class_start_times`, `hs_entry_go_times` | Active / live |
| `hs_result_queue` | `focus_show`, `hs_class_start_times` | results probe state | `result_queue_key`, `show_no`, `focus_day`, `focus_day_key`, `focus_show`, `class_no`, `class_visual_key`, `status`, `attempts`, `last_checked_at`, `next_check_at`, `is_active_focus_day` | `focus_show`, `classes`, `hs_class_start_times` | Active |
| `hs_result_classes` | `focus_show`, `hs_class_start_times`, `hs_result_queue` | class results | `result_class_key`, `show_no`, `focus_day`, `focus_day_key`, `focus_show`, `class_no`, `class_visual_key`, `status`, `is_active_focus_day` | `focus_show`, `classes`, `hs_class_start_times`, `hs_result_queue` | Active |
| `hs_class_results` | `focus_show`, `hs_entry_go_times`, `hs_result_classes` | entry/rider/horse results | `class_result_key`, `show_no`, `focus_day`, `focus_day_key`, `focus_show`, `class_no`, `entry_no`, `horse`, `rider`, `placing`, `score`, `class_visual_key`, `entry_visual_key`, `is_active_focus_day` | `focus_show`, `classes`, `entries`, `horses`, `riders`, `hs_entry_go_times`, `hs_result_classes` | Active |
| future `hs_message_queue` | `focus_show`, source runtime/result row | alerts, two-way, mobile threads, SMS later | `message_key`, `message_type`, `show_no`, `focus_day`, `focus_day_key`, `focus_show`, `trigger_time`, `status`, `source_table`, `source_row_id`, `is_active_focus_day` | `focus_show`, source table helper, optional `classes`, `entries`, `horses`, `riders`, `trainers` | WIP |
| future `hs_message_threads` | `focus_show`, `hs_message_queue` | grouped messages for rider/entry/ring/two-way/mobile views | `thread_key`, `thread_type`, `show_no`, `focus_day`, `focus_show`, `status`, `last_message_at` | `focus_show`, optional `riders`, `horses`, `trainers`, source table helper | WIP |
| `wec-alerts` | `focus_show`, runtime/result source row | legacy/current alert review table until message queue replaces it | `alert_key`, `alert_type`, `show_no`, `focus_day`, `focus_show`, `trigger_time`, `status`, `source_table`, `source_row_id` | `focus_show`, optional source helpers | WIP / transitional |
| future `hs_barn_entry_audit` | `focus_show`, user/barn submitted form, `hs_class_oog`, `hs_entry_go_times`, `hs_class_start_times`, helper tables `horses`/`riders`/`trainers` | expected barn/user entries reconciled against current WEC mapping, missing/mismatched horses surfaced without workflow failure, confirmed hot-add items isolated for later mapping repair | `barn_entry_audit_key`, `show_no`, `focus_day`, `focus_day_key`, `focus_show`, `horse`, `rider`, `trainer`, `submitted_by`, `submitted_at`, `status` | `focus_show`, `horses`, `riders`, `trainers`, `hs_class_oog`, `hs_entry_go_times`, `hs_class_start_times` | WIP |

## 3. Stage Contract

| Stage | Purpose | Action / Lane | Cron Needed? | Status |
|---|---|---|---|---|
| 0 | `focus_show` control | active focus selection, `is_pause`, `live_enrichment`, `results_enabled` | No | Active |
| 1 | heartbeat + ring days | `wec-step1-heartbeat-get-ring-days` | Yes, through baseline stack | Active |
| 2 | update schedule | `wec-step2-update-schedule-only` | Yes, through baseline stack | Active |
| 3 | class OOG / followed entries | `wec-step3-class-oog` | Yes, through baseline stack | Active |
| 4 | runtime prep | `wec-step4-runtime-prep` | Yes, through baseline stack | Active |
| 5 | live enrichment | `wec-step5-live-enrichment` | Yes, separate cron | Active |
| 6 | results | `wec-step6-results` | Yes, separate cron | Active |
| 7 | message / alert probe | future `wec-message-probe` | Yes, later | WIP |
| 8 | message publish/send | future `wec-message-publish` | Yes or event-driven later | WIP |
| 9 | outputs/mobile/print/two-way | mobile, print, PDF, two-way read models | Usually no cron; read endpoints or publish jobs | WIP |
| 10 | helpers | helper sync for rings, horses, riders, trainers, owners | Optional low-frequency cron or manual lane | WIP |
| 11 | audit/workflow_log | workflow audit and handoff proof | Piggybacks on stages | WIP |
| WIP | barn audit / expected entry reconciliation | future `barn_entry_audit` / `expected_entry_reconciliation` | Maybe; manual or event-driven | WIP |

The barn audit row above is the workflow-lock summary. The dedicated implementation contract is:

```text
docs/horseshowing/wec-barn-entry-audit-contract.md
```

## 4. Cron Contract

| Cron | Target | Cadence | Gate | Status |
|---|---|---|---|---|
| baseline Step 1-4 cron | `wec-cadence-step1-step4` | day cadence and reduced overnight cadence | `focus_show.is_pause != true` | Active |
| Step 5 live enrichment cron | `wec-step5-live-enrichment` | live cadence, typically every 3-6 minutes while useful | `focus_show.live_enrichment = true` | Active |
| Step 6 results cron | `wec-step6-results` | every 6 minutes | `focus_show.results_enabled = true` | Active |
| future message probe cron | future `wec-message-probe` | every 1-6 minutes depending message type | future `message_lane_enabled = true` | WIP |
| future message publish cron | future `wec-message-publish` | TBD | send/publish lane enabled | WIP |
| helper sync cron/manual lane | future `wec-helper-sync` | manual, hourly, or daily | helper edits ready to sync | WIP |

## 5. Message / Alert Contract

This section is the workflow-lock summary. The dedicated WIP message/alert contract draft is:

```text
docs/horseshowing/wec-alert-message-contract-draft.md
```

| Message Type | Source Table | Trigger Rule | Creates Record In | Publishes/Sends? | Depends On | Gate | Status |
|---|---|---|---|---|---|---|---|
| `ring_now` | `hs_ring_status`, `hs_class_start_times` | current ring/class state changes | future `hs_message_queue` | No | Step 4/5 | message lane enabled | WIP |
| `ring_status` | `hs_ring_status` | late/running/complete status detected | future `hs_message_queue` | No | Step 4/5 | message lane enabled | WIP |
| `class_start_time` | `hs_class_start_times` | `starts_in` reaches configured threshold | future `hs_message_queue` or `wec-alerts` | No | Step 4 baseline | message lane enabled | WIP |
| `class_status` | `hs_class_start_times` | class status changes: `Today`, `Soon`, `Now`, `Done` | future `hs_message_queue` | No | Step 4/5/results | message lane enabled | WIP |
| `entry_go_time` | `hs_entry_go_times` | `go_in` reaches configured threshold | future `hs_message_queue` or `wec-alerts` | No | Step 5 if live `go_time` exists | message lane enabled | WIP |
| `entry_go_time_change` | `hs_entry_go_times` | OOG/order/go-time changes | future `hs_message_queue` | No | Step 4/5 | message lane enabled | WIP |
| `entry_status` | `hs_entry_go_times` | entry now/started/completed state changes | future `hs_message_queue` | No | Step 5/results | message lane enabled | WIP |
| `entry_result` | `hs_class_results` | result found for entry | future `hs_message_queue` | No | Step 6 | results enabled and message lane enabled | WIP |
| `entry_now` | `hs_entry_go_times` | now/next/soon entry window changes | future `hs_message_queue` | No | Step 4/5 | message lane enabled | WIP |
| `rider_now` | `hs_entry_go_times`, helpers | rider now/next/soon changes | future `hs_message_queue` | No | Step 4/5 plus rider helper | message lane enabled | WIP |
| `rider_results` | `hs_class_results`, helpers | rider result found/summarized | future `hs_message_queue` | No | Step 6 plus rider helper | results enabled and message lane enabled | WIP |

## 6. Gate Semantics

`PASS` means:

- required identity fields are present
- required handoff fields are present
- required link-backs reconcile
- expected source/destination counts pass
- no blocking errors occurred

`PASS_WITH_WARNINGS` means:

- required identity and handoff fields are present
- optional helper, display, or enrichment fields may be missing
- warnings are captured and visible
- the next stage may proceed

`FAIL` means:

- required identity fields are missing
- required handoff fields are missing
- required link-backs do not reconcile
- expected handoff count fails
- a blocking runtime error occurred

Rules:

- `FAIL` only on missing required identity/handoff fields or blocking runtime errors.
- `PASS_WITH_WARNINGS` is allowed for optional helper/display/enrichment misses.
- Counts alone do not prove a clean handoff.
- Link-back validity is part of stage `PASS`.

## 7. workflow_log Contract

`workflow_log` should create one record per workflow step. It supplements `hs_heartbeat`; it does not replace it unless that migration is separately approved.

This section is the workflow-lock summary. The dedicated implementation contract is:

```text
docs/horseshowing/wec-workflow-log-contract.md
```

Fields:

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

## 8. Standard Field Contract

Standard fields to use consistently where applicable:

- `focus_day_key`
- `heartbeat_id`
- `iso_date`
- `focus_day`
- `ring_name_normalized`
- `ring_name_prioritized`
- `ring_name_slugified`
- `ring_visual_key`
- `class_visual_key`
- `entry_visual_key`
- `is_preflight`
- `is_active_focus_day`
- `link_backs_required`
- `helpers_related`
- `focus_day_active`

Field rules:

- `iso_date` and `focus_day` use ISO format: `YYYY-MM-DD`.
- `date_text` is display only and must not be used as the filter key.
- `ring_name_normalized` is readable ring identity.
- `ring_name_slugified` is the safe token for visual keys.
- `ring_visual_key`, `class_visual_key`, and `entry_visual_key` depend on the safe ring token.
- `is_preflight` prevents non-class rows from becoming downstream class inputs.
- `is_active_focus_day` is for Airtable visibility/filtering and must not replace Catalyst source-of-truth logic.

## 9. Drift Prevention Rules

- No old-day fallback.
- No manual endpoint proof as cron proof unless explicitly approved.
- No downstream stage without upstream handoff.
- No live endpoints inside baseline Step 1-4.
- Step 5 live enrichment must stay separately gated.
- Step 6 results must stay separately gated.
- Alerts/messages remain WIP until separately implemented.
- Airtable mirrors are visibility/review unless explicitly defined as source.
- Catalyst remains canonical for `hs_*` runtime tables.
- Counts alone are not enough; source link-back and expected handoff must be visible.
- Optional helper misses should warn, not block, unless the helper is required for identity or handoff.
- Barn entry audit is not `workflow_log`.
- Missing expected barn/user entries create review items, not public workflow failure.
- Confirmed barn/user mismatches may be hot-added and isolated for later mapping repair.
