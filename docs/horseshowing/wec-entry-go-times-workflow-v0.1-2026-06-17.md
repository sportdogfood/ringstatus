# WEC Entry Go Times Workflow v0.1

Date: 2026-06-17

## Source Boundary

`entry_go_times` is built only from:

- `update_schedule_staging` rows for the current `focus_show.focus_day`
- locked class scope already materialized as `class_start_times`
- active-trainer `class_oog` rows linked to those class starts

It does not create classes and does not broaden the class list.

## Repeatable Execution

The repeatable Catalyst function is:

`horseshowing_entry_go_times_runner`

The RingStatus cadence script calls it after:

1. `class_start_times`
2. `class_oog`
3. `class_oog` rollups
4. `get_orders` enrichment into `class_start_times`

It runs before:

1. `class_alerts`
2. lane audit

## Required Handoff

For an active `entry_go_times` row to be created:

- matching `update_schedule_staging` row exists
- matching `class_start_times` row exists
- matching `class_oog` row has active trainer lookup
- `entry_order` exists
- `class_start_time` can be normalized

`entry_go_time` is calculated from `class_start_time + ((entry_order - 1) * pace_seconds)`.

`pace_seconds` is calculated from `class_start_times.n_gone` and `class_start_times.elapsed_seconds` when `n_gone > 6`; otherwise it uses `class_start_times.pace_seconds`, then `120`.

## Live Enrichment

`get_orders` updates `class_start_times` with:

- `n_gone`
- `n_to_go`
- `total`
- `elapsed_seconds`
- current entry/horse

The next `entry_go_times` run inherits that class-level live state for pace calculation.

## Alert Triggers

`sync-class-alerts` in `horseshowing_class_lane_runner` reads:

- `class_start_times` for class start alerts at 60 and 30 minutes
- active `entry_go_times` for entry go alerts at 40 and 20 minutes

Both alert lanes write to `wec-alerts`.

The trigger window is 12 minutes wide, matching cadence behavior:

- trigger when `time_till <= threshold`
- do not trigger when `time_till <= threshold - 12`

The same Catalyst action resolves open `class_start_times` and `entry_go_times` alerts when their window is no longer active.

## Audit Gate

`docs/horseshowing/export-wec-class-stage-audit.js` verifies:

- active `entry_go_times` class+entry rows match `class_oog`
- no extra active `entry_go_times` rows exist
- active rows have required links
- active rows have `entry_go_time`

## Known Trouble

`hs_entry_go_times` in Catalyst currently has a narrow schema. The runner writes only confirmed Catalyst columns there and writes richer operator links/fields to Airtable `entry_go_times`.
