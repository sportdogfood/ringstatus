# WEC Class Stage Audit Workflow v0.1

Date: 2026-06-17

## Purpose

Produce a repeatable operator view of the current focus-day class handoff stage.

This is not a one-off shell export. It is the documented check used after:

1. `focus_show.active` defines the current `focus_day`.
2. `get_rings` is pinged/mirrored for the live focus-day ring state.
3. `update_schedule` is populated for the focus day.
4. `update_schedule_staging.lock_schedule` is populated and linked.
5. `class_start_times` is synced from locked staging.
6. `class_oog` is populated by the approved local HTML probe path and linked back to the class stage.

## Command

```powershell
node C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\horseshowing\export-wec-class-stage-audit.js --show-no 14907 --focus-day 2026-06-17
```

If `--focus-day` is omitted, the script reads Airtable `focus_show.active` for the provided `show_no`.

If `get_rings` link checks fail, run the repeatable live-link path:

```powershell
node C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\horseshowing\sync-airtable-live-links.js --source rings --show-no 14907 --focus-day 2026-06-17
```

This creates missing `entries` helpers from `get_rings.entry_text` when needed and links the `get_rings` mirror rows.

## Source Tables

- Airtable `focus_show`, view `active`
- Horseshowing `get_rings.php`
- Airtable `get_rings`
- Airtable `update_schedule`
- Airtable `update_schedule_staging`, view `lock_schedule`
- Airtable `class_start_times`
- Airtable `class_oog`

## Output Files

All files are written to:

`docs/horseshowing/reports`

Outputs:

- `class-stage-audit-{show_no}-{focus_day}-{timestamp}.html`
- `class-stage-audit-{show_no}-{focus_day}-{timestamp}.json`
- `get-rings-live-{show_no}-{focus_day}-{timestamp}.csv`
- `update-schedule-live-{show_no}-{focus_day}-{timestamp}.csv`
- `class-start-times-live-{show_no}-{focus_day}-{timestamp}.csv`
- `class-oog-live-{show_no}-{focus_day}-{timestamp}.csv`
- `update-schedule-staging-lock-schedule-live-{show_no}-{focus_day}-{timestamp}.csv`

## PASS Criteria

- `focus_show.active` has exactly one record for the requested `show_no`.
- If a `--focus-day` is supplied, it matches `focus_show.active`.
- `get_rings.php` source probe returns parseable JSON.
- If `get_rings.php` returns rows, Airtable `get_rings` has focus-day mirror rows.
- Every focus-day `get_rings` mirror row has required helper links.
- Airtable `update_schedule` has focus-day rows.
- Airtable `update_schedule_staging` has focus-day rows.
- Every locked staging row has required helper links.
- `class_start_times` has no missing locked staging classes.
- `class_start_times` has no extra classes outside locked staging.
- Every `class_oog` row has an `update_schedule_staging` link.
- Every `class_oog` row has a `class_start_times` link.
- Every `class_oog` row has a populated `lock (from update_schedule_staging)` lookup.

## FAIL Criteria

The script exits non-zero if any PASS criterion fails.

## Current Verified Run

Show:

`14907`

Focus day:

`2026-06-17`

Verified counts:

- `get_rings.php` source probe: 1
- `get_rings` mirror: 7
- `get_rings` missing required links: 0
- `update_schedule`: 79
- `update_schedule_staging`: 80
- `update_schedule_staging.lock_schedule`: 36
- `class_start_times`: 36
- `class_oog`: 13
- `class_oog` unique classes: 7
- missing `update_schedule_staging` links: 0
- missing `class_start_times` links: 0
- missing lock lookups: 0
