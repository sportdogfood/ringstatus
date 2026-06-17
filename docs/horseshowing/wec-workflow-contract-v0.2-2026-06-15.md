# WEC Horseshowing Workflow Contract

Version: `0.2`

Date: `2026-06-15`

Status: current operating contract

This document is the source contract for WEC/Horseshowing workflow ownership, active-show control, data flow, audit gates, and conflict handling. It supersedes the `2026-06-10` focus workflow spec where the two disagree.

System scope contract: `docs/horseshowing/wec-systems-scope-contract-v0.1-2026-06-15.md`

Use the system scope contract before changing any WEC-adjacent lane so shared data can move between systems without collisions, duplicate ownership, or silent workflow forks.

## Source Of Truth

Catalyst owns the operational schedule state and customer-facing render payloads.

Airtable owns manual controls, helper edits, operator review, and mirrors of Catalyst output.

Static JSON files are fallback artifacts only. They must not become the normal source of truth for mobile, print, PDF, alerts, or core schedule logic.

## Airtable Owns

Manual control tables:

```text
shows
focus_show
class_hide
```

Helper tables:

```text
rings
horses
riders
trainers
entries
mobile_meta
```

Mirror/output tables:

```text
update_schedule
counts
class_oog
class_start_times
entry_go_times
get_orders
get_rings
wec_alerts
wec_logs
```

Mirror/output tables may be reviewed and linked in Airtable, but they do not override Catalyst core data unless a specific two-way edit path is approved.

## Active Control Rule

The workflow may run only when all active controls are unambiguous:

```text
shows view active has exactly 1 record
focus_show view active has exactly 1 record
shows.active.show_no = focus_show.active.show_no
focus_show.focus_day is populated
```

If any of these checks fail, the workflow must stop before core sync, live sync, render generation, alerts, or mirror writes.

`-ForceSync` does not bypass active-control validation. It only bypasses cadence timing after the active controls pass.

Inactive historical rows may remain for audit/history. They must not drive current link maps, current schedule rows, current render payloads, or current alerts.

## Workflow Lanes

### Helpers

Scripts:

```text
sync-airtable-controls.js
```

Purpose:

```text
read active focus_show
read shows active state
read class_hide
read helper tables
push active controls/helpers to Catalyst
write helper sync logs
```

Rules:

```text
only active focus_show may be pushed as current config
helper backfill may add missing horses, riders, trainers, rings, and entries
trainer active state remains Airtable-owned
horse barn/display names remain Airtable-owned helper values
class_hide supports hide_text and class_no
```

### Cadence Gate

Script:

```text
run-wec-catalyst-workflow.ps1
```

Purpose:

```text
read active controls
determine cadence
stop, slow, or run workflow
call core, live, time, alert, mirror, and audit stages
```

Rules:

```text
no active show = stop
multiple active shows = stop
no active focus_show = stop
active show/focus_show mismatch = stop
active focus_show mode day/evening controls cadence selection
cadence table controls cadence_minutes and night_cadence_minutes
system epoch-tagger-local must not be changed for WEC cadence
```

### Core

Scripts:

```text
sync-airtable-core-workflows.js
```

Horseshowing endpoints:

```text
get_ring_days.php
update_schedule.php
counts.php
class_oog.php
```

Purpose:

```text
build the focus-day schedule spine
mirror core rows to Airtable
upsert helpers from source data
link mirrors to shows, focus_show, rings, ring_days/days, classes, entries, horses, riders, and trainers
```

`update_schedule` is the class schedule source for the focus day. Critical fields:

```text
update_schedule_key
show_no
focus_day
ring_day_no
ring_no
class_no
event_name/class label
time_text
entry_count when present
```

`counts` is the fallback class count source. Critical fields:

```text
show_no
class_no
entry_count
```

`class_oog` is the order-of-go source. Critical fields:

```text
class_oog_key
show_no
focus_day
ring_day_no
ring_no
class_no
class_order when present
entry_order
entry_no
horse
rider
trainer
source
```

`source = NOT A POSTED ORDER` means Horseshowing exposed an entry list for the class but did not label it as a posted order. The rows may still be useful for trainer/horse/rider presence, but the order must be treated with caution until better source evidence exists.

### Class Start Times

Script:

```text
sync-airtable-time-workflows.js
```

Purpose:

```text
derive one class_start_times record per focus-day class
bind each record to update_schedule
bind each record to class_oog when rows exist
carry start time and live pace fields when available
```

Required links:

```text
shows
focus_show
rings
ring_days/days
classes
class_oog where available
```

Required derived values:

```text
class_start_time from update_schedule.time_text
display_time from class_start_time
n_gone/n_to_go/elapsed_seconds/pace_seconds when live/order data exists
```

### Entry Go Times

Script:

```text
sync-airtable-time-workflows.js
```

Purpose:

```text
derive active-trainer entry_go_times from class_start_times + class_oog + trainers.active
link every entry_go_times record back to class_start_times and class_oog
calculate estimated entry_go_time using class_start_time, entry_order, entry_count, n_gone, elapsed_seconds, and pace_seconds when available
```

Required links:

```text
class_start_times
class_oog
shows
focus_show
rings
ring_days/days
classes
entries
horses
riders
trainers
```

Rules:

```text
entries are helpers, not the source of class rollups
entry_go_times must not be built from entries alone
active trainers drive which entries become team/focus rows
multiple active trainers are grouped separately by trainer_display
scratched entries must not remain visible or alerting
```

### Live

Horseshowing endpoints:

```text
get_orders.php
get_rings.php
```

Purpose:

```text
enrich current class/ring status
carry n_to_go, n_gone, total, timestamp, elapsed
improve class_start_times and entry_go_times pace calculations
update customer-facing live rollups only when source data changes
```

Rules:

```text
live data cannot replace core schedule rows
get_orders is class/status oriented and may not always include class_no
get_rings is ring/status oriented and must be linked through available ring/class/status fields
before show/live state, live rows may be empty or incomplete
```

### Alerts

Tables:

```text
wec_alerts
wec_logs
```

Purpose:

```text
create class-start and entry-go alert records from class_start_times and entry_go_times
resolve alerts only when the alert window is no longer active or source row is no longer eligible
log every control/helper check
write 30-minute summaries to wec_logs
```

Required alert fields:

```text
horse
rider
trainer
class_no
entry_no
entry_go_time
class_start_time
alert_type
alert_lane
time_till
```

`wec_alerts` is a result table. It must not become the source for schedule, class_start_times, or entry_go_times.

### Render Outputs

Customer-facing surfaces:

```text
wec-mobile
wec-print
PDF/share/print output
```

Purpose:

```text
read Catalyst live render payloads
use static schedule JSON only as fallback
apply class_hide without requiring embed changes
show active trainer rollups using trainer_display labels and horse barn/display names
```

Rules:

```text
mobile and print must use the same current Catalyst source
print ring placement must be deterministic by row counts or explicitly owned by Airtable ring_groups
customer-facing pages must not show empty schedule during focus_day changes if prior valid payload exists
```

## Audit Gate

Script:

```text
audit-wec-lanes.js
```

Purpose:

```text
prove the active controls, core rows, time rows, required links, Catalyst render payload, and current lane health
write a PASS or FAIL artifact
stop workflow on failure
```

Audit artifacts:

```text
docs/horseshowing/logs/wec-lane-audit-<show_no>-<focus_day>-PASS-<timestamp>.json
docs/horseshowing/logs/wec-lane-audit-<show_no>-<focus_day>-FAIL-<timestamp>.json
```

`PASS` means the audited lane is verified clean for the active show/focus day.

`FAIL` means the workflow exhausted the available automated fix path and still has a proven blocker.

`OPEN` means work is still running and must continue until `PASS` or proven `FAIL`.

## Focus-Day Change Sequence

Human action:

```text
set exactly one shows.active record
set exactly one focus_show.active record
confirm both active records use the same show_no
set focus_show.focus_day
set mode day/evening when cadence differs
```

Workflow action:

```text
focus_show Airtable automation runs docs/horseshowing/airtable-automations/focus-show-on-change.js
automation calls set-show-config
automation calls sync-ring-days with refresh_existing=1
sync controls/helpers
run cadence gate
sync ring days and core update_schedule/counts/class_oog
derive class_start_times
derive entry_go_times from active trainers
sync live rows when available
create/resolve alerts
refresh render payloads
run lane audit
write PASS artifact before declaring completion
```

Cadence execution:

```text
run docs/horseshowing/run-wec-catalyst-workflow.ps1 on the existing RingStatus heartbeat or Windows scheduled task
the runner reads Airtable shows.active and focus_show.active
the runner executes immediately when the active show_no|focus_day|mode changes
otherwise the runner follows Airtable cadence rows
sync-ring-days must be called through Catalyst action sync-ring-days, not by direct local payload scraping
```

## Conflict Rules

These are blocking conflicts:

```text
multiple active shows
multiple active focus_show rows
active shows/focus_show mismatch
active show missing from support links
core update_schedule rows missing for active focus_day
class_start_times missing when update_schedule exists
entry_go_times not linked to class_start_times
class_oog missing required helper links
get_orders/get_rings mirror rows missing required links when source fields are present
time workflow exits with error
audit artifact is FAIL
```

These are accepted non-blockers when documented in the audit artifact:

```text
inactive historical duplicate show rows
entry_go_times count is 0 because no active-trainer entries exist for the focus day
live get_orders/get_rings rows are empty before the source is live
class_oog source is NOT A POSTED ORDER but rows are still present
```

## Change Control

Do not add new tables, new source-of-truth paths, new key shapes, new render sources, or new fallback precedence without approval.

Do not silently route around a broken link by using helper tables as an alternate data source.

Do not log a soft failure and continue with exit code `0` when the customer-facing workflow depends on the failed stage.

Do not treat Airtable mirror rows as current truth when Catalyst has not been reconciled.

Do not mark a workflow complete unless the exact active show/focus day has a current `PASS` audit artifact.
