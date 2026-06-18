# WEC End-to-End Workflow Handoff

Version: `0.1`

Date: `2026-06-17`

Status: current handoff document for the WEC/Horseshowing focus-day workflow.

Parent contract:

```text
docs/horseshowing/locked-workflow-gate-mcp-first-2026-06-17.md
```

## Operating Rule

This workflow is governed by `LOCKED WORKFLOW GATE - MCP FIRST`.

Catalyst is the workflow system. Airtable is a mirror/support surface except for the approved manual levers:

```text
focus_show
update_schedule_staging
helpers: horses, riders, trainers, rings, ring_names, class_hide, alert_templates
```

PowerShell or local shell commands are operator tools only. They may inspect, deploy, trigger, or verify Catalyst. They must not become the workflow or transform production data outside the repeatable Catalyst path.

## Current Repos

Primary RingStatus repo:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus
```

Horseshowing/Catalyst workspace:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-data\catalyst-workspaces\horseshowing
```

## Known Integrations

### Catalyst

```text
org_id: 700800454
project: horseshowing | 5614000000393031
cli: zcatalyst-cli@1.26.1
```

Current Catalyst function targets:

```text
horseshowing_sync
horseshowing_update_schedule_runner
horseshowing_class_oog_runner
horseshowing_class_start_times_runner
horseshowing_entry_go_times_runner
horseshowing_class_lane_runner
horseshowing_results_runner
horseshowing_counts_runner
horseshowing_discovery
horseshowing_viewer
```

### Airtable

Base:

```text
app6XS1RvsPNRT6os
```

Role:

```text
manual controls
helper lookups
mirror visibility
operator review
logs and alerts
```

### Horseshowing

Approved endpoints in this workflow:

```text
show.php?show={show_no}
get_ring_days.php
update_schedule.php
class_oog.php?class_no={class_no}
get_orders.php
get_rings.php
show_results4.php
```

Inactive or non-primary endpoints:

```text
counts.php
shows_happening.php
get_have_times.php
get_ring_day_oc.php
```

`counts.php` is currently deprecated for the class lane because `update_schedule.php` exposes `entry_count`.

### Webflow

Front-facing pages consume Catalyst-hosted code or Catalyst API payloads:

```text
wec-mobile
wec-mobile-pro
wec-print
```

Stable embed/drop files live under:

```text
docs/horseshowing/webflow-drops
```

### SMS / Two-Way

The rich endpoint now exposes indexes intended for SMS lookup:

```text
by_ring
by_class_no
by_entry_no
by_horse
by_rider
by_trainer
```

SMS two-way should consume the rich endpoint or a derived Catalyst index. It should not reconstruct schedule state from static Webflow JSON.

## Workflow Overview

The workflow is staged. Each stage must prepare the next stage.

```text
focus_show
  -> get_ring_days
  -> update_schedule
  -> update_schedule_staging
  -> class_start_times
  -> class_oog
  -> entry_go_times
  -> get_orders / get_rings live enrichment
  -> alerts
  -> results
  -> rich endpoint
  -> wec-mobile / wec-mobile-pro / wec-print / SMS
```

## Stage 1: focus_show

Owner:

```text
Airtable
```

Purpose:

```text
manual show/focus-day control
```

Required fields:

```text
show_no
focus_day
show_start
show_end
show_start_time
show_end_time
active
is_pause
full_lock_count
```

Action:

```text
When focus_day changes, Airtable automation calls Catalyst set-show-config and sync-ring-days.
```

Handoff:

```text
show_no and focus_day drive every downstream stage.
system_day never overrides focus_day.
```

Pause condition:

```text
focus_show.is_pause = checked
no active focus_show
multiple active focus_show records
focus_show.active does not match shows.active
```

Manual pause gate:

```text
focus_show.is_pause is the operator pause while focus_day and update_schedule_staging.lock are being adjusted.
When checked, downstream class/entry/live/result writes must no-op and log status=skipped.
Allowed while checked: get_ring_days, update_schedule, update_schedule_staging population/linking.
Blocked while checked: class_start_times, class_oog, entry_go_times, get_orders/get_rings enrichment, alerts, results.
Front-facing pages should continue to use the last clean published payload until the pause is cleared and the gate passes.
```

Deployed pause-aware functions:

```text
horseshowing_class_start_times_runner
horseshowing_class_oog_runner
horseshowing_entry_go_times_runner
horseshowing_class_lane_runner
horseshowing_results_runner
```

## Stage 2: get_ring_days

Source endpoint:

```text
get_ring_days.php
```

Function:

```text
horseshowing_sync?action=sync-ring-days
```

Targets:

```text
Catalyst support rows
Airtable get_ring_days
helper tables: ring_days, rings, ring_names, show_days, dows
```

Purpose:

```text
identify the ring_day_no blocks available for a show and focus_day
```

Required fields:

```text
show_no
ring_day_no
ring_no
ring_name
date_text
ISO / iso_date
YYYYMMDD
dow
focus_show
show_days
ring_days
rings
ring_names
```

Handoff:

```text
update_schedule runner uses get_ring_days rows for the focus_day.
```

Known trouble:

```text
get_ring_days links must be populated before update_schedule stage accounting is trusted.
ring names are not stable across WEC shows; ring_names helper remains operator-managed.
```

## Stage 3: update_schedule

Source endpoint:

```text
update_schedule.php
POST body: show_no={show_no}&ring_day_no={ring_day_no}
```

Function:

```text
horseshowing_update_schedule_runner
```

Current runner behavior:

```text
Reads focus_day ring_day_no blocks from Airtable get_ring_days.
Calls update_schedule.php directly for each selected ring_day_no.
Parses h3.ring_evt rows.
Writes Catalyst hs_update_schedule.
Mirrors Airtable update_schedule.
Writes Airtable update_schedule_staging.
Writes wec-logs.
Links staging helper fields.
```

Key:

```text
show_no|ring_day_no|ring_no|event_id|class_no
```

Required parsed fields:

```text
show_no
ring_day_no
ring_no
ring_name
date_text
iso_date
event_id
event_name
class_no
class_number
class_name
time_text
class_start_time
entry_count
event_type
oc_id
live_flag
source_endpoint
```

Handoff:

```text
update_schedule is the source mirror.
update_schedule_staging is the review/control layer.
class_start_times must not read directly from update_schedule.
```

Cadence:

```text
Run by focus_day ring_day_no blocks.
Use small block calls, normally one ring_day_no per call, to avoid Catalyst timeout.
```

Known trouble:

```text
Large all-day calls can hit Catalyst runtime limits.
Nested Catalyst-to-Catalyst calls caused timeout; runner now calls update_schedule.php directly.
class_no=0 and no-time rows are source facts, not parse failures by default.
Operator review decides which rows lock for downstream use.
```

## Stage 4: Populate Helpers

Helper tables:

```text
shows
classes
entries
horses
riders
trainers
rings
ring_names
ring_days
show_days
dows
events
class_hide
alert_templates
```

Rules:

```text
Helpers must be upserted if new source values appear.
Helpers must link into staging/mirror records at record creation or maintenance repair.
trainers.active is the active-team gate.
horses.barn_name / horse_display is the display source.
riders and trainers are helpers, not alternate class source lists.
class_hide supports hide_text and class_no.
```

Handoff:

```text
class_start_times, class_oog, entry_go_times, mobile, print, alerts, and SMS rely on helpers being linked.
```

Known trouble:

```text
If barn_name is corrected in Airtable, Catalyst/browser display can stay stale until helper sync runs.
Helper sync needs an immediate automation path for changed horses/class_hide and a scheduled repair path.
```

## Stage 5: update_schedule_staging

Owner:

```text
Airtable manual lever
```

Purpose:

```text
operator review and lock layer for focus_day classes
```

Source:

```text
update_schedule runner writes staging rows from update_schedule.php parsed rows.
```

Important fields:

```text
staging_key
show_no
focus_day / iso_date
ring_day_no
ring_no
event_id
class_no
time_text
entry_count
manual_lock
full_lock
inactive
is_target
not_target
manual_schedule
manual-instructions
last_run_time
wec-logs
```

Required links:

```text
shows
focus_show
classes
ring_days
rings
ring_names
show_days
dows
events
update_schedule
wec-logs
```

Downstream source view:

```text
lock_schedule
```

Handoff:

```text
class_start_times is seeded only from update_schedule_staging.lock_schedule.
If no lock_schedule rows exist, class_start_times pauses.
```

Known trouble:

```text
lock/full_lock/is_target are not the same control.
manual_lock is operator input.
full_lock is the ready-for-downstream formula.
focus_day changes require adding new focus rows before inactive old rows are removed or marked inactive.
```

## Stage 6: class_start_times

Function:

```text
horseshowing_class_start_times_runner
```

Source:

```text
Airtable update_schedule_staging view lock_schedule
```

Targets:

```text
Catalyst hs_class_start_times
Airtable class_start_times
wec-logs
```

Key:

```text
show_no|focus_day|ring_day_no|class_no
```

Action:

```text
Reads only lock_schedule rows.
Rejects rows outside active show/focus_day.
Rejects rows missing class_no, ring_no, event_id, or key.
Builds one class_start_times row per locked staging class.
Syncs Catalyst.
Mirrors Airtable.
Verifies Catalyst and Airtable counts, missing keys, extra active keys, and required links.
```

Handoff:

```text
class_start_times becomes the working class table.
class_oog enriches class_start_times.
get_orders updates live class state on class_start_times.
alerts read class_start_times.
rich endpoint reads class_start_times.
```

Known trouble:

```text
If lock_schedule contains the wrong rows, class_start_times will faithfully build the wrong class set.
Do not add filters here to compensate; fix update_schedule_staging.
```

## Stage 7: class_oog

Source endpoint:

```text
class_oog.php?class_no={class_no}
```

Function:

```text
horseshowing_class_oog_runner
```

Approved source list:

```text
update_schedule_staging.lock_schedule class_no list
```

Approved probe path:

```text
Pull the class_oog HTML for the locked class list.
Parse locally inside the repeatable runner.
Do not use a broad show-level crawl as the class list.
```

Targets:

```text
Catalyst hs_class_oog
Airtable class_oog
helpers: horses, riders, trainers, entries
wec-logs
```

Key:

```text
show_no|ring_day_no|ring_no|class_no|entry_no
```

Required fields:

```text
show_no
focus_day
ring_day_no
ring_no
class_no
class_label
entry_order
entry_no
horse
rider
trainer
source
active
hide
update_schedule_staging
class_start_times
```

Handoff:

```text
entry_go_times is built from class_oog active entries matched to class_start_times.
results lane probes only class_oog active/locked entries.
rich endpoint uses class_oog-derived entry_go_times, not stale hs_entries rollups.
```

Known trouble:

```text
class_oog pages can say NOT A POSTED ORDER.
class_oog may be unavailable or incomplete before official order of go is posted.
Order of go can exist before all class times are stable; staging lock controls whether it is rendered.
Large class lists must be chunked.
```

## Stage 8: entry_go_times

Function:

```text
horseshowing_entry_go_times_runner
```

Source:

```text
update_schedule_staging.locked/full_lock
class_start_times
class_oog active rows
trainers.active
```

Targets:

```text
Catalyst hs_entry_go_times
Airtable entry_go_times
wec-logs
```

Key:

```text
show_no|focus_day|class_no|entry_no
```

Action:

```text
Reads class_start_times for locked classes.
Reads class_oog rows for active trainers.
Links entry rows to class_oog and class_start_times.
Calculates estimated entry_go_time from class_start_time, entry_order, and pace_seconds.
Writes Catalyst and Airtable mirrors.
Inactivates stale rows that no longer match active class_oog.
```

Handoff:

```text
mobile, print, mobile-pro, alerts, rich endpoint, and SMS use entry_go_times for active-team rollups and estimated go times.
```

Known trouble:

```text
entry_go_time is only as accurate as class_start_time, entry_order, and pace_seconds.
No active row should be created without class_start_times and class_oog links.
If class_oog is wrong, entry_go_times will be wrong.
```

## Stage 9: get_orders

Source endpoint:

```text
get_orders.php
```

Functions:

```text
horseshowing_sync?action=sync-orders
horseshowing_class_lane_runner?action=sync-get-orders
```

Targets:

```text
Catalyst hs_get_orders / hs_class_times
Airtable get_orders mirror
class_start_times live fields
```

Purpose:

```text
class-level live state
```

Important fields:

```text
show_no
ring_no
ring_day_no
class_no or class_text/class_number fallback
entry text
total
n_to_go
n_gone
timestamp
elapsed
current_entry_no
current_horse
time
```

Action:

```text
Match get_orders rows back to class_start_times.
Update n_gone, n_to_go, total, elapsed, timestamp, current_entry_no, current_horse, live_source.
```

Handoff:

```text
class_start_times live fields feed mobile/pro/print, class alerts, and entry_go_time recalculation.
```

Known trouble:

```text
get_orders may not return rows before same-day/live context.
get_orders class_no may be absent; matching can require class_number/class_text fallback.
```

## Stage 10: get_rings

Source endpoint:

```text
get_rings.php
```

Functions:

```text
horseshowing_sync?action=sync-rings
```

Targets:

```text
Catalyst hs_get_rings / hs_class_times
Airtable get_rings mirror
```

Purpose:

```text
ring-level status and ring pace
```

Important fields:

```text
show_no
ring_no
ring_day_no
class_no
entry text
total
n_to_go
n_gone
timestamp
elapsed
time
```

Handoff:

```text
get_rings helps calculate ring pace and live status when class-level get_orders data is incomplete.
```

Known trouble:

```text
get_rings is not the primary source for rider/class linkage.
It is useful for ring pace/status, not enough by itself for class/entry rollups.
```

## Stage 11: Alerts

Function:

```text
horseshowing_class_lane_runner?action=sync-class-alerts
```

Sources:

```text
class_start_times
entry_go_times
alert_templates
```

Target:

```text
Airtable wec-alerts
```

Alert payload fields:

```text
horse
rider
class_no
entry_no
entry_go_time
class_start_time
alert_type
alert_lane
time_till
target_time
status
```

Action:

```text
Create class alerts at configured class windows.
Create entry alerts at configured entry windows.
Resolve stale open alerts when the alert window is no longer active.
```

Handoff:

```text
wec-alerts is the alert queue for outbound notification workflow.
```

Known trouble:

```text
Resolved-only alerts indicate the source times/windows are not producing active alerts.
Alert_templates must exist for each alert_type or records will be incomplete/noisy.
```

## Stage 12: Results

Function:

```text
horseshowing_results_runner
```

Source:

```text
Airtable class_oog where active=true, lock=true, hide!=true, class_no present, entry_no present
```

Endpoint:

```text
show_results4.php
```

Targets:

```text
Catalyst hs_result_queue
Catalyst hs_result_classes
Catalyst hs_class_results
Airtable result_queue
Airtable result_classes
Airtable class_results
wec-logs
```

Chunk rule:

```text
default limit=1
maximum limit=5
completed classes are skipped unless force=1
```

Handoff:

```text
result_queue.status marks class result status.
result_classes provides class completion detail.
class_results provides entry-specific result rows.
rich endpoint consumes all three.
```

Known trouble:

```text
Results payload can omit horse/rider text; runner fills blanks only from approved class_oog source rows.
This lane proves completed class status only when result payload exists.
```

## Stage 13: wec-mobile

Primary endpoint:

```text
horseshowing_sync?action=wec-mobile-live
```

Current source:

```text
buildScheduleJson()
class_start_times backbone
entry_go_times active rollups
helpers for display names
```

Purpose:

```text
front-facing mobile schedule
```

Handoff:

```text
Should migrate to rich endpoint when mobile-pro fields are required.
```

Known trouble:

```text
Do not rely on schedule.json as source of truth.
schedule.json is fallback/static only.
Mobile should not show stale hs_entries rollups.
```

## Stage 14: wec-mobile-pro

Primary endpoint:

```text
horseshowing_sync?action=wec-rich-live
```

Sources:

```text
class_start_times
entry_go_times
results
live get_orders/get_rings fields already carried on class_start_times
```

Purpose:

```text
operator/richer mobile schedule with entries, live status, and results attached
```

Handoff:

```text
Can support richer browser UI and SMS lookup without forcing browser-side joins.
```

Verified live check:

```text
show_no=14907
focus_day=2026-06-17
rings=7
classes=36
class 29784 status=completed
class 29784 entries=1
class 29784 results=1
```

## Stage 15: wec-print

Current endpoint aliases:

```text
horseshowing_sync?action=schedule-json
horseshowing_sync?action=wec-print-live
horseshowing_sync?action=wec-schedule-live
```

Preferred endpoint:

```text
horseshowing_sync?action=wec-rich-live
```

Purpose:

```text
print-ready class/entry schedule
```

Handoff:

```text
Print should read Catalyst live payload first and use static JSON only as fallback.
Ring placement should be deterministic by ring group row counts or explicitly owned by Airtable ring group controls.
```

Known trouble:

```text
Older print path still references schedule-json naming.
The endpoint name should be retired or aliased clearly as live Catalyst data to avoid source-of-truth confusion.
```

## Stage 16: SMS Two-Way

Preferred data source:

```text
horseshowing_sync?action=wec-rich-live
```

Rich endpoint indexes:

```text
indexes.by_ring
indexes.by_class_no
indexes.by_entry_no
indexes.by_horse
indexes.by_rider
indexes.by_trainer
```

Purpose:

```text
answer SMS queries about rings, classes, horses, riders, trainers, and current status at receipt time
```

Handoff:

```text
SMS should query Catalyst/API indexes, not scrape Webflow and not use static schedule JSON as primary source.
```

Known trouble:

```text
SMS response policy and Twilio routing still need final implementation/verification against current RingStatus two-way infrastructure.
```

## Stage 17: Rich Endpoint

Function:

```text
horseshowing_sync
```

Actions:

```text
wec-rich-live
wec-rich-api
```

Code path:

```text
buildScheduleJson()
getCatalystEntryGoTimesForSchedule()
getResultsForSchedule()
buildMobileLivePayload()
buildRichApiPayload()
```

Outputs:

```text
outputs.wec_mobile
outputs.wec_mobile_pro
outputs.wec_print
outputs.wec_alerts
outputs.sms
rings
indexes
sources
```

Source boundaries:

```text
backbone: update_schedule_staging.lock_schedule -> class_start_times
entries: hs_entry_go_times
live: hs_class_times / get_orders / get_rings fields carried on class_start_times
results: hs_result_queue / hs_result_classes / hs_class_results
```

Verified live endpoint:

```text
https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/?action=wec-rich-live&show_no=14907&focus_day=2026-06-17&limit=300
```

Verified alias:

```text
https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/?action=wec-rich-api&show_no=14907&focus_day=2026-06-17&limit=5
```

Verified result:

```text
ok=true
show_no=14907
focus=2026-06-17
rings=7
classes=36
class_29784_status=completed
class_29784_entries=1
class_29784_results=1
outputs present: wec_mobile, wec_mobile_pro, wec_print, wec_alerts, sms
```

## Cadence

Current cadence sources:

```text
docs/horseshowing/run-wec-catalyst-workflow.ps1
Airtable cadence rows
focus_show active state
shows active state
```

Known cadence names in the local runner:

```text
wec_heartbeat
wec_alerts_time_check
entry_go_times
live_get_orders
live_get_rings
```

Operational cadence intent:

```text
focus_show change:
  set-show-config
  sync-ring-days
  update_schedule by focus_day ring_day_no blocks
  link update_schedule_staging
  seed class_start_times
  class_oog local probe for locked classes
  entry_go_times
  refresh rich endpoint consumers

live show window:
  get_orders
  get_rings
  class_start_times live enrichment
  entry_go_times recalculation
  alerts
  results probe for eligible/completed classes

outside show window:
  low-frequency helper/mirror repair
  no noisy live polling
```

Known trouble:

```text
The PowerShell runner is still the local cadence orchestrator in repo.
The long-term target is Catalyst cron or an equivalent Catalyst-owned scheduler.
```

## Logs

Primary Airtable log table:

```text
wec-logs
```

Expected lanes:

```text
Helpers
Audits
Core
Live
Alerts
Results
```

Expected log types:

```text
helpers
focus_show
class_hide
rings
horses
riders
trainers
entries
airtable_helpers_summary
horses_missing_active_trainer
core_update_schedule
core_class_oog
core_counts
get-rings
get-orders
class_start_times
entry_go_times
result_classes
```

## Current Verification Evidence

Local tests:

```text
node --test functions/horseshowing_sync/index.test.js
7 pass
0 fail
```

Syntax:

```text
node --check functions/horseshowing_sync/index.js
PASS
```

Live Catalyst endpoint:

```text
action=wec-rich-live
PASS
```

## Known Trouble Register

1. `update_schedule.php` all-day or large block calls can hit Catalyst runtime limits.
2. `update_schedule_staging` is a manual lever; downstream state is wrong if the lock view is wrong.
3. Helper sync must be immediate for changed horse display names, class_hide, and trainers.active.
4. `class_oog.php` can return `NOT A POSTED ORDER`.
5. `class_oog` must be scoped from the approved locked class list, not a broad show crawl.
6. `entry_go_times` must not be active without `class_start_times` and `class_oog` links.
7. `get_orders.php` may not include class_no and can require class text / class number fallback.
8. `get_rings.php` is ring status/pace, not entry/class source of truth.
9. `result` payloads can omit text fields and must only fallback to approved class_oog rows.
10. `schedule-json` naming remains confusing and should be retired or clearly aliased.
11. Static JSON is fallback only.
12. Webflow embeds should not require frequent manual replacement once stable loaders are published.
13. SMS two-way still needs final routing verification against the RingStatus SMS infrastructure.
14. Counts lane is deprecated for class count use unless a later audit proves a missing need.

## Handoff Checklist

Before moving from one stage to the next:

```text
source rows accounted for
target rows accounted for
keys confirmed
required links populated
stale/removed behavior defined
wec-logs written
audit check added or confirmed
next-stage input proven usable
```

Current next-stage priorities:

```text
1. Confirm current focus_show and focus_day.
2. Run update_schedule by focus_day ring_day_no blocks.
3. Confirm update_schedule_staging lock_schedule.
4. Run class_start_times.
5. Run class_oog local probe for locked class list.
6. Run entry_go_times.
7. Run get_orders/get_rings live enrichment inside show window.
8. Run alerts.
9. Run results.
10. Verify rich endpoint.
11. Verify wec-mobile, wec-mobile-pro, wec-print, and SMS index consumers.
```
