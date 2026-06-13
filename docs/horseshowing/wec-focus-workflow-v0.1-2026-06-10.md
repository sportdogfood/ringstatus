# WEC Horseshowing Focus Workflow

Version: `0.1`

Date: `2026-06-10`

Status: working workflow specification and Codex handoff

Primary show in scope: `14906`

Primary focus day in scope: `2026-06-10`

Primary Airtable base: `wec_schedules`

Airtable base id: `app6XS1RvsPNRT6os`

Catalyst project id: `5614000000393031`

Catalyst function endpoint:

```text
https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/
```

## Purpose

This workflow supports WEC/Horseshowing data using the same broad shape as the existing RingStatus SGL model, while accepting that Horseshowing data is thinner and less reliable.

The goal is not to force commercial-grade SGL depth where the source does not provide it. The goal is to build the fullest reliable schedule possible, then fold team/focus entry detail into that schedule when the data exists.

The workflow maintains two related views:

```text
1. Full class schedule
2. Active trainer entry schedule
```

The full class schedule is the complete schedule spine for the focus day.

The active trainer entry schedule is a filtered and enriched entry-level view derived from the full entry snapshot, using active trainers as the team/focus selector.

## Non-Negotiable Data Principle

Do not reduce or delete source rows to create focus rows.

The full source and derived tables remain complete. Filtering happens only in render/output layers.

Required behavior:

```text
hs_entry_go_times
= full derived entry log/snapshot table
= keep all rows
= do not delete rows because they are not active/focus
```

Then:

```text
active trainer schedule
= hs_entry_go_times filtered by trainers.active = true
```

This keeps logs, snapshots, future audits, and rollbacks intact.

## Core Data Sources

### Horseshowing Endpoints

Active endpoints:

```text
get_ring_days.php
update_schedule.php
counts.php
class_oog.php
get_rings.php
get_orders.php
```

Inactive or optional endpoints:

```text
show.php
shows_happening.php
get_have_times.php
get_ring_day_oc.php
```

Current practical use:

```text
get_ring_days.php
-> ring days, dates, rings

update_schedule.php
-> class schedule by ring_day_no

counts.php
-> class-level entry_count fallback

class_oog.php
-> entry_order, entry_no, horse, rider, trainer by class_no

get_rings.php
-> day-of status overlay

get_orders.php
-> day-of status/order overlay
```

## Repository Boundaries

Main repo:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus
```

Main workflow/render files:

```text
docs/horseshowing/show-input-form.html
docs/horseshowing/sync-airtable-controls.js
docs/horseshowing/build-focus-schedule-view.js
docs/horseshowing/build-entry-schedule-view.js
docs/horseshowing/workflow.json
docs/horseshowing/helpers/{show_no}/
docs/horseshowing/reports/
```

Data repo:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-data
```

Data preparation files:

```text
docs/horseshowing/prepare-catalyst-imports.js
docs/horseshowing/catalyst-import/{show_no}-{focus_day}/
docs/horseshowing/normalized/{show_no}-{focus_day}/
catalyst-workspaces/horseshowing/functions/horseshowing_sync/index.js
```

## Airtable Control Surface

Airtable is the manual operator UI.

Catalyst is runtime/storage.

Repo CSV helpers are local mirrors/sync ledgers.

Do not require the operator to open Catalyst to set show focus or helper display fields.

### Airtable Tables Used as Controls or Helpers

```text
focus_show
class_hide
rings
entries
horses
riders
trainers
```

### focus_show

Purpose: manual show/focus configuration.

Expected fields:

```text
focus_show_key
show_no
show_start
show_end
focus_day
name
source
```

Important behavior:

```text
focus_day is manually set.
system day does not override focus_day.
focus_day can be tomorrow, today, or another manually selected date.
```

The workflow must not block because focus_day differs from the system clock.

### class_hide

Purpose: display suppression list for class rows.

Expected fields:

```text
show_no
hide_text
active
```

Example:

```text
14906 | Ticketed Schooling | true
```

This hides matching classes from the rendered schedule display only. It does not delete source rows.

### rings

Purpose: managed ring display labels.

Expected fields:

```text
ring_no
ring_name
ring_display
priority
ring_group
source
```

The render uses:

```text
rings.ring_display
```

Do not invent ring display labels in the render if Airtable has a value.

### entries

Purpose: entry helper and entry display bridge.

Expected fields include:

```text
entry_no
horse
rider
trainer
horse_display from horses
rider_display from riders
trainer_display from trainers
```

Entries are not currently the focus selector. They are part of the enrichment layer.

### horses

Purpose: horse display helper and future tagging/filter support.

Expected fields include:

```text
horse
horse_display
barn_name
tag
active
```

Do not assume horse fields exist unless confirmed in Airtable/schema or CSV mirror.

### riders

Purpose: rider display helper.

Expected fields include:

```text
rider
rider_display
tag
active
```

Important known correction:

```text
Do not assume riders.cwf exists.
```

### trainers

Purpose: trainer display helper and current focus selector.

Expected fields:

```text
trainer
trainer_display
tag
active
```

Required addition:

```text
trainers.active
```

This should be an Airtable checkbox.

Current focus filter:

```text
trainers.active = true
```

If more than one trainer is active, the active trainer schedule must group by trainer.

## Environment Variables

Use the existing WEC/Horseshowing convention:

```powershell
$env:WEC_AIRTABLE_BASE_ID = "app6XS1RvsPNRT6os"
$env:AIRTABLE_TOKEN = "..."
```

Optional Catalyst override:

```powershell
$env:HORSESHOWING_CATALYST_ENDPOINT = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/"
```

Known env warning:

```text
AIRTABLE_BASE_ID may point to another RingStatus base.
Use WEC_AIRTABLE_BASE_ID for this WEC schedule base.
```

## Local Helper CSV Mirror

The helper CSVs are maintained intentionally. They are not throwaway exports.

Path:

```text
docs/horseshowing/helpers/{show_no}/
```

Current helper CSVs:

```text
focus_show.csv
class_hide.csv
rings.csv
classes.csv
entries.csv
horses.csv
riders.csv
trainers.csv
```

Purpose:

```text
Airtable helper tables
-> local CSV mirrors
-> render builders
-> Catalyst sync where applicable
```

The CSVs make the workflow inspectable without clicking through every Airtable or Catalyst table.

## Airtable Helper Sync

Status contract version: 2026-06-12-stage-1

Script:

```text
docs/horseshowing/sync-airtable-controls.js
```

Command:

```powershell
$env:WEC_AIRTABLE_BASE_ID = "app6XS1RvsPNRT6os"
node docs/horseshowing/sync-airtable-controls.js
```

What it does:

```text
1. Reads Airtable focus_show
2. Reads Airtable class_hide
3. Reads Airtable rings
4. Reads Airtable horses
5. Reads Airtable riders
6. Reads Airtable trainers
7. Reads Airtable entries
8. Immediately writes one wec-logs row for each helper table check
9. Runs helper backfill from Catalyst focus-day data when due
10. Writes airtable_helper_backfill to wec-logs
11. Writes local helper CSV mirrors
12. Pushes focus_show into Catalyst using set-show-config
13. Pushes active trainers into Catalyst using set-active-trainers
14. Pushes class_hide rules into Catalyst using set-hide-classes
15. Writes airtable_helpers_summary to wec-logs every 30 minutes
```

Current expected output shape:

```text
focus_show_rows
class_hide_rows
rings_rows
horses_rows
riders_rows
trainers_rows
entries_rows
helper_root
catalyst_synced
```

Stage 1 completion rules:

```text
PASS requires all helper tables to be read:
focus_show
class_hide
rings
horses
riders
trainers
entries

PASS requires class_hide.class_no to exist in Airtable.
The field may be empty; an empty field means hide-by-class-no is supported by schema but not exercised by data.

PASS requires wec-logs rows for every helper check:
log_type = airtable_check
workflow_lanes = Helpers
check_name = focus_show | class_hide | rings | horses | riders | trainers | entries
records_seen = Airtable row count for that table
status = ok
summary = "{table} checked"

PASS requires helper audit rows:
check_name = airtable_helper_backfill
workflow_lanes = Audits

PASS requires a 30-minute helper summary row:
check_name = airtable_helpers_summary
workflow_lanes = Audits
summary = focus_show={n}; class_hide={n}; rings={n}; horses={n}; riders={n}; trainers={n}; entries={n}
```

Stage 1 current verified counts on 2026-06-12:

```text
focus_show = 1
class_hide = 4
rings = 9
horses = 986
riders = 590
trainers = 230
entries = 834
active trainer = Alan Korotkin
trainer_display = CWF
active class_hide rows = Midway Drag, FEI Only, Ring Maintenance, Ticketed Schooling
```

Stage 1 log timing:

```text
Each helper table check is logged immediately after that table is read.
airtable_helper_backfill is logged after focus_show rows are normalized and backfill is evaluated.
airtable_helpers_summary is logged only when the 30-minute summary timer is due.
```

## Airtable Formula Key Protection

Status contract version: 2026-06-12-stage-1b

Purpose:

```text
Protect Airtable formula/key fields from runtime writes.
Runtime scripts write only to explicit mirror/run key fields.
Formula fields remain Airtable-owned.
```

Reason:

```text
Airtable formulas, links, lookups, and rollups are operator/audit structure.
Workflow runners must not overwrite those fields.
If a field is or will become a formula, the writable runtime value must use a separate mirror/run field.
```

Rule:

```text
Never write to formula-intended key fields.
Never use formula-intended key fields as performUpsert merge fields.
Use the mirror/run field for writes and upserts.
Keep source formula fields available for Airtable-side display, validation, and audit.
```

Formula or formula-intended keys and writable mirrors:

```text
focus_show.focus_show_key
-> focus_show.mirror_focus_show_key

class_hide.class_hide_key
-> class_hide.mirror_class_hide_key

counts.class_key
-> counts.mirror_class_key

update_schedule.update_schedule_key
-> update_schedule.mirror_update_schedule_key

class_oog.class_oog_key
-> class_oog.mirror_class_oog_key

class_start_times.class_start_key
-> class_start_times.class_start_key_mirror

entry_go_times.entry_go_key
-> entry_go_times.entry_go_key_mirror

get_orders.get_orders_key
-> get_orders.get_orders_key_mirror

get_rings.get_rings_key
-> get_rings.get_rings_key_mirror

wec-alerts.alert_key
-> wec-alerts.alert_key_run

wec-logs.log_key
-> wec-logs.log_key_run
```

Current writer contract:

```text
sync-airtable-controls.js
-> writes mirror_focus_show_key, mirror_class_hide_key, log_key_run, alert_key_run

sync-stage2-core.js
-> writes mirror_class_key, mirror_update_schedule_key, mirror_class_oog_key, log_key_run

sync-airtable-time-workflows.js
-> writes class_start_key_mirror, entry_go_key_mirror, log_key_run

sync-airtable-core-workflows.js
-> writes log_key_run

run-wec-catalyst-workflow.ps1
-> writes log_key_run, alert_key_run
-> queries/dedupes/clears alerts using alert_key_run
```

Current Airtable write mode:

```text
Control/helper/log/alert rows:
append or update depending on workflow purpose.

Core mirror tables:
upsert, not destructive snapshot.

Time tables:
upsert on mirror keys.

Live tables:
mirror key fields exist; current repo code does not yet write get_orders/get_rings rows directly.
```

Current upsert keys:

```text
ring_days
-> ring_day_no

update_schedule
-> show_no + days + class_no

counts
-> show_no + class_no

class_oog
-> class_no + entry_no

class_start_times
-> class_start_key_mirror

entry_go_times
-> entry_go_key_mirror
```

Numeric field rule:

```text
Airtable numeric ids must remain numbers when written:
show_no
class_no
ring_no
ring_day_no where schema is number
entry_no
entry_order
rider_no when present
```

Known schema exception:

```text
get_rings.show_no is currently singleLineText.
Do not change this without explicit approval.
```

Backfill verification completed on 2026-06-12:

```text
focus_show.mirror_focus_show_key
scanned = 1
missing = 0
mismatch = 0

counts.mirror_class_key
scanned = 469
missing = 0
mismatch = 0

update_schedule.mirror_update_schedule_key
scanned = 199
missing = 0
mismatch = 0

class_oog.mirror_class_oog_key
scanned = 2661
missing = 0
mismatch = 0

class_hide.mirror_class_hide_key
scanned = 4
missing = 0
mismatch = 0

class_start_times.class_start_key_mirror
scanned = 127
missing = 0
mismatch = 0

entry_go_times.entry_go_key_mirror
scanned = 75
missing = 0
mismatch = 0

get_orders.get_orders_key_mirror
scanned = 1
missing = 0
mismatch = 0

get_rings.get_rings_key_mirror
scanned = 6
missing = 0
mismatch = 0

wec-alerts.alert_key_run
scanned = 116
missing = 0
mismatch = 0

wec-logs.log_key_run
scanned = 1919
missing = 0
mismatch = 0
```

Stage 1B completion rules:

```text
PASS requires every formula-intended key to have a writable mirror/run field.
PASS requires all current writers to use mirror/run fields instead of formula-intended fields.
PASS requires read-back verification with missing = 0 and mismatch = 0.
PASS requires numeric ids to remain numeric where the Airtable schema is number.
FAIL if any writer writes directly to a formula-intended key field.
FAIL if any new formula-intended key is added without a matching mirror/run field.
```

## Catalyst Focus Config

Status contract version: 2026-06-12-stage-2

Action:

```text
set-show-config
```

Direct URL pattern:

```text
https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/?action=set-show-config&show_no=14906&show_title=WEC%20Ocala%20Summer%20Series%201%20CSI2*&show_start_date=2026-06-09&show_end_date=2026-06-14&focus_day=2026-06-10
```

Canonical focus storage should be:

```text
hs_focus_show
```

Stage purpose:

```text
Move the operator-selected Airtable focus_show row into Catalyst.
Catalyst must then resolve the current show/focus day without the browser or Webflow embed carrying focus_day manually.
```

Input owner:

```text
Airtable focus_show
```

Current Airtable fields used:

```text
show_no
show_name
show_start
show_end
focus_day
source
```

Field mapping:

```text
focus_show.show_no
-> set-show-config.show_no

focus_show.show_name
-> set-show-config.show_title

focus_show.focus_day
-> display subtitle

focus_show.show_start
-> set-show-config.show_start_date

focus_show.show_end
-> set-show-config.show_end_date

focus_show.focus_day
-> set-show-config.focus_day
```

Current verified Airtable focus row on 2026-06-12:

```text
record_id = recks9BYWaVwjuNw2
focus_show_key = horseshowing|14906
mirror_focus_show_key = horseshowing|14906
show_no = 14906
name = 14906|2026-06-10
show_name = WEC Ocala Summer Series 1 CSI2*
show_start = 2026-06-09
show_end = 2026-06-14
focus_day = 2026-06-12
subtitle = 2026-06-12
source = manual_input
```

Current verified Catalyst write on 2026-06-12:

```text
action = set-show-config
ok = true
show_no = 14906
focus_day = 2026-06-12
hs_focus_show.ROWID = 5614000000416608
hs_focus_show.focus_show_key = 14906|2026-06-12
hs_focus_show.show_no = 14906
hs_focus_show.focus_day = 2026-06-12
hs_focus_show.show_title = WEC Ocala Summer Series 1 CSI2*
hs_focus_show.show_start = 2026-06-09
hs_focus_show.show_end = 2026-06-14
hs_focus_show.source = manual_input
```

Current verified Catalyst readback on 2026-06-12:

```text
action = focus-day-snapshot
request = show_no only, no focus_day supplied
ok = true
show_no = 14906
focus_day = 2026-06-12
```

Stage 2 completion rules:

```text
PASS requires Airtable focus_show to have exactly one active/current focus row for the show.
PASS requires set-show-config to return ok = true.
PASS requires hs_focus_show to contain show_no and focus_day from Airtable.
PASS requires a Catalyst readback action with show_no only to resolve the same focus_day.
FAIL if focus_day must be manually supplied by Webflow/mobile/print to render the current show.
FAIL if Airtable focus_show is not the operator source of the current focus_day.
```

Known correction:

```text
Older code references hs_shows.focus_day_date as a fallback.
That should be cleaned up so focus_show is the canonical focus source.
```

## Core Data Capture

Status contract version: 2026-06-12-stage-3

Stage purpose:

```text
Capture the focus show core schedule from Horseshowing into Catalyst first, then mirror the working rows to Airtable.
This stage is not Webflow, print, mobile, alerts, or live render.
```

Primary runtime:

```text
Catalyst function:
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-data\catalyst-workspaces\horseshowing\functions\horseshowing_sync\index.js

Local stage runner:
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\horseshowing\sync-stage2-core.js
```

Core source endpoints:

```text
get_ring_days.php
update_schedule.php
counts.php
class_oog.php
```

Required execution order:

```text
1. sync-ring-days
2. sync-focus-day schedule pages from update_schedule.php
3. sync-focus-day counts pages from counts.php
4. sync-focus-day class_oog pages from class_oog.php
5. focus-day-snapshot readback
6. Airtable mirror upserts
7. Core log records
```

Critical runtime rules:

```text
sync-ring-days must bootstrap the Horseshowing session by first visiting show.php?show={show_no}.
sync-ring-days must not do serial per-row helper updates for existing days/rings by default.
sync-ring-days default is refresh_existing = false.
sync-ring-days may refresh existing support rows only when refresh_existing=1 is explicitly requested.

sync-focus-day must page update_schedule work.
sync-focus-day must use days_limit=4.
sync-focus-day must continue using next_offset until no next_offset remains.
sync-focus-day must use stored ring days after sync-ring-days succeeds.

Do not run one unpaged full-day sync-focus-day call for Stage 3.
Do not require Webflow, browser state, or schedule.json to populate Stage 3.
```

Current verified run on 2026-06-12:

```text
show_no = 14906
focus_day = 2026-06-12
catalyst_primary = true

sync-ring-days:
status = 200
parsed_rows = 52
duration = about 2.7s

stage runner:
ok = true
ring_days = 52
update_schedule = 67
counts = 43
class_oog = 919

airtable_mirror:
ring_days seen = 8, changed = 8
update_schedule seen = 67, changed = 67
counts seen = 43, changed = 43
class_oog seen = 918, changed = 918
```

Current verified Catalyst readback on 2026-06-12:

```text
action = focus-day-snapshot
ok = true
show_no = 14906
focus_day = 2026-06-12
update_schedule = 67
counts = 43
class_oog = 919
```

Required Core log types:

```text
sync-ring-days
core_update_schedule
core_counts
core_class_oog
core_airtable_mirror
```

Current verified Core logs on 2026-06-12:

```text
sync-ring-days:
summary = Catalyst get_ring_days rows=52
records_seen = 52
status = ok
workflow_lanes = Core

core_update_schedule:
summary = Catalyst update_schedule rows=103 ring_days=9 pages=3
records_seen = 103
status = ok
workflow_lanes = Core

core_counts:
summary = Catalyst counts rows=452 pages=5
records_seen = 452
status = ok
workflow_lanes = Core

core_class_oog:
summary = Catalyst class_oog classes=67 entries=864
records_seen = 864
status = ok
workflow_lanes = Core

core_airtable_mirror:
summary = Airtable mirror ring_days=8; update_schedule=67; counts=43; class_oog=918
records_seen = 1036
status = ok
workflow_lanes = Core
```

Stage 3 completion rules:

```text
PASS requires live sync-ring-days to return status 200 and not 408.
PASS requires the local stage runner to return ok = true.
PASS requires Catalyst focus-day-snapshot to return nonzero update_schedule, counts, and class_oog for the focus_day.
PASS requires Airtable mirror upserts for ring_days, update_schedule, counts, and class_oog.
PASS requires Core logs for sync-ring-days, core_update_schedule, core_counts, core_class_oog, and core_airtable_mirror.

FAIL if sync-ring-days returns 408.
FAIL if sync-focus-day is run as a single unpaged full-day call.
FAIL if Stage 3 depends on Webflow, print, mobile, or schedule.json to populate core data.
```

## Class Start Times

Status contract version: 2026-06-12-stage-4

Stage purpose:

```text
Create one class_start_times row for each focus-day class that has a real class start time.
This stage is fed by Core data, not Webflow, print, mobile, or schedule-json.
```

Script:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\horseshowing\sync-airtable-time-workflows.js
```

Command:

```powershell
node "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\horseshowing\sync-airtable-time-workflows.js" --stage class-start
```

Source contract:

```text
focus-day-snapshot.update_schedule
focus-day-snapshot.counts
```

Entry count rule:

```text
counts.entry_count is authoritative when present.
update_schedule.entry_count is fallback only.
```

Current operational Airtable upsert key:

```text
class_start_key_mirror = show_no + "|" + focus_day + "|" + ring_day_no + "|" + class_no
```

Current fields written:

```text
class_start_key
class_start_key_mirror
show_no
focus_day
ring_no
ring_day_no
class_no
class_number
class_name
class_start_time
display_time
entry_count
source
last_synced_at
```

Current verified run on 2026-06-12:

```text
show_no = 14906
focus_day = 2026-06-12
source update_schedule rows = 67
source counts rows = 43
class_start_times rows written = 63
counts.entry_count applied = 42
update_schedule.entry_count fallback = 21
missing entry_count after write = 0
```

Rows excluded from class_start_times on 2026-06-12:

```text
4 update_schedule rows had time_text = check time and no normalized time.
They remain in update_schedule but do not become class_start_times until a real time exists.
```

Stage 4 log contract:

```text
table = wec-logs
check_name = class_start_times
workflow_lanes = Alerts
status = ok
records_seen = class_start_times rows written
records_changed = class_start_times rows upserted
payload_json.counts_source_rows = source counts rows
payload_json.counts_applied = class_start rows using counts.entry_count
```

Current verified Stage 4 log on 2026-06-12:

```text
created_at = 2026-06-12T17:36:18.874Z
check_name = class_start_times
status = ok
workflow_lanes = Alerts
records_seen = 63
records_changed = 63
summary = class_start_times upserted=63 focus=2026-06-12
payload_json.counts_source_rows = 43
payload_json.counts_applied = 42
```

Stage 4 completion rules:

```text
PASS requires focus-day-snapshot to return update_schedule and counts.
PASS requires class_start_times rows to be built from update_schedule + counts.
PASS requires counts.entry_count to be used before update_schedule.entry_count when counts has the class_no.
PASS requires Airtable class_start_times focus_day rows to have missing entry_count = 0.
PASS requires a wec-logs row with check_name = class_start_times.

FAIL if class_start_times is built from schedule-json.
FAIL if entry_go_times is run as part of the class-start-only stage.
FAIL if check-time rows are converted into class_start_times without a real class_start_time.
```

## Stage 5 - Change Watch Contract

Status:

```text
Documented requirement.
Not closed until implementation and Airtable readback pass.
```

Purpose:

```text
Keep focus-day class and entry timing current through the day.
Detect changes that should remove or suppress previously valid schedule output and alerts.
```

Required watch inputs:

```text
update_schedule
counts
class_oog
get_orders
get_rings
```

Time change rule:

```text
If update_schedule changes a class start time, class_start_times must upsert the new class_start_time.
entry_go_times must then recalculate entry_go_time from the current class_start_time, entry_order, n_gone, elapsed_seconds, and fallback pace.
wec-logs must record the class_start_times and entry_go_times run.
```

WEC mobile / print display-time rule:

```text
The customer-facing WEC mobile and print API currently read schedule rows from Catalyst schedule-json.
The class row time displayed in WEC mobile/print must use the latest class display time from the same current schedule row, not a stale static file.

Primary display source:
schedule-json row display_time / start_display / class_start_time, whichever is the current Catalyst-normalized field for that class row.

Airtable mirror source:
class_start_times.display_time and class_start_times.class_start_time must be refreshed from the same current update_schedule-derived class time.

Entry source:
entry_go_times.entry_go_time is calculated from the current class_start_time, entry_order, n_gone, elapsed_seconds, and fallback pace.
```

Important:

```text
Class start time can move during the day.
Mobile, print, class_start_times, and entry_go_times must all converge on the same latest current time after each refresh.
```

Class cancel/drop rule:

```text
If a previously tracked focus-day class is no longer present in update_schedule for the same show_no + focus_day + class_no,
or the class is represented only by a hidden/cancel/maintenance row,
the class must not continue to render as an active class.
Alerts for that class must not continue after the class is inactive.
```

Entry scratch/drop rule:

```text
If a previously tracked active-trainer entry is no longer present in class_oog for the same show_no + focus_day + class_no + entry_no,
or class_oog indicates the entry has been removed from the order,
entry_go_times must not continue to present that entry as active.
Alerts for that entry must not continue after the entry is inactive.
```

Logging requirement:

```text
Every check must write wec-logs with check_name = class_start_times or entry_go_times.
Payload must include enough counts to audit changed rows, inactive classes, and inactive entries.
```

Stage 5 completion rules:

```text
PASS requires time changes to update class_start_times and recalculate entry_go_times.
PASS requires WEC mobile/print API display time to use the current schedule-json class time after refresh.
PASS requires removed/cancelled classes to stop rendering and stop alerting.
PASS requires scratched/dropped active-trainer entries to stop rendering and stop alerting.
PASS requires Airtable readback for class_start_times, entry_go_times, and wec-logs.

FAIL if a stale class remains active after update_schedule no longer supports it.
FAIL if WEC mobile/print displays an old class time after Catalyst has a newer class time.
FAIL if a stale active-trainer entry remains active after class_oog no longer supports it.
FAIL if alerts are created for inactive classes or scratched/dropped entries.
```

Verified stale suppression run:

```text
verified_at = 2026-06-12T18:20:52Z
show_no = 14906
focus_day = 2026-06-12

Verification method:
Created one temporary stale class_start_times probe and one temporary stale entry_go_times probe.
Ran the normal sync-airtable-time-workflows.js --stage all workflow.
Verified both probes were marked inactive by the normal suppression logic.
Deleted both temporary probes after verification.

Class probe result before cleanup:
- status = inactive
- inactive_reason = missing_from_update_schedule
- inactive_at = 2026-06-12T18:20:46.461Z

Entry probe result before cleanup:
- status = inactive
- inactive_reason = missing_from_class_oog
- inactive_at = 2026-06-12T18:20:46.461Z

wec-logs Airtable readback:
class_start_times log = 2026-06-12T18:20:51.945Z
- records_seen = 63
- records_changed = 64
- payload_json.inactive_existing_seen = 64
- payload_json.inactive_marked = 1

entry_go_times log = 2026-06-12T18:20:52.136Z
- records_seen = 40
- records_changed = 41
- payload_json.inactive_existing_seen = 41
- payload_json.inactive_marked = 1

Final cleanup readback:
- class_start_times focus_day rows = 63
- entry_go_times focus_day rows = 40
- class probe rows = 0
- entry probe rows = 0
- active class_start_times rows = 63
- active entry_go_times rows = 40
```

Verified stale alert suppression run:

```text
verified_at = 2026-06-12T18:29:10Z
show_no = 14906
focus_day = 2026-06-12

Workflow correction:
run-wec-catalyst-workflow.ps1 now runs sync-airtable-time-workflows before Write-TimeAlerts.
This keeps alert checks behind the current class_start_times and entry_go_times refresh.
mock_live_enrichment is not part of the default heartbeat path and only runs with -RunMockLiveCheck.

Verification method:
Created one temporary open wec-alerts probe with alert_key_run:
entry_go_20|14906|2026-06-12|entry_go|999998|999997|20

Ran the normal workflow entrypoint:
powershell -NoProfile -ExecutionPolicy Bypass -File docs\horseshowing\run-wec-catalyst-workflow.ps1 -ShowNo 14906 -FocusDay 2026-06-12 -ForceSync

Airtable probe readback before cleanup:
- status = resolved
- message = Resolved: alert window is no longer active.
- payload_json.resolved_reason = stale_time_window

Final cleanup readback:
- stale alert probe rows = 0

Latest Airtable wec-logs readback:
- get_rings records_seen = 3
- get_orders records_seen = 3
- sync-ring-days records_seen = 9
- core_update_schedule records_seen = 67
- core_counts records_seen = 452
- core_class_oog records_seen = 864
- class_start_times records_seen = 63
- entry_go_times records_seen = 40
```

Verified default workflow run after mock gating:

```text
verified_at = 2026-06-12T18:34:14Z
command = powershell -NoProfile -ExecutionPolicy Bypass -File docs\horseshowing\run-wec-catalyst-workflow.ps1 -ShowNo 14906 -FocusDay 2026-06-12 -ForceSync

Airtable table readback:
- class_start_times rows = 63
- active class_start_times rows = 63
- inactive class_start_times rows = 0
- entry_go_times rows = 40
- active entry_go_times rows = 40
- inactive entry_go_times rows = 0

Latest default-run wec-logs:
- catalyst_heartbeat records_seen = 57
- get_rings records_seen = 3
- get_orders records_seen = 2
- sync-ring-days records_seen = 9
- core_update_schedule records_seen = 67
- core_counts records_seen = 452
- core_class_oog records_seen = 864
- class_start_times records_seen = 63
- entry_go_times records_seen = 40
- entry_go_times alert check records_seen = 40

Default-run confirmation:
- mock_live_enrichment is absent from the latest default run.
- stale-time alert resolver ran after sync-airtable-time-workflows.
```

## Data Preparation

Script:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-data\docs\horseshowing\prepare-catalyst-imports.js
```

Command:

```powershell
node "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-data\docs\horseshowing\prepare-catalyst-imports.js"
```

Output path:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-data\docs\horseshowing\catalyst-import\14906-2026-06-10\
```

Important generated files:

```text
hs_update_schedule.csv
hs_class_oog.csv
hs_class_start_times.csv
hs_entry_go_times.csv
hs_counts.csv
hs_focus_show.csv
hs_get_orders.csv
hs_get_rings.csv
hs_ring_days.csv
```

## Core Table Purposes

### hs_update_schedule

Source:

```text
update_schedule.php
```

Purpose:

```text
Class-level schedule data.
```

Contains:

```text
show_no
ring_day_no
ring_no
ring_name
date_text
class_no
event_id
event_name
class_number
class_payout
class_name
time_text
class_start_time
dow
focus_day
iso_date
entry_count
event_type
oc_id
live_flag
```

Does not contain:

```text
entry_no
horse
rider
trainer
```

Only entry-related value:

```text
entry_count
```

### hs_counts

Source:

```text
counts.php
```

Purpose:

```text
Class-level entry count fallback.
```

Join:

```text
class_no
```

Use:

```text
counts.entry_count is used if update_schedule.entry_count is missing.
```

### hs_class_oog

Source:

```text
class_oog.php?class_no={class_no}
```

Purpose:

```text
Raw order-of-go / class entry source.
```

Key:

```text
class_no + entry_no
```

Contains:

```text
class_no
entry_order
entry_no
horse
rider
trainer
```

May also contain partial or blank class/ring fields depending on source quality.

### hs_class_start_times

Purpose:

```text
Full class schedule spine for the focus day.
```

Derived from:

```text
update_schedule
counts fallback by class_no
```

Key:

```text
show_no + focus_day + ring_day_no + class_no + class_start_time
```

Contains:

```text
show_no
focus_day
ring_day_no
ring_no
ring_name
class_no
class_name
class_start_time
entry_count
```

### hs_entry_go_times

Purpose:

```text
Full derived entry log/snapshot table.
```

This is the enriched bridge between class schedule and entry detail.

Derived from:

```text
class_oog
update_schedule by class_no
counts by class_no
```

Important:

```text
Keep all rows.
Do not filter this table down to active trainers.
```

Current shape:

```text
entry_go_key
show_no
focus_day
ring_day_no
ring_no
class_no
class_start_time
display_time
entry_count
entry_no
entry_order
horse
rider
trainer
go_time
```

Current key:

```text
show_no + focus_day + class_no + entry_no
```

`entry_go_time` is calculated separately and must not be part of the uniqueness key.

## Join Rules

### update_schedule to class_start_times

Join:

```text
class_no
```

Purpose:

```text
Use update_schedule as the class schedule enrichment source.
```

### counts to class_start_times

Join:

```text
class_no
```

Purpose:

```text
Fill entry_count if update_schedule does not provide it.
```

### update_schedule to class_oog

Join:

```text
class_no
```

Purpose:

```text
Create hs_entry_go_times.
Copy class_start_time/time context onto each entry row.
```

### counts to entry_go_times

Join:

```text
class_no
```

Purpose:

```text
Fill entry_count if update_schedule does not provide it.
```

### trainers to entry_go_times

Join:

```text
trainer
```

Purpose:

```text
Filter active trainer schedule where trainers.active = true.
```

### helpers to entry_go_times

Joins:

```text
horses.horse = hs_entry_go_times.horse
riders.rider = hs_entry_go_times.rider
trainers.trainer = hs_entry_go_times.trainer
entries.entry_no = hs_entry_go_times.entry_no
```

Purpose:

```text
Use helper display fields.
```

Display fields:

```text
horse_display
rider_display
trainer_display
```

## Time Rules

### Source Time

The source time is:

```text
update_schedule.time_text
```

Example:

```text
8:00 am
10:00 am
2:45 pm
```

### class_start_time

Formula intent:

```text
IF(
  {time},
  DATETIME_FORMAT(
    DATETIME_PARSE(
      "2000-01-01 " & UPPER(TRIM({time})),
      "YYYY-MM-DD h:mm A"
    ),
    "HH:mm:ss"
  )
)
```

Examples:

```text
8:00 am  -> 08:00:00
10:00 am -> 10:00:00
2:45 pm  -> 14:45:00
```

### display_time

Formula previously shared:

```text
IF(
  LEN(TRIM({time} & "")) > 0,
  SUBSTITUTE(
    DATETIME_FORMAT(
      DATETIME_PARSE("2000-01-01 " & {time}, "YYYY-MM-DD HH:mm:ss"),
      "h:mmA"
    ),
    "M",
    ""
  ),
  "check time"
)
```

This formula expects `{time}` already normalized to `HH:mm:ss`.

Formula output examples:

```text
08:00:00 -> 8:00A
10:00:00 -> 10:00A
14:45:00 -> 2:45P
blank    -> check time
```

Current compact render requirement:

```text
08:00:00 -> 800A
10:00:00 -> 1000A
14:45:00 -> 245P
blank    -> check time
```

Known distinction:

```text
Airtable formula keeps the colon.
Current render removes the colon because the schedule display was requested compact.
```

## Go Time Estimate

The active trainer entry schedule will use:

```text
estimated_go_time
```

or the future canonical field:

```text
go_time
```

This is unique at entry level because it depends on:

```text
class_start_time
entry_order
estimated minutes per entry
```

Initial formula concept:

```text
estimated_go_time = class_start_time + ((entry_order - 1) * class_estimated_minutes_per_entry)
```

The exact pace should remain configurable because hunter/jumper/ring conditions can differ.

Potential future pace inputs:

```text
class_estimated_minutes_per_entry
ring_default_minutes_per_entry
discipline_default_minutes_per_entry
manual_override_minutes_per_entry
```

Do not hardcode this permanently without a helper/config path.

## Time Till Calculations

These are runtime/render values.

Do not treat them as permanent source truth.

### class_time_till

Used by:

```text
full class schedule
active trainer schedule
```

Definition:

```text
minutes between current system time and class_start_time
```

### entry_time_till

Used by:

```text
active trainer schedule
```

Definition:

```text
minutes between current system time and estimated_go_time
```

Refresh behavior:

```text
Recalculate every render/refresh.
```

## Render 1: Full Class Schedule

Script:

```text
docs/horseshowing/build-focus-schedule-view.js
```

Command:

```powershell
node docs/horseshowing/build-focus-schedule-view.js 14906 2026-06-10
```

Output:

```text
docs/horseshowing/reports/14906-2026-06-10-focus-schedule.html
docs/horseshowing/reports/14906-2026-06-10-focus-schedule.csv
```

Purpose:

```text
Full schedule list.
One row per class after display hide/dedupe rules.
```

Current visible fields:

```text
ring_display
display_time
class_number
class
```

Hidden/metadata fields:

```text
ring_day_no
class_no
entry_count
```

Future required enrichment:

```text
estimated_start_time
class_time_till
active_trainer_count
active_trainers
active_entry_count
active_horse_count
active_horses
has_active_trainer_entries
```

Rollups should come from:

```text
hs_entry_go_times filtered by trainers.active = true
```

This lets the full schedule show which classes matter to the team without expanding every entry.

Verified live output readback:

```text
verified_at = 2026-06-12T18:40Z

Catalyst schedule-json:
- status = 200
- rows = 57
- focus_days = 2026-06-12
- teamRows = 19
- liveRows = 11
- hiddenLeaks = 0

Live WEC mobile:
- url = https://ringstatus.com/wec-mobile
- status = 200
- title = WEC Ocala Summer Series 1 CSI2*
- subtitle = 2026-06-12
- groups = 56
- teamGroups = 19
- CWF badges = 19
- rowsWithTrainerName = 0
- hiddenLeaks = false
- Print link = https://ringstatus.com/wec-print
- console errors/warnings = 0

Live WEC print:
- url = https://ringstatus.com/wec-print
- status = 200
- title = WEC Ocala Summer Series 1 CSI2*
- groups = 56
- teamGroups = 19
- CWF badges = 19
- rowsWithTrainerName = 0
- hiddenLeaks = false
- pageWidth = about 816px
- columns = 4 ring groups left, 3 ring groups right
- PDF button href uses ringstatus-pdf.gombcg.workers.dev with waitForSelector=.ring
- console errors/warnings = 0

PDF worker:
- status = 200
- content-type = application/pdf
- bytes = 89730
- starts with %PDF- = true

Embed parity:
- wec-mobile-webflow-embed.html == wec-mobile-webflow-embed.txt
- wec-print-webflow-embed.html == wec-print-webflow-embed.txt

Default workflow:
- mock_live_enrichment is not present in live embeds.
- mock_live_enrichment is not present in latest default-run logs after 2026-06-12T18:32:30Z.
```

## Render 2: Active Trainer Entry Schedule

Script:

```text
docs/horseshowing/build-entry-schedule-view.js
```

Command:

```powershell
node docs/horseshowing/build-entry-schedule-view.js 14906 2026-06-10
```

Output:

```text
docs/horseshowing/reports/14906-2026-06-10-entry-schedule.html
docs/horseshowing/reports/14906-2026-06-10-entry-schedule.csv
```

Purpose:

```text
Entry-level active trainer schedule.
```

Filter:

```text
trainers.active = true
```

Grouping:

```text
trainer
> ring
>> time | class_number | class
>>> entry_order | entry_no | horse_display | rider_display
```

If more than one trainer is active:

```text
Each active trainer gets a separate top-level section.
```

Future required enrichment:

```text
estimated_go_time
entry_time_till
class_time_till
```

## Display Rules

### Ring Display

Use:

```text
rings.ring_display
```

Do not create ring labels in code unless Airtable helper value is missing.

### Class Hide

Use:

```text
class_hide.hide_text
```

Only active rows apply.

Hide behavior is display-only.

### Duplicate Display Classes

Some classes are functionally duplicate display rows.

Known example:

```text
830A | 280 | Baby Green Hunter 2'6
830A | 281 | Baby Green Hunter 2'6'
```

Display rule:

```text
If ring + display_time + normalized class text match, hide the duplicate display row.
```

Normalization currently ignores apostrophes.

Source rows remain intact.

## Human Operating Steps

### Step 1: Set Show Focus in Airtable

Open Airtable base:

```text
wec_schedules
app6XS1RvsPNRT6os
```

Go to:

```text
focus_show
```

Set or verify:

```text
show_no
show_start
show_end
focus_day
name
source
```

Example:

```text
show_no: 14906
show_start: 2026-06-09
show_end: 2026-06-14
focus_day: 2026-06-10
name: WEC Ocala Summer Series 1 CSI2*
source: manual_input
```

### Step 2: Set Class Hide Rules

Go to:

```text
class_hide
```

Add or verify rows:

```text
show_no
hide_text
active
```

Example:

```text
14906 | Ticketed Schooling | true
```

### Step 3: Set Ring Display Labels

Go to:

```text
rings
```

Verify:

```text
ring_no
ring_name
ring_display
```

The render will use `ring_display` exactly.

### Step 4: Set Active Trainers

Go to:

```text
trainers
```

Add field if missing:

```text
active
```

Type:

```text
checkbox
```

Check `active` for the trainers that should appear in the active trainer schedule.

### Step 5: Sync Airtable Helpers Locally

Run:

```powershell
$env:WEC_AIRTABLE_BASE_ID = "app6XS1RvsPNRT6os"
node docs/horseshowing/sync-airtable-controls.js
```

Verify output includes:

```text
focus_show_rows
class_hide_rows
rings_rows
horses_rows
riders_rows
trainers_rows
entries_rows
catalyst_synced
```

### Step 6: Prepare Catalyst Import CSVs

Run:

```powershell
node "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-data\docs\horseshowing\prepare-catalyst-imports.js"
```

Verify:

```text
hs_class_start_times.csv
hs_entry_go_times.csv
hs_update_schedule.csv
hs_class_oog.csv
```

### Step 7: Build Full Class Schedule

Run:

```powershell
node docs/horseshowing/build-focus-schedule-view.js 14906 2026-06-10
```

Open:

```text
docs/horseshowing/reports/14906-2026-06-10-focus-schedule.html
```

### Step 8: Build Active Trainer Entry Schedule

Run:

```powershell
node docs/horseshowing/build-entry-schedule-view.js 14906 2026-06-10
```

Open:

```text
docs/horseshowing/reports/14906-2026-06-10-entry-schedule.html
```

If the output has no rows, check:

```text
trainers.active exists
at least one trainer is active
trainer names in hs_entry_go_times match trainer names in helpers/trainers.csv
```

## Codex Handoff

### Primary Objective

Continue from the current WEC/Horseshowing workflow and implement the final shape without inventing new tables or filters.

The user has already specified the intended model:

```text
1. Full class schedule
2. Active trainer entry schedule
3. hs_entry_go_times remains full source/log/snapshot table
4. active trainer filtering is render-layer only
```

### Do Not Do

Do not create:

```text
team_entries
riders.cwf
new filter tables
new focus selector concepts
```

Do not reduce:

```text
hs_entry_go_times
```

Do not guess fields. Verify Airtable schema or helper CSV headers.

### Current Main Files

Main repo:

```text
docs/horseshowing/sync-airtable-controls.js
docs/horseshowing/build-focus-schedule-view.js
docs/horseshowing/build-entry-schedule-view.js
docs/horseshowing/workflow.json
docs/horseshowing/helpers/14906/
docs/horseshowing/reports/
```

Data repo:

```text
docs/horseshowing/prepare-catalyst-imports.js
docs/horseshowing/catalyst-import/14906-2026-06-10/
catalyst-workspaces/horseshowing/functions/horseshowing_sync/index.js
```

### Current Verified Counts

For `14906 / 2026-06-10`:

```text
hs_class_start_times: 51 class rows
hs_class_oog: 777 entry rows / 51 classes / 520 entries
hs_entry_go_times: 777 rows
hs_entry_go_times missing entry_count: 0
```

Known missing times:

```text
8 entry rows have no class_start_time because update_schedule.time_text is blank for their classes.
```

Classes:

```text
29331 | $150 Performance Hunter 3'3
29326 | $150 Young Hunter 3'3
```

### Current Implemented Data Logic

`prepare-catalyst-imports.js` now builds:

```text
hs_class_start_times
= update_schedule
+ counts.entry_count fallback by class_no
```

and:

```text
hs_entry_go_times
= class_oog
+ update_schedule by class_no
+ counts.entry_count fallback by class_no
```

`hs_entry_go_times` includes:

```text
ring_day_no
ring_no
class_start_time
display_time
entry_count
entry_order
entry_no
horse
rider
trainer
go_time
```

### Current Implemented Render Logic

Full class schedule:

```text
build-focus-schedule-view.js
```

Uses:

```text
hs_class_start_times
hs_update_schedule by class_no
rings helper
classes helper
class_hide helper
```

Entry schedule:

```text
build-entry-schedule-view.js
```

Uses:

```text
hs_entry_go_times
hs_class_start_times
hs_update_schedule by class_no
rings helper
classes helper
horses helper
riders helper
trainers helper
```

Filter:

```text
trainers.active = true
```

Grouping:

```text
trainer -> ring -> class -> entries
```

### Immediate Next Implementation Work

1. Add/confirm Airtable `trainers.active` checkbox.

2. Rerun:

```powershell
$env:WEC_AIRTABLE_BASE_ID = "app6XS1RvsPNRT6os"
node docs/horseshowing/sync-airtable-controls.js
```

3. Confirm:

```text
docs/horseshowing/helpers/14906/trainers.csv
```

has:

```text
active = 1
```

for selected trainers.

4. Build active trainer schedule:

```powershell
node docs/horseshowing/build-entry-schedule-view.js 14906 2026-06-10
```

5. Add active trainer rollups into full schedule:

```text
active_trainer_count
active_trainers
active_entry_count
active_horse_count
active_horses
has_active_trainer_entries
```

6. Add estimated go time:

```text
estimated_go_time = class_start_time + ((entry_order - 1) * configured pace)
```

7. Add countdowns:

```text
class_time_till
entry_time_till
```

## Known Trouble

### Thin Source Data

Horseshowing is not SGL.

Do not expect:

```text
rich live groups
complete trip timing
reliable go times from source
commercial-grade payload density
```

Build from what exists.

### update_schedule Is Class-Level Only

`update_schedule` does not include:

```text
horse
rider
trainer
entry_no
entry_order
```

Use `class_oog` / `entry_go_times` for entry-level data.

### class_oog May Be Not Posted

Some class OOG data may be marked:

```text
NOT A POSTED ORDER
```

Preserve the data, but do not overstate confidence in actual go order unless verified.

### Blank Times

Some `update_schedule.time_text` values may be blank.

When blank:

```text
class_start_time is blank
display_time should be check time
go_time estimate cannot be reliable
```

Known current blank-time classes:

```text
29331
29326
```

### Display Time Difference

Airtable formula display:

```text
8:00A
```

Current compact render:

```text
800A
```

This was requested for narrow display. Do not change back without confirming.

### Ring Display Is Airtable-Managed

Current Airtable `rings.ring_display` values may be compact labels:

```text
GRAND
INDR_1
INDR_2
ANNEX
HUNT_2
```

The render should use those values exactly.

### Airtable Connector Reauthentication

The ChatGPT Airtable connector may require reauthentication.

If the connector fails, use REST API with:

```text
AIRTABLE_TOKEN
WEC_AIRTABLE_BASE_ID
```

### Env Base Confusion

`AIRTABLE_BASE_ID` may point to another RingStatus base.

For this workflow, prefer:

```text
WEC_AIRTABLE_BASE_ID=app6XS1RvsPNRT6os
```

### Catalyst Focus Source Cleanup

Some Catalyst code may still fallback to:

```text
hs_shows.focus_day_date
```

The canonical focus source should be:

```text
hs_focus_show
```

### Do Not Add More Tables Without Need

Avoid creating extra helper/control tables if the existing tables can carry the intent.

Specific rejected idea:

```text
team_entries.csv
```

The intended selector is:

```text
trainers.active
```

## Future Enhancements

### Active Trainer Rollups on Full Schedule

Add to full class schedule:

```text
active_trainer_count
active_trainers
active_entry_count
active_horse_count
active_horses
has_active_trainer_entries
```

These rollups should derive from:

```text
hs_entry_go_times + trainers.active
```

### Estimated Go Time

Add:

```text
estimated_go_time
```

to active trainer schedule.

Initial estimate:

```text
class_start_time + ((entry_order - 1) * estimated_minutes_per_entry)
```

Future helper config:

```text
ring pace
class pace
discipline pace
manual override pace
```

### Time Till Fields

Add volatile render-time fields:

```text
class_time_till
entry_time_till
```

Recalculate each refresh.

### Live Overlay

When focus day equals system day, optionally enrich with:

```text
get_rings.php
get_orders.php
```

These are day-of status overlays, not primary schedule builders.

Live endpoint accuracy step:

```text
Status: OPEN.
This step remains open until live endpoint data is consumed into the same class/entry timing model and verified by Airtable readback.
```

Live input responsibilities:

```text
get_orders.php:
- class_no where available
- ring_no
- ring_day_no
- current class context
- current entry context
- total / entry_count context
- n_to_go
- n_gone
- elapsed
- current displayed time
- order-of-go/current entry signal when present

get_rings.php:
- ring_no
- ring_day_no
- current class context
- current entry context
- total / entry_count context
- n_to_go
- n_gone
- elapsed
- current displayed time
- ring-level pace signal
```

Live enrichment outputs:

```text
class_start_times:
- update class_start_time/display_time when live/current class time is more accurate than stale core time
- update n_gone
- update n_to_go
- update elapsed_seconds
- update pace_seconds if stored at class level
- preserve class_no + show_no + focus_day identity

entry_go_times:
- recalculate entry_go_time after class_start_time or live pace changes
- copy n_gone
- copy elapsed_seconds
- copy pace_seconds
- preserve show_no + focus_day + class_no + entry_no identity
- do not create duplicate rows when the same entry appears in the same class
```

Future WEC mobile display scope:

```text
Status: SCOPED ONLY.
Do not change current WEC mobile render until explicitly approved.

The WEC mobile API should be allowed to expose live timing fields so future display options can show:
- n_gone
- n_to_go
- elapsed_seconds
- pace_seconds
- current_entry_no
- current_horse
- live_source
- entry_go_time
- time_till

These fields are optional display data.
They must not change the locked class/rollup render unless a future display option is approved.
```

Order-of-go / scratch watch:

```text
class_oog remains the entry roster source.
get_orders/get_rings can improve current position and pace.
If class_oog no longer contains show_no + focus_day + class_no + entry_no, treat the entry as inactive/scratched for render and alert purposes.
If get_orders/get_rings imply a class has moved forward, recalculate time_till and suppress past alerts.
```

Live completion rules:

```text
PASS requires get_orders/get_rings rows to update n_gone, n_to_go, elapsed_seconds, and pace_seconds where available.
PASS requires class_start_times to reflect the current class time used by WEC mobile/print.
PASS requires entry_go_times to recalculate entry_go_time from the current class_start_time and live pace inputs.
PASS requires no duplicate entry_go_times for show_no + focus_day + class_no + entry_no.
PASS requires wec-logs entries for get_orders, get_rings, class_start_times, and entry_go_times.

FAIL if live data changes but WEC mobile/print still shows stale time.
FAIL if n_gone/n_to_go/elapsed are available but not copied into the timing tables.
FAIL if an inactive/scratched active-trainer entry still renders or alerts.
```

Verified live timing run:

```text
verified_at = 2026-06-12T18:12:58Z
show_no = 14906
focus_day = 2026-06-12

Real endpoint smoke:
schedule-json rows = 57
schedule-json live_source counts:
- get_orders.php = 7
- get_rings.php = 3
- update_schedule.php = 47

sync-rings:
parsed_rows = 6
class_no_resolved = 6

sync-orders:
parsed_rows = 4
class_no_resolved = 4

class_start_times Airtable readback:
rows written = 63
sample live class 29136:
- n_gone = 17
- n_to_go = 0
- elapsed_seconds = 628
- pace_seconds = 37
- current_entry_no = 2571
- live_source = get_rings.php
- source = update_schedule.php|get_rings.php

sample live class 29220:
- n_gone = 9
- n_to_go = 3
- elapsed_seconds = 222
- pace_seconds = 30
- current_entry_no = 2021
- live_source = get_orders.php
- source = update_schedule.php|get_orders.php

entry_go_times Airtable readback:
rows written = 40
duplicate entry_go_key_mirror rows = 0
sample active-trainer live class 29178:
- entry_go_time recalculated
- n_gone = 10
- elapsed_seconds = 650
- pace_seconds = 65

wec-logs Airtable readback:
get_rings latest ok log = 2026-06-12T18:00:24.190Z, records_seen = 6
get_orders latest ok log = 2026-06-12T18:00:24.756Z, records_seen = 5
class_start_times latest ok log = 2026-06-12T18:12:58.530Z, records_seen = 63
entry_go_times latest ok log = 2026-06-12T18:12:58.755Z, records_seen = 40
```

### Cron/Cadence

Suggested workflow cadence:

```text
get_ring_days.php
-> low frequency, show setup / date structure

update_schedule.php
-> focus day and future-day schedule changes

counts.php
-> class entry count fallback

class_oog.php
-> entry list and trainer/horse/rider detail

get_rings.php / get_orders.php
-> actual day status overlay
```

Manual focus still drives the workflow.

Do not block sync because system date differs from focus day.

### Catalyst Schema Alignment

Consider adding/updating Catalyst columns for:

```text
hs_entry_go_times.ring_day_no
hs_entry_go_times.ring_no
hs_entry_go_times.class_start_time
hs_entry_go_times.display_time
hs_entry_go_times.entry_count
hs_entry_go_times.estimated_go_time
```

### Airtable Helper Improvements

Add or verify:

```text
trainers.active
horse_display
rider_display
trainer_display
ring_display
class_display
```

Keep helper display fields human-managed.

## Validation Checklist

After changes, verify:

```text
1. sync-airtable-controls.js runs without Airtable auth error
2. helpers/14906/trainers.csv includes active column
3. at least one trainer has active = 1
4. prepare-catalyst-imports.js regenerates import CSVs
5. hs_entry_go_times has 777 rows
6. hs_entry_go_times has entry_count populated
7. hs_entry_go_times has class_start_time where update_schedule has time_text
8. full schedule render builds
9. entry schedule render builds when trainers.active has matches
10. full schedule source rows are not reduced
11. class_hide suppresses only display rows
12. duplicate display classes are hidden only in render
```

## Print Rollup Contract

The WEC print page must preserve the locked portrait print layout:

```text
8.5x11 portrait
2 columns
whole ring-group stacks only, no splitting
ring header full width
class line is one row and clips with ellipsis
rollup sits above the related class row
rollup may wrap
rollup uses the same available class-text width
minor padding above and below rollup
```

Active trainer rollup grouping rules:

```text
1. Use schedule-json/class_oog-derived trainer_rollups as the authority.
2. Do not infer a horse is in another class unless the payload confirms it.
3. Consecutive same-time class rows may be merged into one printed rollup group only when their active trainer rollup key is identical and non-empty.
4. When merged, show the rollup once above the first related class row.
5. When merged, include all related class rows inside the group.
6. When merged, hide repeated time on the following class rows when the time matches the row above.
7. Same-time classes without the same active trainer rollup stay separate.
```

## Current Commands

From main repo:

```powershell
cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus"
```

Sync Airtable helpers:

```powershell
$env:WEC_AIRTABLE_BASE_ID = "app6XS1RvsPNRT6os"
node docs/horseshowing/sync-airtable-controls.js
```

Prepare data repo imports:

```powershell
node "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-data\docs\horseshowing\prepare-catalyst-imports.js"
```

Build full schedule:

```powershell
node docs/horseshowing/build-focus-schedule-view.js 14906 2026-06-10
```

Build active trainer entry schedule:

```powershell
node docs/horseshowing/build-entry-schedule-view.js 14906 2026-06-10
```

Open reports:

```text
docs/horseshowing/reports/14906-2026-06-10-focus-schedule.html
docs/horseshowing/reports/14906-2026-06-10-entry-schedule.html
```

## Stage - 2026-06-12 Current Focus-Day Validation

Status contract version: 2026-06-12-current-focus-validation

### Situation

The current show day is 2026-06-12.

The current focus-day validation must use 2026-06-12.

During this validation window, live feeds may legitimately be empty or low-value because the show day is ending or over:

```text
get_orders.php
get_rings.php
```

Empty live data during this window is not a workflow failure by itself.

Do not block the 2026-06-12 validation workflow because live endpoints return zero rows.

### Time Box

The operator will not change Airtable `focus_show.focus_day` for the next 60 minutes.

During that 60-minute window, testing must use explicit request parameters or local runner parameters for:

```text
show_no = 14906
focus_day = 2026-06-12
```

Do not require the operator to change Airtable focus_day before proving core population for 2026-06-12.

### Core Endpoints Still Actionable

The 2026-06-12 validation test must use the core endpoints:

```text
update_schedule.php
counts.php
class_oog.php
```

These are the required data sources for the 2026-06-12 focus-day validation.

Known caveat:

```text
counts.php and update_schedule.php may look complete or wonky after the show day is over.
```

That caveat must not stop the workflow.

### Validation Run - 2026-06-12

Run target:

```text
show_no = 14906
focus_day = 2026-06-12
```

Core command:

```powershell
node docs\horseshowing\sync-stage2-core.js 14906 2026-06-12
```

Core result:

```text
ring_days mirrored to Airtable = 8
update_schedule rows = 67
counts rows = 43
class_oog rows = 996 mirrored / 997 in Catalyst snapshot
```

Time workflow command:

```powershell
node docs\horseshowing\sync-airtable-time-workflows.js --show-no 14906 --focus-day 2026-06-12
```

Time workflow result:

```text
class_start_times rows = 63
entry_go_times rows = 41
inactive class_start_times rows = 0
inactive entry_go_times rows = 0
```

Catalyst render API check:

```text
focus-day-snapshot.update_schedule = 67
focus-day-snapshot.counts = 43
focus-day-snapshot.class_oog = 997
wec-mobile-live.rings = 7
wec-mobile-live.classes = 56
wec-mobile-live.team_groups = 21
wec-mobile-live.hidden_leaks = 0
schedule-json.rows = 56
schedule-json.team_rows = 21
```

Render join correction:

```text
Root cause: schedule render used a trainer-filtered hs_entries ZCQL query that returned zero rows even though the same focus-day class_oog source contained active-trainer entries.
Correction: getEntriesForSchedule now falls back to getEntriesForClasses and filters active trainers in JavaScript when the filtered ZCQL query returns zero.
Deploy target: horseshowing_sync
Deploy result: success
```

Live page check:

```text
https://ringstatus.com/wec-mobile
PASS: title rendered, 2026-06-12 rendered, CWF groups rendered, no [OBJECT OBJECT], no browser errors.

https://ringstatus.com/wec-print
FAIL: live Webflow page still uses stale print embed code that renders [OBJECT OBJECT].
```

Local print drop check:

```text
docs\horseshowing\webflow-drops\wec-print-webflow-embed-current.html
PASS: title rendered, 2026-06-12 rendered, CWF groups rendered, no [OBJECT OBJECT], no browser errors.
```

Airtable controls/helpers check:

```powershell
node docs\horseshowing\sync-airtable-controls.js
```

Result:

```text
focus_show rows = 1
class_hide rows = 4
rings rows = 9
horses rows = 1010
riders rows = 610
trainers rows = 235
entries rows = 951
catalyst_synced = 4
helper backfill = skipped/not_due
```

Post-helper Catalyst config check:

```text
active_trainers = Alan Korotkin
trainer_displays = Alan Korotkin -> CWF
horse_display_count = 0
```

Remaining issue:

```text
FAIL: Catalyst focus config still does not retain the Airtable horse display map after the helper sync.
Current mobile rollups render because the default display map still covers the visible horses, not because the Airtable horse_display map is confirmed in Catalyst.
```

### Last-Hour State - 2026-06-12 Current Focus Day

Scope:

```text
show_no = 14906
focus_day = 2026-06-12
Do not use 2026-06-13 yet.
```

Core data status:

```text
PASS: update_schedule core data populated for 2026-06-12.
PASS: counts core data populated for 2026-06-12.
PASS: class_oog core data populated for 2026-06-12.
PASS: class_start_times populated for 2026-06-12.
PASS: entry_go_times populated for 2026-06-12.
```

Active trainer render status:

```text
PASS: active trainer source is present in Catalyst for 2026-06-12.
active_trainers = Alan Korotkin
trainer_displays = Alan Korotkin -> CWF

PASS: Catalyst render API now returns active-trainer rollups.
wec-mobile-live.classes = 56
wec-mobile-live.team_groups = 21
wec-mobile-live.hidden_leaks = 0
```

Render join fix:

```text
Problem:
The render path queried hs_entries with trainer filtering and returned zero rows.

Confirmed data:
focus-day class_oog contained active trainer rows.

Fix:
getEntriesForSchedule now falls back to getEntriesForClasses and filters active trainers in JavaScript when the filtered ZCQL query returns zero.

Deployment:
horseshowing_sync deployed successfully.
```

Print status:

```text
PASS: local print drop renders CWF rollups without [OBJECT OBJECT].
FAIL: live /wec-print was stale and still rendered [OBJECT OBJECT] before the operator updated the print embed.
Needs recheck after Webflow publish.
```

Post-publish print recheck:

```text
PASS: live /wec-print renders 2026-06-12.
PASS: live /wec-print renders CWF groups.
PASS: live /wec-print no longer renders [OBJECT OBJECT].
PASS: live /wec-print browser errors = 0.
```

Current print drop source:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\horseshowing\webflow-drops\wec-print-webflow-embed-current.txt
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\horseshowing\webflow-drops\wec-print-webflow-embed-current.html
```

Mobile embed status:

```text
FAIL: live /wec-mobile HTML is stale versus the current local mobile drop.

Evidence:
live /wec-mobile contains schedule-json.
live /wec-mobile does not contain wec-mobile-live.

Current local mobile drop does contain:
wec-mobile-live API source
subtitle/edit wrapper
horse edit action
print proxy URL
rounded time display
hide repeated rounded time bucket
check-time badge
diff-time / diff-oog classes
desktop font clamp
```

Post-fix mobile recheck:

```text
PASS: live /wec-mobile renders 2026-06-12.
PASS: live /wec-mobile renders CWF groups.
PASS: live /wec-mobile does not render [OBJECT OBJECT].
PASS: live /wec-mobile browser errors = 0.

FAIL: live /wec-mobile still does not contain wec-mobile-live.
FAIL: live /wec-mobile still contains schedule-json.
Conclusion: live mobile display is usable, but the Webflow mobile embed is not the current local drop and still needs replacement to carry the latest mobile logic.
```

Current mobile drop source:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\horseshowing\webflow-drops\wec-mobile-webflow-embed-current.txt
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\docs\horseshowing\webflow-drops\wec-mobile-webflow-embed-current.html
```

Answer to whether mobile needs a new embed:

```text
YES.
Not because of the Catalyst active-trainer rollup fix.
Yes because live /wec-mobile is stale and is not using the current local mobile drop that contains the recent UI/API/edit/time logic.
```

Horse display status:

```text
Original failure:
Catalyst debug-show-config returned focus_source.horse_display_count = 0.

Meaning:
Airtable helper horses exist and the local control script can build a horse display payload, but Catalyst is not retaining that horse display map in the current focus config.

Evidence from local helper CSVs:
horses helper rows = 1010
entries helper rows = 951
active trainer scoped horse names = 35
horse display payload count = 44

Likely failing boundary:
set-horse-displays stores the full horse_displays object inside hs_focus_show.source.
The write returns success, but readback from debug-show-config returns zero horse displays.

Impact:
Mobile/print can still show many barn names from the hardcoded default display map.
This is not acceptable as the final path because Airtable horses should be the source for barn_name/horse_display.
```

Troubleshooting proof:

```text
Small payload test:
Sandenal -> Snoop persisted in hs_focus_show.source.

Full payload test:
44 horse display records / 4016 bytes wrote with HTTP 200.
hs_focus_show.source retained only 255 characters.
The truncated source JSON could not be parsed back into horse_displays.
This also risked wiping active_trainers from the parsed focus source.
```

Correction:

```text
set-horse-displays no longer stores the full horse map in hs_focus_show.source.
hs_focus_show.source now stores only small control metadata/counts.
Full horse_displays and horse_display_meta now persist in hs_shows.raw_json.
metaForFocusRender reads hs_shows.raw_json and merges the horse display map into the mobile/print render meta.
debug-show-config now reports resolved.horse_display_count separately from focus_source.horse_display_count.
```

Post-fix proof:

```text
full payload count = 44
payload bytes = 4016
write status = 200
hs_focus_show.source length = 180
hs_shows.raw_json length = 3966
focus_source_horse_display_count = 0
resolved_horse_display_count = 51
active_trainers = Alan Korotkin
mobile_classes = 56
mobile_team_groups = 21

Interpretation:
focus_source_horse_display_count staying 0 is expected after the fix.
resolved_horse_display_count is the correct metric.
```

The workflow must continue as long as the endpoint returns parseable rows that can populate the focus-day schedule.

### Required Population Path

For `show_no = 14906` and `focus_day = 2026-06-12`:

```text
1. sync ring_days only as needed to resolve ring_day_no for 2026-06-12
2. sync update_schedule.php for 2026-06-12
3. sync counts.php and match counts.entry_count by class_no
4. sync class_oog.php for the 2026-06-12 classes
5. upsert Airtable helper/mirror data from those core sources
6. build class_start_times from update_schedule + counts
7. build entry_go_times from class_oog + class_start_times + active trainers
8. expose wec-mobile and print using the current API path
```

### Helper Population Risk

This stage must explicitly prove helper population, not only logs.

Tables to verify after core sync:

```text
horses
riders
trainers
entries
rings
classes
```

Current known risk:

```text
we have not fully proven helper table population from workflows except logs.
```

This stage is not complete until helper rows are verified in the working store and/or Airtable mirror.

### Mobile/Print Requirement

Within this validation stage, the system must be able to populate:

```text
wec-mobile
wec-print
show_focus_day = 2026-06-12
```

The API path must be used for mobile/print data.

Do not depend on a stale static schedule JSON as the primary source.

Static JSON may only be considered fallback.

### Completion Rules

PASS requires:

```text
1. core_update_schedule returns nonzero rows for 2026-06-12 or a proven endpoint-level no-data result
2. core_counts returns parseable rows and count matching is attempted by class_no
3. core_class_oog runs for the focus-day class set and returns entry rows or logs specific class-level no-order/no-data
4. helpers/mirrors are verified for horses, riders, trainers, entries, rings, and classes
5. class_start_times is populated for 2026-06-12 classes with real times
6. entry_go_times is populated for active-trainer entries when class_oog supports them
7. wec-mobile API returns 2026-06-12 rows
8. wec-print can render 2026-06-12 rows from the API path
9. wec-logs contains separate entries for core_update_schedule, core_counts, core_class_oog, class_start_times, entry_go_times, and helper verification
```

FAIL if:

```text
1. live get_orders.php or get_rings.php is empty and the workflow stops because of that
2. focus_day must be changed in Airtable before 2026-06-12 can be tested
3. only logs are populated and helper tables are not verified
4. mobile or print depends on stale schedule JSON as primary source
5. counts/update_schedule being complete or imperfect blocks continued population
```

## Render Contract Update - 2026-06-12 Late

### Grouped Time Display Rule

Duplicate-time suppression applies only to ordinary class rows.

If a class row contains an active-trainer rollup/group, that class row must display its own rounded time even when the prior class in the same ring has the same rounded time bucket.

Verified example:

```text
class 545
ring INDR_4
rollups null
time displayed normally

class 546
ring INDR_4
rollup CWF Paisley/Poptart
time must display even though class 545 has same rounded time bucket
```

### PDF Worker Readiness Rule

The PDF worker must not capture on `.ring` alone.

The print page sets:

```text
html[data-rs-pdf-ready="1"]
```

only after the second `fitToOnePage()` pass.

Mobile and print PDF links must use:

```text
waitForSelector=html[data-rs-pdf-ready="1"]
```

### Verification Evidence

Local drop verification on 2026-06-12:

```text
mobile rings: 7
mobile groups: 56
mobile teamGroups: 21
mobile objectObject: false
class 545 rollup: none
class 545 displayed time: 12:50 PM
class 546 rollup: CWF
class 546 displayed time: 12:50 PM

print rings: 7
print groups: 56
print teamGroups: 21
print objectObject: false
print pdfReady: 1
local generated PDF pages: 1
```
## 2026-06-12 21:46 ET - Focus Day 2026-06-13 Verified Path

Current source of truth for customer-facing WEC schedule render:

1. Airtable `focus_show` owns `show_no`, `focus_day`, `show_name`, `show_start`, `show_end`.
2. Core workflow writes Airtable mirrors:
   - `update_schedule`
   - `counts`
   - `class_oog`
3. `sync-airtable-time-workflows.js` builds:
   - `class_start_times` from `update_schedule + counts`
   - `entry_go_times` from `class_oog + class_start_times/core schedule + active trainers`
4. Webflow Cloud API route reads Airtable directly:
   - `https://ringstatus.com/test/wec-schedule/state?show_no=14906`
5. Webflow pages read that state endpoint:
   - `https://ringstatus.com/wec-mobile`
   - `https://ringstatus.com/wec-print`
6. Mobile print button opens the PDF worker directly against `https://ringstatus.com/wec-print?focus_day={focus_day}&pdf=1`.

Helper display contract:

- `trainers.active=true` selects active trainer entries.
- `trainer_display` is the visible badge text, e.g. `CWF`.
- `horse_display` must come from Airtable `horses.barn_name` or `horses.horse_display`.
- Helper matching is exact show-name first, then normalized exact show-name or normalized `aka`.
- Blank class_oog-created helper rows must not override Airtable horse helper records.
- No fuzzy matching is allowed.

Verified 2026-06-12 21:46 ET:

- Full workflow command:
  `.\docs\horseshowing\run-wec-catalyst-workflow.ps1 -ShowNo 14906 -FocusDay 2026-06-13 -ForceSync`
- Airtable/live API audit:
  - expected active trainer entries: 52
  - `entry_go_times` active rows: 52
  - live state entry keys: 52
  - missing from `entry_go_times`: 0
  - missing from live state: 0
  - hidden leaks: 0
  - schedule rows: 103
  - team rows: 33
- Helper display audit:
  - active `entry_go_times` rows: 52
  - helper mismatches: 0
  - `Indigo Van De Muggenhoek` renders as `Indy`
- Published browser audit:
  - `wec-mobile`: date present, CWF present, Indy present, recovered class 28999 group present, no `[object Object]`, no "No schedule rows", no browser errors.
  - `wec-print`: date present, CWF present, Indy present, recovered class 28999 group present, no `[object Object]`, no "No schedule rows", no browser errors.
- PDF proxy audit:
  - mobile print button href returns HTTP 200
  - content-type `application/pdf`
  - PDF header `%PDF-`
  - page count marker: 1

Known reason class `28999` was missing before this fix:

- `class_oog` had active trainer entries for class `28999`.
- `class_start_times` had no class `28999` row because `update_schedule` had no populated start time for that class.
- The live state route previously only emitted rows from `class_start_times`, so active entries attached to no-time classes were dropped.
- The state route now adds entry-only class rows from `entry_go_times` when a class has active trainer entries but no class_start row.

Known reason `Indigo Van De Muggenhoek` rendered full show name before this fix:

- Airtable `horses` had two records:
  - class_oog-created row with `horse=Indigo Van De Muggenhoek` and no barn fields.
  - helper row with `horse=INDIGO VAN DE MUGGENHOEK`, `barn_name=Indy`, `horse_display=Indy`.
- The workflow used case-sensitive exact mapping and accepted fallback self-mapping from the blank helper row.
- The workflow now ignores helper rows without `barn_name` or `horse_display`, and uses exact-or-normalized matching only.
