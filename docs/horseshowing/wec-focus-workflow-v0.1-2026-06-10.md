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
class_no + entry_no + entry_order
```

Preferred future key:

```text
show_no + focus_day + class_no + entry_no + entry_order
```

`go_time` is intentionally left blank for formula/estimate logic.

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
