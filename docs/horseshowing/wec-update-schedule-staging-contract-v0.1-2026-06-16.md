# WEC Update Schedule Staging Contract

Version: `0.1`

Date: `2026-06-16`

Status: current focused contract for `update_schedule.php` raw-block ingestion and Airtable staging.

Parent contract:

```text
docs/horseshowing/wec-workflow-contract-v0.2-2026-06-15.md
```

## Purpose

This lane exists to get `update_schedule.php` under control with smaller, repeatable focus-day blocks.

It does not replace the broader WEC workflow contract. It defines only:

```text
get_ring_days staging source
fetch-update-schedule-raw by ring_day_no
update_schedule_staging
update_schedule mirror
class_start_times seed
wec-logs evidence
helper links on staging
focus-day staging lifecycle
```

## Source

Primary Horseshowing endpoint:

```text
https://www.horseshowing.com/update_schedule.php
```

Request:

```text
POST
content-type: application/x-www-form-urlencoded; charset=UTF-8
body: show_no={show_no}&ring_day_no={ring_day_no}
```

Source selector inside the returned HTML:

```text
h3.ring_evt
```

Current extracted attributes:

```text
h3 id                     -> event_id
data-show                 -> show_no
data-class                -> class_no
data-time                 -> time_text
data-n_entries            -> entry_count
data-name                 -> event_name / class label
data-re_type              -> event_type
data-oc_id                -> oc_id
data-live                 -> live_flag
ring_day_no               -> supplied from get_ring_days
ring_no                   -> supplied from get_ring_days
ring_name                 -> supplied from get_ring_days
date_text / ISO / YYYYMMDD -> supplied from get_ring_days
```

Do not drop `class_no = 0` rows in `update_schedule_staging` or `update_schedule`.

## Catalyst Functions

Raw helper function:

```text
horseshowing_sync
```

Direct raw action:

```text
action=fetch-update-schedule-raw&show_no={show_no}&ring_day_no={ring_day_no}
```

Current runner function:

```text
horseshowing_update_schedule_runner
```

Runner URL:

```text
https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_update_schedule_runner/
```

Runner call:

```text
?show_no=14906&focus_day=2026-06-14&batch_size=1&window_minutes=60&slot_index=0
```

Runner source of ring blocks:

```text
Airtable get_ring_days
```

Runner filtering:

```text
eligible rows = get_ring_days where ISO >= focus_day
```

Current proven example:

```text
show_no = 14906
focus_day = 2026-06-14
get_ring_days rows = 52
eligible_not_past_focus_day = 9
batch_size = 1
slots = 9
```

The runner now fetches `update_schedule.php` directly. It must not call `horseshowing_sync` as a nested Catalyst function for normal block runs because that path hit Catalyst execution time limits.

## Airtable Tables

Base:

```text
app6XS1RvsPNRT6os
```

Primary tables in this lane:

```text
get_ring_days
update_schedule_staging
update_schedule
class_start_times
wec-logs
```

Helper/link tables:

```text
shows
classes
ring_days
rings
show_days
events
```

## get_ring_days

Purpose:

```text
staging list of available ring_day_no blocks
```

Critical fields:

```text
show_no
ring_day_no
ring_no
ring_name
date_text
ISO
YYYYMMDD
shows
show_days
ring_days
rings
ring_names
dows
focus_show
```

Current verified for `14906`:

```text
total rows = 52
2026-06-14 rows = 9
focus_show linked only to focus-day rows
show_days focus key = 20260614
```

## update_schedule_staging

Purpose:

```text
focus-day working table and human review layer
```

Current rule:

```text
update_schedule_staging should represent the current focus_day working set.
Historical/non-current rows may exist, but must be marked inactive.
```

Core fields:

```text
staging_key
show_no
class_no
ring_day_no
ring_no
ring_name
date_text
iso_date
event_id
event_name
class_name
time_text
entry_count
event_type
oc_id
live_flag
review_status
review_notes
source_key
source
```

Control/review fields:

```text
lock
inactive
is_target
not_target
```

Link fields:

```text
shows
classes
ring_days
rings
show_days
events
update_schedule
wec-logs
```

Current staging key:

```text
show_no|ring_day_no|event_id
```

Example:

```text
14906|3864|58961
```

## update_schedule

Purpose:

```text
all/raw mirror table for update_schedule.php rows
```

Current write rule from the runner:

```text
same parsed source rows as update_schedule_staging
upsert by mirror_update_schedule_key
do not delete
do not prune class_no=0
```

Current mirror key:

```text
mirror_update_schedule_key = staging_key
```

Current verified `14906 / 2026-06-14`:

```text
update_schedule_staging = 109
update_schedule = 109
class_no=0 rows = 24
no-time rows = 31
```

## class_start_times

Purpose:

```text
seed one class timing row for each positive class_no row in staging/update_schedule
```

Current runner rule:

```text
class_start_times receives only class_no > 0 rows
```

Current class_start key:

```text
show_no|focus_day|ring_day_no|class_no
```

Current derived fields:

```text
class_number from event_name leading "{number})"
class_name from event_name after ")"
class_start_time normalized from time_text to HH:mm:ss
display_time from time_text
status = upcoming when time_text exists
status = check_time when time_text is empty
```

Current verified `14906 / 2026-06-14`:

```text
positive class rows = 85
class_start_times = 85
with time = 78
check_time = 7
```

## wec-logs

Purpose:

```text
evidence for each raw update_schedule block fetch
```

Runner writes:

```text
workflow_lanes = Core
log_type = core_update_schedule
check_name = fetch-update-schedule-raw
show_no
focus_day
status = ok
records_seen = 1
records_changed = raw_length
summary = ring_day_no {ring_day_no} raw_length {raw_length}
payload_json = slot/ring_day/raw details
```

`update_schedule_staging.wec-logs`:

```text
linked only to rows touched by the latest runner block
```

Current verified example:

```text
slot 0
ring_day_no = 3864
raw_length = 3953
staging rows touched = 7
wec-log = reccqF7QY4lIsuJBZ
```

## Helper Link Rules

The staging helper links are not guesses.

Required link mapping:

```text
update_schedule_staging.show_no       -> shows.show_no
update_schedule_staging.class_no      -> classes.class_no
update_schedule_staging.ring_day_no   -> ring_days.ring_day_no
update_schedule_staging.ring_no       -> rings.ring_no
update_schedule_staging.iso_date      -> show_days.show_day as YYYYMMDD
update_schedule_staging.event_id      -> events.event_id
```

Current events rule:

```text
events.event_id is numeric
do not link events by primary text/name
```

Current link gating:

```text
Populate staging links before reading update_schedule_staging.lock.
Lock gates downstream class_start_times creation only.
```

Current verified after link population:

```text
rows = 109
locked = 78
unlocked = 31
missing shows/classes/ring_days/rings/show_days/events = 0
```

After `is_target` / `not_target` formulas were added, downstream consumers should use:

```text
is_target = true
```

instead of reimplementing:

```text
lock checked
inactive false
not hidden
other review conditions
```

## Focus-Day Lifecycle

Operating decision:

```text
update_schedule = all/update_schedule.php mirror
update_schedule_staging = current focus_day working table
```

Focus-day change behavior:

```text
1. Read current focus_show.
2. Refresh get_ring_days for the show if needed.
3. Run update_schedule runner for the new focus_day blocks.
4. Upsert new focus_day rows into update_schedule_staging.
5. Mark staging rows with iso_date != focus_day as inactive.
6. Mark staging rows with iso_date = focus_day as active/inactive false.
7. Populate staging helper links for the full focus_day staging set.
8. Re-read staging and build class_start_times from locked positive class_no rows.
9. Log the block fetch and row counts in wec-logs.
```

Current implementation state:

```text
runner writes staging/update_schedule/class_start_times/wec-logs
runner has optional mark_focus_state=1 support
normal block calls skip full focus_state sweep to avoid Catalyst runtime timeout
helper-link population was verified through Airtable maintenance operation
inactive marking was verified through Airtable maintenance operation
```

Required next hardening:

```text
move helper-link population into a short dedicated maintenance action
move inactive focus-state sweep into a short dedicated maintenance action
wire focus_day change to:
  run all needed runner blocks
  run maintenance action for inactive/is_target links
  audit counts
```

## Current Verified 2026-06-14 Dataset

Show:

```text
14906
```

Focus day:

```text
2026-06-14
```

Runner eligible blocks:

```text
9
```

Verified block results:

```text
3864 -> 7 staging rows
3893 -> 28 staging rows
3899 -> 20 staging rows
3905 -> 1 staging row
3911 -> 25 staging rows
4186 -> 11 staging rows
4181 -> 7 staging rows
4179 -> 9 staging rows
4299 -> 1 staging row
```

Verified totals:

```text
update_schedule_staging = 109
update_schedule = 109
class_start_times = 85
class_no=0 rows = 24
no-time staging/update_schedule rows = 31
class_start_times check_time = 7
class_start_times upcoming = 78
```

## Known Runtime Constraint

Nested Catalyst calls caused timeout:

```text
horseshowing_update_schedule_runner -> horseshowing_sync -> update_schedule.php
```

Current correction:

```text
horseshowing_update_schedule_runner -> update_schedule.php directly
```

Normal block runs must stay small:

```text
batch_size = 1
one ring_day_no per call
```

Full-table Airtable maintenance should not run inside every block call.

## Current PASS Requirements For This Lane

PASS requires:

```text
1. each selected ring_day_no returns non-empty raw HTML
2. staging rows are upserted
3. update_schedule rows are upserted
4. class_start_times rows are upserted for class_no > 0
5. wec-logs row is created per block
6. staging rows touched by block link to that wec-log
7. inactive state matches current focus_day
8. helper links are populated for the full focus_day staging set before lock is read
9. class_start_times rows are created only from locked target rows
10. counts match the verified block totals
```

FAIL means no implementation path was found after live proof.

OPEN means continue working.
