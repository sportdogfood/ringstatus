# Watch Schedule PRO Field Audit - 2026-05-11

This audit covers the `watch_schedule` Airtable view `PRO` (`viwGx7wVMLgA89Ta6`) and defines which fields should be treated as value of truth versus inactive duplicates, source evidence, relationship links, operational controls, or derived display fields.

The word `inactive` here means field-ownership inactive for PRO decisions. It does not mean the row-level Airtable checkbox named `inactive`.

Complete field inventory: `docs/watch_schedule_pro_field_inventory_2026-05-11.csv`.

## Live Audit Snapshot

Snapshot time: `2026-05-11`

```text
table: watch_schedule
view: PRO
rows audited: 58
fields processed: 210
```

## All-Field Processing Result

Every field in the `watch_schedule` table was processed against the live `PRO` rows and assigned a status in `docs/watch_schedule_pro_field_inventory_2026-05-11.csv`.

| Status | Field count | Meaning |
| --- | ---: | --- |
| `value_of_truth` | 31 | Canonical field for a concept in PRO or row ownership. |
| `inactive_duplicate_or_lookup` | 18 | Duplicate, lookup, display variant, or legacy/source copy that should not be used as independent truth. |
| `inactive_blank_legacy_link` | 1 | Blank legacy link field in current PRO rows. |
| `source_evidence` | 32 | Endpoint, lookup, or evidence field used to explain a value, not replace truth. |
| `relationship_link` | 22 | Airtable link field; useful for joins, not scalar truth. |
| `operational_control` | 15 | Runner, heartbeat, scope, alert, or audit control field. |
| `derived_calculator_output` | 3 | Calculator output used for display/alerting only. |
| `derived_display_or_filter` | 69 | Formula, rollup, count, or display/filter helper. |
| `blank_in_pro` | 19 | Empty on all 58 current PRO rows. |

The inventory includes field type, Airtable field id, populated count, blank count, unique count, publisher-default flag, action, and top observed values.

## Value Of Truth

| Concept | Value of truth for PRO | Inactive or evidence fields | Rule |
| --- | --- | --- | --- |
| row identity | `sid` | `dt` | `sid` is the stable schedule row identity. `dt` is a date input/display variant, not the row identity. |
| show id | `show_id`, `app_show_idv2` | `app_sid`, `app_show_id` | `show_id` is row-owned SGL show id. `app_show_idv2` is the row scope snapshot. Legacy/app copies are not independent truth. |
| schedule date | `schedule_show_datev2` | `scheduled_date`, `show_date`, `schedule_show_display_datev2`, `app_sql_date`, `app_sql_date (from heartbeat)` | `schedule_show_datev2` is the canonical row date for filtering. `scheduled_date` and `show_date` are synchronized compatibility values. Display/heartbeat fields are not canonical filters. |
| row scope date | `app_sql_datev2` | `app_sql_date`, `app_sql_date (from heartbeat)` | `app_sql_datev2` is the schedule-lane scope snapshot. Heartbeat lookup fields are only meaningful when the row is linked to a matching heartbeat. |
| day label | `app_dow_rawv2` | `schedule_show_display_date_dayv2`, `app_dow_raw (from heartbeat)` | `app_dow_rawv2` is the machine day label. `schedule_show_display_date_dayv2` is display text. |
| ring | `ring_number`, `ringName`, `ring_nickname` | `ring`, `ring_id`, `ring_number (from groups_live)` | Use numeric ring and row display ring fields as row truth. Live/group ring lookups are source evidence. |
| class group | `class_group_id`, `class_groupxclasses_id`, `class_group_sequence`, `group_name`, `group_name_tags` | `record_id (from groups_live)` | Schedule row owns the group identity. Live group record ids are source evidence. |
| class | `class_id`, `class_number`, `class_name`, `class_type`, `schedule_sequencetype` | `class_id (from groups_live)`, `classes`, `classNumbers`, `class_names`, `class_numbers_list` | Use row-owned class fields for PRO. Live arrays are evidence and can help repair, but are not independent truth. |
| status | `status` | `latestStatus`, `latest_status`, `status (from groups_live)` | `status` is canonical. `latestStatus` is display/formula compatibility. |
| estimated start | `estimated_start_time` | `latestStart`, `latest_estimated_start_time`, `___latest_estimated_start_time`, `estimated_start_time (from groups_live)` | `estimated_start_time` is canonical. Lookup/display variants are evidence and should not overwrite manual corrections blindly. |
| estimated end | `estimated_end_time` | derived end/calculator fields | `estimated_end_time` is canonical if populated; derived display fields are not truth. |
| total trips | `total_trips` | `total`, `trips`, `Count (watch_trips)`, `tripsCount` | `total_trips` is total class/group trips from schedule/live. Watch-trip counts are local target/watch counts, not class total. |
| completed trips | `completed_trips` | `gone`, `gone (from groups_live)`, `rs_completed_trips` | `completed_trips` is canonical progress. Live `gone` is source evidence. |

## Current Duplicate/Mismatch Evidence

| Field group | Result | Interpretation |
| --- | --- | --- |
| row-owned date truth fields | `schedule_show_datev2`, `app_sql_datev2`, `scheduled_date`, and `show_date` matched on all 58 PRO rows. | Date ownership is aligned for the current PRO sample. |
| show id fields | `show_id` and `app_show_idv2` matched on all 58 PRO rows. | Show ownership is aligned for the current PRO sample. |
| status fields | Populated status values matched on 51 rows; 7 rows were blank and 2 rows had partial source blanks. | `status` can stay the value of truth. |
| start-time fields | `estimated_start_time` disagreed with `estimated_start_time (from groups_live)` on 6 rows. | `groups_live` is useful evidence, but it cannot be treated as automatic truth over `estimated_start_time`. |
| total trips versus watch-trip counts | `total_trips` differed from `Count (watch_trips)` / `tripsCount` on 51 rows. | These are different concepts. `total_trips` is class/group total; counts are local linked watch-trip row counts. |
| display date/day fields | Display fields differ by format from machine fields on all rows. | Format mismatch is expected; do not classify display fields as truth. |

Start-time mismatch examples:

| Record | `estimated_start_time` | `estimated_start_time (from groups_live)` |
| --- | --- | --- |
| `recjVQhpNGpIphY0F` | `09:05:00` | `09:00:13` |
| `recrRvXzTJ7YJMmlO` | `10:17:00` | `10:40:09` |
| `recEktKyNu7DjiOc3` | `11:11:00` | `11:55:33` |
| `recKwL4KOR6Du67qQ` | `11:11:00` | `11:55:33` |
| `recLmC6USajHypEzd` | `10:17:00` | `10:40:09` |

## Publisher Contract Check

`publisher.js` currently includes these PRO schedule fields by default:

```text
sid
dt
show_id
app_sid
app_sql_date
app_show_idv2
app_sql_datev2
ring_number
ringName
class_groupxclasses_id
class_group_id
class_group_sequence
group_name
group_name_tags
class_id
class_number
class_name
class_type
schedule_sequencetype
latestStart
latestStatus
status
scope_status
estimated_start_time
estimated_end_time
total_trips
rollup_entries
rollup_trips
rollup_horses
```

Some publisher fields are compatibility/display fields, not value of truth. In particular, `app_sql_date`, `app_sid`, `latestStart`, and `latestStatus` should not be treated as independent truth just because they are currently exported.

## Operating Rule

1. Keep `schedule_show_datev2`, `app_sql_datev2`, `show_id`, `app_show_idv2`, `class_id`, `class_number`, `group_name`, `status`, `estimated_start_time`, and `total_trips` as the primary row-owned truth fields.
2. Treat heartbeat lookup fields as current-heartbeat evidence only when the row scope matches the heartbeat.
3. Treat `groups_live` fields as live enrichment evidence, not automatic truth over row-owned schedule fields.
4. Treat display fields such as `latestStart`, display dates, day labels, and rollups as UI/export helpers.
5. Do not hide, rename, or delete fields during live operation; mark inactive status in the inventory first, then clean the Airtable view only after downstream consumers are checked.

