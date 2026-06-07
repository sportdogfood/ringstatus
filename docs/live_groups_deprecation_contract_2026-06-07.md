# Live Groups Deprecation Contract - 2026-06-07

## Locked Decision

`live_groups` is the only active live group lane.

The old `groups_live` table is deprecated and must not be used by active runners, active health checks, or active live-class context lookup.

## Active Live Group Lane

These files own the current live group workflow:

| File | Responsibility |
|---|---|
| `live_groups_daily.js` | Pings live group endpoints, writes `live_groups`, logs `live_group_changes`, and propagates group values to `watch_schedule` / `watch_trips`. |
| `live_class_detail.js` | Reads `live_groups` views for class detail pings. |
| `live_rings_daily.js` | Reads `live_groups` for ring-state snapshots. |
| `schedules_dailyv2.js` | May read `live_groups` for schedule fallback enrichment only. |
| `schedules_calculatorv2.js` | May read `live_groups` for DAY overlay calculations only. |
| `trips_tagger.js` | May read `live_groups` for live class context only. |
| `monitor_watch_trips_health.js` | May report `live_groups` health only. |

## Prohibited

No active runner may:

- Read the `groups_live` table.
- Default an environment variable to `groups_live`.
- Write a `groups_live` linked field to `watch_schedule` or `watch_trips`.
- Report `groups_live` as an active health surface.
- Reintroduce `TABLE_GROUPS_LIVE`.

## Deprecated Tables

These tables are deprecated and scheduled for deletion after the 14-day observation window.

Delete on or after: `2026-06-21`

| Table | Status | Notes |
|---|---|---|
| `groups_live` | Deprecated, active code removed | Replaced by `live_groups`. |
| `active_groups` | Deprecated | Current Node lanes skip active table writes. |
| `active_classes` | Deprecated | Current Node lanes skip active table writes. |
| `active_entries` | Deprecated | Current Node lanes skip active table writes. |
| `watch_conflicts` | Deprecated | Old Airtable script only. |
| `watch_classes` | Deprecated | Old Airtable script only. |
| `watch_entries` | Deprecated | Old Airtable script only. |
| `watch_results` | Deprecated | No executable repo dependency found. |
| `all_trips` | Deprecated | No executable repo dependency found. |
| `all_trips_tagger` | Deprecated | No executable repo dependency found. |
| `all_trip_logs` | Deprecated | No executable repo dependency found. |

## Observation Rule

During the 14-day window, any error, runner output, or Airtable automation that references one of the deprecated tables must be treated as stale workflow evidence.

Resolution path:

1. Identify the runner or Airtable automation that referenced the deprecated table.
2. Disable the stale automation or migrate it to the current table.
3. Do not recreate the deprecated table to satisfy stale code.

## Current Replacement Map

| Deprecated | Replacement |
|---|---|
| `groups_live` | `live_groups` |
| `watch_classes` / `watch_entries` | `watch_schedule` / `watch_trips` |
| `active_groups` / `active_classes` / `active_entries` | `watch_schedule` / `watch_trips` state plus explicit live lanes |
| `all_trips` / `all_trips_tagger` / `all_trip_logs` | `watch_trips`, `schedule_logs`, `trip_logs`, and live snapshot tables as appropriate |

## Historical Files

Older docs, scratch files, and captured console outputs may still contain old table names. They are not workflow authority. Active runner files and this contract are the source for this decision.
