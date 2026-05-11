# Watch Schedule And Trips Field Audit - 2026-05-11

This audit documents the date/show fields currently visible on `watch_schedule` and `watch_trips`.

## Field Ownership

Important naming rule: the same concept name in `heartbeat` and `watch_schedule` does not always mean the same operational thing.

| Table | Field family | Definition |
| --- | --- | --- |
| `heartbeat` | `show_id`, `app_show_id`, `app_sql_date`, `app_dow_raw` | Current app-control scope for this heartbeat run. These fields answer: what show/date is the pipeline targeting now? |
| `watch_schedule` | `show_id`, `show_date`, `schedule_show_datev2` | Row-owned SGL schedule identity. These fields answer: what show/date does this schedule row represent? |
| `watch_schedule` | `app_show_idv2`, `app_sql_datev2`, `app_dow_rawv2` | Row scope snapshot from the schedule lane when the row was created/refreshed. These fields should usually match the row-owned SGL schedule identity. |
| `watch_schedule` | `app_sql_date (from heartbeat)`, `app_dow_raw (from heartbeat)` | Lookup values from the linked heartbeat only. These are only meaningful when the row is linked to a heartbeat whose scope matches the row. |
| `watch_trips` | `show_id`, `schedule_show_datev2`, `scheduled_date`, `show_date` | Row-owned trip/schedule identity. These fields answer what show/date the trip row represents. |
| `watch_trips` | `app_show_idv2`, `app_sql_datev2`, `app_dow_rawv2` | Row scope snapshot from the trips lane when the row was created/refreshed. |
| `watch_trips` | `app_sql_date (from heartbeat)`, `app_dow_raw (from heartbeat)` | Lookup values from the linked heartbeat only. These are only meaningful when the row is linked to a heartbeat whose scope matches the row. |

| Field | Owner | Definition | Current correction |
| --- | --- | --- | --- |
| `app_sql_date (from heartbeat)` | Airtable lookup | Current linked heartbeat app date. This is not the row's schedule date. | `tagger.js` must only link a `watch_schedule` row to the latest heartbeat when the row's stored show/date scope matches the heartbeat app scope. Mismatched lookup links are cleared. |
| `app_dow_raw (from heartbeat)` | Airtable lookup | Current linked heartbeat app day. This is not the row's schedule day unless the row is current scope. | Same correction as above. |
| `show_date` | SGL schedule/live payload | Actual schedule day from the SGL payload or live group row. | Keep as row-owned schedule date. Should match `schedule_show_datev2` for normal rows. |
| `app_sql_datev2` | schedule lane snapshot | App scope date used when the row was created/refreshed. | Keep, but treat as row scope snapshot, not current heartbeat lookup. |
| `schedule_show_datev2` | schedule lane canonical row date | Canonical schedule date for the class/group row. | Prefer this for filtering rows by event date. |
| `schedule_show_display_datev2` | SGL schedule payload | Display date supplied by SGL, usually `M/D/YYYY`. | Display only. Do not use for machine filtering. |
| `scheduled_date` | legacy compatibility | Legacy row schedule date used by downstream trips/calculator code. | Keep synchronized to `schedule_show_datev2` until downstream code no longer needs it. |
| `show_id` | SGL schedule/live payload | Actual show id for the schedule row. | Keep as row-owned show id. |
| `app_show_idv2` | schedule lane snapshot | App show id used when the row was created/refreshed. | Keep, but treat as row scope snapshot. Should match `show_id` for normal rows. |
| `app_dow_rawv2` | schedule lane snapshot | Day-of-week derived from `app_sql_datev2`. | Keep as row-owned day snapshot. |
| `schedule_show_display_date_dayv2` | SGL schedule payload | Human display day/date from SGL. | Display only. Do not use for machine filtering. |

## Root Cause Found

On 2026-05-11, many May 10 `watch_schedule` rows still had their `heartbeat` link repointed to the newest heartbeat after the app default moved to May 28. That made lookup fields show:

```text
app_sql_date (from heartbeat) = 2026-05-28
app_dow_raw (from heartbeat) = Thu
```

while the row-owned schedule fields correctly still showed:

```text
show_date = 2026-05-10
app_sql_datev2 = 2026-05-10
schedule_show_datev2 = 2026-05-10
app_dow_rawv2 = Sun
```

The fix is not to change the May 10 row-owned dates. The fix is to stop globally rebinding `watch_schedule.heartbeat` to the newest heartbeat when the row scope does not match the heartbeat scope.

## Filtering Rule

For row filtering, use row-owned fields:

- show: `app_show_idv2` or `show_id`
- date: `schedule_show_datev2`, then `app_sql_datev2`, then `show_date`
- day label: `app_dow_rawv2` or `schedule_show_display_date_dayv2`

Do not filter historical/current schedule rows by `app_sql_date (from heartbeat)` unless the row is known to be linked to the current heartbeat scope.

## 2026-05-11 `watch_schedule` Cleanup Result

After the scoped relink fix was deployed and the heartbeat lane was run:

```text
watch_schedule heartbeat view rows: 105
rows linked to a heartbeat after cleanup: 18
linked lookup date mismatches: 0
cleared mismatched heartbeat links: 87
```

The 87 cleared rows kept their row-owned May 10 schedule fields. Only the misleading heartbeat lookup link was removed.

## 2026-05-11 `watch_trips` Cleanup Rule

The same relink rule applies to `watch_trips`:

- link to the latest heartbeat only when `watch_trips` row scope matches heartbeat `app_show_id/app_sql_date`
- clear an existing heartbeat link when the row's schedule date differs from the linked heartbeat lookup date
- preserve row-owned trip fields such as `show_id`, `schedule_show_datev2`, `scheduled_date`, `show_date`, `app_show_idv2`, and `app_sql_datev2`

Cleanup result after deployment:

```text
watch_trips heartbeat view rows: 2
cleared mismatched heartbeat links: 2
linked rows after cleanup: 0
linked lookup date mismatches: 0
```
