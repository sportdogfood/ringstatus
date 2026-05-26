# WEC Packing Gate 1 Registry Report

Generated from live Airtable API on 2026-05-26.

Updated on 2026-05-26 after adding `fields_allowed`, `const_env`, and env key names to the two planned WEC packing rows.

Updated again on 2026-05-26 after the live Airtable schema was completed.

Current verified status:

- `wec_pack_waves` exists: `tblWUBY7vpNIKaby8`
- `wec_packing_events` exists: `tblEPLwEYRClYFnar`
- `wec_packing_items` exists: `tblq1SARyF9aTCpO2`
- `wec_packing_item_horses` exists: `tbldmrXz8zCHHonYu`
- Webflow Cloud env keys are present for those four tables.
- `/wec-packing/health` verifies 10 required WEC tables with no missing required fields.
- `/wec-packing/state` returns `needsGeneration: true` because `wec_pack_waves` has no wave records yet.

The older "planned rows" notes below are historical context and should not be used as the current implementation gate.

Purpose: inspect `wec_meta` and Airtable metadata before continuing the WEC packing app build.

## Gate 1 Result

Gate 1 is complete for the two planned WEC packing tables.

The live registry is usable, and the planned app rows now define:

- `fields_allowed`
- `const_env`
- `AIRTABLE__TABLE`
- `AIRTABLE__VIEW`

The physical tables still do not exist because `table_api` is blank. That belongs to Gate 2. No app, embed, or write-route work should proceed until Gate 2 is complete.

## Registry Source

```text
table name: wec_meta
table api:  tbllJywsOstkqT5yZ
```

Live `wec_meta` summary:

```text
total registry rows: 21
physical tables:     18
planned records:     3
ignored rows:        0
```

## Planned Rows

These rows exist in `wec_meta`, but do not yet have physical Airtable table ids.

| table_name | status | table_api | fields_allowed | env keys |
| --- | --- | --- | --- | --- |
| `wec_pack_waves` | planned | missing | present | present |
| `wec_packing_events` | planned | missing | present | present |
| `webflow_slug` | planned | missing | missing | missing |

`webflow_slug` may be support/config rather than part of the WEC packing app. It should be classified before implementation.

## Physical Core Tables

These WEC tables exist physically and are confirmed by Airtable metadata.

| table | table_api | views | role |
| --- | --- | --- | --- |
| `wec_meta` | `tbllJywsOstkqT5yZ` | Grid view | registry |
| `wec_shows` | `tblrOQ1Lygfb4CeE8` | Grid view | show scope |
| `wec_ranges` | `tblJyAURsK2GvWjhA` | Grid view | retired/ignored for active packing |
| `wec_weeks` | `tblnXR9WGc9Y8aTsO` | Grid view | week attendance |
| `wec_horses` | `tblLvYxEneUuGTLcv` | Grid view | horse roster |
| `wec_grooms` | `tblHw7hwwIcVZtmiv` | Grid view | groom source/support |
| `wec_vendors` | `tblgEWGXGchl2T8ca` | Grid view, vendor_focus | vendor/local guide |
| `wec_pack_lists` | `tblHzCwKG4RoHj0kH` | Grid view, quick | source list groups |
| `wec_pack_items` | `tbljKBYJ68yD29WiY` | master, quick_list, grouped, onsite, unresolved, list, feed | source item templates |
| `wec_packing_items` | `tblq1SARyF9aTCpO2` | Grid view | current item progress |
| `wec_packing_item_horses` | `tbldmrXz8zCHHonYu` | Grid view | current horse-item progress |
| `wec_local_tags` | `tbltWUvYgW4al8l1z` | Grid view | local guide tags |
| `wec_place_type` | `tbledsxMSXO52WKsP` | Grid view | local guide classes |

## Supporting Tables

These are physical but should not be treated as WEC packing app tables unless explicitly pulled into a workflow.

| table | table_api | purpose |
| --- | --- | --- |
| `feed_items` | `tblcjW0sR1MMqcMNy` | feed names |
| `horses_change_log` | `tblf5RO3IhDz8c0Ko` | example/support |
| `hp_cls` | `tbld9p6jM55iCjMJb` | example/support |
| `ww_grooms` | `tblKRxPAlapW1h8V2` | source of truth grooms |
| `ww_horses` | `tblliyUZ1ZS88Kfvl` | source of truth horses |

## Current Data Counts

```text
wec_horses:               52
wec_pack_lists:           24
wec_pack_items all:       327
wec_pack_items master:    173
wec_packing_items:        1
wec_packing_item_horses:  1
```

`wec_pack_items` master view by `list_plan`:

```text
per_groom:       63
quantity:        44
horse_specific:  40
per_horse:       14
blank:           12
```

Horse active status:

```text
record_state = active: 1
active checkbox = true: 0
```

This confirms that the source/template layer is populated, but the live worksheet/progress layer has not been generated.

## Gate 1 Blockers

1. `wec_pack_waves` is only planned.
   - no `table_api`
   - `fields_allowed` present
   - env key names present

2. `wec_packing_events` is only planned.
   - no `table_api`
   - `fields_allowed` present
   - env key names present

3. `webflow_slug` is only planned.
   - unclear whether this belongs to WEC packing, Webflow routing, or general support

4. `const_env` is populated for the two planned WEC packing rows.
   - `AIRTABLE_WEC_PACK_WAVES_TABLE`
   - `AIRTABLE_WEC_PACK_WAVES_VIEW`
   - `AIRTABLE_WEC_PACKING_EVENTS_TABLE`
   - `AIRTABLE_WEC_PACKING_EVENTS_VIEW`

5. `wec_ranges` is not used by the active packing app.
   - `wec_pack_waves` carries week/truck grouping.
   - Do not build worksheet calculations from `wec_ranges`.

6. `wec_packing_items` and `wec_packing_item_horses` are not yet populated as frozen worksheet snapshots.

## `fields_allowed` For Planned Tables

These values are now stored on the planned rows in `wec_meta`. They are not proof that the physical tables or fields exist.

### `wec_pack_waves`

```text
wave
show
included_weeks
wave_type
horse_count
groom_count_mode
groom_count_manual
groom_ratio
groom_count_final
active
sort_order
notes
rec_id
table_name
table_api
```

### `wec_packing_events`

```text
event
show
pack_wave
packing_item
packing_item_horse
horse
event_type
quantity_delta
quantity_before
quantity_after
pack_state_before
pack_state_after
decision_before
decision_after
notes
created_at
created_by
rec_id
table_name
table_api
```

## Applied `wec_meta` Updates

The following live Airtable updates were applied to `wec_meta`.

### `wec_pack_waves`

```text
const_env: true
AIRTABLE__TABLE: AIRTABLE_WEC_PACK_WAVES_TABLE
AIRTABLE__VIEW:  AIRTABLE_WEC_PACK_WAVES_VIEW
```

### `wec_packing_events`

```text
const_env: true
AIRTABLE__TABLE: AIRTABLE_WEC_PACKING_EVENTS_TABLE
AIRTABLE__VIEW:  AIRTABLE_WEC_PACKING_EVENTS_VIEW
```

## Next Concrete Action

Proceed to Gate 2: create or confirm the physical Airtable tables.

Minimum next Airtable work:

1. Create physical table `wec_pack_waves`.
2. Create physical table `wec_packing_events`.
3. Add fields matching `fields_allowed`.
4. Add the resulting `table_api` values back to `wec_meta`.
5. Decide whether `webflow_slug` belongs to this app path.

After that, verify Gate 2 against Airtable metadata.
