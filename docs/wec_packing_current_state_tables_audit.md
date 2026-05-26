# WEC Packing Current-State Tables Audit

Generated from live Airtable metadata on 2026-05-26.

Live schema update applied on 2026-05-26.

Scope:

- `wec_packing_items`
- `wec_packing_item_horses`

Decision: both tables are needed.

Do not rename these tables. Do not replace them with new table names.

## Why These Tables Are Needed

The app needs three separate layers:

```text
wec_pack_lists + wec_pack_items
= source/template layer

wec_packing_items + wec_packing_item_horses
= current worksheet state / frozen snapshot layer

wec_packing_events
= history/audit layer
```

`wec_packing_events` cannot replace current-state rows because the mobile app needs fast current answers:

- what is needed
- what is packed
- what is left
- what is satisfied by decision
- which horse-specific members are packed

The event table records how state changed. It should not be the only place state exists.

## Current Physical Tables

Confirmed by Airtable metadata:

```text
wec_packing_items:       tblq1SARyF9aTCpO2
wec_packing_item_horses: tbldmrXz8zCHHonYu
wec_pack_waves:          tblWUBY7vpNIKaby8
wec_packing_events:      tblEPLwEYRClYFnar
```

`wec_packing_events` already has reverse links on both current-state tables.

## Applied Schema Spread

`wec_packing_items` now includes:

```text
pack_wave
show
source_pack_item
pack_list
quantity_needed
quantity_left
list_plan
```

`wec_packing_item_horses` now includes:

```text
quantity_needed
quantity_packed
sort_order
pack_wave
source_pack_item
```

Single select choices are populated:

```text
wec_packing_items.list_plan:
quantity, per_horse, horse_specific, per_groom

wec_packing_items.record_state:
active, inactive, ignore

wec_packing_items.resolution_state:
max, kill, note, purchase_onsite, unresolved

wec_packing_item_horses.horse_pack_state:
not_packed, packed
```

`wec_meta` registry is filled for both tables:

```text
AIRTABLE_WEC_PACKING_ITEMS_TABLE
AIRTABLE_WEC_PACKING_ITEMS_VIEW
AIRTABLE_WEC_PACKING_ITEM_HORSES_TABLE
AIRTABLE_WEC_PACKING_ITEM_HORSES_VIEW
```

## `wec_packing_items`

Role:

One frozen worksheet item row for a specific packing wave.

Current fields:

| field | type | audit |
| --- | --- | --- |
| `item_name` | single line text | keep |
| `item_id` | single line text | keep as snapshot/source key |
| `section` | single select | review choices; current choices are broad prototype sections |
| `category` | single line text | keep |
| `location` | single line text | keep |
| `quantity_base` | number | clarify or replace with `quantity_needed` |
| `quantity_mode` | single select | revise; current choices are old prototype choices |
| `unit` | single line text | keep |
| `quantity_packed` | number | keep |
| `pack_state` | single select | keep |
| `resolution_state` | single select | expand choices |
| `record_state` | single select | keep |
| `ignore` | checkbox | keep |
| `notes` | long text | keep |
| `sort_order` | number | keep |
| `wec_packing_item_horses` | linked records | keep reverse link |
| `rec_id` | formula | keep |
| `table_name` | single line text | keep |
| `table_api` | single line text | keep |
| `wec_packing_events` | linked records | keep reverse link |

Required additions or revisions:

| field | type | reason |
| --- | --- | --- |
| `pack_wave` | linked records to `wec_pack_waves` | every snapshot row must belong to a wave |
| `show` | linked records to `wec_shows` | show scope |
| `source_pack_item` | linked records to `wec_pack_items` | source/template traceability |
| `pack_list` | linked records to `wec_pack_lists` | preserves list organization from source |
| `quantity_needed` | number | explicit frozen needed quantity |
| `quantity_left` | formula or number | app display value; formula preferred if Airtable can support it cleanly |
| `list_plan` | single select | should carry `quantity`, `per_horse`, `horse_specific`, `per_groom` |

`quantity_mode` should not keep the old prototype choices as the main live rule.

Current choices:

```text
base_only
base_plus_horses
```

Live choices needed:

```text
quantity
per_horse
horse_specific
per_groom
```

`resolution_state` currently has:

```text
max
kill
note
```

Live choices needed:

```text
max
kill
note
purchase_onsite
unresolved
```

## `wec_packing_item_horses`

Role:

One frozen horse-member row for a horse-specific worksheet item.

Current fields:

| field | type | audit |
| --- | --- | --- |
| `Id` | autonumber | keep |
| `horse` | linked records to `wec_horses` | keep |
| `horse_pack_state` | single select | keep |
| `notes` | long text | keep |
| `packing_item` | linked records to `wec_packing_items` | keep |
| `item_horse_key` | formula | keep if it keys item + horse |
| `rec_id` | formula | keep |
| `table_name` | single line text | keep |
| `table_api` | single line text | keep |
| `wec_packing_events` | linked records | keep reverse link |

Required additions or revisions:

| field | type | reason |
| --- | --- | --- |
| `quantity_needed` | number | frozen per-horse needed quantity |
| `quantity_packed` | number | optional, supports quantities above 1 per horse |
| `sort_order` | number | stable horse-member ordering |

Optional additions:

| field | type | reason |
| --- | --- | --- |
| `pack_wave` | linked records to `wec_pack_waves` | redundant through parent item, but useful for filtering |
| `source_pack_item` | linked records to `wec_pack_items` | redundant through parent item, but useful for auditing |

Do not add horses from this table in the UI. Rows should be generated from source item membership plus wave-scoped active horses.

## Event Links

Correct event links:

```text
wec_packing_events.packing_item
-> wec_packing_items

wec_packing_events.packing_item_horse
-> wec_packing_item_horses

wec_packing_events.horse
-> wec_horses

wec_packing_events.pack_wave
-> wec_pack_waves
```

This is not a rename and not a new model. It preserves the existing table names and clarifies their roles.

## Current Data Status

Current sample counts:

```text
wec_packing_items:       1 row
wec_packing_item_horses: 1 row
```

The existing rows look like test/placeholder rows and should not be treated as generated worksheet data.

## Next Airtable Action

Treat current rows as test rows unless manually confirmed.

The next implementation step is snapshot generation:

1. Build `wec_packing_items` rows from `wec_pack_items` for a selected `wec_pack_wave`.
2. Build `wec_packing_item_horses` rows only for active horses that are members of a horse-specific item.
3. Write progress changes to current-state rows and append matching `wec_packing_events` rows.
