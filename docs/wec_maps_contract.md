# WEC Maps Contract

Current date: 2026-06-01

Validated against:

- live Airtable table `tblbtNdhOgap9gMat`
- `webflow-cloud-test/src/lib/wec-packing.js`
- `webflow/packing-worksheet/wec-packing.js`

`wec_maps` is the human-readable mapping table for WEC packing labels, source fields, source tables, user-input destinations, and display examples.

It is not currently a runtime dependency. The WEC app does not read `wec_maps` to render the dashboard. Runtime behavior is still implemented in:

- `webflow-cloud-test/src/lib/wec-packing.js`
- `webflow/packing-worksheet/wec-packing.js`

## Airtable Table

- Table name: `wec_maps`
- Table API id: `tblbtNdhOgap9gMat`

Known fields:

- `UI field / element`
- `Source table`
- `Source field / logic`
- `Label`
- `Display example`
- `Notes`

Current live row count checked on 2026-06-01: 41 records.

## Purpose

Use `wec_maps` to document what each visible UI label or user action is supposed to mean.

Examples:

- `rsa-report-title` maps to the wave report title display contract.
- `rs-tab-link` labels map to `wec_pack_lists` tabs/list labels.
- row titles map to source/current item display fields.
- comments map to `wec_commenting`.
- packing actions map to ledger/event destinations.

## Current Runtime Boundaries

`wec_pack_items`

Source catalog for packing items. This is where dynamic item rules live, including:

- `app_name`
- `item_display_name`
- `item_display_name_per_horse`
- `quantity`
- `per_horse`
- `per_groom`
- `list_plan`
- `wec_pack_lists`

Current rule: visible packing rows can render directly from `wec_pack_items`.

`wec_packing_items`

Ledger overlay for packing progress. The app must not require existing `wec_packing_items` rows to show the list.

Current repo rule: if old `wec_packing_items` records are deleted, the app still renders the list from `wec_pack_items`. Quantity, pack-state, decision, and inline item edits append ledger rows through `createPackingLedgerEntry()` and `buildPackingLedgerState()` overlays those rows onto source items.

`wec_packing_events`

Audit/event log for packing actions. Use for action history such as:

- quantity add
- packed/not packed
- clear/max/buy/attention decisions
- onsite task state
- horse-kit state log
- session/count events

Current repo rule: event rows are created by `createPackingEvent()`. The event table is audit history, not the visible item source.

`wec_commenting`

Comment table. Comments are scoped to:

- item
- section
- tab
- wave

Current repo rule: comment add uses `createWecComment()` when `wec_commenting` is configured. Comment edits update only the comment record.

`wec_horses`

Horse roster and wave membership source. Wave One membership is:

- `wec_wave_1` checked
- `wec_not_going` not checked

Horse search should include both `barn_name` and `show_name`.

`wec_pack_waves`

Wave count and dynamic multiplier source. Current Wave One rules:

- use `count_horses_wave_one` for horse count
- use `groom_sanity` for groom count
- do not use stale `horse_count` or `horse_sanity` unless a future manual-lock rule is explicitly approved

`wec_places`

Locale/place source table. Locale rows and place detail modals should render from this source and related tags.

`wec_packing_item_horses`

Do not use this table as the active granular source for horse-kit progress. The current simpler plan avoids per-horse x item record generation. Horse-kit taps should log through the action/event path rather than requiring this table to be populated.

Current repo rule: source-backed horse-kit taps handled by `applyHorseKitState()` create an event row and do not create a `wec_packing_item_horses` record.

## Current `wec_maps` Row Groups

The existing `wec_maps` rows cover these groups:

- Page labels: report title, subtitle, top-level tabs, section-level tabs, section headings, row titles.
- Metrics: `NEED`, `PACKED`, `LEFT`, horse counts, groom count.
- User input paths: add quantity, packed state, clear, max, buy, attention, inline edits, source flags.
- Comments: add/edit comment scoped by item, section, tab, or wave.
- Locale: place rows, place filter links, place modal fields.
- Horse lists: wave filters, active state, horse-kit display/state.

## Rules For Future Changes

1. Updating `wec_maps` alone does not change runtime behavior.
2. If a row describes runtime behavior, the matching code path must also be checked.
3. `wec_maps` should not be used as a write destination.
4. Every user-input row must name the actual destination table.
5. Any schema change must update both the Airtable row and this contract.
6. Keep HPS/LPS/LP out of this contract. This is WEC packing only.

## Known Drift To Watch

Some older `wec_maps` rows still say that actions patch `wec_packing_items` as a current-state worksheet row. That is stale wording for the current repo code. The current repo code is source-backed rows plus `wec_packing_items` ledger overlay. When editing `wec_maps`, update those row notes to match the ledger model.
