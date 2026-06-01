# WEC Packing Live App Project Contract

This document defines the project shape for rebuilding the WEC Ocala packing worksheet as a live RingStatus/Webflow app. The existing local prototype proves the approved interaction cadence. The live app should preserve that cadence while replacing the local/fake data layer with Airtable-backed Webflow Cloud reads and writes.

## Core Goal

Build a mobile-first packing progress app for a horse show.

The app tracks each packing item through:

- needed quantity
- packed quantity
- left quantity
- packed/not packed status
- decision states
- horse-specific packing status
- saved progress over multiple days
- frozen quantities that can be reused for Week 4 pack-up/go-home tracking

This is not a new UI exploration. It is a data architecture and live persistence rebuild using the already approved prototype behavior.

## Approved UI Cadence

The current prototype cadence is approved and must not drift without explicit review.

Preserve:

- mobile-first section lists
- overview progress summaries
- section list rows
- row tap into detail modal
- tight row/table layout
- worksheet controls
- quantity add behavior
- packed/not packed behavior
- decision pill behavior
- horse member packed/not packed behavior
- same button/tap rhythm
- same global CSS/template language

Allowed changes:

- label corrections
- minor global spacing/style refinements
- data-backed text values
- small adjustments required by the live data model

Do not introduce a new frontend framework, carousel pattern, card-heavy UI, different modal cadence, or new interaction model unless explicitly approved.

## Architecture

Use the newer RingStatus method:

```text
Webflow page embed
  -> pinned public CSS/JS assets
  -> Webflow Cloud/Astro API route
  -> Airtable API
  -> Airtable current-state records
  -> Airtable history/event records
  -> response back to browser UI
```

Local storage may exist only as a temporary draft/offline fallback. It is not the source of truth.

Do not expose Airtable tokens in the browser.

## Repo Location

Project documentation:

```text
docs/wec_packing_live_app_project_contract.md
```

Static frontend assets should live under:

```text
webflow/packing-worksheet/
```

Expected future files:

```text
webflow/packing-worksheet/wec-packing.css
webflow/packing-worksheet/wec-packing.js
webflow/packing-worksheet/wec-packing-webflow-embed.html
```

Server/API work should follow the existing Webflow Cloud app pattern in:

```text
webflow-cloud-test/
```

Final route names are still to be confirmed before implementation.

## Airtable Source Tables

The live base already has a WEC table registry in `wec_meta`.

Use `wec_meta` as the authoritative WEC table index before implementation. It is available as:

```text
table name: wec_meta
table api:  tbllJywsOstkqT5yZ
```

Do not hardcode table assumptions from this document alone. The app/API setup should first read `wec_meta`, confirm each required table name/API id, and then inspect the actual table schema through the Airtable metadata API.

`wec_meta` can include planning records before the physical Airtable table exists. A record in `wec_meta` is the intended contract; it is not by itself proof that the table has already been created.

Important `wec_meta` fields:

- `table_name`: intended Airtable table name
- `table_api`: physical Airtable table id when the table exists
- `const_env`: marks rows that should become Webflow Cloud environment constants
- `AIRTABLE__TABLE`: proposed environment key for the table id
- `AIRTABLE__VIEW`: proposed environment key or view name for the view used by the app
- `fields_allowed`: minimum field allowlist to read/write for this app
- `ignore`: exclude this registry row from active implementation

Use `fields_allowed` to define the minimum required API surface, especially for new tables such as `wec_pack_waves` and `wec_packing_events`.

`wec_meta` should be updated when new WEC app tables are planned or added, including `wec_pack_waves` and `wec_packing_events`.

Core source/template layer:

- `wec_pack_lists`
- `wec_pack_items`
- `wec_list_plans`

Show/scope layer:

- `wec_shows`
- `wec_weeks`
- future or rebuilt `wec_pack_waves`

Horse layer:

- `wec_horses`

Current worksheet/progress layer:

- `wec_packing_items`
- `wec_packing_item_horses`

Support/local guide layer:

- `wec_vendors`
- `wec_local_tags`
- `wec_place_type`

History layer:

- add or rebuild a dedicated packing events table, proposed as `wec_packing_events`

## Source List Logic

`wec_pack_lists` organizes `wec_pack_items`.

Use the `master` view on `wec_pack_items` as the source set for worksheet generation unless explicitly changed.

`wec_list_plans` is the preferred rule source for how a source item becomes a worksheet item.

`wec_pack_items.wec_list_plans` should be used first. `wec_pack_items.list_plan` is a legacy/cache fallback only, because the linked `wec_list_plans` table can store plan logic over time.

Known plan values:

- `quantity`
- `per_horse`
- `horse_specific`
- `per_groom`

Blank plan means planning is incomplete. The app should not guess behavior for blank-plan rows.

`purchase_onsite` and `unresolved` should be treated as decision/resolution states in the live worksheet model, not as primary quantity-generation plans.

## List Plan Semantics

`quantity`

Fixed quantity from the source item.

```text
needed = wec_pack_items.quantity
```

`per_horse`

Count math only. Every scoped horse contributes to the quantity. The worksheet does not track named horse packed states for this plan.

```text
needed = wec_pack_items.per_horse * current_going_horse_count
```

`horse_specific`

Dynamic horse-kit rollup. Only current going horses linked to the generic source item contribute to the current checklist. This is not unit quantity math.

```text
expected_horse_kits =
  wec_horses
  filtered by current wave flags
  filtered by not_going != true
  filtered by linked wec_pack_items containing the source item

needed = expected_horse_kits.count
packed = expected_horse_kits where horse_pack_state = packed
left = needed - packed
```

Each horse/source item pair is packed or not packed. The source item label may include quantity detail such as `Bridle (2)`, but the app does not track partial quantity inside the horse-specific row.

`wec_packing_item_horses` stores touched packed/not-packed state for horse kits. Missing rows mean not packed. The app must not bulk-generate every expected row unless that is separately approved.

`per_groom`

Count math only. Grooms should be treated as operational capacity counts, not named people, unless a later approved use case requires named groom assignments.

```text
needed = wec_pack_items.per_groom * pack_wave.groom_count_final
```

The app must show this calculation on the item detail view. The API payload should expose the calculation trace, not just the frozen result:

```json
{
  "quantityCalculation": {
    "plan": "per_groom",
    "formula": "per_groom * groom_count_final",
    "sourceField": "wec_pack_items.per_groom",
    "multiplierField": "wec_pack_waves.groom_count_final",
    "base": 5,
    "multiplier": 5,
    "calculatedNeeded": 25,
    "frozenNeeded": 25,
    "matchesFrozen": true
  }
}
```

## Weeks, Waves, And Scope

`wec_weeks` should represent show/calendar attendance.

Example:

- Week 1
- Week 2
- Week 3
- Week 4

Horse attendance belongs on `wec_horses` through linked weeks.

The packing app needs a separate operational layer:

```text
wec_pack_waves
```

Examples:

- Week 1 Truck
- Week 3 Truck
- Week 4 Pack-Up

`wec_ranges` is not part of the active packing model. `wec_pack_waves` is the packing unit and carries the week/truck grouping needed for outbound, mid-show, and return operations.

`wec_pack_waves` should set or calculate:

- show
- included weeks
- wave type: `outbound`, `mid_show`, `return`
- active horse count
- groom count mode
- groom count manual
- groom ratio
- groom count final
- sort order
- active state
- manual lock

Packing math should be generated from the selected pack wave.

When `wec_pack_waves.manual_lock` is unchecked, the app uses current horse wave flags for effective wave counts. When checked, the app may use the stored wave counts as the locked snapshot. Stale `horse_count` must not drive live behavior for unlocked waves.

## Horse Usage

`wec_horses` is the roster and attendance source.

Use it for:

- horse identity
- barn/show name
- active/inactive state
- linked show/weeks
- linked source `wec_pack_items` for horse-specific requirements
- sorting/display

Do not store packing progress directly on `wec_horses`.

For `per_horse` rows, use horses only for count math.

For `horse_specific` rows, use horses as dynamic current checklist inputs. `wec_horses.wec_wave_1`, `wec_horses.wec_wave_2`, and `wec_horses.wec_not_going` are the live wave filters.

Create or update `wec_packing_item_horses` only when a user changes a horse kit packed state. Missing rows are treated as not packed.

Do not delete stale horse-kit rows. If a horse is removed from a wave, current rollups exclude that horse's kits, but existing state rows and event history remain available for audit.

## Groom Usage

Do not make packing quantities depend on named grooms by default.

Use a wave-level groom count instead.

Recommended logic:

```text
if groom_count_manual is set:
  groom_count_final = groom_count_manual
else:
  groom_count_final = ceil(current_going_horse_count / groom_ratio)
```

This supports operational needs like packing for 5 grooms without needing to know whether Jose, Jimmy, or another groom is available, driving, or rotating weeks.

## Worksheet Snapshot Layer

`wec_packing_items` should store frozen worksheet rows for a specific packing wave.

The row should not be a live formula-only mirror of `wec_pack_items`. Once generated, it should preserve what the app used at the time.

Recommended fields/meaning:

- source pack item link
- pack wave link
- show link
- pack list link
- item name
- location
- unit
- list plan copied from source
- needed quantity
- packed quantity
- left quantity
- pack state
- decision state
- record state
- notes
- sort order

This prevents Week 1 progress from silently changing if horse attendance or source item setup changes later.

## Horse Worksheet Snapshot Layer

`wec_packing_item_horses` should store frozen horse-specific members for a worksheet item.

Use this primarily for `horse_specific` source rows.

Recommended fields/meaning:

- packing item link
- source pack item link
- pack wave link
- horse link
- needed quantity
- packed quantity
- horse pack state
- notes

If Blue was part of the Week 1 saddle row, that membership remains part of the Week 1 worksheet snapshot.

## History/Event Layer

Current state rows are not enough for multi-day packing. Add or rebuild a dedicated event table.

Proposed table:

```text
wec_packing_events
```

Purpose:

- record every meaningful worksheet action
- preserve quantity changes over time
- support trust/recovery
- support Week 4 pack-up review

Recommended fields:

- event id
- show link
- pack wave link
- packing item link
- packing item horse link, optional
- horse link, optional
- event type
- quantity delta
- quantity before
- quantity after
- pack state before
- pack state after
- decision before
- decision after
- notes
- created at
- created by, optional

Recommended `event_type` values:

- `quantity_add`
- `quantity_clear`
- `mark_packed`
- `mark_not_packed`
- `decision_max`
- `decision_kill`
- `decision_note`
- `decision_purchase_onsite`
- `decision_unresolved`
- `decision_clear`

The app should write both:

1. update the current state row
2. append an event row

## Decision States

Decision states satisfy a row without normal full packing.

Use decision states for:

- `max`
- `kill`
- `note`
- `purchase_onsite`
- `unresolved`

Decision behavior must be confirmed before applying. This follows the approved prototype cadence.

Decision state should update the current worksheet row and append a history event.

## Packed Behavior

Quantity items:

- Add quantity increments packed quantity.
- Input clears after add.
- Left reduces as packed quantity increases.
- When left reaches zero, the item becomes packed.
- Manual packed action must confirm and then set packed quantity to full needed quantity.

Horse-specific items:

- User cannot add horses from item detail.
- Active/scoped horse members are generated from source data and wave scope.
- User can only mark the generated horse member packed/not packed.
- Horse member packed states roll up to the parent item.

## Week 4 Pack-Up

Week 4 Pack-Up should not be generated blindly from the original source template alone.

It should be able to use frozen outbound worksheet snapshots and event history to answer:

- what was actually packed
- what was purchased onsite
- what was skipped/killed
- what was accepted as partial/max
- which horse-specific items were packed
- what needs to come home

This is the core reason to snapshot quantities and store action history.

## Reconcile / Rebalance

Source changes after snapshot generation must not silently rewrite packed work.

Examples:

- a horse cancels
- a horse moves from one wave to another
- a temporary/test horse is deleted
- a wave horse count changes
- a groom count changes

The app should run a dry-run reconcile before any sweeping change. The reconcile report should classify:

- orphan horse-member rows with no current horse link or barn name
- stale horse-member rows no longer expected for the selected wave/source item
- missing horse-member rows now expected for manually locked horses
- item quantity mismatches caused by changed wave/source counts
- blocked rows that have packed quantity or event history

Rows with no packed quantity, no event history, and no locked horse can be proposed for removal from the current wave. Rows with packed quantity, event history, or a locked horse must be preserved and reviewed manually.

## Vendor And Local Guide Usage

`wec_vendors` is shared support data.

Inside packing:

- vendors matter when decision state is `purchase_onsite`
- a packing row may reference a vendor if known

As standalone local guide:

- `wec_vendors`
- `wec_local_tags`
- `wec_place_type`

These can support a separate list for local tack, coffee, dining, pharmacy, hardware, feed, and related places.

Do not let the local guide clutter the packing workflow.

## Webflow Embed Shape

The final embed should follow the proven RingStatus static asset + Webflow Cloud API pattern.

Draft shape:

```html
<div id="wec-packing-app">Loading packing worksheet...</div>

<script>
  window.WEC_PACKING_CONFIG = {
    mode: "edit",
    showId: "REPLACE_WITH_SHOW_RECORD_ID",
    packWaveId: "REPLACE_WITH_PACK_WAVE_RECORD_ID",
    apiBaseUrl: "https://ringstatus.webflow.io/test/wec-packing"
  };
</script>

<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@REPLACE_WITH_COMMIT/webflow/packing-worksheet/wec-packing.css">
<script src="https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@REPLACE_WITH_COMMIT/webflow/packing-worksheet/wec-packing.js"></script>
```

Use pinned commits for stable Webflow embeds once deployed.

## API Route Draft

Final route names are not locked yet, but the app likely needs:

```text
GET  /test/wec-packing/state?showId=...&packWaveId=...
POST /test/wec-packing/item-action
POST /test/wec-packing/horse-action
POST /test/wec-packing/generate-wave
GET  /test/wec-packing/health
```

Server responsibilities:

- read Airtable tables
- calculate/scaffold wave worksheet rows
- apply item actions
- apply horse actions
- append event history
- return normalized UI state
- protect Airtable credentials

Do not write directly from browser to Airtable.

## Implementation Checks

Use `wec_meta` as the first implementation check. The registry is the current WEC system index and should drive table discovery instead of relying on stale file notes.

WEC is a privately shared URL app. Do not add tenant gates, login gates, role gates, or active-tenant validation unless explicitly requested later. The table/view settings only define which Airtable records are visible to this worksheet and which records are eligible for writes.

Before code implementation:

1. Read `wec_meta` by table name `wec_meta` or table API id `tbllJywsOstkqT5yZ`.
2. Export or log the active `core_data` WEC table list from `wec_meta`, excluding rows where `ignore` is checked.
3. For each active row, classify it as:
   - registered and physical: `table_api` exists and metadata API confirms it
   - planned: `table_name` exists in `wec_meta` but `table_api` is blank or metadata API does not yet confirm it
   - ignored/supporting: not part of the live implementation path
4. Generate the Webflow Cloud environment-key checklist from rows where `const_env` is checked, using `AIRTABLE__TABLE` and `AIRTABLE__VIEW`.
5. Verify each required physical table from `wec_meta` against the Airtable metadata API.
6. Verify each required physical table has the minimum fields listed in `fields_allowed`.
7. Confirm which existing registered tables are part of the live app:
   - `wec_shows`
   - `wec_weeks`
   - `wec_horses`
   - `wec_pack_lists`
   - `wec_pack_items`
   - `wec_list_plans`
   - `wec_packing_items`
   - `wec_packing_item_horses`
   - `wec_vendors`
   - `wec_local_tags`
   - `wec_place_type`
8. Confirm the planned `wec_pack_waves` record in `wec_meta`, including `fields_allowed`, then create the table if approved.
9. Confirm the final `wec_packing_items` fields for frozen worksheet snapshots.
10. Confirm the final `wec_packing_item_horses` fields for frozen horse-item snapshots.
11. Confirm the planned `wec_packing_events` record in `wec_meta`, including `fields_allowed`, then create the table if approved.
12. Confirm `resolution_state` options, including `max`, `kill`, `note`, `purchase_onsite`, and `unresolved`.
14. Confirm Webflow Cloud route names.
15. Confirm whether current prototype files should be archived, replaced, or kept as reference only.

Before live writes:

- verify current Airtable schema through `wec_meta` plus Airtable metadata API
- verify every planned table being used now has a physical `table_api`
- verify every Webflow Cloud env key derived from `AIRTABLE__TABLE` and `AIRTABLE__VIEW` is set
- verify each write route only accepts fields present in `fields_allowed`
- verify current-state writes and event-history writes target the intended table API ids
- verify route environment variables
- verify browser is loading the intended pinned assets
- verify no Airtable token is exposed client-side
- get explicit approval for PATCH/POST behavior

## Non-Goals

Do not build:

- a new visual system
- named groom scheduling
- full user/role auth
- a generic task manager
- a replacement for Airtable
- a separate horse management product
- a new local-only prototype

The goal is a focused WEC packing app with saved progress, frozen wave snapshots, and action history.
