# Barn Entry AG Review Functionality Reference

Date: `2026-07-08`
Status: `FUNCTIONALITY REFERENCE ONLY`
Source: `prototypes/horseshowing/barn-entry-ag-review-prototype.html`
Published surface: `https://ringstatus.com/barn-entry`

This file documents the behavior in the current barn-entry AG review form so future AG outputs can reference the working flow without copying its styling.

This is not a base stylesheet, skin, button, pill, row-style, or layout authority.

## Functional Scope

The prototype covers one review workflow:

```text
load current show/focus data
load mapped class OOG entry rows
normalize rows into review rows
display rows in AG Grid
enter edit mode
tap row status
add one horse/class row through picker
submit review payload
show submitted review view
print/share review output
```

## URL Inputs

The form reads these query parameters from `location.search`:

| Param | Purpose | Default |
|---|---|---|
| `show_no` | Show identifier used for API reads. | `14909` |
| `focus_day` | Active date used for schedule and mapped rows. | `2026-07-05` |
| `trainer` | Optional trainer filter for mapped rows. | empty |

The page must not hardcode these defaults in future production outputs. Defaults are prototype fallback behavior only.

## API Inputs

The prototype uses one Catalyst sync API base:

```text
https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/
```

Functional reads:

| Function | Action | Purpose |
|---|---|---|
| `loadSchedule()` | `wec-mobile-live` | Loads focus schedule, rings, and class rows. |
| `fetchClassOogRows()` | `export-mirror-table&table=class_oog` | Loads mapped class OOG entry rows for the show/focus day. |
| `fetchHorseHelpers()` | `wec-helper-search&type=horses` | Loads helper horse candidates for add-entry search. |

The production AG system should keep this as a behavior reference only. Endpoint ownership remains separate from AG rendering.

## Load Flow

Startup sequence:

```text
ensure AG Grid script is available
create AG Grid
show loading overlay
load schedule
set focus date text
load class_oog mirror rows
filter mapped rows by trainer when trainer param exists
map source rows into review rows
render AG rows
load helper horses asynchronously
```

The helper horse load is non-blocking. If helper search fails, the grid still renders mapped rows.

## Review Row Contract

Mapped source rows are normalized by `reviewRowFromEntry(row)`.

Review row fields:

| Field | Purpose |
|---|---|
| `review_key` | AG row ID and status-toggle identity. |
| `source` | `mapped` for loaded rows, `user_added` for add-entry rows. |
| `status` | Review state: `pending`, `confirmed`, or `declined`. |
| `show_no` | Show identifier. |
| `focus_day` | Focus date. |
| `focus_show_record_id` | Source focus show record reference when available. |
| `ring_day_no` | Ring-day identity. |
| `ring_no` | Ring identity. |
| `ring_name_normalized` | Ring display/source field. |
| `class_no` | Class identity from source. |
| `class_number` | User-facing class number when available. |
| `class_name` | User-facing class name. |
| `class_start_time` | Source class start time. |
| `display_time` | Display time used in grid/review/print. |
| `entry_no` | Entry number for mapped rows; blank for pencil/add rows. |
| `barn_name` | Horse display name used by barn-entry output. |
| `horse_name` | Source horse/show name fallback. |
| `horse_key` | Helper/source horse key when available. |
| `matched_source_row` | Source row record id when available. |

`barn_name` is the display field expected by barn-entry. Future barn-entry outputs should not render rider names as horse pills.

## Row Identity

Existing rows use `review_key`, `entry_const_key`, `entry_go_key`, or a fallback key built from:

```text
show_no | focus_day | class_no/class_number | entry_no | horse display
```

Add-entry rows use a user-added key:

```text
user | Date.now() | class_no/class_number | normalized horse barn_name
```

The fallback behavior is prototype behavior. Production endpoints should supply stable row keys.

## AG Grid Behavior

The grid is initialized with:

| Setting | Behavior |
|---|---|
| `getRowId` | Uses `review_key`. |
| `defaultColDef.sortable` | `false`. |
| `defaultColDef.filter` | `false`. |
| `defaultColDef.resizable` | `true`. |
| `defaultColDef.editable` | `false`. |
| `rowSelection` | Single row. |
| `animateRows` | `false`. |
| `suppressCellFocus` | `true`. |

The prototype uses AG for display and row identity, not full-row editing.

## Responsive Column Behavior

The prototype switches columns at `max-width: 478px`.

Desktop columns:

```text
Tap | Time | Ring | No | Class | Horse
```

Compact columns:

```text
Tap | Time/Ring/No/Class/Horse combined cell
```

The compact renderer shows two lines:

```text
display_time | RING | class_number/class_no      entry_no
class_name                                      barn_name
```

This split is a functional reference for barn-entry mobile behavior, not a visual style authority.

## Status Toggle

Status values:

```text
pending -> confirmed -> declined -> pending
```

The status cycle is applied by `toggleStatus(review_key)`.

Short labels in edit mode:

| Status | Short Label |
|---|---|
| `pending` | `tap` |
| `confirmed` | `ok` |
| `declined` | `no` |

Long labels in review/print:

| Status | Long Label |
|---|---|
| `pending` | `pending` |
| `confirmed` | `confirmed` |
| `declined` | `declined` |

Future AG outputs must use the approved global tap/button primitive for the visual control. This file only records the state machine.

## Edit Mode

`toggleEditMode()` controls the review form edit state.

Default mode:

```text
edit button visible
print button visible
add button hidden
submit button hidden
status/tap column hidden
```

Edit mode:

```text
edit button label changes to Done
add button visible
submit button visible
status/tap column visible
shell mode changes to edit
```

Leaving edit mode resets add-entry draft state and hides the add panel.

## Add Entry Flow

The add flow uses a flyup panel with two pickers:

```text
horse picker
class picker
save button
```

Class search:

```text
query parts must all match classSearchText(row)
search text includes compact time, display time, ring, class_no, class_number, class_name
suggestions show class name only
selection stores the full class row
```

Horse search:

```text
query matches horseSearchText(row)
search text includes barn_name, show_name, horse_display, horse_name, horse, horse_aka, aka
helper rows are deduped by normalized horse display name
selection stores barn_name, horse_name, horse_key
```

Saving requires both selected class and selected horse.

Saved add row:

```text
source = user_added
status = pending
entry_no = blank
pencil_in = helper horse pencil flag when available
```

The new row is added to `state.rows`, deduped, sorted, and rendered.

## Submit Flow

Submit target in the prototype:

```text
https://ringstatus.com/test/barn-entry
```

Request:

```text
POST
Content-Type: application/json
```

Payload shape:

```json
{
  "source": "barn_entry_ag_review",
  "submitted_at": "ISO timestamp",
  "row_count": 0,
  "rows": []
}
```

On success:

```text
render submitted review view
hide form panel
hide add panel
hide grid panel
hide action container
switch shell mode to print/submitted
scroll review panel into view
```

The batch `rows` payload is documented as prototype behavior. Future production submit contracts must be owned by the endpoint/API contract.

## Review View

After submit, the form renders a non-edit review table.

Desktop review columns:

```text
Status | Time | Ring | No | Class | Horse
```

Compact review columns:

```text
Status | Time/Ring/No/Class/Horse combined cell
```

The review view is intended to look like a submitted/read-only form, not a new workflow.

## Print Behavior

`buildPrintSheet()` creates a dedicated print DOM in `#printSheet`.

Print output includes:

```text
title: Barn Entry Review
show number
focus day
generated timestamp
table rows
```

Print table columns:

```text
Time | Ring | No | Class | Horse | Status
```

`printReview()` rebuilds the print sheet and then calls `window.print()`.

This confirms the functional pattern: build a print-specific sheet from current review state instead of relying on the live AG viewport.

Detailed AG print-layout rules are handled by:

```text
docs/horseshowing/ag-output-references/wec_ag_ring_group_print_function_only.js
```

## Share Behavior

`shareReview()` builds plain text from the current review rows.

Preferred behavior:

```text
navigator.share({ title, text })
```

Fallback:

```text
navigator.clipboard.writeText(text)
```

Text row format:

```text
display_time | ring | class_number/class_no | class_name | barn_name | status
```

## Documented Boundaries

This prototype may be referenced for:

| Area | Use |
|---|---|
| Load sequence | Yes |
| Row normalization | Yes |
| Edit-mode behavior | Yes |
| Status state machine | Yes |
| Add-entry picker flow | Yes |
| Submit/review transition | Yes |
| Print-sheet handoff | Yes |
| Share text shape | Yes |

This prototype must not be referenced for:

| Area | Use |
|---|---|
| Base AG stylesheet | No |
| Brand skin | No |
| Global button/pill primitive | No |
| Final row styling | No |
| Final shell layout | No |
| Endpoint truth | No |
| Final submit payload approval | No |

## Future AG Barn-Entry v2 Rule

Barn-entry v2 should preserve the workflow behavior:

```text
load mapped rows
review statuses
add horse/class row
submit payload
show submitted review
print/share
```

But it must use the approved AG base shell, approved AG button/tap primitive, approved skins, and endpoint-owned data contracts.
