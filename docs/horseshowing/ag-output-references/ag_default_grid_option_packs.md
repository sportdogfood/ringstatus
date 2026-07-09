# AG Default Grid Option Packs

Date: `2026-07-08`
Status: `WIP CONTRACT REFERENCE`

This document organizes the reusable AG option packs that plug into the default RingStatus AG grid.

The purpose is to build reports, inputs, forms, and mobile-pro surfaces from one base shell and one global control language instead of rebuilding buttons, pills, row heights, headers, and print behavior per output.

## Core Rule

Start with the base. Add named option packs. Do not restyle per pack.

```text
base shell
  + base grid options
  + one skin
  + one or more behavior option packs
  + one endpoint/data contract
  = consistent AG output
```

Every output must inherit the same global primitives:

| Primitive | Contract |
|---|---|
| Buttons | Use the approved `tap` button primitive. |
| Pills | Use the same `tap` primitive unless a skin explicitly approves a token-only variant. |
| Toggle buttons | Use `tap tap-toggle`; active is `tap-active`; second active state is `tap-active-2` only when explicitly selected. |
| Row height | Use the shared row/control height token. |
| Header height | Use the shared AG header height token. |
| Action bars | Use the same height/padding rhythm as grid rows. |
| Inputs | Use the approved input primitive; do not create one-off decorated input controls. |
| Sort/filter/menu icons | Hidden in base unless a skin explicitly enables them. |

No option pack may introduce its own button style, pill style, row height, header height, border radius, font weight, or special color system.

## Base Composition

The default output starts from this structure:

```text
app-shell
  app-head
    header-left
    header-right
      action-bar-mini
  action-bar
  action-anchors
  grid-frame
    ag-grid
  action-bar-bottom
  status-line
```

The body/grid area owns vertical scrolling. The shell and action areas stay stable.

## Option Pack Order

Packs should be applied in this order:

```text
1. base_grid_options
2. endpoint_data_options
3. column_schema_options
4. skin_options
5. behavior_options
6. print_options
7. submit/writeback_options, if needed
```

Later packs can add behavior. Later packs cannot override the global control primitive or create visual exceptions.

## Base Grid Options

`base_grid_options` is required for every AG output.

Owns:

| Area | Contract |
|---|---|
| AG theme | Uses approved AG theme wrapper and CSS variables. |
| Row identity | Requires `getRowId` from endpoint-provided key. |
| Default column definition | Shared sortable/filter/resizable/editable/suppress settings. |
| Row height | Uses shared row height token. |
| Header height | Uses shared header height token. |
| Loading state | Uses AG loading overlay plus status-line copy. |
| Empty state | Uses AG empty overlay plus status-line copy. |
| Touch behavior | Large enough row/control targets for mobile. |
| Column min widths | Every column declares `minWidth`; required columns cannot collapse below contract. |

Suggested base settings:

```js
const baseGridOptions = {
  theme: "ag-theme-quartz",
  rowSelection: "singleRow",
  animateRows: false,
  suppressCellFocus: true,
  getRowId: (params) => params.data.row_key,
  defaultColDef: {
    sortable: false,
    filter: false,
    resizable: true,
    editable: false,
    suppressHeaderMenuButton: true,
    suppressSizeToFit: true
  }
};
```

The actual key field can vary by output, but the endpoint must provide it. AG must not invent canonical identity.

## Endpoint Data Options

`endpoint_data_options` is required for live outputs and optional for static mock references.

Owns:

| Area | Contract |
|---|---|
| Fetch | Loads current endpoint payload. |
| Query params | Reads approved URL/input params such as `show_no`, `focus_day`, `trainer`, `ring`, or `view`. |
| Normalize | Maps payload rows into the grid row contract. |
| Freshness | Displays loaded/updated state when available. |
| Failure | Shows error state without trying alternate workflow paths. |

Does not own:

| Area | Reason |
|---|---|
| Workflow repair | Codex/AG is not the runner. |
| Data guessing | Endpoint owns truth. |
| One-off fallback APIs | Manual endpoints are diagnostic unless explicitly approved. |

## Column Schema Options

`column_schema_options` defines the columns for an output.

Owns:

| Area | Contract |
|---|---|
| Field list | Explicit fields and labels. |
| Min widths | Every column has a stated `minWidth`. |
| Responsive columns | Compact/mobile column alternatives. |
| Value formatters | Time/date/status text formatting. |
| Cell renderers | Only when plain value display is not enough. |

Required protected widths:

| Field | Desktop Min | Mobile Min |
|---|---:|---:|
| `time` | `88` | `78` |
| `ring` | `88` | `78` |
| `class_number` / `no` | `60` | `56` |
| `starts_in` / `in` | `60` | `56` |
| `status` | `60` | `60` |
| `class_name` | `170` | `170` |
| `barn_name` | `120` | `120` |

Column schema may define compact renderers, but compact renderers must still use the global row height and text rhythm.

## Skin Options

`skin_options` changes density and approved color treatment.

Owns:

| Area | Contract |
|---|---|
| Shell background | Default/edit/print/submitted mode background. |
| Header/background tokens | Approved shell and AG header colors. |
| Print colors | Black-and-white print-safe styling. |
| Density tokens | Approved row/header/control heights. |

Does not own:

| Area | Reason |
|---|---|
| New button classes | All buttons use global primitive. |
| New pill classes | Pills inherit global primitive. |
| New row accents | Base forbids row/cell accents except approved hover. |
| Random one-off colors | Skins use approved tokens only. |

## Filter Options

`filter_options` adds local filtering after current data has loaded.

Reference:

```text
docs/horseshowing/ag-output-references/wec_ag_ring_group_filter_function_only.js
```

Owns:

| Area | Contract |
|---|---|
| Search text | Local text search over approved row fields. |
| Field filters | Filter API or custom local predicates. |
| Result state | Updates visible rows and count. |
| Controls | Uses action bar, anchors, or approved input area. |

Style:

```text
Filter controls use the global input/tap primitive.
Filter chips use the global tap primitive.
No special filter buttons.
No visible AG filter icons in base.
```

## Hide Options

`hide_options` adds local hide/show behavior.

Reference:

```text
docs/horseshowing/ag-output-references/wec_ag_ring_group_hide_function_only.js
```

Owns:

| Area | Contract |
|---|---|
| Hidden row state | Tracks hidden rows locally. |
| Pending hidden state | Supports staging hide changes before save when needed. |
| Show hidden mode | Toggles whether hidden rows are visible. |
| Save/apply | Applies hide changes to the current view or approved persistence endpoint. |

Style:

```text
Hide toggle label is Hide / Show.
Hide uses tap tap-toggle.
Active hide/show state uses tap-active.
```

## Focus Options

`focus_options` adds a local focus/reduction view.

Reference:

```text
docs/horseshowing/ag-output-references/wec_ag_ring_group_focus_function_only.js
```

Owns:

| Area | Contract |
|---|---|
| Duplicate suppression | Collapses repeated class/time/ring variants where the output explicitly uses focus mode. |
| Focus hidden rows | Tracks rows hidden by focus calculation separately from user-hidden rows. |
| Toggle | Allows focused view on/off when approved by output. |

Style:

```text
Focus toggle uses the global tap toggle primitive.
No row accent is added by focus mode.
```

## Print Options

`print_options` builds a print-specific sheet from current row state.

Reference:

```text
docs/horseshowing/ag-output-references/wec_ag_ring_group_print_function_only.js
```

Owns:

| Area | Contract |
|---|---|
| Print DOM | Builds a dedicated print sheet, not the live AG viewport. |
| Page size | Letter portrait/landscape as selected by output. |
| Margins | Tight approved margins. |
| Columns | One, two, or three print columns when configured. |
| One-page intent | Uses compact print typography and row density. |
| Black-and-white | Print sheet is grayscale/black-and-white. |
| Hidden rows | Prints current approved visible state. |

Style:

```text
Print controls use the same Print tap button.
Print output does not use decorative colors.
Print output may use dense typography approved for print only.
```

## Form Review Options

`form_review_options` adds review/edit/submit behavior.

Reference:

```text
docs/horseshowing/ag-output-references/barn_entry_ag_review_functionality_reference.md
```

Owns:

| Area | Contract |
|---|---|
| Edit mode | Shows/hides edit-only controls and editable status/action column. |
| Status cycle | Cycles approved status values. |
| Add row | Adds an approved row from selected data. |
| Submit | Sends the approved payload to the approved endpoint. |
| Review view | Shows submitted/read-only review. |

Style:

```text
Status/tap controls use the same tap primitive.
Add/Save/Submit/Print buttons use the same tap primitive.
Edit mode can change shell background only through the active skin token.
```

## Picker Options

`picker_options` adds searchable selection controls.

Owns:

| Area | Contract |
|---|---|
| Search source | Local loaded rows or approved helper endpoint. |
| Search text | Approved searchable fields. |
| Suggestions | Approved suggestion rows. |
| Selection | Selected chip/value state. |
| Clear | Clears selected value and reopens search. |

Style:

```text
Search input uses the approved input primitive.
Selected chips use the same global tap/pill primitive.
Clear uses the same tap primitive.
Suggestions do not introduce a new button language.
```

## Detail Flyup / Drawer Options

`detail_options` adds detail panels for rows, rollups, or special full-width rows.

Owns:

| Area | Contract |
|---|---|
| Open target | Flyup, drawer-in, or approved detail panel. |
| Detail payload | Uses endpoint-provided detail data. |
| Empty state | Shows explicit missing-detail state or disables click. |
| Close behavior | Returns to same grid state. |

Style:

```text
Drawer/flyup actions use the same action-bar and tap primitive.
Detail rows use the same row/control height rhythm unless a skin approves dense detail rows.
```

## Special Full-Width Class-Related Row Option

`class_related_full_width_options` enables the special class/rollup row shape.

Reference contract:

```text
ag-full-width-anchor
  class-related-data
    rollup-line
    class-line
```

Owns:

| Area | Contract |
|---|---|
| Full-width row | Renders class summary plus entry rollups. |
| Rollup items | Displays endpoint-provided related entries. |
| Click targets | Opens detail only when underlying data exists. |

Boundary:

```text
This is not the default row.
Use only when explicitly selected for mobile-pro/detail outputs.
Do not use for barn-entry default review rows.
```

## Diff Options

`diff_options` displays changed values from endpoint-provided diff fields.

Owns:

| Area | Contract |
|---|---|
| Diff classes | Applies approved diff class when endpoint marks changed field/row. |
| Diff toggle | Shows/hides changed-only view when approved. |
| Prior/current display | Uses endpoint-provided prior/current values. |

Style:

```text
Diff toggle label is Diff.
Active state uses tap-active.
No Diff label is not used.
Diff colors must come from skin tokens.
```

## Writeback Options

`writeback_options` adds submit/save behavior for inputs or review grids.

Owns:

| Area | Contract |
|---|---|
| Payload shape | Uses approved endpoint contract. |
| Validation | Local basic validation before submit. |
| Submit state | Loading/success/error state. |
| Response handling | Updates local row state only from API response. |

Boundary:

```text
Browser never writes directly to Airtable.
Frontend posts to approved API route.
Workflow/cadence proof is not replaced by manual submits.
```

## Output Recipes

Use these recipes to keep outputs consistent.

| Output | Required Packs |
|---|---|
| Simple report/list | `base_grid_options + endpoint_data_options + column_schema_options + skin_options + filter_options + print_options` |
| Input grid | `base_grid_options + endpoint_data_options + column_schema_options + skin_options + picker_options + writeback_options` |
| Review form | `base_grid_options + endpoint_data_options + column_schema_options + skin_options + form_review_options + picker_options + writeback_options + print_options` |
| Mobile schedule | `base_grid_options + endpoint_data_options + column_schema_options + mobile skin + filter_options + print_options` |
| Mobile-pro detail | `base_grid_options + endpoint_data_options + column_schema_options + mobile-pro skin + filter_options + hide_options + focus_options + detail_options + class_related_full_width_options + print_options` |
| Barn-entry v2 | `base_grid_options + endpoint_data_options + column_schema_options + barn-entry skin + form_review_options + picker_options + writeback_options + print_options` |

## Default Grid Plug-In Shape

Every output should be describable with this shape:

```js
const outputConfig = {
  shell: "ag-base-shell",
  skin: "default",
  endpoint: {
    url: "/approved-endpoint",
    params: ["show_no", "focus_day"]
  },
  rowKey: "row_key",
  columns: [],
  options: [
    "base_grid_options",
    "filter_options",
    "print_options"
  ],
  actions: [
    "Print"
  ],
  statusLine: {
    showRowCount: true,
    showUpdatedAt: true
  }
};
```

For forms:

```js
const outputConfig = {
  shell: "ag-base-shell",
  skin: "barn-entry-v2",
  endpoint: {
    url: "/approved-barn-entry-endpoint",
    params: ["show_no", "focus_day", "trainer"]
  },
  rowKey: "review_key",
  columns: [],
  options: [
    "base_grid_options",
    "form_review_options",
    "picker_options",
    "writeback_options",
    "print_options"
  ],
  actions: [
    "Edit",
    "Add",
    "Print",
    "Submit"
  ]
};
```

These configs are documentation contracts first. They do not require a dynamic runtime registry before the base outputs are built.

## Non-Negotiables

| Rule | Meaning |
|---|---|
| Same primitives | Every button, pill, toggle, input chip, and decorated control uses the same global primitive. |
| Same heights | Row, header, action, anchor, status, and bottom bars use the approved shared height rhythm. |
| Same source of truth | Endpoint owns current data; AG owns display and interaction. |
| Same print method | Print builds from current view/state into an approved print sheet. |
| Same no-icons base | Sort/filter/menu icons stay hidden in base. |
| Same no-rebuild rule | Do not rebuild a special one-off shell for each report/form. |
| Same approval rule | New styling, new status colors, new primitives, new payload contracts, or new endpoint behavior require explicit approval. |
