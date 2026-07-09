# AG Base Shell Reset Handoff

Date: `2026-07-08`
Status: `current reset handoff`
Source prototype: `prototypes/horseshowing/ag-base-shell-reset.html`

## Purpose

This file documents the clean AG base shell reset.

The reset exists because the prior proposed base accumulated too many competing fixes and AG Grid overrides. That made it fragile. This reset is the new simple starting point for AG Grid outputs.

Use this reset as the current base reference.

Do not use the polluted proposed file as the base source.

```text
Do use:
prototypes/horseshowing/ag-base-shell-reset.html

Do not use as current source:
prototypes/horseshowing/ag-base-shell-proposed.html
```

## Broad Role

The AG base shell is a structural starter for RingStatus AG outputs.

It is intended to support:

- mobile outputs
- mobile-pro outputs
- print outputs
- simple lists
- review grids
- form-like grids
- future input/writeback screens

It is not the final brand skin. It is the common structure and control language that later screens can build on.

## Base Principle

The reset base should be simple enough to trust.

The base should avoid:

- competing AG column state
- hidden columns used as layout tricks
- resize handlers that rewrite columns
- one-off focus CSS fights
- redundant AG overrides
- endpoint-specific data assumptions
- output-specific business rules

The base should prefer one source of truth per concern.

## Stack

The reset stack is:

```text
app-shell
  app-head
    header-left
    header-right
      action-bar-mini
  action-bar
  action-anchors
  grid-frame
    agBaseGrid
  action-bar-bottom
  status-line
```

## Stack Blocks

| Block | Purpose |
|---|---|
| `app-shell` | Owns the full viewport shell. |
| `app-head` | Holds page title/context and mini actions. |
| `header-left` | Title and short metadata line. |
| `header-right` | Header-side actions. |
| `action-bar-mini` | Up to three small header actions. |
| `action-bar` | Main commands such as print, hide, diff, input, submit. |
| `action-anchors` | Anchor controls such as ring, class, entry, results. |
| `grid-frame` | Owns AG Grid placement and overflow. |
| `agBaseGrid` | AG Grid mount point. |
| `action-bar-bottom` | Bottom commands such as back, clear, save. |
| `status-line` | Row count and short operational status. |

## Consistency Rules

The reset keeps the same control primitive across all decorated controls:

```text
rs-button tap
rs-button tap tap-active
rs-button tap tap-active-2
```

This primitive is used for:

- header mini buttons
- action buttons
- anchor buttons
- bottom buttons
- status pills when rendered as controls

Do not create separate badge/button/pill systems inside the base.

## AG Grid Rules

The reset base intentionally keeps AG Grid simple.

Current reset AG rules:

- Column truth lives in `columnDefs`.
- No `applyColumnState`.
- No `onGridSizeChanged` column rewriting.
- No hidden columns.
- No horse column in the base.
- No `barn_name` sample data in the base.
- No AG CSS focus override stack.
- No user-resizable columns in the base.
- No visible sort/filter/menu icons in the base.
- `CLASS` is the only flexible content column.
- Utility columns use fixed widths and minimum widths.

Current reset columns:

| Column | Field | Base Sizing |
|---|---|---|
| `TIME` | `time` | `minWidth: 78`, `width: 88` |
| `RING` | `ring` | `minWidth: 78`, `width: 88` |
| `NO` | `no` | `minWidth: 56`, `width: 60` |
| `CLASS` | `class_name` | `minWidth: 170`, `flex: 1` |
| `IN` | `starts_in` | `minWidth: 56`, `width: 60` |
| `STATUS` | `status` | `minWidth: 88`, `width: 88` |

Current reset grid options:

```text
sortable: false
filter: false
resizable: false
suppressMovableColumns: true
suppressCellFocus: true
suppressHeaderFocus: true
```

These are base defaults. Output-specific screens may add sorting, filtering, resizing, or advanced behavior later through an approved option pack.

## Print Role

Print is included as a base path.

The reset print rules:

- hide action rows and header-right controls
- keep title/context
- keep grid visible
- keep status line
- use compact AG row/header sizing
- use `letter portrait` with `.25in` margin

Do not create a separate stale print table in the base.

## What Belongs In This Base

The reset base may include:

- shell stack
- shared button primitive
- neutral colors
- shared row height
- AG Grid mount
- simple column example
- simple hide hook
- print hook
- input/submit placeholder hooks
- compact status line

## What Does Not Belong In This Base

Keep these out unless explicitly approved:

- customer brand skin
- barn-entry-specific logic
- mobile-pro dense grouping
- endpoint-specific payload assumptions
- hidden columns as layout tools
- AG column state patches
- resize/reflow handlers
- focus CSS override fights
- one-off data fixes
- Airtable schema assumptions
- workflow/cadence proof logic

## Current Source Paths

Current reset prototype:

```text
prototypes/horseshowing/ag-base-shell-reset.html
```

Previous polluted proposal:

```text
prototypes/horseshowing/ag-base-shell-proposed.html
```

The proposed file is historical only for this lane. Do not use it as the base source without explicit approval.

## Use Pattern

For a new AG output:

1. Start from `ag-base-shell-reset.html`.
2. Keep the stack names.
3. Keep the shared button primitive.
4. Define the output columns in `columnDefs`.
5. Keep one source of truth for column sizing.
6. Add only the specific AG option pack needed.
7. Add output-specific skin after the base behavior is stable.
8. Verify browser and print.

## Handoff Summary

This reset is the current clean AG base shell.

It is intentionally plain and limited:

```text
structure first
one button primitive
simple AG Grid defaults
one flexible content column
no redundant AG fights
print included
skins and advanced options later
```

Use it to prevent drift and avoid rebuilding every AG output from scratch.
