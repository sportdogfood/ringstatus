# AG Base Shell Handoff

Date: `2026-07-08`
Status: `handoff reference`
Source prototype: `prototypes/horseshowing/ag-base-shell-proposed.html`
Related contract: `docs/horseshowing/ag-output-system-contract.md`

## Purpose

The AG base shell is the shared starting point for RingStatus AG Grid outputs.

Its purpose is not to be a final branded screen, a finished mobile-pro report, or a one-off form. Its purpose is to provide a simple, repeatable shell that keeps future AG outputs from drifting every time a new list, report, print view, or form is built.

The base gives us one consistent structure for:

- mobile outputs
- mobile-pro outputs
- print outputs
- simple lists
- review grids
- form-like grids
- future input/writeback screens

The base should stay simple. Output-specific behavior and stronger business branding belong in later skins or option packs, not in the base shell itself.

## Design Intent

The base uses a restrained, business-readable layout:

- one app shell
- one header area
- one main action row
- one anchor row
- one AG Grid body
- one bottom action row
- one compact status line

The intent is consistency before decoration.

The base should make it easy to answer:

- where does the title/context go?
- where do primary actions go?
- where do anchors go?
- where does AG Grid live?
- where do save/back/clear actions go?
- where does status/loading/count text go?
- how should print hide controls and keep the table readable?

## Stack

The current stack is:

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

| Block | Purpose | Notes |
|---|---|---|
| `app-shell` | Owns the whole output surface. | Uses a simple grid stack and viewport-height behavior. |
| `app-head` | Holds page title and header actions. | Contains `header-left` and `header-right`. |
| `header-left` | Title and short context. | Example: `AG Base Shell` and one metadata line. |
| `header-right` | Small top-right controls. | Contains `action-bar-mini`; on mobile it stacks above `header-left`. |
| `action-bar-mini` | Up to three small header actions. | Uses the same button primitive as every other control. |
| `action-bar` | Primary action row. | Example actions: `Print`, `Hide`, `Diff`, `Input`, `Submit`. |
| `action-anchors` | Navigation/filter anchors below main actions. | Example anchors: `Ring`, `Class`, `Entry`, `Results`. |
| `grid-frame` | Holds AG Grid and controls overflow. | This is the body area for row data. |
| `agBaseGrid` | AG Grid mount point. | Uses AG Grid for rows, columns, print state, filters, and local interaction. |
| `action-bar-bottom` | Bottom command row. | Example actions: `Back`, `Clear`, `Save`. |
| `status-line` | Compact operational status. | Example: row count and option-pack status text. |

## Consistency Rules

The base should be predictable across every AG output.

Shared rules:

- All buttons, pills, toggles, and decorated controls use the same primitive.
- Status pills should not invent their own separate visual system if they are meant to look like buttons.
- `tap`, `tap-active`, and `tap-active-2` are the shared state names.
- `tap-active-2` is only used when explicitly needed.
- Action rows, anchors, AG headers, and AG rows use one shared row-height idea.
- The base should not use one-off patches or `!important`.
- The base should not add special row or cell accents.
- The base should hide AG sort/filter/menu icons unless a later skin explicitly enables them.
- Columns must declare minimum widths so the base does not collapse into unreadable data.
- The stack should not rely on random gaps. Spacing should come from explicit padding, borders, or later skins.

## Button Primitive

The base button language is intentionally simple:

```text
rs-button tap
rs-button tap tap-active
rs-button tap tap-active-2
```

This is used for:

- header mini buttons
- action bar buttons
- anchor buttons
- bottom action buttons
- status-like controls when they are intended to look like tappable pills

The point is that a user should not have to relearn the control language on each output.

## AG Grid Role

AG Grid is the body renderer, not the whole design system.

The shell provides:

- app structure
- action placement
- status placement
- print framing
- consistent controls

AG Grid provides:

- rows
- columns
- row identity
- local filtering/search behavior
- print from current view
- future input/edit/writeback surfaces where appropriate

The endpoint should provide clean data. The grid should display and interact with that data. The grid should not guess workflow truth or repair stale payloads.

## Print Role

Print is part of the base expectation.

The print version should:

- hide action controls
- keep a thin title/subtitle area
- print the table compactly
- keep a thin bottom/status area
- use current visible data, not a separate stale view

Output-specific print behavior can be added later, but the base must preserve the idea that print is a first-class output path.

## What Belongs In The Base

The base may include:

- shell stack
- shared button primitive
- shared row height
- basic AG Grid mount
- base column examples
- hide/diff/input/print hooks as simple references
- print CSS pattern
- neutral colors
- simple status line

## What Does Not Belong In The Base

The base should not absorb every future requirement.

Keep these out of the base unless explicitly approved:

- customer-specific branding
- dense mobile-pro behavior
- barn-entry-specific workflow logic
- one-off Airtable fields
- endpoint-specific payload assumptions
- special row colors
- business-rule fixes
- workflow/cadence proof logic
- production data repair
- alternate visual systems for pills/buttons

## How To Use This Base

Use the base as the first file or reference when creating a new AG output.

Recommended flow:

1. Start with the base stack.
2. Keep the same block names.
3. Keep the same button primitive.
4. Define the output columns and minimum widths.
5. Define the endpoint payload contract.
6. Add only the option packs needed for that output.
7. Apply a skin only after the base behavior is clear.
8. Verify the output in browser and print.

## Current Source

Current local prototype:

```text
prototypes/horseshowing/ag-base-shell-proposed.html
```

Current Webflow Cloud source copy:

```text
webflow-cloud-test/src/assets/ag-base-shell/source.html
```

Current Webflow Cloud route source:

```text
webflow-cloud-test/src/pages/ag-base-shell.js
```

Current loader root:

```text
rs-ag-base-shell-loader
```

Current route target after deployment:

```text
https://ringstatus.com/test/ag-base-shell
```

## Handoff Summary

This base is a structural AG output shell.

It exists to keep RingStatus AG outputs consistent, simple, and fast to start:

```text
header
action
anchors
body/grid
bottom actions
status
print
```

Do not treat it as the final brand skin. Treat it as the common stack and control language that every future AG output can inherit before skins, endpoint-specific behavior, and business rules are added.
