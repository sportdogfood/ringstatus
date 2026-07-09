# AG Ring Classes Failed Run Handoff

Date: `2026-07-08`
Status: `FAIL - do not continue patching current prototype`

## Purpose

This handoff is for the next Codex runner.

The previous run failed to apply the AG output contract cleanly. It repeatedly patched `ring-classes.html` after drifting from the documented base and option-pack behavior. The user should not have to inspect obvious UI failures or restate the same contract.

Do not continue by layering more fixes onto the current broken surface.

## Current User Requirement

The user wants separate AG outputs, not a combined or renamed entry template.

Known output names:

```text
barn-entries
ring-classes
```

The immediate output the user asked to see was:

```text
ring-classes
```

## Current Source Of Truth

Use this as the base:

```text
prototypes/horseshowing/ag-base-shell-reset.html
```

Use this handoff:

```text
docs/horseshowing/ag-base-shell-reset-handoff.md
```

Use this contract:

```text
docs/horseshowing/ag-output-system-contract.md
```

Use these option references:

```text
docs/horseshowing/ag-output-references/ag_default_grid_option_packs.md
docs/horseshowing/ag-output-references/wec_ag_ring_group_filter_function_only.js
docs/horseshowing/ag-output-references/wec_ag_ring_group_focus_function_only.js
docs/horseshowing/ag-output-references/wec_ag_ring_group_print_function_only.js
```

Special-row contract lives in:

```text
docs/horseshowing/ag-output-system-contract.md
section: 6A. Special Full-Width Class-Related Row Contract
```

## Polluted / Failed Artifacts

Do not treat this file as contract-clean:

```text
prototypes/horseshowing/ring-classes.html
```

It was patched repeatedly during a failed run and should be considered polluted until rebuilt from the reset base.

Do not treat this file as aligned/current:

```text
prototypes/horseshowing/barn-entry-ag-review-v2.html
```

The user explicitly said to stop referring to it because it does not align.

Do not use this as current base:

```text
prototypes/horseshowing/ag-base-shell-proposed.html
```

It is historical/polluted.

## What Went Wrong

The failed run:

- treated the contract as loose context instead of a gate
- created `ring-classes.html` but patched drift repeatedly
- invented modal filter behavior instead of using the documented local filter option
- failed to remove or hide visible action surfaces correctly on the first pass
- omitted documented special-row class names on first implementation
- kept responding with explanations instead of contract-proof

## Required Rebuild Direction

Start over from:

```text
prototypes/horseshowing/ag-base-shell-reset.html
```

Create a fresh output file, preferably:

```text
prototypes/horseshowing/ring-classes-reset.html
```

Only replace the reset sample body/script with the `ring-classes` output. Do not mutate the reset base file.

## Ring Classes Output Contract

Output identity:

```html
data-ag-system="ring-classes"
data-ag-output="ring-classes"
```

Visible shell:

```text
app-shell
  app-head
    header-left
    header-right
      action-bar-mini
  action-anchors
  grid-frame
    agBaseGrid
  optional action-bar-bottom for filters only when open
  status-line
```

Do not show the main `.action-bar`.

Header mini actions:

```text
Print
Focus
Horse
```

No `All` anchor.

Anchors:

```text
anchorby-ring_name_normalized
horizontal scroll
no wrap
```

Columns:

```text
TIME | RING | NO | CLASS
```

Hidden columns:

```text
IN
STATUS
```

Required documented options:

```text
3column-print
focus
special-rows-without-underlying-data
anchorby-ring_name_normalized
filterby-barn_name
```

## Filter Contract

Do not use a modal.

Use documented local filter behavior:

```text
activeHorseFilters = Set
horseFilterOptions()
rowMatchesHorseFilters(row)
toggleHorseFilter(key)
```

The `Horse` button opens an `action-bar-bottom` filter row with `rs-button tap` pills.

Filter options should come from endpoint data fields such as:

```text
barn_name
horse_name
horse_display
horse
horse_items[].name
horse_items[].label
```

## Focus Contract

Use the documented focus behavior:

```text
focusMode boolean
group by ring + time + class_name
within duplicate group, keep lowest class_number/class_no
exclude duplicate rows from visible rows
```

No row accent should be added by focus mode.

## Special Row Contract

Special class rows must use this exact shape:

```text
ag-full-width-anchor
  class-related-data has-rollup
    rollup-line
      class-related-rollup
        rollup-item
          rollup-label
    class-line
      time-cell
        class-time
      class-ring
      class-name
      class-entry
        class-token
      class-status
        class-token is-status status-soon
```

If no rollup/detail data exists:

```text
render No Rollups as disabled rollup item
render no detail as explicit status token
do not make the missing-detail row clickable
```

## Print Contract

Print must be a separate `ring-classes` print output.

Use a dedicated print sheet, not the live AG viewport:

```html
<section class="print-sheet" data-ag-output="ring-classes"></section>
```

Print behavior:

```text
3 columns
black and white
tight margins
landscape unless user specifies otherwise
current visible state
exclude special no-detail rows unless explicitly approved
```

## Data Contract

Endpoint used during failed run:

```text
https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/
action=wec-mobile-live
show_no from URL, fallback only for prototype
focus_day from URL, fallback only for prototype
```

This is an output prototype. Do not repair workflow data or run cadence.

## Verification Required Before Final Response

Create or run a local DOM contract check. Minimum assertions:

```text
data-ag-output == ring-classes
no section.action-bar exists or visible count is 0
action-anchors exists
action-anchors flex-wrap == nowrap
action-anchors overflow-x == auto
no anchor with text All
headers == TIME,RING,NO,CLASS
IN and STATUS not visible
filter modal/dialog does not exist
Horse opens action-bar-bottom filter pills
special row selector exists:
  .ag-full-width-anchor .class-related-data.has-rollup
rollup selector exists:
  .rollup-line .class-related-rollup .rollup-item .rollup-label
class line selectors exist:
  .class-line .time-cell .class-time
  .class-line .class-ring
  .class-line .class-name
  .class-line .class-entry .class-token
  .class-line .class-status .class-token.is-status.status-soon
print sheet exists with data-ag-output == ring-classes
JS parses cleanly
```

Use Playwright if available.

## Communication Rule For Next Runner

Do not explain around mistakes.

Final response should be short and proof-based:

```text
Created:
<path>

Verified:
- exact contract checks
- visible headers
- no action-bar
- no modal filter
- special row selectors
- print sheet
```

If any check fails, say `FAIL` and name the selector or behavior that failed.
