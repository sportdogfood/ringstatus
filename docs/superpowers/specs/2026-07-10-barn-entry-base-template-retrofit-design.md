# Barn Entry Base-Template Retrofit Design

Date: 2026-07-10
Status: Approved design; implementation not started

## Goal

Retrofit the current barn-entry workflow into the approved AG base template while preserving the template's locked desktop, mobile, and print behavior. Preserve the working barn-entry workflow, repair the proven horse/Add defects, and publish only after local and rendered verification.

## Governing Sources

- Locked rendered template: `https://ringstatus.com/test/ag-base-shell`
- Routed template source: `webflow-cloud-test/src/assets/ag-base-shell/source.html`
- Current base source: `prototypes/horseshowing/ag-base-shell-reset.html`
- Canonical AG contract: `docs/horseshowing/ag-output-system-contract.md`
- Airtable/Webflow handoff: `docs/horseshowing/ag-form-airtable-webflow-cloud-handoff.md`
- Barn-entry behavior reference: `docs/horseshowing/ag-output-references/barn_entry_ag_review_functionality_reference.md`
- Option-pack reference: `docs/horseshowing/ag-output-references/ag_default_grid_option_packs.md`
- Three-column print reference: `docs/horseshowing/ag-output-references/wec_ag_ring_group_print_function_only.js`
- Airtable reference row: `ag_grids.ag_report_id = barn-entry`

Git remains executable truth. Airtable documents the output contract and allowed surfaces; it does not dynamically rewrite the UI.

## Locked Template Contract

The following cannot change without new explicit approval:

- Existing template DOM order and wrapper hierarchy.
- Existing class names and visual primitives.
- Existing CSS values, spacing, typography, colors, sizes, and radii.
- Existing `@media (max-width: 479px)` behavior.
- Existing print media rules and responsive ordering.
- Existing desktop/mobile stacking, overflow, and grid-frame behavior.
- Existing button shapes and approved visual variants.

Implementation must not add new media queries, breakpoint-specific JavaScript, mobile-only column definitions, or DOM reordering.

The only approved presentation additions are an output-specific option layer containing:

- `.is-hidden { display: none; }`
- pending/default and `tap3`: no row outline
- `tap1`: green 2px outline with `outline-offset: -2px`
- `tap2`: black 2px outline with `outline-offset: -2px`

These additions must not edit or override the locked template block beyond the explicitly approved states.

## Template Element Mapping

| Base element | Barn-entry responsibility | Visibility/behavior |
|---|---|---|
| `app-head` | Barn-entry title, show, and focus-day context | Reuse locked structure |
| `action-bar-mini` | `EDIT`/`DONE` and `PRINT` | Two assigned controls; remaining mini control gets `is-hidden` |
| `action-bar` | None | Entire element gets `is-hidden` |
| `action-anchors` | None | Entire element gets `is-hidden` |
| `grid-frame` | Interactive barn-entry AG Grid | Reuse locked structure |
| `action-bar-bottom` | `EDIT`/`DONE`, `ADD`, and `SEND` | State-controlled existing button slots |
| `status-line` | Visible row count and current `mainStatus` | Reuse locked structure |

Button placement is semantic only. A base button's sample label does not determine the barn-entry action assigned to that locked slot.

## Control States

### Default mode

- Header mini controls: `EDIT`, `PRINT`.
- Bottom controls: `EDIT` visible; `ADD` and `SEND` hidden.
- Rows are not tap-interactive.
- Current row status remains visible only through approved row-state presentation.

### Edit mode

- Both assigned Edit controls display `DONE` and invoke the same existing edit-mode toggle.
- `ADD` and `SEND` become visible in the bottom action bar.
- The existing approved edit-mode background is preserved.
- Tapping anywhere on a data row advances its status.

### Submitted mode

- Preserve the existing submitted review behavior.
- Preserve Print and Share behavior.
- Do not invent a new submitted layout or alter the base responsive contract.

## Interactive AG Grid

The interactive grid uses four sortable columns at every viewport:

1. `TIME`
2. `RING`
3. `CLASS`
4. `HORSE`

There is no Tap or Status column. The same column definition is used for desktop and mobile. The locked template owns responsive sizing and overflow.

Hidden identifiers and payload fields remain available in row data, including show, focus-day, ring, class, entry, horse, source-record, review-key, and status keys.

## Whole-Row Tap State

Whole-row tap toggling is enabled only while edit mode is active.

```text
pending -> confirmed -> declined -> pending
```

| Business status | Tap state | Presentation |
|---|---|---|
| `pending` | default or `tap3` | No outline |
| `confirmed` | `tap1` | Green 2px outline, offset -2px |
| `declined` | `tap2` | Black 2px outline, offset -2px |

The row remains non-interactive outside edit mode. Keyboard activation must follow the same edit-mode gate if the row is made keyboard focusable.

## Canonical Horse Mapping

The interactive Horse column and all picker suggestions must display canonical `barn_name`.

- `horse` is searchable source metadata, not the preferred display value.
- A rider value must never be substituted for a horse or barn name.
- Current `class_oog` rows lack a direct `barn_name`; they must be enriched from the approved horse/helper mapping before entering AG row data.
- If a canonical mapping is unresolved, surface that unresolved state instead of silently displaying the wrong entity.

## Add Entry Picker

The Add flyup retains its existing functional purpose and is not allowed to alter the base shell's responsive rules.

Both candidate lists use compact client-side AG Grid instances with cached Quick Filter. Search begins after the second typed character and performs no Airtable request per keystroke.

### Horse tiers

1. Initially show all horses marked `follow` (normally a very small set).
2. If the horse is not found, load the broader horse roster once and continue client-side filtering.
3. If still absent, allow manual entry for the future hot-patch/binding lane.

Horse search key includes canonical `barn_name` and source `horse`. Suggestions and selected values always display `barn_name`.

The current endpoint returns helper candidates under `top_matches`; the existing form incorrectly reads only `results` or `matches`. Implementation must consume the proven live response shape.

### Class tiers

1. Initially search the 57 current focus-day classes already supplied by the schedule endpoint.
2. If absent, expose the broader preflight-filtered class candidate set.
3. If still absent, allow manual entry.

The class search key includes class number, class ID, full class name, a normalized first-15-character prefix, ring, and time. Suggestions begin on the second character.

### Picker performance

- Client-Side Row Model only.
- Precompute normalized searchable text.
- Use `cacheQuickFilter: true`.
- Use ordinary normalized substring/token matching; do not compile raw user input as a regular expression.
- Do not rebuild from Airtable on each keystroke.

## Status and Error Flow

- Move existing `mainStatus` output into the locked `status-line`.
- Keep the visible-row count in the same status line.
- Horse-helper failure does not prevent the main grid from loading.
- Broader-source failure preserves manual-entry fallback.
- `SEND` disables while its request is pending.
- Failed submission preserves current grid/edit state and reports the server error in `status-line`.
- Successful submission preserves the existing submitted review, Print, and Share behavior.

## Print Contract

`3column-print` is a print option pack, not an interactive AG column definition.

- Build and print a dedicated print sheet, not the live AG viewport.
- Use the existing three newspaper-style print columns.
- Group printed content by Ring headings.
- Preserve the existing compact black-and-white print rules.
- Do not print interactive controls or row tap outlines.
- Do not alter the locked base print media behavior without explicit approval.

## Verification Gates

### Static contracts

- Locked base CSS and responsive/print blocks have not changed.
- No new media queries or breakpoint-dependent column code exists.
- Hidden template elements use only `is-hidden`.
- Interactive columns are exactly `TIME`, `RING`, `CLASS`, `HORSE`.
- Print remains a separate three-column ring-group sheet.

### Functional tests

- Default/edit/submitted control visibility.
- Both Edit controls share one state and handler.
- Whole-row tap works only in edit mode.
- Pending/confirmed/declined state cycle and outline mapping.
- Canonical `barn_name` enrichment; no rider-as-horse fallback.
- Followed, broader, and manual horse tiers.
- Current, preflight, and manual class tiers.
- Two-character threshold and cached local picker search.
- SEND pending, success, and failure behavior.

### Rendered checks

- Desktop rendering against the locked shell.
- Viewport at or below 479px solely to prove locked behavior was preserved.
- Print preview proving three columns grouped by Ring.
- Add flyup and picker grids at desktop and mobile sizes.

Rendered checks do not authorize responsive redesign.

## Publication Gate

Publish only after tests, build, local render, mobile preservation check, and print preview pass. Deploy the `webflow-cloud-test` app at mount `/test`, then verify the live GET/rendered route. Do not use a live barn submission as deployment proof unless the user separately approves that production-data action.

## Non-Goals

- No changes to upstream cadence or Catalyst workflow ownership.
- No Airtable record repair.
- No redesign of the locked shell.
- No responsive redesign.
- No new button functions beyond the approved barn-entry workflow.
- No implementation of the future manual horse/class hot-patch binding lane beyond retaining the approved manual fallback boundary.
