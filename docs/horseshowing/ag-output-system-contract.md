# AG Output System Contract

Version: `v0.1`
Date: `2026-07-08`
Status: `WIP contract`

## 1. Purpose

This document locks the working model for AG-based RingStatus outputs.

It exists so future work does not restart from scattered screenshots, local HTML files, browser tests, or incomplete workflow docs.

This document does not approve code changes, endpoint changes, schema changes, deploys, Webflow publishes, or workflow runs.

## 2. Current Truth

| Item | Status | Decision |
|---|---|---|
| AG Grid as output foundation | `LOCKED DIRECTION` | Use AG Grid for mobile, mobile-pro, print, simple lists, review grids, and form-like grids where it helps. |
| Endpoint-first data | `LOCKED DIRECTION` | Endpoints must return current normalized data. AG should not guess workflow truth. |
| Fresh API loading | `LOCKED DIRECTION` | Prefer AG loading current endpoint payloads over stale static JSON. |
| Shared base shell | `LOCKED DIRECTION` | Use one base shell pattern across outputs. |
| Skins | `WIP` | Use the same base with different skins for default, mobile, mobile-pro, and barn-entry-v2. |
| AG option sets | `WIP` | Isolate reusable option sets from the working templates before rebuilding outputs. |
| Barn-entry current | `REFERENCE` | Current wired barn-entry remains reference; build v2 against the shared base/skin model. |
| Mobile-pro Slate test | `REFERENCE` | Proves a pro template can run on Catalyst Slate. |
| `rs-schedules` Webflow input/output method | `LOCKED METHOD` | Use the Packing-style root/config/CDN CSS+JS/Webflow Cloud API route pattern for input and published render. Do not use injected full-HTML loaders as the new input/output method. |

## 2A. AG Output/Data Endpoint Owner Scope

`AG output + data endpoint owner`

Meaning:

- I own review, documentation, and approved changes for AG Grid outputs and the data endpoints that power them.
- I own ensuring every published output has the expected endpoint payload shape before the UI is treated as correct.
- I own the contract between endpoint payloads and AG display behavior: row keys, display fields, hidden underlying data, indexes, filters, anchors, print rows, detail/flyup rows, and submit/writeback payloads.
- I own AG Grid implementation surfaces, Webflow Cloud output routes, and Catalyst Slate output routes when they are serving published RingStatus outputs.
- I own making sure AG outputs use the approved base shell, option packs, skins, global button/pill primitives, row heights, print behavior, and documented special-row behavior.
- I can inspect upstream workflow/runtime/Core data only to verify whether an endpoint is consuming the correct source data.
- I should not patch Core runner cadence, live-enrich, time-engine, results, alerts, or other workflow lanes from this AG/output thread unless you explicitly approve crossing that lane.
- If an AG output is wrong because the endpoint is missing fields, stale, not indexed, or not returning underlying detail data, I trace and fix the endpoint/output contract in this lane.
- If an AG output is wrong because upstream runtime/workflow data is incorrect or missing, I report the upstream blocker and route it to the correct lane/session.
- I do not use manual/direct workflow calls as proof that a scheduled/cadence lane works. Manual calls are diagnostic only unless you explicitly accept manual proof.

Owned surfaces:

| Surface | Ownership |
|---|---|
| AG Grid prototypes | Build and maintain base/output prototypes that follow the AG contract. |
| AG option packs | Maintain reusable behavior packs such as filter, hide, focus, print, form review, picker, detail, diff, and writeback. |
| Output endpoints | Ensure each output endpoint returns current, normalized, display-ready and detail-ready payloads. |
| Webflow Cloud routes | Own output-facing routes where they serve AG/mobile/print/form data. |
| Catalyst Slate routes | Own output-facing Slate routes where they serve AG/mobile/print/form data. |
| Webflow embeds/loaders | Own embed contracts for AG outputs when the embed loads an output endpoint or hosted AG surface. |
| Print outputs | Ensure print payloads and print sheet layouts support expected page/column behavior. |
| Underlying data | Ensure detail/flyup/special rows either have underlying data or explicitly declare no-detail state. |
| Index/search fields | Ensure endpoint payloads include the fields needed for anchors, filters, search, grouping, and print. |

Not owned unless explicitly approved:

| Lane | Boundary |
|---|---|
| Core 1-4 runner cadence | Inspect only to verify output consumption. Do not patch cadence from this lane. |
| Live enrich | Report blocker unless approved to cross lanes. |
| Time engine | Report blocker unless approved to cross lanes. |
| Results/alerts/publish runners | Report blocker unless approved to cross lanes. |
| Airtable record repair | Do not repair records as a substitute for a repeatable endpoint/workflow fix. |
| One-time data wrangling | Not accepted as output proof unless explicitly approved. |

Definition of done for an AG output:

| Gate | Required Proof |
|---|---|
| Endpoint payload | Current endpoint returns all front-facing display fields and all required underlying/detail fields. |
| Row identity | Stable row keys exist for AG `getRowId`, submit/writeback, print, and detail rows. |
| Display fields | Fields used in visible columns are present and normalized. |
| Search/filter fields | Fields needed for search, anchors, filters, and focus are present. |
| Detail fields | Flyup/drawer/special-row detail payload is present, or no-detail state is explicit and non-clickable. |
| AG base compliance | Output uses approved base shell, global primitives, row heights, and option packs. |
| Print compliance | Print builds from current visible state and uses approved print layout behavior. |
| Publish/embed contract | Webflow or Slate published surface points to the correct current endpoint/output. |
| Verification | Browser/API verification proves the output consumes the expected endpoint payload. |

## 3. Reference Files

| Reference | Status | Use |
|---|---|---|
| `https://wec-ag-flat-jykrtsgw.onslate.com/` | `REFERENCE` | New mobile-pro test hosted in Catalyst Slate. |
| `docs/horseshowing/ag-output-references/wec_ag_ring_group_flyup_test.pre-related-data-structure-20260705-154136.html` | `REFERENCE` | Mobile-pro dense data delivery and flyup behavior. |
| `docs/horseshowing/ag-output-references/wec_ag_ring_group_flyup_test.backup-20260705-140656.html` | `REFERENCE` | Mobile-pro backup reference. |
| `docs/horseshowing/ag-output-references/wec_ag_ring_group_filter_function_only.js` | `REFERENCE` | Isolated filter behavior from the ring group reference. |
| `docs/horseshowing/ag-output-references/wec_ag_ring_group_hide_function_only.js` | `REFERENCE` | Isolated hide/show behavior from the ring group reference. |
| `docs/horseshowing/ag-output-references/wec_ag_ring_group_focus_function_only.js` | `REFERENCE` | Isolated focus behavior from the ring group reference. |
| `docs/horseshowing/ag-output-references/wec_ag_ring_group_print_function_only.js` | `REFERENCE` | Isolated print-sheet behavior and print layout attributes from the ring group reference. |
| `docs/horseshowing/ag-output-references/barn_entry_ag_review_functionality_reference.md` | `FUNCTIONALITY REFERENCE ONLY` | Existing barn-entry AG review load/edit/add/status/submit/print behavior; not a style or skin source. |
| `docs/horseshowing/ag-output-references/ag_default_grid_option_packs.md` | `WIP CONTRACT REFERENCE` | Organized AG option packs for report/input/form/mobile-pro outputs using the same base grid and global primitives. |
| `docs/horseshowing/ag-output-references/barn_hardware_packing_list.html` | `REFERENCE ONLY` | Webflow-exported barn hardware packing list; future print/form reference only, not an AG base or style source. |
| `docs/horseshowing/rs-schedules-webflow-input-output-method-contract.md` | `LOCKED METHOD` | Current `rs-schedules` Webflow input/output integration method and shared `styles.css` styling surface. |
| `docs/horseshowing/ag-output-references/wec_ag_styled_template_live_LOCKED_2026-07-04_v0.1.html` | `REFERENCE` | Locked lightweight mobile reference. |
| `docs/horseshowing/ag-output-references/wec_ag_styled_template_live.html` | `REFERENCE` | Working lightweight mobile reference. |
| `docs/horseshowing/ag-output-references/wec_tabulator_right_drawer.html` | `REFERENCE ONLY` | Drawer interaction reference only; not the primary AG base. |
| Current barn-entry route/template | `REFERENCE` | Existing wired barn-entry behavior; use for v2 comparison, not as final style source. |
| `C:\Users\gombc\Downloads\ag_entries_time_review(2).html` | `BASE REFERENCE` | Default AG shell, action bar, grid frame, status line, and print pattern. |
| `docs/horseshowing/ag-base-shell-reset-handoff.md` | `CURRENT BASE HANDOFF` | Reset handoff for the clean AG base shell. |
| `prototypes/horseshowing/ag-base-shell-reset.html` | `CURRENT BASE SOURCE` | Current clean AG base shell source. Use this for new AG outputs. |
| `prototypes/horseshowing/ag-base-shell-v2.html` | `HISTORICAL REFERENCE` | Earlier base attempt; do not use as the current base source. |
| `prototypes/horseshowing/ring-classes.html` | `CURRENT RING-CLASSES PROTOTYPE` | Separate `ring-classes` output built from `ag-base-shell-reset.html` with ring anchors, focus, `filterby-barn_name`, special no-detail rows, and 3-column print. |
| `prototypes/horseshowing/barn-entry-ag-review-v2.html` | `CURRENT BARN-ENTRY FORM PROTOTYPE` | Contains two named outputs: `barn-entries` for the interactive entry review form and `ring-classes` for the separate 3-column class print output. Built from `ag-base-shell-reset.html` with reset stack/classes and `rs-button tap` primitives plus `3column-print`, `focus`, `special-rows-without-underlying-data`, `anchorby-ring_name_normalized`, and `filterby-barn_name`. |
| `prototypes/horseshowing/ag-base-shell-proposed.html` | `HISTORICAL/POLLUTED REFERENCE` | Earlier proposed base became polluted; do not use as the current base source. |

Repo-local reference copies are stored under:

```text
docs/horseshowing/ag-output-references/
```

## 4. Base Shell Contract

The base shell is the shared output wrapper. It should be reusable before skins are applied.

| Area | Contract |
|---|---|
| Root | `app-shell` |
| Layout | `header/action area -> body/grid -> bottom/status` |
| Height | Mobile-first viewport fit, normally `100vh` or equivalent container height. |
| Width | Constrained max width for phone/tablet preview, currently around `800px` in the base reference. |
| Body behavior | Shell stays stable; grid/body is the scrollable area where appropriate. |
| Header | Title and context/date/focus information. |
| Action area | Buttons such as edit, print, submit, share, clear, add. |
| Grid frame | Owns AG Grid placement and overflow. |
| Bottom/status | Row counts, loading state, save state, or short operational status. |
| Print | Uses the same data/view, with print CSS removing non-print controls. |

Base reference attributes from `ag_entries_time_review(2).html`:

| Selector | Attribute Pattern |
|---|---|
| `html, body` | `height: 100%; margin: 0;` |
| `body` | `overflow: hidden;` |
| `.app-shell` | `height: 100vh; display: grid; grid-template-rows: auto 1fr auto; gap: 8px; padding: 8px;` |
| `.control-bar` | grid layout with title and toolbar |
| `.control-title h1` | compact title, about `16px`, weight `600` |
| `.control-title p` | compact metadata, about `12px` |
| `.toolbar` | flex buttons, wraps when needed |
| `.button` | compact pill/button, min-height around `32px` |
| `.grid-frame` | `min-height: 0`, full width, max width, hidden overflow |
| `#entryGrid` | full frame height/width, min-height around `320px` |
| `.status-line` | compact bottom text, about `12px` |

## 5. Skin Contract

The base shell should not be rebuilt for each output. Skins change density and visual treatment.

| Skin | Status | Purpose |
|---|---|---|
| `default` | `WIP` | Basic AG report/form shell. |
| `mobile` | `WIP` | Lightweight customer-facing schedule/output. |
| `mobile-pro` | `WIP` | Dense ring/class/entry data delivery with flyups/details. |
| `barn-entry-v2` | `WIP` | Review/form workflow using the same shell and button/table language. |

Skin variables should cover:

| Variable Group | Examples |
|---|---|
| Typography | font family, title size, row font size, status font size |
| Color | shell background, grid background, header background, text, accent |
| Density | row height, header height, padding, gap |
| Buttons | background, text, border, radius, active/selected/disabled states |
| Status colors | default, edit, print, submitted, pending, confirmed, declined |

Global base rule:

| Rule | Meaning |
|---|---|
| No explicit font weight in base | Base shell should not set `font-weight`; skins may only add weight if explicitly approved. |
| No row/cell accents in base | Base shell should not color individual rows or cells, except one neutral rollover/hover default. |
| Base columns require min width | Every base grid column must declare a `minWidth`; `time`, `ring`, `class_number`, `in`, and `status` cannot collapse beyond their minimums. |
| No filter/sort icons in base | Base grid may support filtering/sorting behavior, but visible filter/sort/menu icons are hidden unless a skin explicitly enables them. |
| One decorated element primitive | Buttons, pills, selects, status pills, and any decorated base control must share the same base attributes. |
| Tap cadence names | Base decorated controls use `tap`, `tap-active`, `tap-active-2`, `tap`; `tap-active-2` is used only when explicitly added. |
| Tap toggles | Toggle controls use `tap tap-toggle`; active state is `tap-active`, and label changes such as `Hide` / `Show` are handled by the shared toggle behavior. |
| Same element for same visual | If a status/control must look identical to a button, it must render as the same element/class path, not a parallel span/badge class. |
| Base button primitive | Base buttons/pills use one primitive: `1.5px` black border, `999px` radius, `30px` minimum height, `3px 12px` padding, `12px` uppercase text, no font weight. |
| Base button motion | Base buttons may use only conservative shared shadow and rollover motion from the primitive; variants must not define separate motion. |
| No special input styling | `Input` controls use the default tap/button primitive unless a later skin explicitly changes them. |
| Clean toggle text | Hide toggle labels are `Hide` / `Show`; diff toggle label stays `Diff` and uses active state, not `No Diff`. |
| No built-in stack gaps | Base shell stack uses no layout gap; spacing must come from explicit section padding, borders, or skin-approved rules. |
| No `!important` in base | Base styling must not rely on `!important`, patch overrides, or hard-coded one-off fixes. |
| Header split | Base header supports `header-left` and `header-right`; on small widths `header-right` stacks above `header-left`. |
| Mini action bar | `action-bar-mini` is allowed in `header-right` with a maximum of three buttons and the same global button primitive. |
| Action anchors | `action-anchors` may appear below the main `action-bar`; anchors use the same global button primitive. |
| Shared row height | Base uses one row/control height token for action bars, anchors, AG header, and AG rows; header may be taller only because it contains title/meta content. |
| Shared AG edge color | Base AG header background and table border color must use the same token so column headers and table edges do not visually drift. |

## 6. AG Option Sets

Reusable options should be named instead of copied ad hoc between templates.

| Option Set | Status | Purpose |
|---|---|---|
| `base_grid_options` | `WIP` | Default row height, column sizing, touch behavior, suppressions, row ID, loading behavior. |
| `mobile_options` | `WIP` | Lightweight mobile list/report behavior. |
| `mobile_pro_options` | `WIP` | Dense grouped data, flyups, related details, anchors. |
| `form_review_options` | `WIP` | Edit/review/add/submit behavior. |
| `print_options` | `WIP` | Print-safe layout and current-view print behavior. |

Known AG capabilities to consider:

| Feature | Intended Use |
|---|---|
| API row loading | Pull current endpoint data instead of stale JSON. |
| Filter API | Local list filtering after current payload loads. |
| Text filters | Search names, rings, classes, status text. |
| Number filters | Class numbers, entry numbers, counts, minutes. |
| Column sizing | Fit mobile and print layouts. |
| Column groups | Group related fields when helpful. |
| Column spanning | Compact mobile rows and readable summaries. |
| Row spanning | Ring/class grouping where it improves scanning. |
| Value parsers/formatters | Time, dates, class text, result scores, status labels. |
| Hide / prebuilt hide | Simple user views without creating new endpoints. |
| Diff classes | Highlight values changed since last snapshot. |
| Print | Browser print from current view/state. |

## 6A. Special Full-Width Class-Related Row Contract

The `ag-full-width-anchor -> class-related-data` shape is a special row shape. It is not the default AG row and must not replace the base row/cell model.

Use it only when a class row needs a compact class summary plus related entry rollups.

Shape:

```text
ag-full-width-anchor
  class-related-data
    rollup-line
      class-related-rollup
        rollup-item
          rollup-label

    class-line
      time-cell / class-time
      class-ring
      class-name
      class-entry / class-token
      class-status / class-token
```

Data behavior:

| Area | Contract |
|---|---|
| Class line | Always renders when class display fields exist. |
| Entry rollups | Render from populated `horse_items`, `entries`, or equivalent endpoint-provided rollup data. |
| Click behavior | Rollup items and class rows are clickable only when the row has underlying data for a flyup or drawer. |
| Visual fallback | If underlying detail data is missing, the row may still render visually, but the missing detail state must be explicit in the flyup/drawer or the click target must be disabled. |
| Flyup/drawer target | Click opens the approved detail surface for the output: flyup, drawer-in, or equivalent mobile-pro detail panel. |
| Data truth | Endpoint supplies class, entry, horse, rider, trainer, status, and rollup fields. AG must not invent missing rollup data. |
| Styling | Rollup pills and class tokens inherit the approved AG base pill/button/token primitive. |
| Default row boundary | Standard reports, lists, and barn-entry rows continue to use the normal AG row/cell model unless this special shape is explicitly selected. |

Required detail payload fields depend on the output, but the row contract expects:

| Field Type | Examples |
|---|---|
| Row identity | `row_key`, class key, ring key, optional entry key |
| Class display | `time`, `ring`, `class_number`, `class_name`, `entry_count`, `status` |
| Rollup display | rollup label, horse/barn name, entry order, trainer |
| Detail data | class detail rows, entry rows, rider/trainer/horse detail, timing detail |
| Missing-data state | explicit empty detail message or disabled click state |

## 7. Endpoint Contract

Endpoints own the data truth. AG owns display and interaction.

| Endpoint Responsibility | Rule |
|---|---|
| Fresh data | Return current focus data from the canonical runtime/source tables. |
| Normalized fields | Return consistent names, dates, keys, status, and display fields. |
| Const keys | Include canonical keys for show/focus/ring/class/entry identity. |
| Display fields | Include user-facing text such as ring, class, barn name, time, status. |
| Timing fields | Include starts/ends/go timing fields when available. |
| Rollups | Precompute important ring/class/entry rollups when they are workflow truth. |
| Diff fields | Include changed flags or prior/current values where diff display matters. |

AG may calculate local presentation state:

| AG Local Responsibility | Rule |
|---|---|
| Filter/search | Local filtering of loaded rows is allowed. |
| Sort/group | Local sorting/grouping is allowed for display. |
| Hide/show | Local hide/prebuilt hide is allowed. |
| Print state | Print current user-visible state. |
| UI selection | Maintain selected/expanded rows locally until submitted. |

AG must not infer workflow truth:

| Not AG Responsibility | Reason |
|---|---|
| Guess missing horses/classes | Endpoint/workflow must supply the best known data. |
| Repair stale data | Workflow/cadence issue, not UI issue. |
| Decide canonical keys | Keys come from backend contract. |
| Rebuild runtime state | Runtime prep/time engine own that. |

## 8. Enrichment Method

The enrichment method is endpoint-first, AG-enhanced.

| Layer | Owns |
|---|---|
| Workflow/runtime | Build source rows, probe/process entries, runtime rows, live enrichment, time engine. |
| Endpoint | Return exact current payload needed by a report/form. |
| AG | Render, filter, group, print, hide/show, collect UI input. |
| Form submit/API | Send user input to Airtable/API/trigger lane. |

This means:

```text
cadence/runtime data
-> endpoint payload
-> AG display/filter/print/input
-> optional submit/trigger
```

The endpoint should be shaped for the output. AG should not need to stitch stale JSON or unrelated payloads together in the browser.

## 9. Output Types

| Output Type | Examples | Base |
|---|---|---|
| Lightweight mobile | current mobile schedule, customer output | base + mobile skin + AG attributes |
| Dense mobile-pro | ring/class/entry flyups, dense data delivery | base + mobile-pro skin + AG attributes + advanced options |
| Print | mobile print, class/ring lists, review print | same base plus print options |
| Simple lists | alert logs, rosters, results lists, comments lists | base + default/list skin |
| Forms/review | barn-entry-v2, profile detail, review grids | base + form layer |
| Trigger grids | alert/input/subscribe/favorite actions | base + form/trigger behavior |

## 10. Required Feature Inventory

| Feature | Status | Notes |
|---|---|---|
| Print | `REQUIRED` | Must be a first-class output behavior, not an afterthought. |
| Hide | `REQUIRED` | User can hide rows/fields where supported. |
| Prebuilt hide | `REQUIRED` | Preset simplified views. |
| Inputs to Airtable/API | `REQUIRED` | Forms/review grids can submit durable records. |
| Inputs as triggers | `REQUIRED` | Some inputs create downstream trigger/message records. |
| Rollups | `REQUIRED` | Important but must be endpoint-backed where it reflects workflow truth. |
| Diff classes | `REQUIRED` | Show attributes changed since last snapshot. |
| Comments UI | `WIP` | Simple UI for user/operator comments. |
| Subscribe/favorite | `WIP` | Lists can support follow/favorite/subscription behavior. |
| Profile detail forms | `WIP` | Future simple forms for profile/user details. |

## 11. Airtable AG Reference Tables

Airtable AG reference tables are instruction/reference surfaces, not an excuse to make the UI fully dynamic before it is useful.

Known tables:

| Table | Purpose |
|---|---|
| `ag_grids` | Report/form/grid-level configuration and reference notes. |
| `ag_tables_allowed` | Table/source options allowed by a grid. |
| `ag_fields_allowed` | Field options allowed by a grid. |
| `ag_end_points_allowed` | Endpoint/reference URL options where used. |

Current reference rows:

| Row | Purpose |
|---|---|
| `ag_default_theme_v0` | Default theme/style starting point. |
| `barn-entry` | Clean current barn-entry reference row. |
| `barn-entry-mock` | User/example row; reference only. |

Rules:

| Rule | Meaning |
|---|---|
| Do not over-automate first | These rows can be reference instructions before becoming dynamic configuration. |
| Do not add fields casually | Add fields only when they reduce repeated rebuild friction. |
| Keep current files authoritative until replaced | Working templates still prove behavior. Airtable rows describe intent. |

## 12. AG External References

| Reference | Use |
|---|---|
| `https://www.ag-grid.com/theme-builder/` | Theme exploration and token direction. |
| `https://github.com/ag-grid/ag-grid` | AG Grid source and style package reference. |
| `https://github.com/ag-grid/ag-grid-figma-design-system/` | Figma/token design system reference. |
| `community-modules/styles` | AG style package source. |
| `quartz-light-example-tokens.json` | Example AG theme tokens. |
| `paramNames.js` | Supported AG theme parameter names. |
| `tokens-to-ag-grid-theme.js` | Token-to-theme conversion pattern. |

Use these as references. Do not block near-term output work on a full token pipeline.

## 13. Do Not Drift Rules

| Rule | Meaning |
|---|---|
| Do not rebuild the base for every output | Use the shared shell. |
| Do not treat stale JSON as proof | Load current endpoint data. |
| Do not make AG guess workflow state | Endpoint supplies truth. |
| Do not create new table/report systems casually | Extend this contract first. |
| Do not bury new decisions in chat only | Add them here or to a linked implementation doc. |
| Do not replace production/mobile pages from an unproven prototype | Create v2/test paths until approved. |
| Do not use old docs as current truth unless marked current | Status labels matter. |

## 14. Open Decisions

| Decision | Status |
|---|---|
| Exact named option sets from working templates | `OPEN` |
| Which AG Enterprise features justify license cost | `OPEN` |
| Final mobile skin variables | `OPEN` |
| Final mobile-pro dense/flyup option set | `OPEN` |
| Barn-entry-v2 endpoint and submit method | `OPEN` |
| Diff-class payload shape | `OPEN` |
| Rollup payload shape per output | `OPEN` |

## 15. Next Useful Work

The next useful work is to extract only the option sets that are already proven in working templates.

Do not review whole files unless needed. Point to a specific behavior and capture it into one named option set.

Examples:

| Behavior | Target Option Set |
|---|---|
| Ring group flyup | `mobile_pro_options` |
| Print current view | `print_options` |
| Barn-entry review/add/submit | `form_review_options` |
| Lightweight mobile row density | `mobile_options` |
| Default shell sizing | `base_grid_options` |
