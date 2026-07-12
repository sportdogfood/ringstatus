# WEC Mobile-Pro Local Prototype Design

Date: 2026-07-11
Status: Pending written-spec approval

## Goal

Build a local `wec-mobile-pro` prototype for review before publishing. Start from the handed RSS special-row template and preserve its CSS format exactly. This work is local only: no publish, deploy, workflow execution, endpoint mutation, or data repair.

## Locked Sources

- Source bundle: `C:\Users\gombc\Downloads\rswp-special-rows-grid-template-current.zip`
- Template CSS entry: `rswp-special-rows-grid-template.css`
- Exact CSS SHA-256: `768A8516FA141030F9E4DE8EA494FF90900E5DE80ED65901BF453D19E3903521`
- Shared CSS: pinned `webflow/packing-worksheet/styles.css` URL at commit `df8d9d4eb0652f76c952ed718b36e774100148f4`
- AG Grid: Community `36.0.0` with Quartz
- AG functionality reference only: `docs/horseshowing/ag-output-references/wec_ag_styled_template_live.html`
- Data contract: `horseshowing_sync?action=wec-schedule-ui`

The copied template CSS must remain byte-for-byte identical. Do not reformat, reorder, rename, minify, merge, or append CSS.

## Local Files

Create only these prototype files plus one contract test:

```text
prototypes/horseshowing/wec-mobile-pro/index.html
prototypes/horseshowing/wec-mobile-pro/rswp-special-rows-grid-template.css
prototypes/horseshowing/wec-mobile-pro/wec-mobile-pro.js
tests/wec_mobile_pro_contract.test.js
```

Do not touch the current user-owned dirty files.

## Chosen Approach

Use an isolated three-file clone: exact copied CSS, handed HTML structure, and a JavaScript clone adapted to the mobile-pro contract.

Rejected alternatives:

1. Extending the shared AG runtime risks replacing the handed full-width-row contract.
2. Inlining everything into one HTML file changes the approved CSS/file format.

## Data and Identity

Use these read-only views:

| Surface | List | Detail identity |
|---|---|---|
| Schedule | `view=overview` | `rowKey` |
| Classes | `view=class_list` | `view=class_detail&rowKey=...` |
| Entries | `view=entry_list` | `view=entry_detail&entryDayKey=...` |
| Rings | `view=ring_list` | `view=ring_detail&ringKey=...` |
| Results | `view=results_list` | `view=result_detail&resultKey=...` |
| Alerts | `view=alerts_list` | `view=alert_detail&triggerKey=...` |

The overview is flat chronological `rows[]`. Each class repeats ring state and may include compact `entryRollups`. Full records come from detail requests. Never guess fallback identities.

## Locked UI and AG Behavior

- Preserve the handed switchers, drawers, special-row DOM classes, and full-width renderer construction.
- Use `getRowId` with persisted `rowKey`.
- Use required value getters and formatters.
- Preserve conditional row sizing and row class rules.
- Use Grid API row updates and stable-ID anchor navigation.
- Build dynamic ring ANCHOR controls.
- FOCUS is a screen-only prebuilt filter; keep its matching rule isolated for later data review.
- FILTER is screen-only, multi-select OR behavior, and uses only confirmed fields.
- HIDE persists saved `rowKey` values in browser local storage and affects PRINT.
- PRINT is a separate three-column sheet. It ignores FOCUS and FILTER but excludes saved HIDE rows.
- Switchers expose class, entry, ring, results, and alerts list/detail states.

## Getters and Formatters

Getters must own class labels, search text, ring identity, FOCUS grouping inputs, and prepared status values. Formatters must own time/minute labels, counts, status labels, null placeholders, and drawer presentation. Preserve text class numbers such as `812b` and class names beginning with `$`. Missing values remain null/blank rather than artificial zeroes.

## Error Handling

- Show explicit loading, empty, and error states.
- Do not substitute sample data after a live request fails.
- A failed list/detail request must not disable the base schedule.
- Prevent an older detail response from replacing a newer selection.
- Do not retry commands or requests to force external state changes.

## Test-First and Verification Contract

Before implementation, add a failing contract test for:

1. exact CSS SHA-256;
2. three-file structure and pinned dependencies;
3. special-row/full-width renderer markers;
4. stable IDs, getters, formatters, row sizing, class rules, and Grid API updates;
5. five list/detail views and identity parameters;
6. three-column PRINT separated from FOCUS/FILTER and tied to HIDE;
7. dynamic ring ANCHOR behavior;
8. explicit loading, empty, and error states.

Then implement the minimum code to pass, serve locally, and verify the live read-only overview, switchers, drawers, focus, filters, hide, anchors, and print behavior in a browser.

## Completion Boundary

The prototype is ready for user iteration when tests pass, the CSS hash is unchanged, the live overview renders locally, available lists/details open correctly, PRINT/HIDE and screen-only filter boundaries are verified, ANCHOR works, and existing files remain untouched.
