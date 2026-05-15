# Schedule Display Scope

Locked: 2026-05-15

This document locks the compact schedule display system for `daily_schedule_app_ui`.
It applies to schedule-by-ring, schedule-by-time, future filtered views, future detail modals, and any page that renders these same schedule records.

This is a UI/display contract only. Do not change Airtable extraction, source workflow, source keys, runner behavior, or feed generation from this scope.

## Non-Negotiable Rule

The schedule skeleton is reusable and identical everywhere.

Any new page, modal, filter state, detail surface, or alternate schedule view must reuse the same row geometry, token geometry, typography, radius, column widths, and rollup row structure. Do not create a visually similar but separately styled version.

If a class row appears in Ring view, Time view, a modal, a filtered view, or a future page, its columns, token sizing, and rollup styling must remain interchangeable.

## Source Of Truth

Current preview:

```text
render/visual_identifier_preview.html
```

Current builder:

```text
build_visual_identifier_preview.js
```

Current visual contract:

```text
visual_identifier_contract.json
```

The preview builder is the current implementation reference. This document records the design intent so future work does not drift.

## Views

### Schedule By Ring

The Ring view groups classes under a ring card. The ring card header shows:

```text
ring_nickname | NOW NEXT DONE
```

The status actions are right justified in the eyebrow area. They are both legend and tap-toggle filters. Tap once activates; tap again clears. They do not replace row styling.

Each class card contains:

```text
class-line
trip-rollups
```

The status outline belongs to the whole class card, not only the class line. The outline must wrap the class line and its trip rollups as one unit.

### Schedule By Time

The Time view uses the exact same row and rollup components as Ring view. It is not allowed to have its own token styling, class number styling, rollup styling, or status control styling.

The Time view header also shows:

```text
Time | NOW NEXT DONE
```

The Time eyebrow status controls must be exact matches to the Ring eyebrow controls.

## Column Skeleton

Ring view class rows keep this strict column shape:

```text
time | ring | class_number | class_name | class_type | status | trips
```

Time view class rows keep this strict column shape:

```text
time | ring | class_number | class_name | class_type | status | trips
```

The visual implementation currently renders status in the row outline/eyebrow controls and trips as the child rollup line. The skeleton still reserves the same conceptual fields.

Every line keeps the same minimum widths even when values are empty.

Current CSS grid:

```css
--schedule-cols: minmax(8ch, 8ch) 4.5ch 4ch minmax(0, 1fr) 4ch;
```

The rendered columns are:

```text
time | ring_abbrev | class_number | class_name | class_type
```

The status/trips behavior is attached to the band and child rollups, not extra visual columns in the current compact pass.

## Row Geometry

Shared schedule rows:

```css
display: grid;
grid-template-columns: var(--schedule-cols);
column-gap: 3px;
row-gap: 3px;
align-items: center;
min-height: 38px;
padding: 8px 10px;
```

Ring class cards own their outer spacing:

```css
.class-card.schedule-band {
  display: grid;
  row-gap: 3px;
  padding: 8px 10px;
}
```

Inside a Ring class card, the class line removes duplicate padding:

```css
.class-card .schedule-line {
  min-height: 22px;
  padding: 0;
}
```

This keeps equal top padding above the class line and bottom padding under the trip rollup while keeping only a small gap between class line and trip rollup.

## Status Bands

Status band outlines are outline-only. Do not shade/fill the band background for status.

Current status outline behavior:

```text
NOW  -> green outline
NEXT -> blue outline
DONE -> muted/slate outline
```

DONE/completed rows remove schedule sequence color from `class_name`; they should render as normal completed text, not active sequence shade.

Time text must use the same status palette behavior as class name. A done time should visually calm down the same way a done class name does.

## Time Cell

Time is not a pill.

Time is a fixed column with a clock glyph subcolumn and a fixed text subcolumn:

```css
grid-template-columns: 11px minmax(6ch, 6ch);
column-gap: 3px;
justify-content: end;
min-height: 22px;
height: 22px;
padding: 0;
```

Clock SVG:

```css
width: 11px;
height: 11px;
```

Time font:

```css
font-family: "Roboto Mono", "SFMono-Regular", Consolas, "Liberation Mono", monospace;
font-size: 12px;
font-weight: 560;
line-height: 1.35;
```

The time text aligns by the right edge of the time column so `7:45A`, `10:00A`, and `11:45A` line up by the final AM/PM character. Do not use `&nbsp;` to force alignment.

## Class Name

Class name is plain text, not a pill.

Current class name styling:

```css
min-height: 22px;
height: 22px;
display: flex;
align-items: center;
font-size: 11px;
font-weight: 560;
padding-left: 3px;
overflow: hidden;
text-overflow: ellipsis;
white-space: nowrap;
```

Schedule sequence type is not rendered as its own pill column. It colors the class name text only:

```text
Over Fences -> OVF -> teal class_name text
Under Saddle/Flat -> U/S -> violet class_name text
```

For DONE/completed rows, remove sequence color from class name.

## Tokens

All row tokens use the shared token radius:

```css
--token-radius: 6px;
```

Schedule row tokens must conform to:

```css
min-height: 20px;
padding: 1px 4px;
font-size: 9.5px;
font-weight: 560;
line-height: 1;
border-radius: var(--token-radius);
```

This applies to:

```text
ring token
class_number token
class_type token
```

`class_type` must not drift. It uses the shared `.cell-token` geometry and only changes shade by class type.

Current class type shades:

```text
HUN -> teal
EQ  -> violet
JMP -> amber
```

Ring abbreviation tokens use the ring identity palette and the same geometry as other schedule tokens.

## Ring Abbreviations

Ring abbreviations live in `visual_identifier_contract.json`.

Examples currently tested:

```text
Ring 6   -> R6
Intl     -> INTL
Grand    -> GRA
Hunter 1 -> H1
Derby    -> DER
```

The token must adapt to short and longer abbreviations without changing radius, font size, padding, or row height.

## Trip Rollups

Trip rollups are children of the class card. They must visually read as subordinate to the class line.

Trip rollup shape:

```text
horse | time | order
```

Do not render:

```text
In:
Walk:
```

Current rollup example:

```text
Darcy | 10:45A | 2/22
```

Rollup container:

```css
display: flex;
justify-content: flex-end;
gap: 6px;
overflow-x: auto;
padding: 0;
```

Rollup row:

```css
display: inline-grid;
grid-template-columns:
  minmax(0, max-content)
  minmax(calc(6ch + (var(--rollup-cell-x) * 2)), calc(6ch + (var(--rollup-cell-x) * 2)))
  minmax(calc(5ch + (var(--rollup-cell-x) * 2)), calc(5ch + (var(--rollup-cell-x) * 2)));
height: 20px;
font-size: 9px;
font-weight: 560;
border-radius: var(--token-radius);
padding: 0;
```

Rollup cell padding:

```css
--rollup-cell-x: 7px;
padding: 0 var(--rollup-cell-x);
```

Horse:

```text
auto width
max visual width based on 8ch plus cell padding
ellipsis when too long
```

Time:

```text
fixed 6ch content width plus cell padding
Roboto Mono
never ellipsis
never wraps
```

Order:

```text
fixed 5ch content width plus cell padding
text-style numeric display
never ellipsis
never wraps
```

Trip rollup cells must include visual separators between data points. The separators belong between horse/time/order cells, not as free-floating text.

If there is one rollup, it justifies right. If there are multiple rollups, they remain in the same horizontal strip and slide horizontally when needed.

## Horizontal Rails And Filters

Ring rail:

```text
horizontal, one row, slides when longer than viewport
acts as anchor navigation, not a data filter
```

Horse rail:

```text
horizontal, one row, slides when longer than viewport
acts as on-page filter
shows only that horse and its related full group/class/entry/trip/rollup context
hides unrelated rows
```

Quick filters:

```text
1UP
AM
PM
Team
```

Team is a left-right toggle switch with label on the right. There is no ON/OFF text.

Filters are tap toggles:

```text
tap active item -> active
tap same active item again -> inactive
```

Active state must be visible.

## Future Click Targets

Future interaction coverage must support:

```text
tap class line -> class detail modal
tap individual trip rollup -> trip/detail modal
```

Example with one class and two rollups has three target concepts:

```text
class 411
Knox rollup
Poptart rollup
```

Do not wire flyups yet unless explicitly asked. This scope only locks the display and future target requirements.

## Typography Summary

Current compact display type:

```text
class_name: 11px / 560 / Inter
time:       12px / 560 / Roboto Mono
tokens:     9.5px / 560 / Inter
rollups:    9px / 560 / Inter, except rollup time uses Roboto Mono
```

Use `font-variant-numeric: tabular-nums` for numeric token and rollup alignment where present.

Do not increase weight independently for class number, ring token, class type, or rollup values. Identical token types must remain identical.

## Page And Modal Reuse Requirement

Any future page or modal that displays schedule rows must import or replicate this exact skeleton:

```text
time column
ring token
class_number token
class_name text
class_type token
status band behavior
trip rollup child row
```

No modal-specific larger token padding.
No filter-specific alternate class number styling.
No Time-view-specific token styling.
No Ring-view-specific class type styling.
No separate rollup table style.

When a row is reused elsewhere, only container placement may change. The internal row and rollup system must not change.

## Verification

Run:

```powershell
node --check .\daily_schedule_app_ui\build_visual_identifier_preview.js
node --test .\daily_schedule_app_ui\build_visual_identifier_preview.test.js
node .\daily_schedule_app_ui\build_visual_identifier_preview.js
```

Preview:

```text
http://127.0.0.1:8765/render/visual_identifier_preview.html
```
