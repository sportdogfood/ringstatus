# Codex Handoff: Daily Schedule App UI

Scope version: `2026-05-15.v1.0`

Workspace: `C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\daily_schedule_app_ui`

This is the UI/display project only. Do not change `daily_schedule_app_source`, Airtable extraction, source keys, runner behavior, feed generation, or backend workflows from this workspace.

## Current Source Of Truth

- Scope doc: `daily_schedule_app_ui/SCHEDULE_DISPLAY_SCOPE.md`
- Visual contract: `daily_schedule_app_ui/visual_identifier_contract.json`
- Preview builder and CSS source: `daily_schedule_app_ui/build_visual_identifier_preview.js`
- Generated preview: `daily_schedule_app_ui/render/visual_identifier_preview.html`

Build preview:

```powershell
node .\daily_schedule_app_ui\build_visual_identifier_preview.js
```

Serve preview:

```powershell
cd .\daily_schedule_app_ui
python -m http.server 8765 --bind 127.0.0.1
```

Preview URL:

```text
http://127.0.0.1:8765/render/visual_identifier_preview.html
```

## Locked App Shape

Primary nav:

```text
START | PRO | HORSES
```

Start subviews:

```text
FOCUS | TIME | THREADS
```

`PRO` owns the full schedule display, filters, ring/time schedule surfaces, app-native lookup actions, print/PDF preview task, and locked class modal.

`HORSES` owns tenant horse roster/profile behavior. Horses default inactive, become active when matched to trips, and active horses are automatically favorited. Unfavorite is an ignore attribute and should hide horse detail app-wide. Editable horse profiles belong to `HORSES` only.

The class overview modal is locked as the class detail surface for this UI scope. Do not create another class detail card or class detail page from it.

## WEC / HorseShowing Mapping

Use this mapping when adapting WEC HorseShowing schedule/trip data into the locked UI template.

### Ring Row

Template:

```text
time | ring_number | ring_name | trips | gone | left
```

Example:

```text
8:40A | 6 | Ring 6 {42m late} {takes 5m} | 45 | 22 | 23
```

Field mapping:

```text
time        <- ring current/next class start time, normalized compact time
ring_number <- numeric ring id when available, e.g. 6
ring_name   <- display ring name, e.g. Ring 6
trips       <- total trips for current ring block/context
gone        <- trips completed/gone
left        <- trips remaining
```

Conditional ring name suffix:

```text
ring_name {ring_late} {ring_takes}
```

Only render each brace segment when its source exists.

Examples:

```text
Ring 6
Ring 6 {42m late}
Ring 6 {takes 5m}
Ring 6 {42m late} {takes 5m}
```

### Group / Class Row

Template:

```text
time | class_number | class_name | type
```

The class name spans the two removed metric columns. Do not render empty metric cells in this row.

Example:

```text
8:40A | 411 | Small Pony Hunter U/S | HUN
```

Field mapping:

```text
time         <- class scheduled/current time
class_number <- class number, e.g. 411
class_name   <- class display name
type         <- class_type abbreviation, e.g. HUN, EQ, JMP
```

### Trip Rows

Template:

```text
time | entry_number | horse + rider | order | in_or_ends | leave
```

Example:

```text
8:55A | 10002 | Knox + Lainey | 5 | 15m | 2m
```

Field mapping:

```text
time         <- trip go time
entry_number <- entry number; fixed-width visual slot regardless of digits
horse + rider <- horse name, plus rider name if available
order        <- order of go number only, e.g. 5, not 5/14
in_or_ends   <- starts_in for upcoming/next context, or ends_in for current/live context
leave        <- leave_in / walk-in lead time; not ring walk_time
```

Important distinction:

```text
ring walk_time  != trip leave_in / walk_in
```

`walk_time` is ring-level, e.g. `takes 5m`. `leave_in` is trip-level, e.g. `2m`.

## Main Schedule Skeleton

Ring rows must keep strict columns:

```text
time | class_number | class_name | class_type | status | trips
```

Time rows must keep:

```text
time | ring | class_number | class_name | class_type | status | trips
```

Current preview implementation uses:

```css
--schedule-cols: minmax(8ch, 8ch) 4.5ch 4ch minmax(0, 1fr) 4ch;
```

Do not create separate Ring-vs-Time token styling. Identical is identical.

## Status And Tokens

Status tokens:

```text
NOW NEXT FOL UPC DONE
```

Status lives in the Ring/Time eyebrow controls and as row color semantics. The status pill is not a row column in the current skeleton.

Schedule sequence type does not render as a pill column. It shades the `class_name` text by sequence type.

Class type uses a token in the final slot:

```text
HUN | EQ | JMP
```

Ring abbreviations are in `visual_identifier_contract.json`.

## Class Modal / Flyup Contract

The class modal is the locked class detail surface. Treat it as a flyup/modal, not a separate page and not a larger card.

Header:

```text
class_status | Class Overview | X close icon
```

Close is a simple SVG X icon button with `aria-label="Close"`.

Body sections:

```text
Ring summary
Group/Class summary
Trips
```

The visible `RING`, `GROUP`, `TRIPS` section labels are hidden. Each section owns its own label row.

Label rows:

```text
Ring:  Time | No | Name | Trips | Gone | Left
Group: Time | No | Name | Type
Trips: Time | No | Name | Order | In or Ends | Leave
```

Labels are left-justified and live in the same slots as the values they describe.

At viewport `<390px`:

```text
data row
empty-time | name
```

The `Name` label is hidden at this breakpoint. The name value drops to row 2 starting at the second column.

Actions:

```text
Save to Thread
Share
```

No `Class Detail` action. No additional class detail card or page.

## Trip Flyups

Trip flyups are not wired yet. When implemented, they must reuse the same modal geometry and token system.

Expected trip overview source row:

```text
time | entry_number | horse + rider | order | in_or_ends | leave
```

Trip modal may link to:

```text
horse profile/detail
rider detail
```

But those links must not introduce a new schedule table style.

## Lookup Flyups

Ring eyebrow may include an app-native lookup action later.

Target flow:

```text
ring eyebrow action -> lookup command modal -> selected command -> lookup endpoint -> in-app response
```

This replaces SMS-only interaction inside the app. SMS compose can remain a helper only when the user wants to text rendered information to another person; the user chooses/confirms recipient and manually sends.

Lookup modal should list supported command combinations generated from current context:

```text
ring
visible horses
visible riders
backend-supported lookup vocabulary
```

Do not show unsupported combinations.

## CSS Contract

The full CSS source of truth is embedded in `build_visual_identifier_preview.js` and emitted into `render/visual_identifier_preview.html`. A new Codex runner should edit the builder, not the generated preview.

### Key Variables

```css
--token-radius: 6px;
--schedule-cols: minmax(8ch, 8ch) 4.5ch 4ch minmax(0, 1fr) 4ch;
--modal-overview-cols: minmax(8ch, 8ch) 6ch minmax(0, 1fr) 5ch 6ch 6ch;
```

### Modal Label CSS

```css
.modal-label-row {
  display: grid;
  grid-template-columns: var(--modal-overview-cols);
  column-gap: 3px;
  align-items: center;
  color: var(--faint);
  font-size: 12px;
  font-weight: 560;
  line-height: 1;
  text-transform: uppercase;
  white-space: nowrap;
}

.modal-label-cell {
  min-width: 0;
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  font-size: 8px;
}

.modal-label-time,
.modal-label-number,
.modal-label-name,
.modal-label-order,
.modal-label-starts,
.modal-label-leave {
  text-align: left;
}
```

### Modal Row CSS

```css
.modal-output-section .schedule-line {
  grid-template-columns: var(--modal-overview-cols);
  min-height: 22px;
  padding: 0;
}

.modal-name-span {
  grid-column: span 3;
}
```

### Modal Breakpoint

```css
@media (max-width: 390px) {
  .modal-output-section .schedule-line {
    grid-template-columns: minmax(8ch, 8ch) 6ch 5ch 6ch 6ch;
    row-gap: 2px;
  }

  .modal-label-row {
    grid-template-columns: minmax(8ch, 8ch) 6ch 5ch 6ch 6ch;
  }

  .modal-label-name {
    display: none;
  }

  .modal-class-line .class-name-col {
    grid-column: 2 / -1;
    grid-row: 2;
    height: 18px;
    min-height: 18px;
    padding-left: 0;
  }

  .modal-name-span {
    grid-column: 2 / -1;
  }
}
```

### Modal Close Button CSS

```css
.modal-head {
  min-height: 38px;
  display: grid;
  grid-template-columns: minmax(56px, max-content) minmax(0, 1fr) 24px;
  align-items: center;
  gap: 8px;
  padding: 7px 8px;
  border-bottom: 1px solid var(--line);
}

.modal-action--icon {
  width: 24px;
  min-width: 24px;
  padding: 0;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  justify-self: end;
}

.modal-action--icon svg {
  width: 12px;
  height: 12px;
  fill: none;
  stroke: currentColor;
  stroke-width: 1.8;
  stroke-linecap: round;
}
```

## Do Not Drift

Do not add:

```text
larger modal-specific tokens
separate class detail cards
separate class detail pages
Ring-specific token padding
Time-specific token padding
new row skeletons for future filters
card-heavy class detail layouts
alternate rollup tables
```

If a new WEC/HorseShowing field does not fit, map it into the existing slot grammar first. Only add a new slot after updating `SCHEDULE_DISPLAY_SCOPE.md`, `visual_identifier_contract.json`, tests, and the preview builder together.
