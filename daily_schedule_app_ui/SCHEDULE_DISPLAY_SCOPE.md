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

## Bottom Navigation

The app uses a fixed bottom navigation to define the primary screens. These destinations are locked because they map to the schedule skeletons and scoped variants already defined here.

Bottom nav order:

```text
START | PRO | HORSES
```

Rules:

- Bottom nav changes the primary screen, not the row styling.
- Every destination that renders schedule rows must reuse the locked schedule skeleton.
- Do not create a separate visual language for Start, Pro, Horses, Start subviews, modals, filtered rows, or drilldowns.
- Status controls, ring rails, horse rails, AM/PM/1UP quick filters, and the Team toggle remain in-page controls; they are not bottom nav items.
- Flyups/modals are not wired yet, but their future schedule content must reuse the same class row and trip rollup components.

### START

Default screen.

Purpose:

```text
device session start / endpoint trigger hub
```

Primary display:

```text
show context -> start/restart session -> endpoint trigger buttons
```

Start is the entry point for beginning a user device session.

Start session behavior:

- `Start Session` starts a new session on the user's device.
- Starting a session pulls the needed show/app details from the required endpoint(s).
- Pulled details are saved to device memory for 7 days.
- This device memory must survive closing the tab in Apple Edge; do not rely on tab-only state.
- Starting a session sends a webhook back to RingStatus.
- Once a session is active, a `Restart Session` button appears.
- `Restart Session` starts a new session again, refreshes device memory, and sends the session webhook again.

Start trigger buttons:

- Other buttons on Start act as endpoint triggers.
- A trigger button pulls its required endpoint first.
- After the endpoint response is available and device memory is updated as needed, the user is sent to the designated page/subview.
- Trigger buttons should not navigate to a stale page before the endpoint pull completes.
- Trigger buttons should not change the locked row skeleton used by the destination.

Start header/context should show the current show/session identity, for example:

```text
CRT Daily Show
sid 200000054 | 2026-03-15
generated 2026-03-15T22:30:04-04:00
```

Start contains these subviews/sections:

```text
FOCUS
TIME
THREADS
```

These are not bottom nav destinations. They live inside Start.

#### FOCUS

Purpose:

```text
what you need to know now
```

Primary display:

```text
highest-priority current context -> locked class rows -> related trip rollups -> active horse/context detail when needed
```

Focus is a dedicated now-only screen. It should reduce the app to the immediate operational context: current class, next class, active horse/trip matches, first-up items, alerts that matter now, or thread/context items that require current attention.

Focus rules:

- Focus is not the full schedule.
- Focus is not the horse roster.
- Focus is not the alerts feed.
- Focus is a filtered/current attention surface.
- Any schedule row inside Focus must use the locked class row and trip rollup skeleton.
- Active-horse-detail may appear in Focus, but it is app-wide context display, not the editable HORSES profile UI.

#### TIME

Purpose:

```text
schedule sorted by time across all rings
```

Primary display:

```text
time-sorted class rows -> trip rollups
```

This screen uses Schedule By Time exactly as defined below. It must not use different token padding, class number styling, class type styling, time styling, or rollup styling.

#### THREADS

Purpose:

```text
saved context / comments / outbound share
```

Primary display:

```text
saved schedule context -> related class rows -> related trip rollups -> attached comments
```

Threads are saved by long-pressing a screen/context inside Start or elsewhere. Threads may originate from Start, Time, Schedule, Horses, Pro, selected ring, selected class, selected trip rollup, selected rider, selected group, or alerts context.

Thread rules:

- A Thread is saved by user action; it is not an automatic notification.
- A Thread can carry user comments.
- A Thread can include an email-out action connected only to the user's mobile device flow.
- Any schedule records inside a Thread must use the same class row and trip rollup components.
- Thread detail must not introduce a different schedule table style.

### PRO

Purpose:

```text
full schedule by ring
```

Primary display:

```text
ring card -> class cards -> trip rollups
```

This screen uses Schedule By Ring exactly as defined below. Ring rail buttons act as horizontal anchors, not data filters.

PRO detail behavior:

- PRO contains detail modals for selected schedule entities.
- Detail modals can open from class rows, trip rollups, ring context, class context, or related dataset context.
- Clicking a class row opens a class overview modal.
- The class overview modal includes a link to the class detail page.
- Clicking a trip rollup opens a trip overview modal.
- The trip overview modal includes links to horse detail and rider detail.
- A PRO detail modal must include a clear close action.
- A PRO detail modal may include a link to the actual full detail page.
- The full detail page must show a clear `<-- back` control.
- The `<-- back` control from a PRO-originated detail page must point back to PRO.
- Schedule rows or rollups shown inside a modal or full detail page must reuse the locked skeleton.
- Detail modals and full detail pages must not create alternate token sizing or alternate schedule table styling.

Class overview modal:

- Opens when the user clicks/taps a class row in PRO.
- The modal is an overview, not the full class detail page.
- The modal must include a clear close action.
- Closing returns the user to the same PRO scroll/context state.
- The modal must include a link to the actual class detail page.
- The class detail page must include a clear `<-- back` control pointing back to PRO.
- The modal should show the selected class identity:

```text
ring
time
class_number
class_name
class_type
status
schedule_sequence_type-derived class_name shade
```

- The modal should show related trip rollups using the locked trip rollup skeleton:

```text
horse | time | order
```

- The modal may include actions:

```text
Close
Class Detail
Save to Thread
Share to person
```

- `Save to Thread` creates or updates a Thread from this selected class context.
- `Share to person` opens the user's device message composer with rendered class information prefilled; the user must choose/confirm recipient and manually send.
- The modal must not create a larger card-heavy class table style.
- The class identity line and related trip rollups must stay visually compatible with the locked PRO skeleton.

PRO lookup behavior:

- Ring eyebrow may include an app-native RingStatus lookup action.
- The action opens a lookup command modal.
- The lookup command modal lists valid supported command combinations for the current context.
- Command options may be generated from current ring, visible horses, visible riders, and supported backend lookup vocabulary.
- Selecting a command calls a RingStatus lookup endpoint from inside the app.
- The endpoint response should render in-app, not require device SMS.
- Expected response shape follows the existing SMS service concept: `As of ...`, `Now`, `Next`, and `Following`.
- The response may be saved to Threads or linked to related class/trip/horse/rider detail later.
- Do not show unsupported command combinations.
- Do not silently send SMS from this action.
- A future SMS compose helper may exist as a separate assisted action, but app-native lookup is the preferred in-app flow.
- The final prepopulated helper action can support texting the rendered information to another person.
- This share-to-person action should open the user's device message composer with the selected information prefilled.
- The user must choose/confirm the recipient and manually send.
- Share-to-person must not be confused with RingStatus lookup, RingStatus alerts, subscriptions, or backend notifications.

PRO print/PDF task:

- PRO must include a print button.
- The print button prints/exports the PRO schedule to PDF.
- The PDF layout is a fully rendered schedule by Ring.
- The printed page target is 8.5 x 11 paper.
- The print layout is two columns.
- The print layout must be usable from phone and desktop.
- The phone preview is not a mobile reading layout.
- The phone preview is a print-page preview showing what the final printed page will look like.
- On phone, the user may need to scroll vertically and horizontally to inspect the full print page preview.
- Do not collapse the print preview into a phone-optimized single-column schedule.
- Expect obstacles around browser print behavior, mobile PDF preview, scaling, page size, and preserving the two-column layout. Treat this as a dedicated implementation task, not a side effect of the normal PRO screen.
- The print/PDF layout may use print-specific CSS, but schedule row geometry and token language must remain consistent with the locked skeleton unless print constraints explicitly require a documented exception.

### HORSES

Purpose:

```text
horse scoped schedule
```

Primary display:

```text
horse filter rail/search -> matching full class rows -> related trip rollups
```

Horses is the primary team-related screen for showing only rows with related trip rollups and then narrowing by horse. It must still show the full class context: time, ring, class_number, class_name, class_type, status band, and trip rollups.

Rows without related trip rollups can be hidden on this screen, but the rows that remain must keep the same skeleton.

### ALERTS

Alerts are not notifications.

Alerts are an endpoint feed rendered in the app:

```text
alerts endpoint -> alerts feed screen/surface -> alert rows/actions
```

Alerts may be reachable from Start, Thread, or a secondary surface. They are not currently a locked bottom-nav destination.

Alert rules:

- Do not treat alerts as device push notifications.
- The app renders alerts generated by the backend/feed.
- Alert rows may include an SMS out button.
- The SMS out button assists setup to the user's mobile device; it does not silently send hidden notifications.
- Alert rows may include a subscribe icon.
- The subscribe icon acts as an email subscribe request.
- Upon receipt, RingStatus backend applies the subscription state.
- If the user is already receiving the alert/email subscription, the subscribe icon must already appear active.
- If an alert includes schedule context, that context must render with the same locked class row and trip rollup skeleton.

### Datasets That May Need Views

These datasets must be acknowledged in the app model even if they do not become bottom-nav destinations:

```text
RIDERS
GROUPS
CLASSES
RINGS
RESULTS
```

Potential roles:

- RIDERS: searchable/filterable person scope, likely related to Horses and Thread.
- GROUPS: class grouping scope, useful for schedule drilldown and status rollup.
- CLASSES: class-number/class-name scope, useful for detail modal and direct lookup.
- RINGS: schedule anchor scope, currently represented by the Pro screen and ring rail.
- RESULTS: future completed/results scope; not required for the current skeleton but must not force a separate row style later.

If any of these become a page, modal, search result, or filter target, schedule rows inside them must reuse the same locked class row and trip rollup skeleton.

### Non-Nav Surfaces

These are not bottom nav destinations:

```text
FOCUS
TIME
THREADS
NOW / NEXT / DONE
1UP / AM / PM
Team toggle
ring rail
horse rail
search / lookup
detail modal
alerts feed
status legend
settings/help
```

These remain controls, overlays, or secondary surfaces inside the primary destinations.

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
