# Daily Schedule App UI

This folder owns the display/design preview work for the future daily schedule app.

Current locked UI scope: `2026-05-15.v1.0`

It is separate from `daily_schedule_app_source`, which owns Airtable extraction, flat source lanes, feeds, validation, and calculator provenance.

## Boundary

This UI folder may define:

- visual identifier contracts
- token geometry
- ring/time preview rows
- compact display language
- future detail-card/modal display shapes

This UI folder must not define:

- Airtable source truth
- extraction workflow
- source key rules
- active runner or heartbeat behavior
- final app nesting

## Project Overview

The app is a compact daily schedule display for RingStatus. The base schedule skeleton is locked and must remain visually consistent across `START`, `PRO`, `HORSES`, Start subviews, filters, modals, detail pages, and future lookup surfaces.

Scope `2026-05-15.v1.0` locks the compact schedule system, primary nav, class modal, modal label rows, responsive modal name behavior, and the rule that future schedule surfaces must reuse the same visual geometry.

Primary navigation:

```text
START | PRO | HORSES
```

`START` begins or restarts a device session, stores required session details in device memory for 7 days, sends the session webhook, and hosts subviews such as `FOCUS`, `TIME`, and `THREADS`.

`PRO` is the full schedule surface. It owns the schedule-by-ring view and the locked class/trip overview modal surfaces. The class overview modal is the class detail surface for this UI scope; do not create an additional class detail card or class detail page from it.

`PRO` must also include a print/PDF task for a fully rendered two-column schedule by Ring on 8.5 x 11 paper. Phone preview is a print-page preview, not a phone-optimized reading layout; the user may need to scroll vertically and horizontally to inspect the printed-page layout.

`HORSES` is the feed-backed tenant horse roster/profile surface. Horses default inactive, become active when matched to trips, and active horses are automatically favorited. Unfavorite acts as an ignore attribute that hides horse detail app-wide. Profile editing belongs here only.

App-native RingStatus lookup:

```text
ring eyebrow action -> lookup command modal -> selected command -> lookup endpoint -> in-app response
```

This replaces the SMS-only text flow for in-app use. The modal should list valid supported command combinations, call a RingStatus lookup endpoint, and render the same kind of `As of / Now / Next / Following` response inside the app. SMS compose can remain a future helper, but the primary app behavior should not require texting. A final prepopulated helper action can open the user's message composer when they want to text rendered information to another person; the user must choose/confirm the recipient and manually send.

## Current Preview

Visual identifier contract:

```text
visual_identifier_contract.json
```

Generated preview:

```text
render/visual_identifier_preview.html
```

Locked compact schedule display scope:

```text
SCHEDULE_DISPLAY_SCOPE.md
```

Use this scope before creating any schedule-by-ring, schedule-by-time, filtered, modal, detail, search, or bottom-nav display. The row skeleton, token geometry, rollup geometry, typography, and spacing must remain identical across every surface. The locked bottom nav is `START | PRO | HORSES`; `FOCUS`, `TIME`, and `THREADS` live inside Start.

Build it with:

```powershell
node .\daily_schedule_app_ui\build_visual_identifier_preview.js
```

## Current Display Rules

Ring view class rows keep this strict seven-column shape:

```text
time | ring | class_number | class_name | class_type | status | trips
```

Time view rows reuse the same row and rollup display:

```text
time | ring | class_number | class_name | class_type | status | trips
```

Every line keeps the same columns and minimum widths even when a value is empty.

Status language uses compact text tokens:

```text
NOW NEXT FOL UPC DONE
```

Ring abbreviations live in `visual_identifier_contract.json`.

## Verify

```powershell
node --test .\daily_schedule_app_ui\build_visual_identifier_preview.test.js
node --check .\daily_schedule_app_ui\build_visual_identifier_preview.js
```
