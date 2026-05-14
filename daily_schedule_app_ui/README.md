# Daily Schedule App UI

This folder owns the display/design preview work for the future daily schedule app.

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

## Current Preview

Visual identifier contract:

```text
visual_identifier_contract.json
```

Generated preview:

```text
render/visual_identifier_preview.html
```

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
