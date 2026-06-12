# WEC Airtable Two-Way Edit Contract

Version: 2026-06-12.1

## Source Pattern

Use the HPS two-way Airtable pattern:

```text
browser
  -> Webflow Cloud route
  -> Airtable PATCH
  -> Airtable audit/log row
  -> Airtable automation pushes Catalyst
  -> mobile/print reload from Catalyst schedule-json
```

Do not write directly from browser JavaScript to Airtable.

Reference files:

```text
docs/hps_horses_webflow_airtable_connector_readme.md
webflow-cloud-test/src/pages/hps/horses.js
```

## WEC Route

Server route:

```text
webflow-cloud-test/src/pages/wec-schedule/edit.js
```

Expected deployed path:

```text
/test/wec-schedule/edit
```

Supported actions:

```text
set-focus-day
set-barn-name
```

## Focus Day Edit

Browser payload:

```json
{
  "action": "set-focus-day",
  "show_no": "14906",
  "focus_day": "2026-06-13",
  "source": "wec-mobile"
}
```

Server behavior:

1. Find `focus_show` by exact `show_no`.
2. Validate `focus_day` is inside `show_start` and `show_end` when present.
3. PATCH `focus_show.focus_day`.
4. Create one `wec-logs` row with `log_type=webflow_edit`, `check_name=focus_show`, `workflow_lanes=Helpers`.
5. Airtable automation handles Catalyst sync.

## Barn Name Edit

Browser payload:

```json
{
  "action": "set-barn-name",
  "show_no": "14906",
  "horse": "Cartoon Sd Z",
  "barn_name": "Cartoon",
  "focus_day": "2026-06-12",
  "source": "wec-mobile"
}
```

Server behavior:

1. Prefer `horse_record_id` if the browser has it.
2. Otherwise find `horses` by exact `horse` show-name match.
3. Do not fuzzy match, dedupe, or infer.
4. PATCH `horses.barn_name`.
5. Create one `wec-logs` row with `log_type=webflow_edit`, `check_name=horses_barn_name`, `workflow_lanes=Helpers`.
6. Airtable automation handles Catalyst helper sync.

## Frontend Requirement Before Barn Edit

Do not guess missing barn names from display text alone.

The schedule payload should expose enough identity for each displayed horse:

```text
horse
horse_record_id when available
barn_name or horse_display
barn_name_missing boolean or equivalent
```

Only then should WEC mobile show a barn-name edit control.
