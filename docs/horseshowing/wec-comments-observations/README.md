# WEC Comments and Observations Lane

Version: 2026-06-14 v0.2

## Purpose

This is a separate lane from the locked WEC mobile/print schedule outputs. It uses schedule data as an input, but it should be developed, tested, and released independently.

This lane extends the WEC mobile schedule into a scoped commenting and field-observation workflow.

The schedule remains the main browsing surface. A user can start a session, open a ring, drill into classes, drill into entries, and leave comments or answer structured prompts. The structured prompts are intended to work like Waze-style confirmations: the system asks a small, time-relevant question and the user answers quickly.

The long-term value is not only comments. The comments and observations should help tighten `class_start_times`, `entry_go_times`, and alert timing by confirming what is actually happening ringside.

## Current Customer-Facing Concept

The user opens WEC comments/session UI.

The UI shows:

1. Show and focus day.
2. Active sessions/users.
3. Rings.
4. Classes inside the selected ring.
5. Entries inside the selected class.
6. Comment controls scoped to the selected ring, class, or entry.

The comments UI should follow the same basic scoped nest as `wec-mobile`, but with more depth:

```text
show
> focus_day
>> ring
>>> class
>>>> entry
```

Unlike the mobile schedule rollups, this comments view must show all entries for the class, not only active trainer entries. Active trainer entries are marked visually with a CSS class, not filtered.

## Current Implemented Frontend/Backend Pieces

### Hosted Widget

Route:

```text
https://ringstatus.com/test/wec-schedule/session-widget
```

Source:

```text
webflow-cloud-test/src/pages/wec-schedule/session-widget.js
```

The hosted widget currently:

- starts a session
- assigns or saves `user_name`
- stores the session/device locally
- lists active sessions
- loads nested schedule data from `comment-state`
- renders rings
- renders classes inside a ring
- renders all entries inside a class
- marks active trainer entries with `cwf-entry`
- allows comments scoped to ring/class/entry
- writes comments through the edit API

### Comments State Endpoint

Route:

```text
https://ringstatus.com/test/wec-schedule/comment-state
```

Source:

```text
webflow-cloud-test/src/pages/wec-schedule/comment-state.js
```

This route builds the nested comments model from Airtable mirror tables.

Current source tables:

- `focus_show`
- `class_start_times`
- `class_oog`
- `class_hide`
- `rings`
- `trainers`

Current response shape:

```text
show_no
show_name
focus_day
show_end_date
rings[]
  ring_no
  ring_name
  classes[]
    class_no
    class_number
    class_name
    class_start_time
    start_display
    entries[]
      entry_no
      entry_order
      horse
      horse_display
      rider
      rider_display
      trainer
      trainer_display
      is_cwf
      entry_class
```

Current verified behavior:

- `comment-state` returned `26` classes.
- `comment-state` returned `444` entries.
- `comment-state` returned `26` CWF-marked entries.
- A mixed class rendered all entries and only CWF entries carried `card entry cwf-entry`.

### Edit API

Route:

```text
https://ringstatus.com/test/wec-schedule/edit
```

Source:

```text
webflow-cloud-test/src/pages/wec-schedule/edit.js
```

Current actions:

- `set-focus-day`
- `set-barn-name`
- `hide-classes`
- `start-session`
- `session-heartbeat`
- `list-sessions`
- `ring-checkin`
- `ring-checkout`
- `list-ring-checkins`
- `add-comment`
- `list-comments`
- `add-observation`

Current verified comment write:

- A live `add-comment` call created a `wec_comments` record.
- The record linked back to:
  - `rings`
  - `classes`
  - `entries`

Current verified ring check-in / observation write:

- `ring-checkin` writes `wec_ring_checkins`.
- `ring-checkout` changes the check-in status to `ended`.
- `add-comment` writes to both scoped table and master `wec_comments`.
- Entry comments write to `wec_entry_comments`.
- Comments made while checked in to the same ring carry `source_confidence = first_hand`.
- `add-observation` writes `wec_observations`.
- Observations made while checked in to the same ring carry `source_confidence = first_hand`.
- Observation records link back to `rings`, `classes`, and `entries`.

## Current Webflow Embed

Embed file:

```text
docs/horseshowing/webflow-drops/wec-comments-proof-embed.txt
```

Current embed:

```html
<iframe
  src="/test/wec-schedule/session-widget"
  title="WEC Session Comments"
  style="width:100%;height:620px;border:0;display:block;overflow:hidden;"
  loading="eager"
></iframe>
```

Known issue:

```text
https://ringstatus.com/wec-sessions
```

was still serving an old inline Start Session test embed during the last verification. The hosted widget and backend are live, but the public Webflow page must use the iframe embed above to show the current system.

## Airtable Tables Involved Now

Base:

```text
wec_schedules / app6XS1RvsPNRT6os
```

Current session/comment tables:

- `wec_sessions`
- `wec_comments`
- `wec_ring_checkins`
- `wec_observations`
- `wec_ring_comments`
- `wec_class_comments`
- `wec_entry_comments`

Current comments/prompt config tables:

- `wec_comment_presets`
- `wec_question_templates`
- `waze_users`
- `waze_session_footprints`

Use-case:

- `wec_comment_presets` stores prebuilt comment choices for the UI. Each row is scoped to `ring`, `class`, or `entry` and can optionally target a specific `show_no`, `focus_day`, `ring_no`, `class_no`, or `entry_no`. These rows are operator-managed options that help users tap a known comment quickly. They are not user comment logs.
- `wec_question_templates` stores dynamic prompt templates for observation questions. Each row is scoped to `ring`, `class`, or `entry` and controls the prompt label/text, answer type, choices, sort order, and optional trigger context. Answers generated from these prompts write to `wec_observations`.
- `wec_ring_comments`, `wec_class_comments`, `wec_entry_comments`, and `wec_comments` are output/log tables. They capture what users actually submitted.
- `wec_observations` is the output/log table for prompt answers such as yes/no/unsure checks.
- `waze_users` stores the latest known user identity/display-name record used by WEC Waze-style sessions. It carries current cookie state, latest visit, last known geo target fields, device/session summary fields, and user-agent/viewport context.
- `waze_session_footprints` stores per-event session history. It is the table for tracking session starts, heartbeats, cookie checks, geo checks, ring/class/entry opens, ring check-ins, comments, and observation submissions over time.

Schema/index rule:

- These WEC comments/Waze tables are indexed in `table_index`: `wec_sessions`, `wec_comments`, `wec_ring_checkins`, `wec_observations`, `wec_ring_comments`, `wec_class_comments`, `wec_entry_comments`, `wec_comment_presets`, `wec_question_templates`, `waze_users`, and `waze_session_footprints`.
- Each table has `rec_id` as a formula field using `RECORD_ID()`.
- Each table links to `classes`, `entries`, `shows`, `focus_show`, and `rings`.
- Each table except `waze_users` also links to `waze_users`.
- These links exist so comment/session/observation records can be joined back to the same helper tables used by WEC mobile, WEC print, alerts, and schedule workflows.

User/session tracking split:

- Use `waze_users` for the current user profile/state: `cookie_success`, `cookie_date`, `cookie_expire`, `last_visit`, `geo_lat`, `geo_lng`, `geo_accuracy_m`, `geo_source`, `geo_allowed`, `device_id`, `last_session_id`, `session_count`, `timezone`, `user_agent`, and `viewport`.
- Use `waze_session_footprints` for history. One user can have many footprint records across a day/session. This avoids overwriting the trail of what the user did while still keeping `waze_users` easy to inspect.
- `waze_session_footprints` links to `classes`, `entries`, `shows`, `focus_show`, `rings`, `waze_users`, and `wec_sessions`.
- The geo fields are target fields for browser geolocation or IP-derived location. Browser geolocation should be treated as user-permission based; IP geo should be treated as approximate.

Seed/proof script:

```text
docs/horseshowing/seed-airtable-wec-waze-system.js
```

Use-case:

- Creates or updates one linked seed record in each WEC comments/Waze table.
- Uses one existing show/ring/class/entry/focus_show chain.
- Proves `rec_id` resolves and links populate across `classes`, `entries`, `shows`, `focus_show`, `rings`, and `waze_users`.

Current seed keys:

- `session_id`: `seed_session_wec_waze_14906_20260614`
- `checkin_id`: `seed_checkin_wec_waze_14906_20260614`
- `comment_id`: `seed_comment_wec_waze_14906_20260614_*`
- `observation_key`: `seed_observation_wec_waze_14906_20260614`
- `preset_key`: `seed_preset_wec_waze_ring_14906_20260614`
- `question_key`: `seed_question_wec_waze_entry_14906_20260614`

Current data/model tables used by comments state:

- `focus_show`
- `class_start_times`
- `class_oog`
- `class_hide`
- `rings`
- `trainers`

Current helper/control tables related to rendering and tagging:

- `horses`
- `riders`
- `trainers`
- `entries`
- `classes`
- `rings`
- `ring_days`
- `shows`
- `focus_show`

## Source-of-Truth Model

The expected model is:

- Catalyst/backend workflows own the core computed data.
- Airtable mirrors and helper/control tables are used for inspection, edits, and operator management.
- Webflow embeds should call hosted backend endpoints instead of requiring full embed changes for every logic change.

Current comments/session work is using Webflow Cloud hosted endpoints under:

```text
/test/wec-schedule/*
```

The comments widget should not depend on a static `schedule.json` as its primary source. Static JSON can remain a fallback only.

## Comment Scope Model

Comments should be scoped by where the user is in the nest.

### Ring Comment

Origin:

```text
ring
```

Required links/fields:

- `session_id`
- `user_name`
- `show_no`
- `focus_day`
- `ring_no`
- linked `rings`
- `comment_text`
- `created_at`

### Class Comment

Origin:

```text
ring > class
```

Required links/fields:

- `session_id`
- `user_name`
- `show_no`
- `focus_day`
- `ring_no`
- `class_no`
- linked `rings`
- linked `classes`
- `comment_text`
- `created_at`

### Entry Comment

Origin:

```text
ring > class > entry
```

Required links/fields:

- `session_id`
- `user_name`
- `show_no`
- `focus_day`
- `ring_no`
- `class_no`
- `entry_no`
- linked `rings`
- linked `classes`
- linked `entries`
- `comment_text`
- `created_at`

## Recommended Comments Table Split

For Airtable usability, the comments should be split by scope while optionally keeping a combined audit table.

Recommended tables:

- `wec_comments`
- `wec_ring_comments`
- `wec_class_comments`
- `wec_entry_comments`

Recommended rule:

- Ring view writes to `wec_ring_comments`.
- Class view writes to `wec_class_comments`.
- Entry view writes to `wec_entry_comments`.
- Optional automation mirrors all scoped comments into `wec_comments`.
- Current implementation writes both: one scoped row plus one master `wec_comments` row.

Reason:

The combined table is useful for audit, but separate scoped tables are easier to inspect, filter, link, and automate.

## Waze-Style Prompt Concept

The prompt system should be structured observations, not normal free-text comments.

When a user opens a ring, class, or entry, the system can show a bottom-sheet prompt related to the most relevant current state.

Example:

```text
Is Curtis in the ring?

[ Yes ] [ No ] [ Unsure ]
```

Behavior:

- Prompt flies up from the bottom.
- User can answer.
- User can dismiss with `X`.
- Dismissal is logged but does not affect timing.
- Answer writes a structured observation.
- The sheet should not block schedule browsing.

Recommended new table:

```text
wec_observations
```

Current implementation:

- `wec_observations` exists.
- The hosted widget shows a bottom sheet after selecting a ring.
- The bottom sheet can write `yes`, `no`, `unsure`, or `dismissed`.
- The prompt currently uses selected ring/class/entry context plus class schedule/order fallback.

Recommended fields:

- `observation_key`
- `session_id`
- `user_name`
- `show_no`
- `focus_day`
- `scope`
- `ring_no`
- `class_no`
- `entry_no`
- `entry_order`
- `prompt_key`
- `prompt_label`
- `answer`
- `observed_at`
- `source`
- `distance_miles`
- `confidence_before`
- `confidence_after`

## Time-Relevant Prompt Logic

When a user opens a ring, the system should calculate the most relevant current context using time and live data.

Inputs needed:

- `update_schedule`
- `class_start_times`
- `class_oog`
- `counts`
- `entry_go_times`
- `get_orders`
- `get_rings`
- `wec_alerts`

Expected ring prompt workflow:

1. Determine focus show and focus day.
2. Determine selected `ring_no`.
3. Read current clock time.
4. Find the most relevant class for that ring.
5. Determine current or next expected entry.
6. Ask a direct prompt, such as:

```text
Is [rider/horse] in the ring?
```

7. Use the answer to tighten timing estimates.

Priority for current/next entry:

1. `class_start_times.current_entry_no` / `current_horse`
2. live `get_orders` progress
3. live `get_rings` pace/status
4. `entry_go_times` estimate
5. `class_oog` order fallback

## Data Sources Needed for Timing

### `update_schedule`

Use for:

- class schedule
- `time_text`
- ring/day/class relation
- `live_flag` where present

### `class_start_times`

Use for:

- normalized class start time
- `display_time`
- `current_horse`
- `current_entry_no`
- `n_to_go`
- `n_gone`
- `elapsed_seconds`
- `pace_seconds`
- `live_source`
- `live_flag`

### `class_oog`

Use for:

- full order of go
- `entry_order`
- `entry_no`
- horse
- rider
- trainer
- class/ring/day link context

### `counts`

Use for:

- class entry count fallback
- sanity checks against `class_oog`

### `entry_go_times`

Use for:

- estimated go time per entry
- active trainer schedule
- `time_till`
- confidence/alert windows

### `get_orders`

Use for:

- live class progress
- `n_to_go`
- `n_gone`
- `total`
- `timestamp`
- `elapsed`
- current class state when available

### `get_rings`

Use for:

- live ring status
- ring-level pace
- total/n_to_go/n_gone/elapsed context

### `wec_alerts`

Use for:

- timing-action output
- upcoming prompts
- alert windows
- class start windows
- entry go windows
- tightening candidates

`wec_alerts` should not be the source of truth for core schedule data. It should be generated from `class_start_times`, `entry_go_times`, and live payloads.

## Geo Gate Concept

The system can restrict session/comment access to users near the venue.

Recommended gate:

- Use browser geolocation for the real 5-mile rule.
- Use Geo-IP only as a coarse fallback or diagnostic.

Recommended focus pin source:

- `focus_show`
- or a new `focus_day_geo` table

Recommended fields:

- `show_no`
- `focus_day`
- `venue_name`
- `pin_lat`
- `pin_lng`
- `allowed_radius_miles`

Recommended `wec_sessions` geo fields:

- `geo_status`
- `geo_source`
- `lat`
- `lng`
- `distance_miles`
- `within_radius`
- `geo_accuracy_meters`
- `geo_checked_at`
- `focus_pin_lat`
- `focus_pin_lng`

Recommended cadence:

- On Start Session: request browser location.
- During active session: refresh every 5-10 minutes.
- On comment/observation submit: refresh if older than 5 minutes.
- On browser visibility return: refresh if stale.
- If denied: do not repeatedly ask.

Do not display exact coordinates publicly. Show status or rounded distance only.

## Session Concept

The session identifies a user/device for comments and observations.

Current behavior:

- User starts a session.
- System assigns a guest name if needed.
- User can edit/save a name.
- Session/device info is stored client-side.
- Session is written to Airtable.

Desired behavior:

- Session can be geo-gated.
- Session can heartbeat while open.
- Session can support comments.
- Session can support observations.
- Active sessions can be shown to other users/admins.

## Ring Check-In Concept

Ring check-in identifies that a user is actively watching a ring.

Current implementation:

- A user with a session can check in to a selected ring.
- One active ring check-in per session is supported.
- Checking into another ring ends the prior active check-in.
- A checked-in user is treated as `first_hand` for comments and observations in that same ring.
- A user can leave the ring check-in.

Table:

```text
wec_ring_checkins
```

Key fields:

- `checkin_id`
- `session_id`
- `user_name`
- `show_no`
- `focus_day`
- `ring_no`
- `rings`
- `checked_in_at`
- `last_seen_at`
- `status`
- `source_confidence`
- `source`

## Active Trainer / CWF Marker

The comments entry drilldown must show all entries.

CWF entries are identified using `trainers.active`.

Display rule:

- Do not filter to CWF entries.
- Do not put trainer names before every horse.
- Add a CSS marker/class for active trainer entries.
- Current marker class:

```text
cwf-entry
```

Current visual badge:

```text
CWF
```

## Known Trouble Areas

### Public Webflow Page Still Old

The hosted widget is live, but `https://ringstatus.com/wec-sessions` was still serving the old inline test embed during last verification.

Needed:

- Replace old inline Webflow code with the iframe from `wec-comments-proof-embed.txt`.
- Publish.
- Verify that the public page contains `session-widget`.

### Static JSON Must Not Become Primary

The comments/session system should call hosted endpoints directly.

Static JSON can be fallback only.

### Data Freshness

The timing prompt system depends on fresh core/live data.

Critical workflows:

- core: `update_schedule`, `class_oog`, `counts`
- computed: `class_start_times`, `entry_go_times`
- live: `get_orders`, `get_rings`
- output: `wec_alerts`, `wec_observations`

### Airtable Mirrors Must Stay Linked

When records are created or updated, they should link back to:

- shows
- focus_show
- rings
- ring_days
- classes
- entries
- horses
- riders
- trainers

This is especially important for:

- `class_oog`
- `class_start_times`
- `entry_go_times`
- comments
- observations

### Prompt Logic Must Not Overstate Confidence

If the system cannot confidently identify the current entry, it should show a small candidate set or use a lower-confidence prompt.

Example:

```text
Is one of these entries in the ring?
```

instead of pretending a single entry is certain.

## What Is Done

- Built hosted comments/session widget route.
- Built hosted `comment-state` route.
- `comment-state` uses `class_start_times` plus `class_oog`.
- `comment-state` returns all entries, not only active trainer entries.
- `comment-state` marks CWF entries using `trainers.active`.
- Widget renders ring > class > entry nesting.
- Widget uses `cwf-entry` marker for active trainer entries.
- Widget supports starting sessions.
- Widget supports saving/displaying user names.
- Widget lists active sessions.
- Widget supports ring/class/entry comments.
- Widget supports ring check-in.
- Widget supports ring checkout.
- Widget shows check-in state for the selected ring.
- Widget shows bottom-sheet prompts.
- Widget writes structured observations from prompt answers.
- `add-comment` writes linked comments to Airtable.
- `add-comment` writes to scoped comment tables and master `wec_comments`.
- `ring-checkin` writes `wec_ring_checkins`.
- `ring-checkout` ends active ring check-ins.
- `add-observation` writes `wec_observations`.
- First-hand comments and observations are tagged when session ring check-in matches the comment/observation ring.
- Verified live `comment-state` route returned 26 classes and 444 entries.
- Verified live mixed class rendered 24 entries with 2 CWF-marked entries.
- Verified live comment write linked ring/class/entry records.
- Verified live ring check-in write.
- Verified live ring checkout.
- Verified live scoped entry comment write.
- Verified live observation write.
- Verified UI ring check-in control.
- Verified UI bottom-sheet prompt render.
- Verified UI prompt answer writes an Airtable observation.
- Created stable iframe embed file for Webflow.

## What Still Needs To Be Built

- Replace old `wec-sessions` Webflow inline test embed with hosted iframe embed.
- Decide whether `wec_comments` remains master/audit long term.
- Add prompt helper table, likely `wec_comment_prompts` or `wec_observation_prompts`.
- Improve prompt selection logic using live timing confidence.
- Add browser geolocation gate.
- Add focus-day geo pin table or fields.
- Add session geo fields and audit logic.
- Feed observations back into timing calculations.
- Confirm `wec_alerts` generation and alert windows.
- Tighten `class_start_times.current_horse`, `current_entry_no`, `n_to_go`, `n_gone`.
- Tighten `entry_go_times` forecast logic using live progress and observations.
- Ensure all mirrors stay linked on update and new records.
- Add audit view/report for missing linked records.
- Add stale-data checks for core/live timing sources.
- Add final public-page browser verification after Webflow publish.
