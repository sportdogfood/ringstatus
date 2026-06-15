# WEC Comments and Observations Lane

Version: 2026-06-14 v0.1

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
- `add-comment`
- `list-comments`

Current verified comment write:

- A live `add-comment` call created a `wec_comments` record.
- The record linked back to:
  - `rings`
  - `classes`
  - `entries`

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
- `add-comment` writes linked comments to Airtable.
- Verified live `comment-state` route returned 26 classes and 444 entries.
- Verified live mixed class rendered 24 entries with 2 CWF-marked entries.
- Verified live comment write linked ring/class/entry records.
- Created stable iframe embed file for Webflow.

## What Still Needs To Be Built

- Replace old `wec-sessions` Webflow inline test embed with hosted iframe embed.
- Create scoped comment tables: `wec_ring_comments`, `wec_class_comments`, `wec_entry_comments`.
- Decide whether `wec_comments` remains master/audit or is replaced by scoped tables.
- Create `wec_observations` table.
- Add bottom-sheet prompt UI.
- Add prompt helper table, likely `wec_comment_prompts` or `wec_observation_prompts`.
- Add prompt selection logic based on current ring/class/entry timing.
- Add browser geolocation gate.
- Add focus-day geo pin table or fields.
- Add session geo fields and audit logic.
- Add observation write endpoint/action.
- Feed observations back into timing calculations.
- Confirm `wec_alerts` generation and alert windows.
- Tighten `class_start_times.current_horse`, `current_entry_no`, `n_to_go`, `n_gone`.
- Tighten `entry_go_times` forecast logic using live progress and observations.
- Ensure all mirrors stay linked on update and new records.
- Add audit view/report for missing linked records.
- Add stale-data checks for core/live timing sources.
- Add final public-page browser verification after Webflow publish.
