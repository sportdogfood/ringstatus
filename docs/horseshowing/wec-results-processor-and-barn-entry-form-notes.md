# WEC Results Processor And Barn Entry Form Notes

## Purpose

This note captures the current Step 6 results processor and the related barn-entry quick form direction.

It does not replace the full workflow lock. It documents the current behavior and the next form pattern so the work does not restart from memory.

## Results Processor

Action:

```text
wec-step6-results
```

Gate:

```text
focus_show.results_enabled = true
```

Source tables:

```text
hs_class_start_times
hs_entry_go_times
```

Result endpoint:

```text
show_results4.php
```

Source request rule:

```text
send native class_no only
do not send internal keys upstream
```

Current key behavior:

```text
result_queue_key = class const key
result_class_key = class const key
class_result_key = entry const key + result identity
```

Current result fields:

```text
place
entry_no
horse
rider
owner
score
time
prize
```

Parsing rule:

```text
place is parsed when present
score is parsed only when the source has a Score column
time is parsed only when the source has a Time column
prize is parsed only when the source has a Prize/Money column
blank source values stay blank
no fake result rows
```

Latest verified state:

```text
show_no = 14909
focus_day = 2026-07-05
results_enabled = true
class_const_key_rows = 69
entry_const_key_rows = 38
our_rider_scoped_class_count = 24
check_results_count = 24
bounded run limit = 3
completed_classes = 3
class_results = 20
fake_results_created = 0
step_1_5_run = false
result_alerts_run = false
```

Known schema note:

```text
Catalyst hs_class_results has place, score, prize.
Airtable hs_class_results now has time.
Catalyst hs_class_results.time still needs to be confirmed/created before Catalyst can persist time directly.
Current code safely avoids breaking if Catalyst time is missing.
```

## Barn Entry Quick Form

Existing prototype:

```text
prototypes/horseshowing/barn-entry-helper-lookup-prototype.html
```

Relevant handoff pattern:

```text
Webflow page
-> embed/config
-> frontend JS/CSS
-> Webflow Cloud or approved server route
-> Airtable write target
-> change-log/audit row if used
-> UI refresh/print review
```

Do not expose Airtable tokens in browser code.

## Proposed Form Flow

Default scope:

```text
use default active focus_show
load only rings for focus_show
```

Ring lookup:

```text
dropdown/typeahead
search ring_name and ring_name_normalized
allow misspell/alias matching with Fuse or equivalent
show immediate suggestions
expected user input: ring_name
```

On ring select:

```text
load horses for focus_show + ring_day_no + ring_no
```

Horse lookup:

```text
dropdown/typeahead
search barn_name and show_name
allow misspell/aka matching
show immediate suggestions
expected user input: barn_name
```

On horse select, add one line:

```text
time | ring | class_no | horse
```

Add behavior:

```text
[add] clears the lookup form and prepares for the next row
already-added entries are hidden from suggestions
already-added rows cannot be added again
```

Save behavior:

```text
save writes the selected rows to the approved Airtable/server target
after save, show printable Barn Entry Review
```

## Minimum Data Needed For Form

Ring suggestion row:

```text
show_no
focus_day
ring_day_no
ring_no
ring_name
ring_name_normalized
ring_name_prioritized
```

Horse/class suggestion row:

```text
show_no
focus_day
ring_day_no
ring_no
class_no
class_start_time
ring_name_normalized
barn_name
show_name
horse
entry_no
```

## Handoff Rules

- Confirm whether the target is prototype, Webflow page, Webflow Cloud route, or Airtable webhook before changing implementation.
- Browser can render/search, but secrets and durable writes belong behind an approved server route or webhook.
- Do not redesign the form while wiring data.
- Do not invent extra fields or lanes before the ring/horse/add/save loop works.
