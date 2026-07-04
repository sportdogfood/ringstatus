# WEC Catalyst Step 6 Results Contract

## Purpose

Step 6 is the WEC results lane. It checks HorseShowing results for current active-focus classes that are relevant to our rider scope, writes only real source results, and mirrors those rows for Airtable visibility.

Step 6 is separate from the Step 1-5 stack. It does not rebuild baseline schedule/runtime data and it does not run alerts or output lanes.

## Action

`wec-step6-results`

## Gate

Step 6 runs only when:

`focus_show.results_enabled = true`

If the gate is false or blank, Step 6 must skip with no source fetch and no writes.

## Sources

Catalyst runtime source tables:

- `hs_class_start_times`
- `hs_entry_go_times`

Step 6 must not read legacy Airtable `class_oog_staging.active_entries`.

## Result Endpoint

HorseShowing endpoint:

- `show_results4.php`

## Source Request Rule

Step 6 sends HorseShowing-native values upstream:

- `class_no`

Step 6 must not send these internal keys upstream:

- `ring_visual_key`
- `class_visual_key`
- `entry_visual_key`

Visual keys are internal runtime/result identity keys only.

## Our-Rider Scope

Step 6 scopes classes through current active-focus runtime entries in `hs_entry_go_times`.

A class is in our-rider scope when current active-focus `hs_entry_go_times` contains a matching `class_visual_key` and entry identity.

## Check Results Rule

Step 6 computes:

`check_results = true`

when:

`now >= class_start_time + (entry_count * 3.3 minutes)`

This is only a probe-readiness rule. It does not mean the class is Done.

## Retry Contract

`hs_result_queue` owns retry state.

Fields:

- `attempts`
- `status`
- `last_checked_at`
- `next_check_at`

Statuses:

- `pending`
- `completed`
- `exhausted`

Rules:

- Probe a class at most 5 times.
- Retry only every 6 minutes.
- If no results and `attempts < 5`, set `status=pending` and `next_check_at = now + 6 minutes`.
- If no results and `attempts >= 5`, set `status=exhausted`.
- If results are found, set `status=completed`.

## Completion Rule

Step 6 marks:

`hs_class_start_times.class_status = Done`

only when real results are found from `show_results4.php`.

Step 6 must not mark Done from the time estimate alone.

Step 6 must not create fake result rows.

## Outputs

Catalyst outputs:

- `hs_result_queue`
- `hs_result_classes`
- `hs_class_results`

Airtable mirrors:

- `hs_result_queue`
- `hs_result_classes`
- `hs_class_results`

## Latest PASS Evidence

Verified current active focus:

- `show_no = 14909`
- `focus_day = 2026-07-03`

Step 6 proof:

- `results_enabled = true`
- our-rider scoped class count: `14`
- completed result classes: `13`
- Catalyst `hs_result_queue = 13`
- Catalyst `hs_result_classes = 13`
- Catalyst `hs_class_results = 174`
- Airtable `hs_result_queue = 13`
- Airtable `hs_result_classes = 13`
- Airtable `hs_class_results = 174`
- Airtable mirror counts match Catalyst counts.

Bounded proof:

- Step 6 ran in bounded `limit=3` passes.
- Final pass returned `probed_classes = 0`.
- No pending classes needed `next_check_at` because every probed class returned real results.
- Exhausted classes: `0`
- Fake results created: `0`

## Explicit Exclusions

Step 6 must not run or touch:

- Step 1-5
- alerts
- mobile
- print
- PDF
- UI
- fake result rows
- Webflow publish
- Production deploy

## Next Gate

Decide whether Step 6 gets its own Catalyst scheduler/job.
