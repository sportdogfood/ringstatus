# WEC Results Lane Workflow v0.1

Date: 2026-06-17

## Contract

This lane follows `LOCKED WORKFLOW GATE - MCP FIRST`.

Catalyst owns the repeatable workflow. Airtable mirrors the result tables for visibility. PowerShell or local shell calls are only operator verification and must not transform result data.

## Source

Approved source list:

1. Airtable `class_oog`
2. Same focus `show_no` and `focus_day`
3. `active = true`
4. `lock = true`
5. `hide != true`
6. `class_no` and `entry_no` present

No other class list is approved for this lane.

## Runner

Catalyst function:

`horseshowing_results_runner`

Deployed URL:

`https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_results_runner/`

Request parameters:

- `show_no`
- `focus_day`
- `offset`
- `limit`
- `force=1` only for operator recheck

Chunk rule:

- Default `limit = 1`
- Maximum `limit = 5`
- Repeated cadence can call `offset=0&limit=1`; completed classes are skipped
- Operator force-refresh can call explicit offsets

## Endpoint

Horseshowing endpoint:

`show_results4.php`

The runner first bootstraps the Horseshowing show session with `show.php?show={show_no}`, then posts:

- `class_nos = JSON.stringify([class_no...])`
- `sect_nos = JSON.stringify([])`

## Targets

Catalyst:

- `hs_result_queue`
- `hs_result_classes`
- `hs_class_results`

Airtable mirrors:

- `result_queue`
- `result_classes`
- `class_results`

Log:

- Airtable `wec-logs`
- `workflow_lanes = Results`
- `log_type = result_classes`
- `check_name = probe_results`

## Status Rule

If `show_results4.php` returns a result block for a class:

- `result_queue.status = completed`
- `result_classes.completed_at` is written
- `result_queue.completed_at` is written

If no result block is returned:

- `result_queue.status = pending`
- no completed result class is written

## Entry Result Rule

`class_results` writes only result rows matching the approved `class_oog` active source entries by:

`class_no|entry_no`

If the result payload omits rider or horse text, the runner fills the blank field from the approved `class_oog` source row. It does not use any other helper list for that fallback.

## Verified Run

Verified focus:

- `show_no = 14907`
- `focus_day = 2026-06-17`

Approved source:

- `class_oog` source rows: 8
- unique source classes: 6

Verified Catalyst:

- `hs_result_classes`: 6
- `hs_result_queue`: 6
- `hs_class_results`: 7

Verified Airtable:

- `result_classes`: 6
- `result_queue`: 6
- `class_results`: 7

Completed classes verified:

- `29784`
- `29748`
- `29855`
- `29776`
- `29754`
- `30012`

Verified active-entry result rows:

- `1296` Calou Us / Tanner Korotkin
- `1045` Qastaar Van't Heike / Tanner Korotkin
- `1042` Maiki / Tanner Korotkin
- `227` Sandenal / Tanner Korotkin
- `226` Kinmar Quality Hero / Tanner Korotkin
- `2667` Calypso / Tanner Korotkin
- `2389` Fanfan Boy / Sydney Matalon

## Known Trouble

The result payload table shape can omit rider text for some rows. The repeatable runner handles this by filling blanks from the already-approved `class_oog` source entry.

The runner uses bounded upstream requests with retry and `connection: close`; this replaced the previous native-fetch-first timeout failure.

## Handoff

Next stages can read:

- completed class status from `result_queue.status`
- class completion timestamp from `result_classes.completed_at`
- entry-specific result detail from `class_results`

No entry alerts should be created directly from this lane until the alert workflow explicitly consumes these tables.
