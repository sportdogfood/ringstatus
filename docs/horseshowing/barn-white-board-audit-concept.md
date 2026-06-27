# Barn White Board Audit Concept

Status: concept only

## Purpose

Define a future audit concept for comparing WEC customer-facing outputs against barn white board observations when those observations are approved as an explicit reference standard.

The intended audit would answer whether WEC mobile, print, and related status output agree with the approved barn white board view for the same `show_no`, `focus_day`, ring, class, time, and horse/team context.

## Current Status

`barn-white-board-audit` is not currently implemented as a runner, endpoint, workflow lane, or proof gate.

Existing WEC mobile and print audits compare output behavior to Catalyst state and render contracts. They do not compare mobile, print, or status data to a barn-white-board standard.

## Relationship To `barn_board_hot_patches`

`barn_board_hot_patches` exists in the Catalyst/Horseshowing code path and is related, but it is not the same thing as `barn-white-board-audit`.

Current `barn_board_hot_patches` behavior:

- uses Airtable table `barn_board_hot_patches`
- stores board-style hints such as `board_line`, `ring_hint`, `time_hint`, `class_name_hint`, `horses`, `entry_go_times`, `match_status`, `hot_patch_active`, `release_status`, and `focus_day`
- can match active `entry_go_times`
- can produce active hot patch rows for rendered schedule output
- has a `sync-barn-board-hot-patches` action

That implementation is an operational hot patch overlay/sync surface. A barn white board audit would be a separate verification concept unless explicitly approved otherwise.

## Proposed Audit Inputs

Possible read-only inputs for a future audit:

- active `focus_show` control row
- WEC mobile output payload
- WEC print output payload
- class/status output payload, if approved as part of the comparison
- Catalyst state tables used by current outputs
- Airtable `barn_board_hot_patches`, if approved as the board observation source
- any future approved barn white board source table or capture artifact

The audit should preserve partition keys:

- `show_no`
- `focus_day`
- `ring_no`
- `ring_day_no`
- `class_no`
- `entry_no`, where applicable
- visual keys, where applicable

## PASS/FAIL Conditions

PASS should require:

- approved board source exists for the audited `show_no` and `focus_day`
- mobile and print use the same approved focus day
- mobile and print agree on core ring/class/time/status fields
- every board row intended for comparison maps to one unambiguous output row or approved exception
- no unapproved board row is silently dropped
- no stale mobile/print/status row conflicts with the approved board source
- audit output includes counts, mismatches, skipped rows, and blockers

FAIL should occur if:

- no approved board source is configured
- board source fields are ambiguous or unmapped
- mobile and print disagree for the same ring/class context
- output data conflicts with approved board source
- a board row maps to multiple live rows without an approved tie-breaker
- a live row appears to be stale compared with the approved board source
- the audit would require writes, syncs, workflow runs, helper repair, raw fetches, or publisher changes

## Non-Goals

This concept does not:

- create a runner
- create an endpoint
- create or modify Airtable records
- replace `barn_board_hot_patches`
- trigger schedule staging
- trigger class OOG, class start time, or entry go time sync
- repair helpers
- fetch raw upstream pages
- publish Webflow
- deploy Catalyst
- define the barn white board as source of truth without approval

## Future Implementation Gates

Before implementation, the following gates must be approved and verified:

1. Source of truth: identify the exact approved barn white board source.
2. Field contract: freeze required fields, optional fields, helper keys, and identifiers.
3. Scope: choose read-only audit only or an operational workflow lane.
4. Comparison model: define row keys, matching rules, tie-breakers, and accepted exceptions.
5. Output contract: define proof JSON fields, PASS/FAIL summary, and blocker format.
6. Safety: confirm the audit cannot write Airtable, Catalyst, Webflow, or generated outputs.
7. Verification: run against one approved focus day and prove no workflow/sync/raw/helper lane was triggered.
