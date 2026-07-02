# WEC Cadence Contract

Purpose: keep WEC work focused on the repeatable cadence, task, and lane pipe. This document is for humans first. It defines what counts as a real workflow pass, where fixes belong, and what must not be treated as proof.

## 1. Role Boundary

Codex is not the runner.

The runner is the scheduled task, workflow wrapper, deployed Catalyst action, or approved lane process that owns the business operation.

Codex may:

- inspect source, logs, records, and endpoint output
- propose scoped fixes
- patch approved repeatable code paths
- verify approved pass conditions

Codex must not:

- keep trying one-off commands until numbers appear
- manually repair records and call that workflow proof
- use direct endpoints as substitutes for cadence proof
- continue after a failed gate unless a new scoped fix is approved

## 2. Canonical Cadence Path

The accepted WEC cadence path is:

```text
Windows Scheduled Task: ringstatus-heartbeat
  -> run_tagger_heartbeat_lane.ps1
  -> heartbeat_slot_orchestrator.js
  -> approved child wrapper/action for the selected branch
```

Anything outside this path is diagnostic unless explicitly approved as a manual proof.

Examples of diagnostic-only paths:

- direct Catalyst action URL
- direct runner script
- local shell command
- one-off Airtable helper repair
- direct PowerShell wrapper invocation outside the heartbeat path

Diagnostic-only output can explain a blocker. It cannot prove cadence health.

## 3. Active Control Record

The active `focus_show` record controls the WEC lifecycle.

Required control fields:

| Field | Meaning |
| --- | --- |
| `active` | Selects the one current WEC focus record. Must be unique. |
| `show_no` | Current HorseShowing show number. |
| `focus_day` | Current show day as ISO date. |
| `is_pause` | Stops cadence writes when checked. |
| `is_lock` | Releases downstream non-live processing when checked. |
| `live-enrichment` | Allows day-of-show live endpoints when checked. |

The cadence must resolve these dynamically. Do not hardcode `show_no` or `focus_day`.

## 4. Branch Matrix

| Branch | Required Control State | Allowed Lanes | Forbidden Lanes | Stop Point |
| --- | --- | --- | --- | --- |
| Pause | `is_pause=true` | none | all writes, syncs, alerts, live endpoints | immediately |
| Bootstrap | `is_pause=false`, `is_lock=false` | `get_ring_days`, `update_schedule`, `update_schedule_staging` | `class_start_times`, `class_oog`, `entry_go_times`, alerts, `get_orders`, `get_rings` | after staging |
| Locked Non-Live | `is_pause=false`, `is_lock=true`, `live-enrichment=false` | `class_start_times`, `class_oog`, `class_oog_staging`, `entry_go_times` if supported without live pace, alerts no-send if due | `get_orders`, `get_rings` | after no-send alerts |
| Live Enrichment | `is_pause=false`, `is_lock=true`, `live-enrichment=true` | Locked Non-Live lanes plus `get_orders` and `get_rings` before live-dependent enrichment | fallback go-times, stale `go_time`, stale `display_time`, external notifications | after no-send alerts |

## 5. Stage Contracts

### Stage 1: Bootstrap

Producer:

```text
heartbeat_slot_orchestrator.js
  -> docs/horseshowing/run-wec-catalyst-workflow.ps1
  -> get_ring_days / update_schedule / update_schedule_staging actions
```

Inputs:

- active `focus_show.show_no`
- active `focus_show.focus_day`
- active `focus_show` record id

Required outputs:

- current-day `get_ring_days` rows
- current-day `update_schedule` rows
- current-day `update_schedule_staging` rows

Pass condition:

- `get_ring_days` current-day count > 0
- `update_schedule` current-day count > 0
- `update_schedule_staging` current-day count > 0
- no old-day fallback rows
- downstream did not run

Failure examples:

- source empty
- source parsed but not written
- staging timeout
- stale output fallback
- blocker mislabeled as source empty

Fix location:

- Stage 1 wrapper or Stage 1 action only.
- Do not repair downstream to hide Stage 1 failure.

### Stage 2: Staging / Schedule Handoff

Producer:

```text
sync-update-schedule-staging-from-mirror
```

Consumer:

```text
class_start_times
class_oog
```

Required outputs:

- current-day `update_schedule_staging`
- required source keys preserved
- helper/link fields populated where configured and available

Pass condition:

- staging rows are current active focus only
- staging count > 0
- no stale rows returned as current day
- missing optional helper fields do not hard-fail unless required by the next lane

Fix location:

- staging materializer or helper contract only.
- Do not use `update_schedule` mirror as final downstream source.

### Class Start Times

Producer:

```text
sync-class-start-times
```

Source:

```text
update_schedule_staging
```

Required outputs:

- current-day `class_start_times`
- `active_entries` view rows where applicable
- ring identity preserved through `ring`, `ring_name`, `ring_no`, or `ring_day_no`

Pass condition:

- current-day count > 0
- active count is reported
- no stale focus day rows
- no live endpoint dependency unless `live-enrichment=true`

Fix location:

- class lane runner or staging source mapping.

### Class OOG Mirror

Producer:

```text
class_oog lane
```

Source:

```text
update_schedule_staging
```

Required outputs:

- `class_oog` mirror rows
- `class_oog_staging` downstream rows

Mirror rule:

- `class_oog` is a mirror table.
- downstream should use `class_oog_staging`.
- `confirm_delete` is the only permission path for Catalyst `hs_class_oog` deletes.

Pass condition:

- `class_oog` current-day count > 0
- `class_oog_staging` current-day count > 0
- staging dedupe happens only while materializing `class_oog_staging`
- mirror rows are not rewritten to fake staging success

Fix location:

- class OOG materializer or staging handoff.
- Do not one-off create `entries`, `horses`, or helper links to clear one row.

### Entry Go Times

Producer:

```text
horseshowing_entry_go_times_runner
```

Source:

```text
class_oog_staging
```

Required rule:

- Do not generate fake fallback `entry_go_time`.
- Do not use stale `go_time`.
- Do not use `display_time` as fallback.
- Blank/null `entry_go_time` is acceptable when proven pace data is missing.

Pass condition:

- current-day `entry_go_times` count > 0
- source is `class_oog_staging`
- fallback/generated fake go-time count = 0
- source-derived pace count is reported

Fix location:

- entry go-times runner or live enrichment handoff.

### Live Enrichment

Allowed only when:

```text
focus_show.live-enrichment = checked
```

Allowed lanes:

- `sync-get-orders`
- `sync-get-rings`

Forbidden when unchecked:

- `get_orders`
- `get_rings`

Pass condition:

- cadence log shows `live_enrichment_enabled=true`
- `sync-get-orders` attempted from the cadence-owned path
- `sync-get-rings` attempted from the cadence-owned path
- live fields are copied into the intended downstream table
- no fallback go-times are created

Fix location:

- heartbeat orchestrator live gate
- class lane enrichment action
- handoff mapping from live source to canonical downstream table

### Alerts

Source views:

- `class_start_times.active_entries`
- `entry_go_times.active_entries`

Allowed writes:

- `wec-alerts` only

External sends:

- disabled unless separately approved

Pass condition:

- records are created only when trigger time has been reached
- no future candidate records are created
- no entry alerts from blank/insufficient `entry_go_time`
- no duplicate open alert keys
- `notifications_sent=0` when no-send is required

Fix location:

- alert lane only.
- Do not add upstream table gates to make alerts easier to count.

## 6. What Counts As Proof

Cadence proof requires all of the following:

- the canonical cadence path ran
- active `focus_show` was resolved dynamically
- the expected branch was selected
- allowed child lanes ran
- forbidden child lanes did not run
- row counts came from current active show/day
- logs show the final branch summary
- no old-day fallback rows appeared

Manual proof requires explicit user approval that manual proof is accepted.

## 7. What Does Not Count As Proof

These do not prove cadence health:

- direct endpoint returned 200
- manual runner produced rows
- local shell script created or counted records
- a one-row helper repair succeeded
- a table count looks correct after manual intervention
- Webflow/mobile/print output looks current
- a field appears populated but no cadence writer is proven

## 8. Fix Classification

Every fix should be classified before work starts.

| Classification | Meaning | Can It Produce PASS? |
| --- | --- | --- |
| Cadence contract fix | Changes the repeatable owned path | Yes, after cadence proof |
| Lane implementation fix | Fixes the child action called by cadence | Yes, after cadence proof |
| Diagnostic command | Explains state or blocker | No |
| Manual proof | Runs approved manual path | Only if user requested manual proof |
| One-off repair | Changes current records only | No |
| Drift cleanup | Removes or documents unowned changes | No, unless tied to a gate |

## 9. Stop Rules

Stop and return `FAIL` when:

- the cadence branch is ambiguous
- a required handoff field is missing
- a child action fails
- a manual path differs from cadence
- an optional helper is being treated as required without proof
- a timeout is not classified
- counts are produced only by direct/manual execution
- a field has no repeatable writer

Do not patch after a failed verification unless a new scoped edit is approved.

## 10. Final Branch Summary Requirement

Each cadence run should emit one final machine-readable summary.

Example:

```json
{
  "event": "wec_cadence_branch_summary",
  "branch": "locked_non_live",
  "focus_show_id": "recsfIXwb6GCUCEzG",
  "show_no": "14909",
  "focus_day": "2026-07-02",
  "is_pause": false,
  "is_lock": true,
  "live_enrichment_enabled": false,
  "get_ring_days_run": false,
  "update_schedule_run": false,
  "update_schedule_staging_run": true,
  "class_start_times_run": true,
  "class_oog_run": true,
  "class_oog_staging_run": true,
  "get_orders_run": false,
  "get_rings_run": false,
  "entry_go_times_run": true,
  "alerts_run": true,
  "external_notifications_sent": 0,
  "stop_reason": null,
  "pass": true
}
```

If this summary is missing, the run is not fully proven.

## 11. Current Review Checklist

Use this checklist before fixing any WEC issue:

- Which branch is this?
- Which cadence-owned command should run?
- Which child action is allowed in this branch?
- Which source table/view is allowed?
- Which destination table is allowed?
- Which keys must be preserved?
- What count proves the handoff?
- What log proves the handoff?
- Is this a cadence fix or a one-off repair?
- If this fix works once, will the next cadence run do the same thing without intervention?

## 12. Preferred Next Repair Focus

The next repair should be the smallest change that improves the canonical cadence proof, not the fastest way to get a count.

Recommended order:

1. Add or verify final branch summary logging.
2. Verify bootstrap branch summary.
3. Verify locked non-live branch summary.
4. Verify `live-enrichment` branch summary only when checked.
5. Classify any remaining manual scripts as diagnostic-only or cadence-owned.

