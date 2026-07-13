# WEC Future Workflow Stage Handoff

Date: `2026-07-12`

Status: `TARGET WORKFLOW AND OPERATING CHECKLIST`

## Purpose

This document describes how the future Horseshowing workflow should operate,
stage by stage. It also records known failure patterns that must be checked
during implementation, date rollover, scheduled acceptance, and later cleanup.

WEC is the proving dataset. The lane boundaries remain applicable when another
show schedule or WEF is incorporated into the same narrow rider-focused scope.

This document does not replace scheduled proof. Current implementation and
acceptance evidence remain in the operating handoff.

## Related Authorities

- `wec-rider-timing-alert-policy.md`
- `wec-chatgpt-codex-subagent-operating-handoff-2026-07-12.md`
- `wec-current-future-stack-function-inventory-2026-07-12.md`
- `wec-stack-and-preflight-handoff-2026-07-12.md`

## Workflow Shape

```text
focus_show
  -> classes and normalization
  -> FAST probe
  -> parse raw documents
  -> stage tracked records
  -> prepare entries

static helpers support classes and entries without blocking them

prepared classes and entries
  -> Live enrichment
  -> Time Engine calculate
  -> Time Engine expedite
  -> trigger and alert events
  -> Statewise snapshots
  -> Rider Results
  -> read-only outputs

router and change logs record every boundary
```

No single request must keep the complete workflow open from Core through
Results. Each scheduled responsibility reads prepared state, performs its owned
work, records evidence, and stops.

## Stage 1 - Show

Owner: Airtable `focus_show`

Required control fields:

```text
show_no
focus_day
active
live_enrichment
results_enabled
```

Changing the active show or focus day makes the new dataset eligible for Core
preparation. Live and Results remain independently gated.

## Stage 2 - Classes and Normalize

Source: `update_schedule`

Prepared records:

```text
hs_update_schedule
hs_class_start_times
normalized ring identities
```

Responsibilities:

- attach `show_no` and `focus_day`;
- exclude preflight-only rows such as Ticketed Schooling;
- preserve the source class name;
- avoid making class-number parsing a workflow blocker;
- normalize `ring_name_prioritized`;
- create stable ring and class identities;
- seed every eligible class into `hs_class_start_times`;
- preserve full class current-state coverage, not only followed classes.

The future target works away from `hs_get_ring_days`. The current legacy Core
still uses that source, so retirement requires a separately proven Core cut.

## Stage 3 - FAST Probe

Inputs:

```text
eligible hs_update_schedule classes
tracked trainer and helper evidence
```

Output: `hs_class_oog_raw`

The probe checks quickly for evidence that a class document contains tracked
entries. It does not parse the complete document.

Rules:

- write only useful source documents;
- retain attempt count and certainty evidence;
- stop after the bounded no-match policy;
- move follow-up retries to non-blocking `3A2` work;
- do not hold initial runtime preparation while retries continue.

## Stage 4 - Parse

Input: unparsed `hs_class_oog_raw` documents.

Rules:

1. Match horses first.
2. Use trainers as fallback evidence.
3. Normalize conservatively for apostrophes, accented characters, numbers, and
   likely misspellings.
4. Preserve uncertain but useful records for review.
5. Mark terminal no-match documents rather than parsing them indefinitely.
6. Move secondary retries to non-blocking `3B2` work.

The parser works from the unparsed queue until no eligible documents remain.

## Stage 5 - Stage Tracked Records

Output: `hs_class_oog`

Each staged record preserves show/day, ring, class, entry, horse, rider,
trainer, source order, confidence, and review evidence.

A recognizable trainer with an uncertain horse may continue through the
workflow with review status. It must not silently disappear.

## Stage 6 - Entries

Input: `hs_class_oog`

Output: `hs_entry_go_times`

Each row represents one tracked entry in one class.

Prepared values:

```text
entry_order
go_time
horse
rider
trainer
class and entry identities
```

Current values added later:

```text
entry_order_now
entries_ahead
entry_go_time_now
go_in
```

## Stage 7 - Static Helpers

Tables:

```text
hs_horses
hs_riders
hs_trainers
hs_rings
```

Airtable remains the operator-editable surface for `follow`, aliases,
`barn_name`, ignore, and review status.

Helper synchronization is non-blocking. The intended cadence is approximately
every 60 minutes plus an operator push. Helper failure must not stop Core,
Live, Time Engine, Statewise, or Results.

## Stage 8 - Live Enrichment

Gate: `focus_show.live_enrichment = true`

Source: `get_rings`

`get_orders` remains retired from the hot Live lane.

Live updates current projections with:

```text
current_class_no
is_live
entry_count_now
n_gone_now
n_to_go_now
observed and frozen class start
accepted current pace
current entry position
current entry go-time estimate
ring now and next
estimated class end
schedule-slack-adjusted ring lateness
```

Not-live is valid current state. It must still be processed so stale live state
does not survive.

## Stage 9 - Time Engine Calculate

Time Engine calculates:

- ring progression and remaining delay;
- class `starts_in` and `ends_in`;
- managed class status;
- entry `entries_ahead` and `go_in`;
- threshold eligibility;
- result readiness;
- false-to-true transition state.

Accepted pace is derived from snapshot progression and remains within
`105-285` seconds.

## Stage 10 - Time Engine Expedite

The expeditor prepares calculated lane state and events:

```text
ringwise  -> ring state
classwise -> class timing and state
entrywise -> entry timing and state
riderwise -> result readiness
```

The current proven July 12 projection is 9 ring, 85 class, and 37 entry rows.
Only tracked-class events are restricted to classes represented in
`hs_entry_go_times`. The complete class projection remains.

## Stage 11 - Triggers and Alerts

Canonical event table: `time_engine_triggers`

Customer events:

```text
ring_late_15
ring_late_30
class_start_60
class_start_30
class_live
entry_go_40
entry_go_20
entry_class_10_gone
entry_10_away
result_available
```

Internal events:

```text
ring_class_change
result_ready
statewise_snapshot_due
```

Events are append-only and deduplicated by show, day, entity, event type, and
threshold or transition identity. Message delivery remains separate.

## Stage 12 - Statewise

Output: `statewise_now`

Generation occurs every 12 minutes, for an SMS request, and for manual refresh.
Statewise prepares the nearest `now` and `nextup` state for rings, tracked
horses, and tracked riders. It consumes Time Engine state and does not
recalculate timing.

Catalyst stores complete snapshot generations. Airtable receives append-only
changed-state evidence using its existing narrow schema. It is not a complete
snapshot mirror.

## Stage 13 - Rider Results

Gates:

```text
focus_show.results_enabled = true
tracked result_ready exists
```

Flow:

```text
result_ready
  -> scheduled Results polling
  -> two identical scheduled result-block hashes
  -> operational finality
  -> hs_rider_results
  -> result_available
  -> class_status done
  -> stop polling completed class
```

A matching placing creates terminal `placed`. Absence from the final stable
block creates terminal `no_place`. Place, score, and finished time are
preserved. Legacy result tables and outputs remain active until separately cut
over.

## Stage 14 - Outputs

Outputs are read-only consumers:

```text
schedule
rings
classes
entries
results
alerts
print
mobile
mobile-pro
mobile-entry
dense entity details
```

Outputs read prepared Catalyst state and the latest complete Statewise
generation. They do not calculate workflow state, repair data, or mutate
source identity.

## Stage 15 - Router and Change Logs

`hs_router_logs` records workflow start, stage start, step completion, stage
completion, errors, and final completion.

Supporting histories include ring, class, entry, result, alert, Statewise, and
Time Engine logs. Logs provide evidence and policy-refinement input. They do
not become current workflow truth.

## Cadence Model

```text
Focus-day change -> Core preparation until runtime_ready
Live             -> every 6 minutes when enabled
Time Engine      -> every 6 minutes
Statewise        -> every 12 minutes plus on-demand
Results          -> scheduled and gated by tracked result_ready
Helpers          -> every 60 minutes plus operator push
Outputs          -> read current prepared data
```

## Known Trouble Checklist

| Stage | Known trouble | Required check |
|---|---|---|
| `focus_show` | Scheduler uses a stale or inactive show/day | Confirm the single active record, current focus day, and feature flags |
| Router | Old deployment or scheduler invokes a retired action | Log action, deployment, scheduler source, run ID, show, and day |
| Schedule | Legacy `hs_get_ring_days` dependency drops a ring | Compare normalized ring count and identities with `update_schedule` |
| Schedule | Schedule and Live use different ring names | Normalize stable identity independently from presentation name |
| Preflight | Ticketed Schooling reaches probe or runtime | Exclude it before FAST probe |
| Class parsing | `812b)` or a name beginning with `$` blocks Stage 2 | Preserve source `data-name`; do not require class-number parsing |
| Keys | ISO and compact dates create different identities | Canonicalize dates before key construction |
| Writes | Append behavior creates duplicate source rows | Verify canonical keys and intended create/update behavior |
| FAST probe | Probe parses documents or runs classes serially | Probe only for tracked evidence |
| FAST probe | No-match classes repeat indefinitely | Enforce bounded attempts and non-blocking `3A2` retries |
| Parse | Every raw document is reparsed | Process only unparsed or explicitly retryable documents |
| Parse | Name punctuation or international characters lose horses | Normalize conservatively and use trainer fallback |
| Parse | Recognized trainer but uncertain horse blocks progression | Preserve for review and continue |
| Runtime | Airtable visibility blocks Catalyst runtime | Runtime completion must not depend on Airtable review writes |
| Runtime | Empty strings reach typed Catalyst columns | Use `null` or omit optional typed fields |
| Helpers | Helper sync stops production | Keep helpers non-blocking |
| Live | `get_orders` returns to the hot lane | Confirm only `get_rings` runs |
| Live | Not-live leaves stale `is_live=true` | Reconcile true and false observations |
| Live | ISO strings reach date-format APIs directly | Convert to `Date`; serialize per target system |
| Counts | Prepared count is used after live observation | Prefer current count fields |
| Pace | `elapsed / n_gone` creates impossible pace | Use timestamp delta divided by `n_gone` delta |
| Pace | Rejected pace contaminates estimates | Accept only `105-285`; otherwise use `null` |
| Class start | Live updates keep moving observed start | Freeze the first valid live start |
| Entry timing | Entry go time freezes after class start | Continue current position and go-time calculations |
| Ring lateness | Raw delay carries through a schedule gap | Consume schedule slack before late triggers |
| Time Engine | Prepared values override valid current values | Prefer validated current fields |
| Trigger scope | Class events fire for every class | Restrict the four tracked-class events only |
| Triggers | Tags exist without event rows | Verify 60, 40, ten-gone, and ten-away writers |
| Triggers | State is emitted as a customer event | Do not emit live/gate/not-live/schedule state as customer events |
| Trigger spacing | Useful-entry events arrive together | Enforce 20-minute spacing |
| Deduplication | Previous state uses an unordered truncated read | Page and order state reads |
| Airtable triggers | Catalyst-only fields leak into Airtable | Use a separate live-schema-verified allowlist |
| Statewise | Catalyst receives an ISO datetime | Use Catalyst datetime; use UTC ISO for Airtable |
| Statewise | Partial generations appear in output | Require a completion receipt and matching row count |
| Statewise | Airtable becomes a complete snapshot mirror | Append changed-state evidence only |
| Statewise | Every future item becomes `nextup` | Select the nearest applicable item |
| Results | Candidate fallback widens Rider Results | Use tracked `result_ready` and tracked entries |
| Results | One result response is treated as final | Require two identical scheduled result-block hashes |
| Results | Missing entry becomes `no_place` early | Wait for operational finality |
| Results | Raw JSON is truncated mid-value | Store a block hash and guarantee valid bounded JSON |
| Results | Completed classes keep polling | Stop when every tracked entry is terminal |
| Results | `result_available` is classified as internal | Emit it as a rider-facing event |
| Outputs | Airtable history is treated as current state | Read Catalyst projections and complete Statewise generations |
| Outputs | Endpoint recalculates workflow state | Keep outputs read-only |
| Scheduler | HTTP 200 hides an internal stage failure | Verify terminal router state, results, and writes |
| Scheduler | Manual success is used as proof | Require scheduler-owned execution |
| Verification | One cycle hides duplicate behavior | Require consecutive cycles and duplicate checks |
| Isolation | Acceptance silently invokes another lane | Verify router events and deployment scope |

## Rollover Minimum Check

```text
correct active focus_show
correct show and focus-day keys
all expected rings
preflight exclusions applied
FAST probe remains fast
runtime reaches expected ring/class/entry counts
Live reconciles false and true state
Time Engine consumes current fields
Results waits for operational finality
all active workflow schedulers return HTTP 200
no duplicate events or history rows
```

## Current Proof Boundary

```text
Task 05 Live                      PASS
Task 06 Time Engine and Statewise PASS
Task 07 Rider Results             PASS
Global combined workflow review   READY FOR SCHEDULED REVIEW
```

Individual lane proof does not automatically establish combined global proof.
