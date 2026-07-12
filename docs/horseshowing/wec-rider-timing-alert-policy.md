# Rider Timing and Alert Policy

Date established: `2026-07-12`

Status: `APPROVED TARGET CONTRACT - IMPLEMENTATION NOT PROVEN`

Proving dataset: WEC Horseshowing

## Purpose

This document is the canonical policy for rider-facing timing, current-state
projections, trigger eligibility, Statewise snapshots, and tracked rider
results.

It records the business rules separately from executable code and runtime
logs. Code and tests implement this policy. Catalyst observations measure its
accuracy. Runtime behavior must not silently redefine it.

WEC is the proving dataset, not a WEC-only architecture. The same narrow
ringwise, classwise, entrywise, riderwise, and timewise contract applies when
new show schedules or WEF are incorporated. Broader legacy data collection is
not automatically part of this contract.

## Authority and Boundaries

This policy was recovered from the discussion beginning with:

```text
Yes. time_engine_triggers now receives the trigger records directly from
Time Engine.
```

and continuing through the July 12 workflow-transition package.

The policy owns:

- timing formulas and accepted inputs;
- managed current-state rules;
- customer and internal trigger vocabulary;
- tracked-class and tracked-entry scope;
- alert transition, spacing, and deduplication rules;
- `statewise_now` snapshot behavior;
- terminal `hs_rider_results` behavior;
- indexed quick-name lookup eligibility.

It does not authorize:

- message delivery;
- schedule changes;
- Core changes;
- output redesign;
- deletion or rewriting of historical records;
- removal of a legacy responsibility before scheduled replacement proof.

## Lane Model

| Lane | Current-state source | Timing responsibility |
|---|---|---|
| Ringwise | `hs_ring_status` enriched by `get_rings` | Ring progression, current and next class, remaining delay after schedule slack |
| Classwise | `hs_class_start_times` | Current counts, observed start, estimated end, class status |
| Entrywise | `hs_entry_go_times` | Current position, entries ahead, dynamic go time |
| Riderwise | `hs_rider_results` | One terminal result for each tracked class and entry |
| Timewise | Time Engine and `time_engine_triggers` | Calculation, eligibility, transition detection, trigger identity |
| Statewise | `statewise_now` | Prepared current and next snapshots; no independent timing calculation |

Time Engine calculates approved state. Current-state lanes retain their owned
values. Trigger rows are append-only events, not delivery records.

## Tracked Scope

The tracked class set is the unique nonempty class identity set represented in
`hs_entry_go_times` for the active show and focus day.

Use `class_const_key` where available and retain `class_no` as the source class
identity.

The tracked entry set is the matching `hs_entry_go_times` rows.

These events are limited to tracked classes:

- `class_start_60`;
- `class_start_30`;
- `class_live`;
- `result_ready`.

Entrywise events are naturally limited to tracked entries. Ringwise lateness
continues for active rings because it describes ring state.

Class trigger records include:

- `followed_class=true`;
- `tracked_entry_count`.

## Live Observation Contract

`get_rings` is the retained live source. `get_orders` is retired from the hot
Live lane.

A live observation can provide:

- show and focus-day identity;
- ring and current-class identity;
- source observation timestamp;
- `total`;
- `n_gone`;
- `n_to_go`;
- live or not-live state.

Not-live is valid current state and must continue through the lane. It is not a
reason to stop processing or preserve stale `is_live=true` values.

## Current Counts

Preserve the discovery-time count and current live count separately:

```text
entry_count       discovery or schedule count
entry_count_now   latest get_rings.total
n_gone_now        latest live n_gone
n_to_go_now       latest live n_to_go
```

`entry_count_now` is updated continually, including before the class becomes
live and throughout the live class. Do not freeze it when the class starts.

Prefer `get_rings.total`. `n_gone_now + n_to_go_now` is only a fallback or
consistency check because the in-ring entry can create a difference of one.

Time Engine uses current fields when available and uses prepared fields only
before current observations exist.

## Pace

`elapsed` from `get_rings` is not class elapsed time and must not be divided by
`n_gone`.

Calculate pace only from progression between two observations of the same
show, focus day, ring, and class:

```text
estimated_pace_now =
  (new timestamp_value - previous timestamp_value)
  / (new n_gone - previous n_gone)
```

Requirements:

- `n_gone` must increase;
- retain full precision during calculation;
- round only the final seconds value;
- accept only `105` through `285` seconds, inclusive;
- store `null` when the calculation is unavailable or outside the range;
- do not update downstream timing from a rejected pace;
- one observation cannot establish pace.

## Classwise Timing

Before a class becomes live:

- continue updating the estimated `class_start_time`;
- continue updating `entry_count_now`;
- continue updating tracked-entry estimates.

When the source first establishes that the class is live:

- freeze `class_start_time` as the observed class start;
- record `live_started_at`;
- continue updating current counts and accepted pace;
- continue recalculating estimated end and entry timing.

Current estimated end uses the latest observation and remaining work:

```text
remaining_count = entry_count_now - n_gone_now
estimated_end_time = observation_time + remaining_count * estimated_pace_now
ends_in = estimated_end_time - current_time
```

Compare `remaining_count` with `n_to_go_now`. A difference greater than one
marks the estimate uncertain rather than silently publishing it as precise.

Managed class progression:

```text
today  default prepared class
soon   approved 60-minute threshold reached
now    latest valid ring state identifies the class as current
done   the same ring positively advances from the class
```

Do not derive managed status from stale alert records. Result availability is
not required to mark every class done.

## Entrywise Timing

Before the tracked class is live, retain the prepared go-time estimate and
recalculate it as class start or accepted pace changes.

After the class becomes live:

```text
entry_order_now = entry_order - n_gone_now
entries_ahead = max(entry_order_now - 1, 0)
entry_go_time_now =
  observation_time + entries_ahead * estimated_pace_now
go_in = entry_go_time_now - current_time
```

Continue these calculations after class start. The frozen class start remains
historical truth; it does not freeze a tracked entry's current go-time estimate.

As the entry approaches, prioritize:

1. current `n_gone_now`;
2. `entries_ahead`;
3. `entry_go_time_now` as an estimate.

## Ten-and-Ten Policy

Riders value two progress points:

- the class crosses ten gone;
- their tracked entry crosses ten away.

These are useful only when they add material notice. Apply a minimum of 20
minutes between useful entry alerts.

Candidate rules:

```text
entry_class_10_gone:
  previous n_gone_now < 10
  current n_gone_now >= 10
  tracked entry has not gone
  current go_in >= 20 minutes

entry_10_away:
  previous entries_ahead > 10
  current entries_ahead <= 10
  current entries_ahead > 0
  at least 20 minutes since the previous useful entry alert
```

Timing thresholds:

```text
entry_go_40: go_in crosses from above 40 to 40 or below
entry_go_20: go_in crosses from above 20 to 20 or below
```

Spacing rules:

- send the first useful eligible candidate;
- require 20 minutes before another entry alert;
- suppress `entry_go_20` when a ten-gone or ten-away alert occurred less than
  20 minutes earlier;
- suppress a ten-away alert when ten-gone occurred less than 20 minutes
  earlier;
- do not create ten-gone or ten-away alerts for shallow orders where they add
  no useful notice.

Examples:

- Entry order 4: use the timed alert; neither ten event is useful.
- Entry order 15: ten gone leaves roughly five ahead; prefer the useful timed
  alert rather than adding a distracting progress alert.
- Entry order 35 at fast pace: ten gone may be useful; suppress ten away if it
  follows only 15 minutes later.
- Entry order 35 at slower pace: ten gone and ten away may both be useful when
  at least 20 minutes apart; suppress a later timed alert if it is too close.

## Ringwise Timing and Lateness

Ringwise forecasts remaining delay after available schedule slack. It does not
carry raw current lateness forward automatically.

```text
schedule_buffer = next_scheduled_start - estimated_current_end
projected_next_start = max(next_scheduled_start, estimated_current_end)
projected_late = max(0, projected_next_start - next_scheduled_start)
```

Interpretation:

- the first class establishes the live baseline;
- the second class begins pace and schedule comparison, but confidence remains
  limited;
- later classes use accumulated observations, accepted pace, actual starts,
  and actual ends;
- a 60-minute schedule gap absorbs a 20-minute late finish and leaves the next
  class projected on time;
- ring lateness is delay debt remaining after schedule gaps absorb delay.

Create `ring_late_15` and `ring_late_30` only when projected delay remains after
schedule slack is consumed.

`ring_gate` can remain a status where positively known. Do not infer a customer
gate alert merely because no current or next calculated class exists.

## Trigger Registry

### Customer Events

| Lane | Trigger | Eligibility | Transition identity |
|---|---|---|---|
| Ring | `ring_late_15` | Remaining projected delay reaches 15 minutes after slack | Ring, affected next class, threshold 15 |
| Ring | `ring_late_30` | Remaining projected delay reaches 30 minutes after slack | Ring, affected next class, threshold 30 |
| Class | `class_start_60` | Tracked class crosses 60 minutes before current estimated start | Class and threshold 60 |
| Class | `class_start_30` | Tracked class crosses 30 minutes before current estimated start | Class and threshold 30 |
| Class | `class_live` | Tracked class legitimately transitions into live state | Class and live sequence |
| Entry | `entry_go_40` | Tracked entry crosses 40 minutes before current go estimate | Class, entry, threshold 40 |
| Entry | `entry_go_20` | Tracked entry crosses 20 minutes before current go estimate, subject to spacing | Class, entry, threshold 20 |
| Entry | `entry_class_10_gone` | Class crosses ten gone and the alert remains useful | Class, entry, ten-gone threshold |
| Entry | `entry_10_away` | Tracked entry crosses ten away and spacing permits | Class, entry, ten-away threshold |
| Rider | `result_available` | A tracked entry receives a terminal placed or no-place result | Class, entry, terminal result |

### Internal Events

| Trigger | Purpose |
|---|---|
| `ring_class_change` | Freeze prior class timing, establish the next class, and support result readiness |
| `result_ready` | Begin results polling for a tracked class |
| `statewise_snapshot_due` | Create the scheduled twelve-minute Statewise snapshot generation |

### State, Not Customer Events

- `ring_live` is state, not a customer trigger.
- Raw or inferred `ring_gate` is not a customer trigger.
- Repeated unchanged live state is not a new customer event.

Historical rows are never deleted because a trigger is retired from future
customer emission.

## Trigger Identity and Deduplication

Trigger history is append-only. Never update, reopen, resolve, or delete an
earlier business event.

Base event identity:

```text
show_no
+ focus_day
+ entity_type
+ entity identity
+ trigger_type
+ threshold or transition identity
```

One-time thresholds create one event per show day and entity. Legitimate later
state transitions such as re-entering live use a sequence or other stable
transition identity.

An unchanged cadence creates no duplicate event. Deduplication must not depend
on an unordered, truncated previous-state read that can suppress a later valid
transition.

## Result Readiness and Rider Results

`hs_rider_results` is entrywise within a class despite its name.

Identity:

```text
show_no + focus_day + class_no + entry_no
```

`result_ready` applies only to tracked classes. Prefer completion evidence in
this order:

1. the ring advances away from a class that was live;
2. the class reaches `n_to_go_now = 0`;
3. accepted live-derived estimated end is reached as fallback.

After `result_ready`:

1. poll results for that tracked class;
2. continue until the approved operational finality rule is satisfied;
3. match each tracked `class_no + entry_no`;
4. write place, score, and finished time when supplied;
5. write terminal `no_place` when the tracked entry is absent from final
   placings;
6. create one Catalyst `hs_rider_results` record and its approved Airtable
   record;
7. mark the class terminal after every tracked entry has a terminal result;
8. never poll that completed class again.

The source does not expose a reliable final, official, complete, or equivalent
marker. Use this approved operational finality rule:

```text
result_ready exists for the tracked class
and the class is no longer live
and a parsed result block exists for the class
and the complete normalized result block is identical across two consecutive
scheduler-owned Results polls
```

The normalized result-block identity includes the class header entry count and
all parsed result rows and result fields. Any change resets the stability count.
A manual or direct poll does not count toward finality.

Before the second unchanged scheduled poll:

- placed rows may be observed but are not terminal for class completion;
- absent tracked entries remain unresolved;
- do not create `no_place`;
- continue scheduled polling.

After the second unchanged scheduled poll:

- matching tracked entries become terminal `placed` results;
- absent tracked entries become terminal `no_place` results;
- create one `result_available` event per terminal tracked result;
- mark the matching Catalyst `hs_class_start_times.class_status` as `done`;
- record the class completion time and reason when supported by the existing
  Catalyst schema;
- stop polling the class after every tracked entry is terminal.

Do not mark the class `done` from `result_ready`, a single nonempty result
response, or a manual poll. This Results confirmation supplements the normal
ring-progression `done` state and must not reopen or regress an already-done
class.

## Statewise Now

`statewise_now` is a prepared snapshot surface. It consumes Time Engine output
and does not independently recalculate timing.

Create snapshot generations:

- every 12 minutes;
- immediately for an SMS request;
- immediately for manual `Refresh now`.

Snapshot sources:

```text
scheduled
sms_request
manual_refresh
```

Each snapshot records:

- `snapshot_id` and `snapshot_source`;
- `as_of_time` and freshness;
- lane and entity identities;
- `state` as `now` or `nextup`;
- `sort_order` as 1 for `now` and 2 for `nextup`;
- current and next class or tracked-entry state;
- live counts and `entry_count_now`;
- `entries_ahead` and `go_in` where applicable;
- `starts_in` or `ends_in` as applicable;
- accepted current pace;
- applicable status tags;
- horse, rider, and trainer where applicable.

Required views:

```text
Ring/name lookup:
  now  -> time, class, ends_in, current counts and pace
  next -> time, class, starts_in

Tracked person lookup:
  now  -> time, class, ends_in, current counts and go_in
  next -> time, class, starts_in, go_time, horse, entry_no and go_in
```

Endpoint behavior:

- no `as_of_time`: return the latest complete snapshot;
- `as_of_time=<ISO timestamp>`: return the closest available snapshot with up
  to three earlier and three later snapshots;
- `as_of_time=true`: server time is the fallback anchor;
- the device should send an ISO timestamp including timezone when available.

The refreshable page displays:

- the snapshot's `as_of_time`;
- minutes until the next scheduled refresh;
- a `Refresh now` action.

The SMS remains static and links to the refreshable current-state page.

Snapshot and trigger writes are idempotent within the same scheduled or
on-demand generation. A later generation remains independently eligible.

### Airtable Statewise Log

Catalyst stores the complete scheduled and on-demand snapshot generations.
Airtable `statewise_now` is a direct, append-only changed-state log created by
Catalyst. It is not a mirror of every Catalyst snapshot row.

For each stable projection identity, compare the new prepared values with the
latest Airtable logged state. The stable projection identity excludes
`snapshot_id`, `as_of_time`, and logging timestamps and includes:

```text
lane
+ lookup_type
+ lookup_key
+ state
+ entity identity
```

Append one Airtable row only when a tracked prepared value changes. Tracked
values include the current or next entity identity, counts, `entries_ahead`,
`go_in`, `starts_in`, `ends_in`, accepted pace, and status tags where
applicable.

Requirements:

- Catalyst creates the Airtable row directly when the changed state occurs;
- do not poll Catalyst later to reconstruct the log;
- do not update or upsert an older Airtable history row;
- do not append an unchanged state on every twelve-minute cadence;
- retrying the same generation creates no duplicate Airtable row;
- use the existing Airtable table and supported fields; this policy does not
  authorize an Airtable schema change;
- preserve all earlier Airtable history rows.

## Quick Name Lookup

Use Catalyst indexed search. Do not create a second quick-index table or scan
Airtable in the hot path.

Eligible horses:

```text
barn_name is populated OR follow=true
```

Eligible riders:

```text
follow=true
```

Search approved names, display names, barn names, and aliases. Rank exact,
normalized exact, then prefix matches. Return a choice when more than one
eligible entity matches.

## Refinement and Change Control

Runtime logs may reveal that a threshold, pace range, or spacing rule needs
revision. Such evidence creates a proposed policy change; it does not alter the
contract automatically.

Every revision must record:

- observed evidence;
- requested policy change;
- affected lanes and triggers;
- regression tests;
- scheduled acceptance gate;
- effective date.

## Implementation Status at Creation

| Area | Status on 2026-07-12 |
|---|---|
| Baseline Time Engine trigger creation | Implemented |
| Direct Time Engine to `wec-alerts` | Stopped |
| Correct tracked trigger scope and registry | Not implemented |
| Snapshot-derived pace and current runtime propagation | Task 05 scheduled PASS; Task 06 consumption not proven |
| `statewise_now` endpoint | Exists |
| `statewise_now` producer | Not implemented |
| `hs_rider_results` endpoint | Exists |
| `hs_rider_results` producer | Not implemented |
| Dense schedule/entity endpoint | Implemented; upstream target fields remain incomplete |

This status table is historical context only. Scheduled workflow evidence is
required before any target behavior is marked operationally `PASS`.
