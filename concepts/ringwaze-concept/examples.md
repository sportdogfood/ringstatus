# RingWaze Worked Examples

## Purpose and category discipline

These examples keep four categories separate:

1. authoritative facts;
2. observed facts;
3. historical or current execution information;
4. machine-generated state and timing.

They illustrate the current invention boundary. They are not final claim language and do not establish that every described feature is novel.

## Example 1 — A 30-minute delay that becomes stale

### Authoritative structure

- Ring 3
- Class 410 scheduled for 1:00 PM
- Class 412 follows Class 410
- Class 412 has 100 entries in an official order of go
- Rider 56 is the 56th entry

### Authoritative update

At 1:10 PM, the organizer reports:

```text
delay = 30 minutes
```

That is authoritative. It changes the official schedule baseline.

### Why the update becomes operationally incomplete

At 1:35 PM, “30 minutes late” does not establish:

- whether the ring resumed at 1:30, 1:32, or not at all;
- which entry is currently on course;
- whether pace is faster or slower than expected;
- when Rider 56 is now expected to go.

The organizer's update remains authoritative, but its timing consequences need continuing execution evidence.

### Later observed facts

```text
1:32:14 — ring_resumed
1:34:08 — entry_on_course = 456
1:42:10 — entry_finished = 567
```

These are reports of witnessed conditions. The observers are not asked to predict a delay or Rider 56's time.

### Historical and current timing

```text
historical average trip = 2:35
historical transition = 0:25
recent accepted live pace = calculated from current-day anchors
```

### Machine operation

The system evaluates the observations, attaches accepted reports to positions in the authoritative order, updates current execution timing after the official delay, blends qualifying historical and current timing, and recalculates expected execution times for Rider 56 and other downstream participants.

The key relationship is:

```text
official delay
→ later accepted execution facts
→ current position and pace within official order
→ different updated times for downstream riders
```

## Example 2 — Entry on course as a timing anchor

### Authoritative order

```text
123 → 234 → 345 → 456 → 567
```

### Crowd reports

```text
Observer A, 1:34:08 — entry_on_course = 456
Observer B, 1:34:10 — entry_on_course = 345
```

Neither report is automatically true. Neither is a prediction.

### Reliability evaluation

The system may consider:

- report freshness;
- ring/location association;
- independent corroboration;
- prior reporter reliability;
- timing plausibility;
- consistency with earlier accepted execution state;
- consistency with the official order.

The system may accept one report, defer both, keep competing candidate states, or wait for later evidence.

### Accepted anchor

If the first report is accepted, the system records a credible first observed timestamp for entry 456 being on course. Because the authoritative source already identifies 456's position, the observation becomes a current timing anchor.

The system may use that anchor to update remaining positions and future timing. Any inference that earlier entries passed should retain inferred provenance rather than be mislabeled as directly observed.

## Example 3 — Ring drag is an observed condition, not a predicted delay

### Observed facts

```text
1:20:00 — ring_drag
1:29:00 — ring_resumed
```

The first observer is reporting that a drag is happening. The observer is not predicting that it will last nine minutes.

### Historical information

```text
historical drag duration for comparable conditions = 8:40
```

### Machine-generated result

Before resumption is observed, the system may use historical drag duration to estimate likely continuation. After `ring_resumed` is accepted, it has an actual interruption interval.

The system can add the interruption to downstream timing without treating those nine minutes as normal participant throughput. This preserves a more accurate execution-rate model.

Interruption separation supports the integrated mechanism but is not asserted as the invention by itself.

## Example 4 — Sparse observations with provenance

### Authoritative order

```text
123 → 234 → 345 → 456 → 567
```

### Accepted observations

```text
10:02 — entry_finished = 123
10:08 — entry_on_course = 456
```

### Possible machine state

| Entry | State | Provenance |
| --- | --- | --- |
| 123 | complete | observed |
| 234 | passed | inferred from accepted later anchor and authoritative order |
| 345 | passed | inferred from accepted later anchor and authoritative order |
| 456 | current | observed |
| 567 | pending | authoritative order plus current state |

The crowd did not have to report every transition. The distinction among observed, inferred, authoritative, and calculated information is retained.

Sparse reconstruction is supporting functionality and has relevant prior art; it should not replace the stronger official-update-to-downstream-timing center.

## Example 5 — Participant-specific propagation

### Current inputs

- latest authoritative order;
- accepted observation of the current entry;
- an accepted resumption time after a hold;
- current-day pace;
- historical trip and transition duration;
- remaining scheduled breaks.

### Outputs

```text
Rider 12 — 2:36 PM
Rider 56 — 4:48 PM
Rider 90 — 6:30 PM
Next class — 7:05 PM
```

The important result is not simply “Class 412 now starts at 2:30.” Each downstream participant receives a different expected time based on position in the authoritative order and the refreshed execution state.

## Example 6 — Multiple rings and observer coverage

An event may operate several rings concurrently. Competitors are occupied with preparing or executing their own trips, and organizers or vendors may not observe every transition at every location.

A broader physically situated population can submit more geographically distributed execution evidence without requiring official personnel or instrumentation at each point. This explains the product and architecture value of the crowd layer.

It is not by itself the patent center: CBS, Tapstats, ScoreStream, and Azra were discussed as prior art for broad or distributed live-event reporting and confidence. The stronger center is how accepted observations maintain an independently authoritative ordered schedule and its downstream participant timing.

## Data-category guardrail

| Statement | Correct category |
| --- | --- |
| “Organizer reports a 30-minute delay.” | Authoritative fact |
| “Ring drag is happening.” | Reported observed fact |
| “Entry 456 is on course.” | Reported observed fact |
| “Entry 456 is likely on course.” | Candidate/inferred state unless directly accepted as an observation |
| “Comparable drags average 8:40.” | Historical execution information |
| “Rider 56 is expected at 4:48.” | Machine-generated prediction |
| “Entries 234 and 345 have passed because 456 is current.” | Inferred progression, with provenance |
