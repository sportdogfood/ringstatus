# RingWaze Invention Boundary

## Frozen controlling boundary

> Authoritative ordered event schedule + intermittent authoritative updates + broad physically situated crowd observations of actual execution + confidence/reliability filtering + historical execution data → repeatedly refreshed participant-specific timing for downstream competitors as actual execution proceeds between official updates.

This boundary controls the working analysis. It should not be redefined as generic crowdsourcing, generic sequence reconstruction, generic ETA prediction, generic sports reporting, or RingWaze as an entire product.

## Strongest current center

The strongest center is the computer-implemented maintenance of an authoritative ordered event structure as an operationally current timing system:

1. Repeatedly receive an event structure from an external authoritative source, including scheduled activities, authoritative states or changes, and an ordered sequence of participant executions.
2. During actual execution, receive timestamped observations from devices associated with people physically present at event locations.
3. Do not restrict submission to a predetermined population of competitors, organizers, officials, vendors, or event-operating personnel.
4. Treat submitted content as reports of witnessed facts occurring or having occurred, rather than predictions of future execution.
5. Evaluate reports as execution evidence using one or more objective factors such as freshness, location association, independent corroboration, prior reporter reliability, timing plausibility, and consistency with the authoritative order.
6. Associate accepted observations with positions, transitions, interruptions, or states in the authoritative event structure.
7. After an authoritative schedule state or change, use later accepted observations to repeatedly update the continuing timing effect of that state or change without requiring the authority to publish every later transition.
8. Maintain historical execution information for participant executions, transitions, interruptions, locations, event types, and scheduled-versus-actual timing.
9. Determine updated expected execution times for multiple downstream participants according to their respective positions in the authoritative order.
10. Repeat the state and timing updates when qualifying authoritative information or observer evidence arrives.

## Necessary role separation

### Authoritative source

Defines what is officially scheduled and officially changed. Examples include the ring, class sequence, order of go, scheduled times, scratches, moves, additions, delays, and holds.

### Physically situated observer population

Reports what is witnessed during execution. The population is broader than the bounded group operating or participating in the event. Individual observers may still be competitors, officials, or vendors; the point is that submission is not limited to those roles.

### Reliability process

Determines whether, when, and with what confidence an observation may alter the machine-maintained execution state. This process is functionally important because the broad population is not inherently authoritative.

### Historical execution information

Provides timing context such as trip duration, transition duration, interruption duration, current and prior pace, location-specific timing, event-specific timing, and scheduled-versus-actual differences.

### Timing process

Uses accepted execution anchors and historical information to update timing for downstream participants according to their positions in the authoritative order.

## The operative information chain

```text
authoritative schedule/order and official change
                    +
later crowd reports of witnessed execution
                    ↓
reliability/confidence evaluation
                    ↓
accepted timestamped execution anchors
                    ↓
current execution relative to authoritative order
                    +
historical/live execution timing
                    ↓
different updated times for downstream participants
```

## What is inside the proposed core

- An independently authoritative ordered event structure.
- Intermittent authoritative states or changes that remain controlling but may become operationally stale.
- Independent, physically situated observation of actual execution between official updates.
- A submission population not restricted to predetermined event-operating or participant roles.
- Computational acceptance/rejection or weighting of noisy observations.
- Association of accepted observations with the authoritative ordered structure.
- Continued updating of an official change's timing consequences as execution proceeds.
- Historical execution information used with current accepted facts.
- Different expected execution times for multiple downstream participants based on their positions.

## Supporting mechanisms that should not displace the center

- observation-type normalization;
- physical location or check-in methods;
- historical reporter accuracy;
- sparse observations and inferred intervening progression;
- observed versus inferred provenance;
- interruption-time separation;
- historical/live pace blending;
- candidate execution states;
- class start/end estimates;
- notification delivery.

These features may support claims or specification detail, but the conversation did not establish any one as the invention by itself.

## Explicit exclusions

The current patent position should not depend on claims that:

- crowdsourcing itself is new;
- spectators reporting sports is new;
- observer confidence or reputation is new;
- an observer predicts the current entry;
- the crowd replaces the official source;
- an official source lacks real-time input in all prior systems;
- a crowd is the only possible means of obtaining coverage;
- simple scale or a larger number of users is patentable;
- a generic computer receiving data and predicting times is enough;
- every RingWaze product feature belongs in the patent core.

## Objective terminology for eventual drafting

Prefer objective operational language over conversational shorthand:

| Conversational phrase | Safer operational description |
| --- | --- |
| Broadly available crowd | Submission is not restricted to a predetermined set of event participants, organizers, officials, vendors, or event-operating personnel. |
| Physically situated | Device or user is associated with an event location through location, check-in, QR, beacon, credential, explicit selection plus corroboration, or another disclosed mechanism. |
| Real time | Received during execution or sufficiently contemporaneously to update the current execution state. |
| Continuously | Repeatedly recalculated in response to qualifying new information. |
| Operationally stale | Later observations update timing without requiring a further authoritative change for each execution transition. |
| Reporter accuracy | Reliability derived from prior observations later assessed through accepted observations, authoritative information, or other confirmation. |

## Boundary test

A proposed claim or description remains centered only if it answers all of these:

1. What independently authoritative ordered structure is maintained?
2. What official state or change has been received?
3. What later actual-execution fact was witnessed, rather than predicted?
4. Why can the report be accepted as evidence?
5. Where does that fact attach within the authoritative order?
6. How does it update the continuing temporal effect of the official state or change?
7. What historical execution information is used?
8. How are different times produced for multiple downstream positions?
