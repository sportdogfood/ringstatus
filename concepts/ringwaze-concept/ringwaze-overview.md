# RingWaze Product Overview

## What RingWaze is as a product

RingWaze is the live observation layer for RingStatus. It helps turn an official event plan into a more useful view of what is actually happening at the venue now and what that means for people later in the day.

The preferred example is a horse show, but the product concept can extend to other scheduled, ordered, or multi-location activities such as dog shows, gymnastics meets, track and swim meets, motorsports, livestock competitions, tournaments, auditions, and judging events.

## The product problem

Event organizers and official systems publish the authoritative structure:

- rings or locations;
- classes or activities;
- scheduled start times;
- participant identities;
- orders of go;
- scratches, moves, holds, additions, and official changes.

That information is essential, but official updates are often intermittent. A ring can start late, resume, slow down, run faster, drag, hold, skip entries, or change courses between published updates.

An organizer's notice that a ring is “30 minutes late” is authoritative and useful when issued. Twenty-five minutes later, it may no longer answer the operational question. It does not necessarily say whether the ring resumed, which entry is now on course, how fast the ring is moving, or when a participant deep in the order will actually go.

For a rider who is 56th in a 100-entry class running roughly three minutes per trip, the most useful question is usually not just:

> When does the class start?

It is:

> When am I now expected to go?

One delay can affect nearly every competitor assigned to that ring, and its effect differs by the competitor's position in the order.

## How the product works

### Official layer

RingWaze receives or displays the latest authoritative schedule, order, and official changes. The authoritative source remains controlling.

### Observation layer

People physically present at a ring or other event location can report concise facts such as:

- class started or ended;
- current class;
- entry on course or finished;
- next entry;
- trips gone or remaining;
- ring drag or course change;
- scheduled, weather, medical, or other hold;
- ring resumed;
- ring delayed or ahead;
- class moved, added, skipped, or closed.

The observer pool may include spectators, trainers, family, team members, competitors when able, organizers, officials, vendors, or others. Submission is not inherently limited to the bounded group formally operating or participating in the event.

### Trust layer

Because a broad observer pool is noisy, the system may assess freshness, location association, corroboration, reporter history, timing plausibility, consistency with the authoritative order, and prior accepted state. Reports may be accepted, rejected, deferred, weighted, or retained as competing candidate states.

### Timing layer

The system can combine:

- the latest official schedule and order;
- accepted current execution observations;
- historical trip, transition, interruption, and class timing;
- current-day execution pace;
- previous scheduled-versus-actual differences.

It can then refresh current status, delays, event starts and finishes, and participant-specific expected execution times.

## Facts are not forecasts

RingWaze separates human observation from machine prediction.

| Category | Example |
| --- | --- |
| Authoritative fact | Organizer reports a 30-minute delay. |
| Observed fact | An observer reports `ring_drag` at 1:20. |
| Observed fact | An observer reports `entry_on_course = 456` at 1:34:08. |
| Historical information | Similar drags average 8:40; trips average 2:35. |
| Machine output | Updated expected times for riders 57, 78, and 90. |

An observer reporting entry 456 on course is not predicting that entry 456 is on course. The report is offered as evidence of a witnessed fact. The system decides whether to accept it and what it means for downstream timing.

## Product scope versus patent core

RingWaze as a product may contain much more than the present patent center.

| Capability | Product role | Present patent treatment |
| --- | --- | --- |
| Display schedules, maps, entries, and ring information | Core user experience | Supporting/product functionality by itself |
| User accounts, profiles, roles, badges, and notifications | Engagement and delivery | Supporting/product functionality by itself |
| Comments, photos, community interaction, and moderation | Community experience | Product functionality unless tied to the claimed mechanism |
| Organizer tools and official publishing | Authoritative operations | Supporting input source; not the invention alone |
| Broad physically situated observation submission | High-frequency execution evidence | Patent-core input relationship, but not novel alone |
| Reliability/confidence filtering | Makes open observation evidence usable | Patent-core supporting mechanism, but not novel alone |
| Mapping accepted observations into the authoritative order | Establishes current execution relative to official structure | Patent core |
| Keeping an official change current through later observations | Prevents a discrete official update from becoming operationally stale | Strong patent center |
| Historical execution modeling | Supplies timing context | Patent-core supporting input, but not novel alone |
| Position-specific timing for multiple downstream participants | Delivers the rider-specific operational result | Strong patent center |

## Product value beyond patent scope

The broader application can improve awareness, coordination, venue coverage, and communication even when a function does not fall within a final patent claim. The patent documentation deliberately avoids treating the entire app, brand, interface, community, notification system, or data platform as the invention.

See [invention-boundary.md](invention-boundary.md) for the narrower proposed invention.
