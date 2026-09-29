# Location Layer Concept

## Purpose

Define how RingStatus can represent the live location of horses, grooms, riders, trainers, barns, and rings so show-day teams can understand movement and estimated arrival times.

## Scope

- Reusable places, ring locations, and showground reference points.
- Time-stamped location or check-in signals for the people and horses involved in an event.
- Movement, proximity, and ETA calculations that can support show-day decisions.
- Clear consent, visibility, retention, and accuracy rules for any collected location data.

This is a concept definition, not a production tracking implementation.

## Current direction

Pursue location data as an additional live-data layer for RingStatus. Connect it to known event, schedule, horse, person, barn, and ring context rather than treating coordinates as useful on their own. The layer should supply trusted inputs to Ring Waze-style operational guidance.

## Next considerations

- Decide which location signals are useful: active device location, deliberate check-ins, fixed places, or a combination.
- Define identity, event, timestamp, freshness, and accuracy requirements.
- Establish permissions, privacy controls, retention, and safe fallbacks when location is missing or stale.
- Test the smallest show-day question the layer must answer, such as whether a horse or person can reach a ring on time.
