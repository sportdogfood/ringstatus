# Ring Waze Concept

Existing implementation: [Sessions, scoped comments, ring check-ins and observations — system register](../../docs/horseshowing/wec-comments-observations/EXISTING-SYSTEM-REGISTER.md). This records the WEC dataset implementation, June test evidence and remaining gaps so those paths are not treated as unbuilt concepts.

## Purpose

Develop a show-day decision layer that helps teams understand who needs to move, where they need to go, and when.

## Scope

- Ring-specific human updates, comments, and check-ins.
- Live schedule, timing, location, and responsibility context.
- Operational prompts for horses, grooms, riders, trainers, barns, and rings.
- A shared view of current conditions and the next action that needs attention.

Ring Waze remains focused on horse-show operations. It is not intended to become general-purpose GPS navigation.

## Current direction

Continue the Waze concept by combining trusted live state with location, timing, and responsibility logic. Preserve the existing RingStatus concept of one ring-specific update feed across web, SMS, and check-ins, then use that context to support practical movement and readiness decisions.

## Next considerations

- Define the first decision or alert the concept must produce reliably.
- Separate observed facts, human reports, calculated estimates, and recommendations.
- Establish freshness, confidence, acknowledgement, correction, and escalation rules.
- Identify the minimum ring, show-day, participant, schedule, and location data needed for a useful prototype.
