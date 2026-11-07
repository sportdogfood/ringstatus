# WEF Live — locked protected baseline

**Version 2026.11.07.3 · Locked 2026-11-07T08:31:03-05:00 · America/New_York.**

Current runtime: independent groups, rings and class-live-details; decoded-trip acquisition and live-trips updates paused. Status uses canonical Shows priority-0 date and existing published Eastern time. `completed:null` is accepted as an empty completed list. Four active source feeds use the existing shared proxy. No new deployment was needed for the null clarification.

Further code, schema, source routing, cadence, cleanup policy, deployment or resume changes require owner approval. Normal scheduled operation and authorized read-only checks/reviews continue. This is a documented lock; no permissions, task, processes or schedules were changed. No manual collector/provider runs or record repairs.

Protected hashes/task snapshot: [baseline](evidence/lock-20261107/baseline.json). Runtime still matches [current deployed manifest](evidence/shows-time-20261107/deployed.json). [README known trouble](README.md#known-trouble-and-operating-limits) is part of this lock.

## Known trouble retained in this lock

- Immediate Records chat transport is not configured; local/Airtable logging is not delivery.
- Two existing conditional cleanup-reporting P2s: unexecuted plans can remain Running; interrupted stop can leave stale latest/Airtable summary.
- An empty-object widget response can produce no rings without a warning.
- Collection heartbeat native show/date cells are not populated.
- Paused trips/results/identities are retained but not refreshed; new UI-only rows may lack decoded identity IDs.
- Three-minute checks do not guarantee three-minute completion; IgnoreNew/ten-minute task policy is unchanged.
- Published Shows time may lag; the existing missing-time fallback remains.

These are known limitations, not newly authorized repairs or new gates. Latest inspected completed collection at08:27Eastern had zero collection/logging issues and zero decoded calls; no cleanup stop file was present. Decoded trips is the only open workflow design topic. Stages8/old24h workflow remain held; approved delete-only cleanup remains active. Resolved completed:null and old direct-response failures are not listed as open fixes.

Acceptance: [two normal pause runs](evidence/pause-decoded-20261107/acceptance.json), [Shows-time normal decision](evidence/shows-time-20261107/acceptance.json), [null code review](evidence/completed-null-review-20261107/review.md). Future natural-event coverage is not fabricated; old evidence remains dated history. Previous lock preserved at [previous LOCK](evidence/lock-20261107/previous-LOCK.md).
