# Entries/CWF schedule-link rollout â€” 24 October 2026

Status: plural transport/consumer support deployed and verified through two ordinary
entries-to-CWF scheduled cycles. Actual multi-group source mapping remains open.

Owner approved implementation of agreed multiple-schedule links, preservation of
keys/helpers/privacy, coordination with schedules/live, and two normal runs.
No manual collector runs, substitute source calls, or record repairs are allowed.

Baseline: repository HEAD `28ddf2815d487dbb28144dde34452fdcb38a72d3` plus preserved
pre-existing changes in `git-before.txt`. Exact modified-file backups are in
`before/`; tested copies are in `staging/`. `enabled.json` records activation and
deployed hashes. `records-before.json` contains the GET-only record baseline.
Task definitions are in `entries-task-before.xml` and `cwf-task-before.xml`.

110 offline tests pass (`tests.txt`), including repeat CWF consumption of two
schedules/two rings with inactive helper preservation, strict transport validation,
missing/dropped/ambiguous references, source failure retaining observation time and
history, stale dated health, valid empty collection, and existing privacy tests.
Saved ordinary people payloads also normalized with the new singleton transport;
this replay is diagnostic only, not scheduled proof.

Changes: source `schedule-keys` and `observation-status` metadata; safe fields in
existing CWF/live export views; automatic CWF source-field propagation; CWF reader
and central writer transport, observation status, date health, retained history.
Local native schedule/ring links are resolved by keys, not cross-base record IDs.
No task, endpoint, decoder, legacy model, shared entity base, or permission changes.

Coordination: live confirmed compatible reader ready before entries writer enabled.
Live owns its code and normal-run proof. Schedules accepted the unchanged key and
date-health contract; schedules producer remains untouched by this rollout.

## Explicit remaining source limitation

Current saved people payloads do not establish participation-specific group
eligibility. Coordinator confirmed class/group context alone is insufficient.
Production still accepts only unique eligible singleton schedules and rejects
ambiguity. The pure selector/serializer and consumers support plural links, but
a proven source adapter for multi-group eligibility is not implemented or claimed.
Natural multiple-group proof remains blocked by missing attributable source
evidence. Fixture success is not evidence of natural multi-group participation.

Both private follow lists remain empty; populated personal paths are fixture-only.
Shared entity migration and user-specific private delivery remain outside this scope.

## Normal scheduled acceptance — PASS

All times America/New_York, 24 October 2026. No manual collector runs or repairs.

| Pair | Entries run | CWF consuming run | Result |
| --- | --- | --- | --- |
| 1 | 13:04:01–13:04:31 | 13:07:02–13:09:45 | 9 current, 1 historical; 9 local links verified; zero duplicate people fetches |
| 2 | 13:09:02–13:09:36 | 13:12:05–13:15:18 | Same identities, links and counts; zero duplicate people fetches |

`run-pair-1.json` and `run-pair-2.json` contain GET-only Airtable readback and
successful heartbeat/run-log evidence. Each CWF receipt identifies the exact
entries completion it consumed. `repeat-comparison.json` confirms distinct runs
with unchanged record IDs, schedule/ring/helper links, statuses and active slices.
Existing helper keys, nicknames, notes, aliases and disabled flags were preserved;
retired CWF entry records were unchanged. No private owner/source provenance was
exported. New array/status fields arrived through automatic Airtable sync only.
Both before/after task XML pairs match exactly; deployment hashes were unchanged.

Live owner independently confirmed two normal participation-link passes:
17:07:13–17:08:03Z and 17:12:13–17:13:05Z, both TODAY references current,
all six prior native links preserved, no link or logging issues. Overall live
collection remains Partial solely for its pre-existing class429 identity issue.
See `wef-live/evidence/participation-run-1.json`, `participation-run-2.json` and
`participation-link-release.md`; those are separate live-owner evidence.

Do not interpret this acceptance as natural multi-group eligibility proof.
The source limitation above remains; no records were fabricated to demonstrate it.
