# CWF failure visibility, maintenance alignment and AC sleep — 28 October 2026

Owner-approved scope completed. No manual collector runs, business record repairs,
task/cadence changes, schema changes or duplicate source collection.

## Changes and preserved baseline

- CWF `collector.js`: caught failures now publish Failed heartbeat even before a
  show is validated. Unknown show/date are null; no stale identity is claimed as current.
  Failure to publish status remains explicit in the terminal report.
- CWF `entry.js` and `completion-guard.js`: reuse the tested independent schedules
  guard, changing only its CWF label. Default failure until terminal collector and
  saved receipt complete; pending natural exit or premature exit 0 produces exit 1
  and an `incomplete_run` local error. No retry or record repair is introduced.
- Collection, source adapters, archive-before-delete, helper ownership, synced
  fields/privacy, links and launcher are unchanged. `before/` retains edited originals;
  `deployment.json` records deployed/protected hashes.
- Designated schedules maintenance copy aligned from verified deployed schedules.
  `maintenance-alignment.json` verifies all 58 inspected package files. Old differing
  files preserved in `maintenance-before/`; no files deleted. Runtime, secrets,
  local history and installed dependencies were excluded. Deployed schedules code
  was not replaced. Final README/manifest documentation hashes match both copies.

## Power settings

Existing Balanced plan retained. AC idle sleep changed 18,000 seconds to 0 (never);
AC unattended sleep 120 to 0; AC lid action Sleep to Do nothing. AC hibernate already
0 and remains 0. All DC settings and other inspected values unchanged. Full before/
after queries and `power-verification.json` record the readback. The PC was on AC.
No service, background keep-awake process or automation was added. No deliberate
lid closure/sleep was induced: configuration and ordinary-run operation are verified,
not forced power-transition acceptance or immunity to manual shutdown/power loss.

## Local verification

The three new early-failure tests initially failed against the staged original.
Six of seven entry-point tests initially failed, including the silent exit-0 case.
After the scoped correction, all 94 CWF offline tests passed (`tests.txt`).
Inert adapters only, no production requests in fixtures. All 134 schedules fixtures
passed in the aligned maintenance copy (`maintenance-tests.txt`).

## Two ordinary scheduled runs — PASS

All times Eastern, 28 October 2026.

| Trigger | Run ID | Wrapper completion | Result |
| --- | --- | --- | --- |
| 18:22:01 | adeff827-49a4-4eb7-8620-e0de1cea9aab | 18:25:02 | Exit 0; completion marker; Success heartbeat/run-log; 3 entries/3 links; 4 complete dates; 0 errors/people fetches. |
| 18:27:01 | 2210f79e-5713-4683-818a-53d77ef74c2c | 18:30:01 | Same successful checks; distinct run/log; stable protected helper fields and trainer settings. |

`normal-run-1.json` / `normal-run-2.json` contain connector readback summaries;
`receipt-1.json` / `receipt-2.json` preserve the runner receipts. Each run's 15 archive
events were checked against decompressed local bytes and SHA256. Both retain 76
horses, 37 riders and 3 trainers. Their inspected IDs, names/nicknames, notes,
aliases, disabled flags and trainer configuration are stable across both readbacks.
These checks do not claim unqueried helper fields were independently compared.

All five before/after task XML pairs match byte for byte (`task-comparison.json`):
shows, schedules, entries, CWF and live. Existing intervals remain unchanged (live
three minutes; the other inspected collectors five minutes). No day/night schedule
was introduced under this preserve-cadence scope.

Failure branches are fixture-proven; no production outage, test invalidation or
synthetic records were induced. Historical Running records remain untouched. This
release fixes visibility, not the unproven cause of historical interrupted processes.
No claim of broader new-show/all-consumer acceptance or shared-storage migration.
