# wef-trip-calculator
Updated November 7, 2026, America/New_York. Owns individual trip status, planned order, supported trips ahead and estimated go time. Default pace is fixed at **2.5746 minutes/trip**. Ring and Class remain upstream owners.

## Current result
Airtable table `appyoLTcNmLrgpHcg/tblYZ4aRBuRVmp4OA` (trip-status) created with 31 fields; names/types independently read back, schema.json. Table has no manually calculated business rows. Candidate is not deployed or connected to a runner.

First offline verification failed before later checks: source fixture contains 656 total November6 trip rows, while Active filtering selected592. test.js incorrectly hardcodes656 expected outputs. Exact output in evidence/20261107/writer-command-log.json. No test correction, calculator retest or deployment after failure. Required correction: preserve source fixture/filter and compare against the actual Active November6 subset, not all captured records. Separate explicit APPROVED TO EDIT is required under the owner's failed-verification rule.

## Existing calculation and inputs
Baseline commit/hashes in evidence/20261107/baseline.json. Legacy trips_calculator.js computes start + (order-1)*pace and clears pending order for gone trips. This adaptation uses the same arithmetic with supported pending rank and fixed pace; reuses existing orderValue/providerTime and Class timestamp/hash helpers. It does not replace or modify existing legacy/Entry/Class/Ring code.

Read-only sync review: w-shows, w-days, ring-status, class-status, wef-data.schedules/classes/trips and wef-live.rings/groups/classes/entries already exist. Existing class-status sync exposes legacy class-timing-v1 payloads. Verified Class start plus previous-class estimated end anchor upcoming trips; current Ring class plus compatible Live trip observations anchor remaining-work estimates. Ring pace is not substituted for the approved fixed pace. Provider estimates are separate sourced values. Unsupported order100000, incomplete roster, ambiguous appearances, shared-group sequence, non-base phase, missing Class/Ring or future clocks leave derived ETA unavailable. Original source timestamps remain separate from calculation time.

November6 saved fixture has no matching Class/Ring status answers. The Ring sync containsNovember7 rows, excluded from the retrospective evaluation. Source captures are retained saved rows inspected November7, not a reconstructed sequence of historical inventories. Actual source times are preserved; trips_json is projected to its original trip/entry/UUID identities, group source-json to original phase flags. Synthetic dependency cases in test.js are labelled synthetic and cannot establish live predictions. Remaining tests were not reached after first failure.

## Cleanup contract
The November 8 contract applies to supported tables checked in the Trip base's Airtable `cleanup` table. The current allowlist is `heartbeat`, `run-logs` and `errs-log`. The existing `run-logs` table holds invocation runs.

- At 1,000 records, trim a checked log table to 500 total rows, oldest eligible first. For 1,340 records, select up to 840 eligible rows. Continue an unfinished trim on later normal runs, including below 1,000. Keep `trimPending` per table until 500 or fewer remain.
- At the first normal run at or after 7 p.m. Eastern, clear eligible records from all three checked log tables. Persist the cutoff date after successful cleanup.
- A verified default-show ID change clears eligible logs from checked tables and resets any supported checked live tables. The Trip base currently has zero such live tables. A new-show log clear at or after the daily cutoff can satisfy both triggers after successful cleanup. A same-show date change leaves live records in place.
- Active runs, pending writes, unresolved errors and explicitly protected records are ineligible. Synced inputs, trip snapshots, timing answers, checkpoints and control or reference tables are outside the Trip deletion allowlist.

The caller supplies verified show identity, checked flags from Airtable, complete paginated table rows and a complete protection inventory. `cleanup()` rechecks targets, saves durable intent, deletes the selected IDs and verifies their absence. Persist the returned `trimPending` with the verified intent result, including on a normal check where protected rows leave no eligible deletions. Use the fresh table count on each subsequent run. An uncertain deletion leaves the intent pending for reconciliation.

`cleanup.js` remains an offline-tested module with an injected transport. Connecting it to a normal runner is a separate delivery step.

Independent review evidence belongs under evidence/20261107/reviewer/. Table creation, offline logic and review are distinct from deployment/normal scheduled proof; no live delivery is claimed.
