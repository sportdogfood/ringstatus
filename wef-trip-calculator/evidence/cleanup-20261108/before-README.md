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
cleanup.js adapts the existing Class cleanup eligibility and injected-transport pattern for this Trip base only. No timer, task registration, Records alert or production transport is introduced.

- At the first existing invocation at/after19:00 Eastern, clear eligible completed logging records once per local date. Existing run-logs holds runs; this base has no separate runs table. heartbeat and errs-log are also logging tables.
- Independently every3hours, if a logging table contains1,000+records, select its oldest500eligible rows once. Do not drain repeatedly below the threshold. The caller persists quickAt only for the completed three-hour check, and dailyDay only for successful daily cleanup; neither resets the other.
- A verified default-show ID transition clears eligible owned old-show trip-status/timing answers and retired checkpoints, plus completed old-show logging rows. Same show/day-priority change is not a new-show event. Caller persists show only after success.
- Protect active/current runs, pending/uncertain writes, unresolved errors, referenced answers, active checkpoints and current/future records. Synced Ring/Class/Data/Live tables, w-shows/w-days, cleanup controls, code reviews and archive catalogs are never deletion targets. Explicit complete protection inventory and checked owned-table selection required; missing protection data is not empty protection data.
- Existing transport must durably save/read back intent, re-read unchanged targets, delete only selected IDs, verify absence, then finish intent. Failure preserves pending intent and stops that path. No record backup/archive or source repair is introduced. The injected transport and state are an integration requirement, not a claim of deployed cleanup.

Independent review evidence belongs under evidence/20261107/reviewer/. Table creation, offline logic and review are distinct from deployment/normal scheduled proof; no live delivery is claimed.
