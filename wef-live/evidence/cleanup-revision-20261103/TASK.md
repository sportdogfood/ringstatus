# Revised cleanup — November 3, 2026

Direct owner APPROVED TO EDIT authorizes this change, testing, deployment and normal scheduled verification, overriding only the matching locked cleanup behavior.

Baseline: locked November2 release,24 current JS and existing hidden wef-live-collector. Delta: cleanup.js, its Airtable deletion-policy boundary and lanes.js quiet-dispatch due evaluation. No task/cadence/schema/dependency change or manual run/repair. Existing hourly/three-minute Live checks continue; cleanup is evaluated by that same runner. No independent schedule. Records supplies alerts; existing errs-log/local failure receipts remain.

New Eastern calendar day clears all completed eligible rows from heartbeat/run-logs/errs-log, regardless of count. Otherwise every3 elapsed hours check those tables and, at1000+ total, delete oldest500 eligible completed rows. New show clears all rows in the five checked owned Live tables, regardless of show-id, plus eligible completed rows in the three logging tables. Active/current logs and diff children stay protected. New planning show-day has no separate trigger. New-show/day full clears subsume the threshold action and restart its3hour clock. Selection controls retain their existing checked-table scope. cleanup-logs and catalogs remain protected action history, not the three collection logging tables.

Delete only, metadata-only receipts; existing exact-value/ID/canonical pre-delete and absence readback remain. Stage8/old24h retention and all unrelated collectors unchanged. Existing two reporting P2 findings stay separate. No source/event replay, forced failure or stop clearing. Natural show/day/threshold branches are not claimed proved by unchanged-show runs.
