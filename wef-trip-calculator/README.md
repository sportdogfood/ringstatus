# wef-trip-calculator

Updated November 8, 2026, Eastern time.

Trip reads today's active synced trips for the default show. It uses saved Live, Ring and Class records to identify the current and next trip, calculate pending order and estimate go time. Measured Class pace takes priority, followed by compatible Ring pace and the 2.5746 minutes per trip default. An incomplete queue or missing timing evidence leaves the affected value blank. Each snapshot preserves the source observation time separately from the calculation time.

The linker fills empty snapshot references from matched records. The runner saves and reads back each snapshot before marking the five local calculated fields on its synced trip. The staged hook in `evidence/review-fixes-20261108/candidate/` runs Trip after the existing Ring and Class cleanup steps if at least 90 seconds remain. The required Airtable fields are listed in `evidence/end-to-end-20261108/schema-changes.json`.

## Cleanup contract

Cleanup acts on supported tables checked in Airtable. Trip supports `heartbeat`, `run-logs` and `errs-log`.

- At 1,000 rows or more, delete the oldest eligible rows until 500 remain. Keep an unfinished trim pending across normal runs until a fresh count confirms 500 or fewer.
- On the first normal run at or after 7 p.m. Eastern, clear eligible checked logs once per cutoff.
- When the default show changes, clear eligible checked logs and reset supported checked live tables. Trip currently has no owned live table eligible for reset. A date change within the same show leaves live records in place.

Active runs and unresolved errors stay protected. Synced inputs, trip snapshots, timing answers and control records are outside the deletion allowlist.
