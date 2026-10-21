# CWF horse and rider identity policy — 20 October 2026

Scope: existing wef-tenants package and wef-tenant-cwf metadata only. Existing
wef-schedules/legacy workflows, task cadence and source data are protected.
No manual collector runs, source endpoint calls or record repairs.

## Policy

Unique populated values, independently in each column of each table:
- Horses: horse-key, horse-id, horse-name, name, nick-name, show-name.
- Riders: rider-key, rider-id, rider-name, name, nick-name.
- Riders use first-name and last-name instead of show-name. First/last names
  may repeat individually; full names remain unique.
- Names normalize Unicode NFKC, case and whitespace; numeric IDs are positive
  safe integers. Opaque stable keys preserve case and are required.
- Optional blanks remain blank. No fabricated nickname/ID or collision repair.

Collector checks both standing plans before helper writes. The Airtable adapter
validates retained records plus partial updates before each roster upsert.
Lookups reject conflicting policy columns; errors identify table, field and
conflicting stable keys. Direct manual Airtable edits are detected on the next
scheduled run, not blocked instantly by an Airtable native uniqueness constraint.
This does not claim cross-table transactional writes.

Rider show-name was renamed in place to legacy-display-name, preserving its
field ID and all values. This retired provenance is excluded from producer,
configuration, lookup and computed search. No business records were rewritten
by the metadata setup. Horse show-name stays active.

## Evidence

Baseline, metadata before/after, deployed package hashes, fixture results and
passive scheduled-run snapshots: wef-tenants/evidence/identity-policy-20261020/.
42 offline fixtures passed; independent read-only review found no blocker.
Live acceptance PASS through ordinary task starts at 13:47:01 and 13:52:01
America/New_York, 20 October 2026. Both task exits 0. Receipts:
- 6faf88ae-43b9-41d5-a6ed-fcc22f26f3b9, complete 13:47:42.
- 4e6dd875-6433-49f1-b4c9-6c57b34cd9d6, complete 13:52:45.

Both show 71 horses and 33 riders with no duplicate populated policy columns,
13 unknown horse IDs and 33 blank rider nicknames retained. All previous static
values and record IDs are preserved, including retired rider display values.
Twelve archive events verified across both runs. Comparison confirms 12 stable
tables and one new run-log; no earlier log overwritten. Current-show entries
remain correctly empty. Nonempty/new-collision behavior is fixture-proven;
no artificial live collision or attendance data was introduced.

Protected schedules source hashes and existing task definitions, including the
tenant collector cadence, remain unchanged. A passive verifier initially expected
formula field names; Airtable returns field IDs. Only that verification assertion
was corrected, and readback passed; collector code was unchanged after deployment.

Airtable direct edits can temporarily introduce a collision until the next
scheduled validation. No claim of native immediate uniqueness enforcement.

