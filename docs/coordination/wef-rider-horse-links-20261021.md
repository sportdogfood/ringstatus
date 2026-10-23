# Direct rider/horse links in CWF

Owner authorization: link horses directly to riders using collected entries;
preserve helpers and existing links; test and verify normal scheduled runs only.
No manual collector runs or record repairs.

Baseline is the existing deployed tenant collector after the accepted all-active
trainer rider release. Exact pre-change source, hashes, task XML and Airtable
records/schema are saved under `wef-tenants/evidence/rider-horse-links-20261021/`.

The normal collector unions each rider's existing `horses` links with horse
record links from its active reconciled entries, after the entry save succeeds.
Only rider-key and horses are supplied to this additional upsert. Existing static
helper values, prior associations and all entry links are preserved. Associations
accumulate from observed collected entries across shows; they do not indicate
ownership. No history is replayed and no source endpoint is added.

`wef-tenants/configure-rider-horses.js` is schema-only configuration: it creates
`riders.horses` linked to horses and verifies Airtable's reciprocal `horses.riders`.
It refuses conflicting fields/parallel relationships and reuses a correctly
configured field on repeat. It never accesses or writes business records. The
collector's existing field allowlist already permits these linked fields, so
schema.json and the standing helper implementation do not need changes.

Offline regression first reproduced missing direct horse links. All 54 tenant
tests pass on the staged candidate, including retained static values/associations,
repeat and duplicate prevention, empty later entries, entry-save failure, and
schema conflict/idempotence checks. Independent read-only review found no
actionable issues and independently passed 23 targeted runner/schema tests.

Live schema configuration created only `riders.horses` (`fldKt8o5QoLla9eYL`)
and the automatic reciprocal `horses.riders` (`fld1NS33LT88kykq1`) in CWF base
`appxKPBtdl2Rdq7CS`; no business records were written by configuration. The tested
collector was deployed at 2026-10-22T01:21:39.240Z (21 October, 9:21 p.m.
Eastern), with no active collector lock. All 54 deployed tests passed.

First normal run: 21 October, 9:22 p.m. Eastern, ID
`d5c98ab6-113c-471e-90f0-727a4cf710df`, completed 9:23:49 p.m., task exit 0.
The expected four rider/horse associations and their reciprocal links are
correct. All 36 rider and 75 horse helper IDs/static values and all six entry
IDs/links are preserved, uniqueness checks pass, and all 16 archive hashes verify.
Existing schema fields and task XML are unchanged. Heartbeat and run-log report
Success.

Repeat normal run: 21 October, 9:27 p.m. Eastern, ID
`a133d221-e933-4225-aaba-9b08081cbe10`, completed 9:28:39 p.m., task exit 0.
The same four direct associations and reciprocal links are correct, with stable
helper/entry identities, no duplicates and preserved original helpers/links.
All 16 archive hashes verified again. Heartbeat and run-log report Success.
Scheduled acceptance PASS across both runs. No manual collection or record repairs.

Read-only acceptance script:
`wef-tenants/evidence/rider-horse-links-20261021/verify.cjs`. It only reads Airtable,
metadata and existing scheduled-run receipts/archives; it never invokes a collector.
