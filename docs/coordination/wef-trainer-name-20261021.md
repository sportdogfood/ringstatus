# CWF trainer names from the existing people source

Owner authorization: populate trainer-name from source, preserve existing workflows,
test and verify through normal scheduled runs. No manual collection or record repairs.

Baseline: commit `6a3991ed31df187d1b9566c1a5368669b6ba85c1` plus the existing
deployed collector and isolated decoder changes. The baseline collector bytes are
saved in `wef-tenants/evidence/trainer-name-20261021/collector-before.js`; exact
before/after hashes and deployment timestamp are in `deployment.json` there.

Smallest change: the existing trainer status upsert now also includes a nonblank,
trimmed `people.name` after the matching people identity, source archive, normalized
entries, daily archives and collections have succeeded. Missing, blank or non-text
names preserve the stored value. This applies to each configured active trainer,
including valid responses with no entries. It does not change configuration or
introduce source calls. Current archived source for 80970 says `TAYLOR COWLES`.

Changed implementation: `wef-tenants/collector.js`. Regression coverage added to
`wef-tenants/tests/runner.test.js`: source name population with no entries, stable
trainer record IDs/configuration on repeat, unusable name preservation and invalid
identity/archive failure protection. Red test reproduced the missing name; all 45
tenant tests passed in isolation and again against the deployed file. Read-only
independent review reported no actionable findings.

The scheduled task directly loads the approved checkout. Candidate testing occurred
in a separate temporary directory without runtime credentials, then only the tested
collector file was promoted while its lock was absent. No tasks or routes changed.

Normal scheduled verification evidence is recorded under
`wef-tenants/evidence/trainer-name-20261021/`. `verify.cjs` only reads existing
receipts, archives and Airtable; it never imports or invokes the collector. It
checks source names, configuration, six stable entry IDs/links, archive hashes,
heartbeat and the exact run-log.

First normal run: 21 October, 8:37 p.m. America/New_York, run ID
`e0faabe9-6e2e-4502-a6d5-172b282ca14b`, completed 8:38:57 p.m., task exit 0.
Trainer 80970 now reads `TAYLOR COWLES`; all three names match their archived
source. Six entry record IDs, identities and links are unchanged; all 16 archive
hashes verified, and heartbeat/run-log report Success. Trainer slots, roles and
active flags are unchanged. Task XML is identical before and after deployment.
Second normal run: 21 October, 8:42 p.m. America/New_York, run ID
`29c63cd0-64d0-497b-8e94-e7d0ccbc223f`, completed 8:43:40 p.m., task exit 0.
The same trainer names, record IDs/configuration and all six entry identities/links
remain correct. All 16 archive hashes verified again; heartbeat/run-log report
Success. Both scheduled runs passed. No manual runs or record repairs occurred.
