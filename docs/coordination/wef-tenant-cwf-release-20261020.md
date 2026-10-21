# WEF tenant CWF release — 20 October 2026

Owner approved the individual tenant base, shared directory and ring references,
testing, deployment and normal scheduled verification. No manual business runs.

## Targets and source

- `wef-tenant-cwf`: https://airtable.com/appxKPBtdl2Rdq7CS
- Shared directory: https://airtable.com/appUq5Qz9kARqFija/tblrbItiPaMu1I1AE
- Shared rings-helper remains the existing `tblgFqVUAx7QlbnK8`; existing record
  IDs and fake walking values are preserved.
- Package: `wef-tenants/`; baseline checkout HEAD
  `1e357b5315479c2fbebaf031b5a7da2e4a630698`, with existing uncommitted WEF
  releases preserved. Exact baseline file hashes and task XML are under
  `wef-tenants/evidence/`.
- Base schema and approved CWF configuration were created through authenticated
  Airtable UI and supported Airtable metadata/record APIs. Connector base creation
  returned403; its token lacks new-base access. The existing private schedules
  runtime credential successfully read the new schema and was reused at the new
  namespaced destination without exposing/changing the original credential.

## Accepted structure

See `wef-tenants/README.md` and `schema.json`. A tenant base contains tenant-details,
trainers, heartbeat, show-details, collections, rings, rings-helper, ring-walks,
horses, riders, entries, run-logs and code-reviews. The starter meta table has no
business role. Existing global Tenants identity/access tables remain untouched.

The shared index maps a tenant and source-selected show to the individual base.
It keeps active collection configuration separate from proof of attendance.
Only the main trainer8778 is initially enabled; up to five additional trainers
can be configured. Owner home pin and six temporary walk values are preserved.

## Verification status

Independent read-only review PASS; 19 offline tests PASS. Review corrections
covered ambiguous entry dates, isolated transport paths and terminal failure
status in the directory/heartbeat. Evidence: `wef-tenants/evidence/offline-tests.txt`.

Dedicated task `wef-tenant-cwf-collector` was registered without starting it.
First ordinary trigger: 12:57 PM Eastern, then every five minutes (:02/:07/etc).
The existing schedules task remains at :00/:05/etc. No existing task was changed.
Task definition: `wef-tenants/evidence/tenant-task-deployed.xml`.

Independent technical review was recorded/read back in tenant `code-reviews`,
table `tblXKj2knWLvJ80mQ`, record `recMaNo3AhWEUNJDK`. New read-only Monday09:00
America/New_York review heartbeat: `wef-cwf-monday-code-review`. Existing schedule
review `wef-collector-monday-code-review` remains unchanged. Initial next review:
26 October. A review finding does not authorize fixes or business runs.

## First normal scheduled run — PASS

Normal 12:57 trigger started 12:57:01 and completed 12:57:40 Eastern, task exit0.
Run ID `3134705d-cac2-4a5d-afbc-90ce13fdab8f`; read-only independent receipt,
Airtable and archive verification PASS in
`wef-tenants/evidence/scheduled-3134705d-cac2-4a5d-afbc-90ce13fdab8f.json`.

Actual source selected ESP Fall III200000075, October22–25, selected October22.
Trainer8778 returned a validated explicit empty trip dataset: `No entries`,
zero errors, four complete daily collections, one heartbeat/show, 24 daily rings,
six shared helper projections and six CWF example walks. Five archive events
(one trainer payload and four daily datasets) read back with matching SHA256.
Content-addressed empty daily datasets reuse the same verified physical file;
each dated archive use is documented separately in run-logs.

There are no horses/riders/entries derived from this current-show response.
Historical legacy roster records were not copied or presented as current-show
attendance. Nonempty/multiple-trainer paths are fixture-proven, not live-proven.

## Final guard before acceptance

An additional offline adverse test exposed a failure-handler retry risk after
an unresolved Airtable creation response (two POSTs instead of one). The adapter
now remembers unresolved keys for that invocation and requires an observed
saved record before another handler can patch it; it cannot resubmit the POST.
The same test was observed failing, then the full19-test suite passed. Independent
read-only review of this delta passed. Product hashes refreshed at13:00:58,
between natural triggers; no manual business execution.

The12:57 pass remains evidence for source-valid empty handling, not acceptance
of the final guard. Final-version acceptance will use the normal13:02 and13:07
triggers plus source/task/static-value and repeat-identity comparisons.

Final-version normal13:02 trigger completed13:02:33 Eastern, exit0, no errors.
Run `4d11f084-4719-4d6f-9b15-bf7a3dcfff10`; saved read-only verification has
four complete day collections, 24 daily rings, six helpers, six example walks,
two accumulated run-logs and five archive events. All trainer/show/day keys,
ring projection identities, references, log/receipt counts and archive hashes
passed. The13:07 repeat was pending at that checkpoint; its completed evidence
is recorded below.

## Final scheduled acceptance — PASS

Normal13:07 trigger started13:07:01 and completed13:07:35 Eastern, task exit0.
Run `12ba78a7-24dc-4c79-b36b-4c7211f1c261`; corresponding saved verification
under `wef-tenants/evidence/scheduled-12ba78a7-24dc-4c79-b36b-4c7211f1c261.json`
passed. Both final-version13:02/13:07 runs returned validated `No entries` for
main trainer8778 and kept four complete daily collections, one show,24 daily
rings, six helper projections and six example walking records. No source errors.

`repeat-comparison.json`: all12 data/reference/config tables retained the same
record IDs and values, except expected check/run timestamps. The second run
added exactly one run-log and preserved both earlier log rows. Ten dated archive
events across both final-version runs were verified by compressed readback and
SHA256. No repeated reference/collection record creation or duplicate keys.

`final-boundaries.json`: final deployed tenant code remained unchanged between
these runs; original schedules source files and all four original task definitions
remain unchanged. `shared-static-check.json` independently confirms all six
original shared helper records and the existing CWF tenant/home location/example
values remain unchanged. This adds a new task; it does not modify legacy tasks.

Tenant code-review result was updated/read back after acceptance, retaining next
Monday26 October09:00 Eastern. This metadata update is intentionally after the
saved run-to-run comparison. CWF collector remains enabled every five minutes;
next ordinary trigger after acceptance13:12. No manual runs, record repairs,
legacy workflow edits or publishing occurred.

Coverage limit: nonempty entries, multiple active trainers, failed trainer
preservation, rollover deletion and log-slot retention are offline fixture-proven.
They were not manufactured in live Airtable. Current live source produced no
entries, so horse/rider/entry tables are empty rather than fabricated or filled
with historical legacy attendance.

Independent final read-only evidence review PASS: both final scheduled receipts,
all10 archive-event hashes and stable record identities verified without another
production call or write. Airtable UI proof: `wef-tenants/evidence/airtable-run-logs.png`.
Repository-wide whitespace checking reported three pre-existing documentation
EOF warnings outside this release; those protected files were not changed.
