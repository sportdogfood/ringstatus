# Independent WEF tenant collector

## Plural schedule-link reader - 24 October 2026

The deployed CWF consumer accepts the existing scalar or strict `schedule-keys`
JSON transport from entries. It resolves every key to a CWF-local synced schedule,
derives distinct local rings from those schedules, and preserves helper fields,
roster identities, reciprocal associations and the active-team/private boundaries.
Array rows require `observation-status=successful`; existing current-show,
collection, heartbeat and 30-minute participation freshness checks still apply.
Missing/ambiguous/dropped references stop consumption without clearing prior links
or falling back to duplicate source collection. Retained links are historical;
a failed run is not evidence of current eligibility.

Reader deployed before entries writer activation. Normal CWF runs at 13:07 and
13:12 Eastern on 24 October verified all nine links with no duplicate source fetches;
see [entries rollout](../wef-entries/evidence/multiple-schedules-20261024/release.md).
Actual multiple-group source eligibility is not yet proven; no inference from class
membership is allowed. Helpers and retired `entries` records remain protected.


Current entry source (23 October 2026): CWF consumes the native read-only
`wef-entries.entries` sync (`tbl7ychn5HELB3HTr`), with
`WEF_TENANTS_ENTRY_SOURCE_MODE=synced`. Duplicate people-endpoint entry fetching
is stopped. The five-minute task still maintains helpers, links, collection
archives, heartbeat and run-logs. Old `entries` records are retained unchanged;
use `wef-entries.entries` for current participation. Two scheduled shadow runs
passed before switching; the 13:22 and 13:27 Eastern consumer runs both passed
with zero people fetches and nine linked entries.
See [cutover evidence](evidence/entries-consumer/README.md).

Owner authorization, 20 October 2026: build `wef-tenant-cwf`, `wef-tenant-index`
and a shared rings helper following the schedules pattern. Existing workflows stay
untouched. Production collection is scheduled only; no manual executions or repairs.

## Ownership and baseline

- Tenant base: `appxKPBtdl2Rdq7CS` (`wef-tenant-cwf`).
- Shared base: `appUq5Qz9kARqFija` (`wef-schedules`).
- Shared directory: `wef-tenant-index`, `tblrbItiPaMu1I1AE`.
- Shared authoritative `rings-helper`: existing `tblgFqVUAx7QlbnK8`.
- Physical ring identity, nickname and optional coordinates are maintained there.
  The tenant base receives references on each normal run. This is a one-way
  collector projection, not an Airtable native sync or cross-base linked field.
- Tenant walking minutes live in `ring-walks`, keyed by tenant/start point/ring.
  The six CWF examples (8, 10, 6, 12, 14, 16) remain marked examples. Existing
  shared example fields remain untouched for compatibility. Tenant walk edits
  are not overwritten by later shared-reference refreshes.

The supplied schedules implementation and its October20 release evidence are the
baseline. `evidence/schedules-code-before.json` and four task XML snapshots record
its exact state. This package adapts the same lock, verified compressed archives,
readback and stable-key patterns at a separate destination. It copies the proven
SGL PowerShell transport without importing executing legacy collectors.
Legacy source identity mapping comes from `trips_dailyv2.js`
`buildAuxiliaryRowsForTenant` and `lib/airtable/trainer_info.js`.

## Collection behavior

### Standing horse and rider helpers — owner correction, 20 October

Horses/riders must populate even when the main trainer has no current-show
entries. The normal collector now reads the existing legacy CWF tenant and its
horse/rider reference records before show discovery or people-entry fetching.
The exact source is `apptdhhNzduxm5gjn`: `ww_tenants`, `ww_horses`, `ww_riders`.
Those three tables are GET-only in the adapter; no source records are changed.
Explicit tenant links and configured trainer associations supply the standing
list. All active trainers supply horses and riders;
previous explicit tenant memberships are retained. Absence never deletes helpers.

New helper fields include name, nick-name, show-name, aliases, notes, tenant-id,
trainer-id/trainer-ids, trainer-name and source reference/status fields. Riders
also have first-name/last-name from the existing properFirst/properLast or explicit
last-first source. Horse show-name means registered/show display name, not a
competition name or evidence of attendance. Initial horse nicknames come from
existing barn_name. No rider nicknames are invented.

Static fields are seeded once, then owner edits—including cleared fields—remain
untouched by both standing refresh and later entry collection. Source-name,
source-inactive and helper-refreshed communicate source state separately. The
initial source inactive flag seeds disabled only for a newly imported helper;
later tenant-owned disabling stays independent. Legacy horses without a numeric
ID retain a stable source-record key and source-id-missing flag; IDs are never
guessed. Later source ID completion preserves an unambiguous existing key.

The computed lookup-text field supports Airtable search across helper names and
identifiers, updating immediately on owner edits. `lookupHelpers` supports exact
normalized name/nickname/show-name, rider first/last, horse/rider ID and trainer/
tenant ID filters. It returns every matching record when a name is ambiguous.
Names are never used to merge identities. No public endpoint was introduced.

Every normal run archives the exact standing source before applying helper
updates. The run-log summary reports standing counts and missing numeric IDs
separately from current-show entry counts. Initial release inventory:71 horses,
33 riders (32 under8778 plus one explicitly linked CWF rider under19676),13
horses lacking source numeric IDs and9 source-inactive horses.
`verify-standing-roster.js` is the read-only initial-release acceptance utility;
its expected examples/counts describe this dated inventory, not a future quota.

- Read the shared default show's source-selected date. Never substitute today.
  Reject invalid dates, multiple defaults or references older than 30 minutes.
- Retain all show dates, including days with no entries. The shared directory
  maps tenant + show to its base; enabled collection does not assert attendance.
- Exactly one active main trainer in slot1; up to five additional active trainers
  in slots2–6. No duplicate IDs or slots. Initial main trainer is Alan Korotkin,
  people/trainer ID8778. No additional trainers are guessed or enabled.
- Each trainer is fetched and validated independently. Require matching people
  identity, explicit trips array and consistent supplied show IDs. A small valid
  empty array is `No entries`; `{}`, wrong identity, inconsistent counts and
  missing arrays fail. No direct endpoint is invoked by setup or tests.
- Match entries to the shared class schedules and retain stable show/date/entry/
  class keys. If a dateless entry has multiple possible days, fail rather than
  assign attendance. No new calls to class/rider/horse endpoints.
- All active trainers supply horses and riders.
  Their entries retain rider source IDs. Historical roster records and owner
  disabled flags survive a show with no entries.
- Shared entries deduplicate across trainers and retain source associations.
  Failed trainer associations remain active until successfully refreshed.
- Linked fields stay within the tenant base. Cross-base schedule references use
  the shared stable schedule key and Airtable record URL.

## History and logs

Each acquired run has a run-log with timestamps, outcome, counts, per-trainer
results, errors and every archive path/hash. Every valid trainer payload and
every daily normalized dataset is compressed, read back and SHA256 verified
before collection writes. Archives are content addressed and reused only after
identical-byte verification. Errors propagate as failed process exits.

Keep current-show entries and collections. After a completely successful run,
archive exact retired records and acknowledge their archive in run-logs before
deleting them. Old show/ring references and standing roster identities remain.
Run-logs retain 1000 finished/running rows; the oldest finished row must be
verified in an archive before its slot is reused. No guessed duplicate repair.
Creation POSTs are not retried after uncertain responses; read by stable key.

## Runtime and verification

New dedicated task: `wef-tenant-cwf-collector`. Five-minute interval, offset
from the existing schedules task, interactive current user and hidden window.
Only this task starts `run.ps1` / `entry.js` against the private runtime.
Environment keys use `WEF_TENANTS_`; no legacy environment fallback. Credentials
and session material remain under ignored `runtime/`, never release evidence.

Offline tests use memory-only Airtable/source fixtures and temporary archives.
Live acceptance must come from ordinary scheduled runs, with a separate receipt
readback, stable IDs across repeats, verified archives, and unchanged existing
source/task hashes. An empty live result does not prove the nonempty-entry path;
that path remains fixture-proven until naturally occurring entries arrive.

`setup.js` is schema and approved configuration only, not a business collector.
Do not use it as workflow proof. `verify.js` (when supplied) reads receipts and
saved Airtable/archive state; it must never trigger collection or repair records.

## Roster identity policy � 20 October 2026

Each populated value is unique within its own column and tenant table.
Horses: horse-key, horse-id, horse-name, name, nick-name, show-name.
Riders: rider-key, rider-id, rider-name, name, nick-name. Riders use separate
first-name and last-name fields; individual first/last names may repeat.
Names ignore case and extra whitespace. IDs must be positive integers when
known. Stable keys remain exact and required. Unknown IDs and optional nicknames
remain blank; no invented values, automatic renaming or merging.

The collector checks the full standing plan before either helper table is
written, and each roster upsert checks its merged target table before mutation.
Helper lookups also reject duplicate policy values. A manual Airtable edit is
checked on the next scheduled run, not instantly by a native constraint.
Conflicts stop roster writes and identify the column/records in run-logs.

Rider show-name is retired. Its original values remain in legacy-display-name
for provenance, excluded from collection writes, helper lookup and lookup-text.
Horse show-name remains active. Metadata configuration never runs collection.

## Direct rider and horse links - 21 October 2026

Riders have a horses linked-record field, with reciprocal riders on horses.
After verified entry writes, normal collection retains existing associations and
adds horses from active collected entries. Links accumulate across shows and do
not indicate ownership. Existing helper values and entry links are preserved.
Configure only these fields with configure-rider-horses.js; it is schema-only
and never collects data or writes business records.

## Synced schedule references - 21 October 2026

The collector now reads the existing wef-schedules.shows, wef-schedules.rings
and wef-schedules.schedules tables in CWF by their configured IDs. These are
read-only references; source editing is Off in all three syncs. Incomplete or
stale synced relationships stop entry processing without dropping valid entries.
CWF entries use shows, synced-rings and schedules links; collections use shows.
Local rings-helper records link to synced-rings. Existing tenant-owned horses,
riders, nicknames, notes, locations, walking times and rider-horse links remain.
The former show-details/rings copies are frozen history, retired only after
normal-run verification. Their historical links remain intact; no data is deleted.
Use configure-synced-references.js for the new link fields. Do not rerun the
original initial setup.js against a migrated base: it describes the old layout.
See docs/coordination/wef-synced-references-20261021.md for current acceptance.

## Active team and roster slices - 24 October 2026

CWF consumes the existing native wef-entries.entries sync. The five-minute
runner, with WEF_TENANTS_TENANT_SLICES_ENABLED=true, maintains local
active-team, team-status and team-checked-at fields. Active team contains
current trainer participation only when neither horse nor rider is disabled
or source-inactive. All participation and helpers continue to be collected.
Active roster views on horses and riders exclude both inactivity flags; the
original Grid views retain everything. Personal-only follows and ownership
are excluded from the shared CWF sync. These views are not user authentication.
See evidence/tenant-slices-20261024/release.md for acceptance and live limits.
