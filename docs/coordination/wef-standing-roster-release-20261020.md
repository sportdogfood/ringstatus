# CWF standing horse/rider helpers — 20 October 2026

Owner requested populating CWF horses/riders even when8778 has no entries in the
current show, with static helpers and lookups by names, nicknames, trainer/tenant
IDs, horse show-name and rider first/last names. This extends the approved
independent tenant collector. Existing business tasks/cadence and source records
remain unchanged; no manual collector runs or production record repairs.

## Proven starting point and smallest delta

The completed tenant release is `docs/coordination/wef-tenant-cwf-release-20261020.md`.
Package `wef-tenants/` was running normally every five minutes; its standing lists
were only built from current-show entries. Current show200000075 has no8778
entries, so those lists remained empty. Retaining absent rows did not populate
the initial standing roster. This release adds an independent reference import,
without replacing the show/day/entries/archive implementation.

Exact before code hashes, task XML, previous normal receipt, changed schema and
test output: `wef-tenants/evidence/standing-roster-20261020/`.

Read-only source inspection confirmed legacy base `apptdhhNzduxm5gjn` tenant
record `recUKyrLUr0khLCjV` is8778/Castlewood Farm/CWF with69 horse/29 rider links.
Its standing trainer/tenant associations identify71 horses and32 riders overall.
All71 horses have existing barn_name helpers;13 lack numeric horse IDs;9 carry
source inactive. Rider properFirst/properLast/lf_name supply existing names.
These are retained references, not verified current-show attendance.

## Behavior and ownership

The normal runner reads only the three legacy roster tables, validates tenant
scope, archives that source bundle, and fills existing horses/riders tables in
`wef-tenant-cwf` (`appxKPBtdl2Rdq7CS`). It runs before shared-show discovery and
entry fetching. No roster endpoint, alternate manual run or repair is used.

Owner helpers are seeded once and then preserved, including intentionally blank
names/nicknames/notes and tenant disabling. Current source name/inactive status
are separate. Name matches never merge rows; source/numeric identity conflicts
stop for inspection. Missing source IDs use stable legacy-record keys, remain
blank numerically and are flagged. No fabricated nicknames for riders or IDs.

New metadata and dynamic formula search fields were created without populating
business records. Population is owned by ordinary scheduled execution only.
Horse show-name is the horse's registered/show display name; it is not a
competition-event link. No current-show links/attendance are added to helpers.

## Verification

31 offline fixture tests PASS; independent read-only technical review PASS.
Coverage includes empty/failed current-show source, unavailable show discovery,
owner static fields and cleared names, source inactive versus local disabling,
additional-trainer rider exclusion, stable missing-ID keys, identity collisions,
ambiguous lookups, computed field omission and strict legacy GET-only access.

Product update completed13:26:31 Eastern, before the normal13:27 trigger.
## First normal run — PASS

Normal13:27 run completed13:27:53 Eastern, task exit0, no errors. Read-only
saved verification `scheduled-e39b1bc9-d0ef-4d44-957e-78b5a9071ba6.json` reports
71 horses and33 riders,13 missing source horse IDs flagged,9 source-inactive
horses and six verified archive events. Eight live helper lookup checks passed;
current-show entries remain zero across all four dates.

The33rd rider is Tanner Korotkin9590 (`recwXIsvDsflzecIN`), explicitly linked
in the legacy CWF tenant, although his current pid is19676. The initial32 figure
was the trainer8778-only filter; full standing membership also preserves this
existing tenant link. No additional trainer was enabled and no source record
was changed. The read-only acceptance utility's initial hardcoded32 expectation
failed; inspection of the exact scheduled source archive established the missing
membership and the utility now derives expected coverage from that archive.
The actual business collector succeeded and was not changed after this run.

## Second normal run and final acceptance — PASS

Normal13:32 run completed13:32:46 Eastern, task exit0, no errors. Saved evidence:
`scheduled-2c41252f-12c2-4fa7-a7cf-2974b10d729c.json`. Both final-version runs
contain71 horses/33 riders, four complete empty-entry days,24 rings,6 shared ring
helpers and unchanged6 CWF example walks. All eight helper lookup checks passed
on each readback. Six archive events per run, including the exact105-record
standing source bundle, were read back and SHA256 verified.

`repeat-comparison.json` confirms the same record IDs and static values in all12
data/reference tables. Only expected timestamps changed (including per-helper
helper-refreshed); exactly one run-log was added, and earlier logs were preserved.
No duplicate creation or manufactured current-show attendance.

`boundaries.json` confirms unchanged tenant task definition/cadence, unchanged
shared schedules source, and unchanged deployed collector runtime between these
runs. Legacy source access is restricted to GET on exactly the three roster
tables; negative write tests passed. No manual collector invocation, source
record repair or live helper-edit injection was used for verification.

Final offline suite32 PASS, including explicit retained tenant membership for a
rider with a different current trainer. Code-review result was updated/read back
after the saved run-to-run comparison; next Monday26 October09:00 Eastern.
Owner-edit/cleared-field and nonempty-entry interactions are fixture-proven;
live current-show entries remain absent. Source numeric IDs remain missing for
13 horses; those records are usable by source key/name/nickname and clearly flagged.

Direct tables: horses `tblxMpG75KL3Nmt1v`; riders `tblll5l7eG5wc27uP` in
tenant base `appxKPBtdl2Rdq7CS`. Name/nickname/alias and identifier search is
available through the helper fields and computed lookup-text. No new public API.
