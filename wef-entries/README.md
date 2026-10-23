# Independent CWF entries collector

This function reads CWF tenant configuration, the three trainer feeds (8778,
50121 and 80970), and explicit `main-user` / `user-2` favorites. It writes only
the entries base. The existing CWF and schedules collectors remain their own
writers. CWF does not yet consume this new binder.

## Cadence and ownership

`wef-entries-cwf-collector` runs every five minutes, starting at 08:19 Eastern
on 23 October 2026. Task Scheduler launches `run-hidden.vbs`, which hides its
PowerShell child. The current Windows user must be signed in and this computer
must be awake. Overlapping instances are ignored; execution is limited to four
minutes. No collector was started manually for deployment verification.

`entry.js` is the scheduled entry point. `verify.js` only reads the receipt,
Airtable records and local archives. `configure.js` configures destination
schema and the two documented favorites fields; it is not a collector.

## Tables

Destination: `appSxF4pYMBaxpTZd` (`wef-entries`).

| Table | Purpose |
| --- | --- |
| entries | One participation per show/date/entry/class; horse/rider IDs, raw source, feed associations and native schedule link. |
| collections | Five source selections for each retained show date; separate private-owner membership, counts and archive locations. |
| wef-schedules.schedules | Existing schedules view synced read-only; automatic refresh, no editing source records. |
| run-logs | Every run, result, timestamp, row count, errors and archive evidence. |
| heartbeat | Latest completed run for this independent function. |

The pre-existing `meta` table is left alone. Destination fields reuse the
prepared CWF types, without changing its tables. Schema IDs are recorded in
`evidence/schema-configured.json`.

Read-only input: CWF `tenant-details` / `trainers`; canonical `wef-shows.shows`;
wef-schedules `schedules`; tenants `favorites`. The only input-schema additions are
`favorites.owner-key` and `favorites.entry-key`. No favorites were populated.
Use `owner-key` = `main-user` or `user-2`, and `entry-key` =
`show-id|YYYY-MM-DD|entry-id|class-id`. Status `Active` or blank selects the row;
`Inactive`, `Disabled` and `Removed` exclude it. At most ten per owner for this
show. Unknown states, malformed or unresolved selections fail rather than
silently disappearing. Source lookup uses the selected class's entries.

## Safety and consumer boundary

The source-selected show supplies retained dates; the local schedule sync must
match its current source identities and values before collection. Missing or
stale inputs fail visibly. A confirmed empty source is a successful zero-row
collection. Source failures retain previous verified membership. Raw payloads,
every source/date dataset and previous entries are archived and verified before
participation updates. No record deletion or repair is performed.

Inactive horses/riders remain collected. `model.activeTeam` provides the future
team projection excluding inactive roster relationships; it never filters the
collector. Private selection ownership stays on collections, not the shared
participation. This proof does not implement user access screens or expose a
public export. Consumers must apply the agreed private visibility rules before
sharing. No CWF consumer or timing/output connection has been switched.

Trainer IDs on entries identify collected feed association; they do not assert
that the feed person is the horse's confirmed trainer. Trips remain in raw
source JSON; no live timing or results collector is introduced here.

## Verification

**PASS â€” 23 October 2026.** Normal scheduled starts at 08:19 and 08:24 Eastern
both completed successfully with 7 active participation rows, 20 dated
collections and 24 verified archive events each. Every entry linked to the
correct local schedule. Both runs kept the same entry and collection record
IDs. Run logs, heartbeat, archive files and hashes were read back successfully.
All 16 offline tests and 65 existing CWF tests passed. Protected code hashes,
both existing task definitions and both CWF/schedules schemas were unchanged.
See `evidence/release-verification.json` for exact receipts and checks.

The two private lists are empty. Populated private-only, overlapping and
unresolved selections are tested offline; live populated-list proof awaits
actual owner selections. No actual list was invented for this release.

The unchanged CWF task had a source HTTP 502 before the first new run, then
recovered on its own 08:22 normal run, completing at 08:24:08 with 7 entries.
No corrective action was taken against that workflow.

Offline tests cover valid empty sources, private-only and overlapping follows,
inactive participation, failed source retention, schedule mismatch, archives,
stable repeat IDs, uncertain creates and destination write restrictions.
Normal-run receipts and read-only verification snapshots belong in `evidence`.
Private runtime credentials, raw archives and daily operational logs remain in
ignored `runtime/`. Stop a failed scheduled path and report it; do not invoke a
manual substitute or repair records.

## Direct show authority — 23 October 2026

The existing recurring collector now reads show metadata directly from
`wef-shows` (`appCaSmL819KK7UFJ`, table `tblJA5CUon5s1sWib`).
No discovery calls or schedules-show fallback. Existing freshness validation,
participation keys, local synced schedule links and CWF inputs are preserved.
Task-produced receipts record the canonical base, record and checked timestamp.
Normal-run acceptance is blocked: the first post-deployment run at 12:09 Eastern
read the canonical show successfully, then trainer 50121 returned HTTP 502.
Seven existing entry IDs and schedule links were preserved. No source fix or
manual retry was made. See
`../wef-shows/evidence/consumer-connection/`.


Source handling update, 23 October 2026: one bounded transient HTTP/timeout
retry is now applied inside each normal run. Permanent errors still fail.
The 13:04 Eastern normal run passed with seven entries and their links intact;
second-run acceptance is pending in `../wef-live/evidence/source-fix/`.


Final acceptance at 13:11 Eastern: entries completed normal runs at 13:04 and
13:09, with seven and nine entries respectively. Both had no source errors and
passed record/link/archive checks. The first full verification passed. The
second full verification was blocked by concurrent changes to
wef-tenants/airtable.js, collector.js and settings.js at 13:07, outside this
task's deployment file list. Those changes were left untouched; expected hashes
were not changed to hide the drift.
Live's 13:02 normal run ended Failed with persistent class 200030347 HTTP 502;
no missing-trip identity error remained and 68 archives verified. Live is disabled.
No production patch or manual retry followed that failed gate. Overall work is
incomplete: persistent 502, concurrent protected drift, and two live runs remain.
Full evidence: wef-live/evidence/source-fix/release-status.json.
