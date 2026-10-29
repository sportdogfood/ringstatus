# Current Entries deployment - 29 October 2026

Data-input switch deployed at 17:27:23 UTC after independent review and 36 offline checks. Two normal scheduled runs verified at 13:29 and 13:34 Eastern: both business-ok Partial, no processing errors. Evidence: evidence/data-input-switch-20261029/.

Entries links accepted retained Data to canonical Shows and read-only Schedules: rings, groups, classes, horses, riders and actual trainers. No direct source acquisition or tenant selection. CWF is outside this module and its acceptance. Optional Season references are nonblocking.

Existing five-minute task, launcher, keys, native links and helpers remain unchanged. Historical task name wef-entries-cwf-collector does not mean a tenant dependency. Module heartbeat key: wef-entries.

Source observation times and revision provenance are preserved. Replay never refreshes source time. No consumer TTL is invented. Automatic absence retirement is disabled. No retained class evidence fails unavailable; explicit complete zero-trip classes succeed. Ordinary class/date links continue while uncertain group links are flagged.

Added fields: source-trainer-ids, schedule-link-status, schedule-link-basis, group-link-status, link-issues. Future results/messages/waze links do not block supported linking.

## Historical notes - superseded where inconsistent above

# wef-entries: recurring collection and linking module

## Schedule-link rollout - 24 October 2026

Deployed at 13:00 America/New_York and verified through two ordinary entries-to-CWF
cycles: entries 13:04/13:09, consumed by CWF 13:07/13:12. All nine current links
passed with preserved keys/helpers/privacy and zero duplicate source fetches.
Actual multi-group source eligibility remains the explicit limitation below.

- Existing participation `entry-key` and schedule `stage-key` remain unchanged.
- `schedule-keys` is JSON text containing sorted unique exact schedule keys.
  A singleton agrees with `schedule-key`; multiple links require a blank scalar.
  Readers fall back to the scalar only when the array value is absent. Malformed,
  duplicate, unordered or conflicting arrays fail closed.
- Existing central and CWF `schedules` fields hold destination-local native IDs.
  CWF derives its `synced-rings` from all resolved schedules, keeping distinct
  rings. It does not overwrite plural links in its legacy scalar-link loop.
- `last-seen` remains the last validated successful participation observation.
  `observation-status` is `successful`, `failed`, `unresolved` or `dropped`.
  Failed source collections preserve prior links/time as history; disappearance
  marks a record dropped. Failure/edits never refresh successful observation time.
- Entries checks complete, fresh schedule collections for every retained show
  date (30 minutes, using `completed`, not schedule `changed-at`). This is a
  conservative all-date gate; no partial acceptance is inferred from global health.
- Safe transport/status fields are exposed through the existing CWF team and
  live reference views. Source JSON and private follow ownership remain hidden.
  Live consumes participation schedules through its existing entry reference.
- Rollout flag: `WEF_ENTRIES_MULTIPLE_SCHEDULE_LINKS=true`. Reader compatibility
  and automatic synced fields were ready before writer activation. No cadence,
  keys, source endpoints, decoder, helpers or permission changes were made.

**Source limitation:** the saved normal people payloads contain no attributable
participation group evidence. Production therefore continues unique-singleton
selection and rejects ambiguous multiplicity. The pure group selector, plural
transport, native links and CWF multi-ring consumption are fixture-tested, but
no production source adapter for multiple eligible groups is asserted. Class
membership alone is not proof. Genuine multi-group source mapping and natural
multi-group acceptance remain open; no whole-show expansion or guessed IDs.

Evidence: [rollout record](evidence/multiple-schedules-20261024/release.md).
`verify-schedule-links.js` is GET-only; it never invokes a collector.

## Current operation — checked 24 October 2026

**Running on schedule.** On 24 October, entries runs at 13:04 and 13:09
America/New_York succeeded with 9 current entries, 1 historical entry and no
errors. CWF consumed those observations at 13:07 and 13:12 and verified all nine
local schedule/ring links with zero duplicate source fetches. Airtable heartbeat,
run-logs and readback agree. See [normal-run proof](evidence/multiple-schedules-20261024/release.md).
This is dated verification, not ongoing monitoring.

| Scheduling detail | Current configuration |
| --- | --- |
| Runner | Windows Task Scheduler on this computer; not a Codex heartbeat |
| Task | `wef-entries-cwf-collector` |
| Cadence | Every five minutes, at :04, :09, :14, :19, :24, :29, :34, :39, :44, :49, :54, :59 |
| Time reference | America/New_York; original start 23 October 2026, 08:19 AM, UTC-04:00 |
| Launch | `wscript.exe` → `run-hidden.vbs` → `run.ps1` → `entry.js` |
| Working folder | This repository's `wef-entries` folder |
| Requirements | Computer awake, current Windows user signed in; hidden launch |
| Overlap / limit | Overlapping starts ignored; four-minute execution limit; process lock |
| Configuration | Ignored `runtime/collector.env`, `WEF_ENTRIES_` variables |

Cloudflare supplies decoded source responses; it does not schedule this collector.
Codex inspects, makes approved repeatable edits and reads evidence. The scheduled
task executes collection. No manual collector runs or record repairs.

## Responsibilities and downstream handoffs

Collection remains **tenants → CWF**. Whole-show collection is still undecided.

| Component | Responsibility |
| --- | --- |
| wef-shows | Owns canonical show metadata, selected source date and retained dates; entries reads it directly. |
| wef-schedules | Owns schedule/class/ring facts; entries validates its local read-only sync and joins `schedule-key` to `stage-key`. |
| wef-entries — this module | Independently collects selected feeds/follows, normalizes and deduplicates participation, binds show/date/class/horse/rider/feed/schedule identities, preserves raw trip evidence, archives and logs each run. |
| tenants / CWF | Supplies trainer parameters: main 8778, additional 50121 and 80970. `tenants.favorites` supplies private selections for placeholder `main-user` and `user-2`. |
| CWF consumer | Consumes native team sync; maintains horse/rider roster, local helpers and links; applies active-team visibility without duplicate people-endpoint entry fetching. |
| wef-live | Independently owns live state; uses entry/schedule identities to attach live facts. Entries does not run live collection. |
| wef-timing | Expected to consume normalized identities/facts and continually calculate; calculations belong there. |
| Results, messages, publish and waze | Expected to attach results, alerts/two-way messages, published endpoints and interactions using stable identities; each retains its own writer and cadence. |

Downstream responsibilities are contracts, not a claim that every connection is
implemented. Each consuming chat must acknowledge keys, required fields, freshness,
failure handling and privacy before its connection is called complete.

Participation key: `show-id|YYYY-MM-DD|entry-id|class-id`. A horse can have many
participations. `horse-id` and `rider-id` identify the related entities.
`trainer-ids` means feed association, not proof of a confirmed trainer.
Do not invent missing trip or entity identities.

Inactive horses/riders remain collected. CWF now maintains local `active-team`,
`team-status` and `team-checked-at` fields. Its Active team and Active roster views
exclude `disabled` or `source-inactive` participants while retaining collection,
helpers and links. See [slice acceptance](../wef-tenants/evidence/tenant-slices-20261024/release.md).

Personal-only entries and follow ownership stay out of shared CWF sync. Overlap
may share the team participation without revealing who follows it. Personal lists
are currently empty: populated paths are fixture-tested, not live-proven. Shared
Airtable views do not provide user-specific authorization or private delivery.

## Where to check

- [Airtable wef-entries](https://airtable.com/appSxF4pYMBaxpTZd): `heartbeat` for
  latest result; `run-logs` for every run, counts, errors and archive evidence.
- `collections`: source/date membership, completion, counts and archive locations.
  Valid empties succeed; source failures are not empty successes.
- `runtime/last-run.json`: latest completed scheduled receipt.
- `runtime/collector-YYYY-MM-DD.log`: local launch, output and exit log.
- `runtime/history/`: verified source, dataset and pre-change archives.
- `evidence/`: dated acceptance records; no credentials or private runtime config.

Report failed/stale/missing scheduled evidence; do not substitute manual collection
or repair records. Offline tests are separate from normal-run proof.

## Implementation and historical acceptance

The dated notes below preserve earlier proof and blockers. Current entries status
above supersedes older pending/blocked entries wording. Historical wef-live notes
are not a current status check of that separate stage.

This function reads CWF tenant configuration, the three trainer feeds (8778,
50121 and 80970), and explicit `main-user` / `user-2` favorites. It writes only
the entries base. CWF now consumes its native read-only team sync and maintains
local helpers/links without duplicate people-endpoint entry fetching. Schedules
retains its own writer. The approved CWF migration passed two normal validation
runs before cutover and two normal consumer runs afterward on 23 October.
See [migration proof](../wef-tenants/evidence/entries-consumer/README.md) and the
[linking-module working contract](../docs/coordination/wef-data-boundaries-working.md).

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

Inactive horses/riders remain collected. `model.activeTeam` expresses the
team projection; the deployed CWF consumer now maintains the native active-team
slice described above. Neither filters the collector. Private selection ownership stays on collections, not the shared
participation. This proof does not implement user access screens or expose a
public export. Consumers must apply the agreed private visibility rules before
sharing. CWF has switched to the verified team sync. Timing/output connections
are not established by this release; their owner acknowledgments remain pending.

Trainer IDs on entries identify collected feed association; they do not assert
that the feed person is the horse's confirmed trainer. Trips remain in raw
source JSON; no live timing or results collector is introduced here.

## Verification

**PASS — 23 October 2026.** Normal scheduled starts at 08:19 and 08:24 Eastern
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

## Direct show authority � 23 October 2026

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
