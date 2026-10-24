# Existing WEF entry relationships — 24 October 2026

Documentation only. No code/configuration, Airtable, task, source requests, manual
collection or record repairs. Baseline: repository commit
`28ddf2815d487dbb28144dde34452fdcb38a72d3` plus the separately approved saved
entries/CWF/live changes. Existing working implementations remain the foundation.

**Keep the current chain:** schedules → selected participation → CWF local
horse/rider links; live independently links trips to that participation when it
exists. Do not create another horse/rider directory to make this chain work.
The two material gaps are entries' single-schedule assumption and an agreed owner
for persistent shared entity references beyond CWF.

## Identity rules

| Concept | Existing identity / meaning | Boundary |
| --- | --- | --- |
| Source/customer | Current canonical WEF authority requires customer 15; show key is `15|show_id`. | Entries is currently scoped to this source/customer. Numeric IDs are not proven globally unique across providers/customers. Keep source/customer context; do not rekey existing operational records. |
| Horse | Source `horse_id`; CWF `horse-id` plus retained `horse-key`. | Not `entry_id`. Preserve existing helper keys; missing numeric IDs can retain `legacy:<source record ID>`, not fabricated IDs. |
| Person / rider / trainer | Source `people_id`; explicit `rider_id` or `trainer_id` where supplied. | Do not assume every rider ID equals a people ID without a supported crosswalk. A trainer feed can contain another rider's participation. |
| Show entry | Source `entry_id`. | Different from horse identity and from a particular ride/class/day. No independent new raw-entry directory is required for the working chain. |
| Participation | `entry-key = show_id|date|entry_id|class_id`. | One dated class participation, not one horse and not one trip. Used by favorites, CWF and live. Preserve it. |
| Live trip | `live-entry-key = show_id|date|class_id|trip_id`; separate `entry-key` points to participation. | Multiple trips may belong to one participation. Never replace trip ID with entry ID. |
| Schedule | Producer's exact `stage-key = show|date|row.key`; entries copies it into `schedule-key`. | Do not reconstruct `row.key` from display class number, ring or a guessed formula. |
| Ring | Schedule daily ring key uses customer/show/date/**ring number**; live ring key uses show/date/**ring ID**. | Compare source-supported fields/context, not these different key strings. |

`trainer-ids` in entries is **feed association**, not confirmed trainer assignment.
Live's scalar `trainer-id` comes from decoded trip identity and its trainer name
from the corresponding public/decoded source; preserve that provenance separately.
Names/nicknames are display/helper values, not substitutes for validated IDs.

## Table-by-table working map

Base aliases below are exact existing bases:

- **Shows:** `wef-shows` / `appCaSmL819KK7UFJ`.
- **Schedules:** `wef-schedules` / `appUq5Qz9kARqFija`.
- **Entries:** `wef-entries` / `appSxF4pYMBaxpTZd`.
- **CWF:** `wef-tenant-cwf` / `appxKPBtdl2Rdq7CS`.
- **Live:** `wef-live` / `appgzosUcVqA4vZdf`.
- **Tenants:** `tenants` / `app7BXcw801n19QKF`.

| Existing table / ID | Exact relationship and local link target | Writer / readers; status |
| --- | --- | --- |
| Shows `shows` / `tblJA5CUon5s1sWib` | Canonical `show-key`, show/customer IDs, selected date, available dates, `last_checked`. Entries/live read this directly; participation keeps scalar show ID. | **wef-shows** writes; schedules/entries/live read independently. Working canonical input; not a native cross-base link. |
| Schedules `schedules` / `tblqiTFqqO3zk2KJC` | Source `stage-key`, show/date/class, ring number, status. Schedules owns daily rings `tbliUwvXYdMhvJ3Fm` and dated `show-collection` `tblvGpwxGjXXNLKAU`. | **wef-schedules** writes; entries/live/CWF read native synced copies. Preserve producer keys and dropped history. |
| Entries `wef-schedules.schedules` / `tblVcbGJHhzLYZRxn` | Local automatic read-only schedule copy. Entries `entries.schedules` links to these local records after exact `schedule-key = stage-key` lookup. | Airtable sync copies source; **wef-entries** writes participation's link. Working for unique matches. |
| Entries `entries` / `tblqkWmrsYMs2hyQ6` | Scalars `horse-id`, `rider-id`, `entry-id`, `class-id`, `trainer-ids`, `show-id`, date; `entry-key`, `schedule-key`; raw trip `source-json`; native `schedules`. **No native horse/rider directory links here today.** | **wef-entries** writes; CWF/live consume limited sync views. CWF-selected collection, not all-show capture. Latest documented entries proof: 11:29/11:34 ET, nine rows, heartbeat/log readback. |
| Entries `collections` / `tbltCxLRlhpdHdHSw` | `CWF|show|date|source-kind|source-id`; native `entries` points to central participation record IDs. Trainer feed versus user-follow membership remains separate. | **wef-entries** writes source membership, counts, completion and archive paths; CWF validates trainer collections. Not a public/private-owner export. |
| CWF `wef-entries.entries` / `tbl7ychn5HELB3HTr` | Native team sync matched by `entry-key`. Local links: `horses` → `tblxMpG75KL3Nmt1v`; `riders` → `tblll5l7eG5wc27uP`; `trainers` → `tbljBHTXEJn71RRIl`; `shows` → `tblNABa50eiRKfZS4`; `synced-rings` → `tblK8EqyEVUktiLOI`; `schedules` → `tbl3QiMIXd6Spi336`. | Sync owns source fields. Existing **wef-entries chat / CWF runner** owns six local links and active-team fields. CWF broader ownership requires **wef-tenants-coordinator** acknowledgment, not an assumed transfer. Working migration and slice acceptance. |
| CWF `horses` / `tblxMpG75KL3Nmt1v`; `riders` / `tblll5l7eG5wc27uP` | Participation links use exact numeric horse/rider IDs with one matching helper. Reciprocal participation fields are named **`wef-entries.entries`**. Old **`entries`** fields still point to retained old table `tblDhAutHYlDbvzNd`; do not repoint/delete them. | CWF scheduled runner grows/refreshes rosters; owner edits helpers. No duplicate directory is needed to use these today. Proven retention: 76 horses, 37 riders; inactive horses still refreshed. |
| CWF `riders.horses` ↔ `horses.riders` | Native many-to-many association, union of existing links and horses from observed active participation. Rider-key retained. Accumulated historical relationship, **not ownership or a claim that every linked horse is ridden today**. | CWF runner writes `riders.horses`; Airtable maintains reciprocal `horses.riders`. Working normal-run proof retained. |
| CWF `tenant-details` / `tblUoIrtdQ2Wg7DjB`; `trainers` / `tbljBHTXEJn71RRIl` | Active CWF 8778; approved ordered trainers 8778, 50121, 80970. Trainer links derive feed membership, not guessed confirmed trainer identity. | Tenant configuration is input; entries reads it. Current entries code explicitly validates these slots; arbitrary tenants are not implemented. |
| Tenants `favorites` / `tblyp3d7BZtZmqAVM` | `owner-key`, full participation `entry-key`, `Status`; current owners main-user/user-2, up to ten each. | Owner selection input; entries reads and records separate private collection membership. Both lists empty; nonempty paths fixture-proven only. |
| Live `wef-entries.entries` / `tblfj77GmXxuiAoB4` | Identity-only native sync: entry-key/show/date/entry/class/horse/rider/status. Live `live-entries.entry` targets this local copy; reciprocal `live-entries` exists here. | Sync copies identity, **wef-live** writes trip link. Not a direct native link into CWF horses/riders; follow scalars for entity identity. |
| Live `wef-schedules.schedules` / `tblGPXIUnD0sfTEIX` | Local schedule reference; native target for both `live-classes.schedules` and `live-groups.schedules`. | Sync owns source facts; **wef-live** owns local links. One active match per group; a class may have multiple valid group schedules. |
| Live `live-rings` / `tblw8bodkRbCItxIH` | `ring-key`, ring ID/number/name; `current-group`, `next-group`, `completed-groups` → local live-groups. | **wef-live** source-backed observation writer; timing reads in its agreed contract. |
| Live `live-groups` / `tblMhQtjpvyODEUSn` | `group-key = show|date|group-id`; `ring` → live-rings; `classes` → live-classes; `schedules` → local schedules for **this group**. | **wef-live** writes. Do not give every group every schedule for a multi-group class. |
| Live `live-classes` / `tblNaM9ehOJFsrveV` | `class-key = show|date|class-id`; `groups`, `ring`, `schedules`, `collection` are native local links. | **wef-live** writes. Multiple groups/schedules supported, identity-error records preserved. |
| Live `live-entries` / `tblf7550e7UGT8ZSF` | One trip: `trip-id`, scalar entry/horse/rider/trainer IDs; `class` → live-classes; `entry` → identity-only synced participation; `collection` → Live collections `tble6fwMDBbeeGxly`. | **wef-live** writes from paired current source evidence. Existing exact-entry-key link; no native horse/rider helper link yet. |

Native Airtable record IDs are local to the target base/table. Central participation,
CWF sync and live sync have different record IDs. Join by stable keys, then write
the destination-local record ID. A reciprocal link in a synced copy does not
establish a reverse relationship in the central source base.

## Existing helper candidates — do not activate by assumption

24 October connector metadata confirmed the following candidates; it did not
establish populated records or an active recurring writer. The Tenants rows below
are from the 22 October saved schema and need fresh readback before adoption.

| Candidate | Prepared structure | Disposition / accountable review |
| --- | --- | --- |
| `horses` base `apprSZEfQsjUPrPNk`, table `horses` `tblEdWUvCqi7VRB41` | horse_id, show_name, barn_name, trainer_id, tenant_id, rider_list/rider_names and helpers. Lists are text, not proven native rider relationships. | Candidate only. **wef-season-coordinator + wef-tenants-coordinator** to reconcile ownership; no writer in inspected entries/live paths. |
| `riders` base `appfabN9KQwDHSqbG`, table `riders` `tbl3Kie66Ys8DoTBv` | Primary `rider_id` has a leading U+FEFF; separate people_id, pid, names, history fields. | Candidate only; field-name and role crosswalk compatibility must be checked before use. Do not silently equate IDs. |
| Tenants `horses` `tbljPvAk5EezdCQME`, `riders` `tbllUlTlcEMgZiW3h`, `people` `tblyTBgrgdhT3Tcg7` | Existing native horse/rider/people links; `sources` `tblRswgKjA9dt3PED` has source_system/source_entity_type/source_scope/external_id/entity_uid. `tenant-roster` `tblwcczj8lFhVcbyt` has Horse/Rider/Tenant links. | Prepared crosswalk/roster candidates, **tenant owner** review. Not the current CWF standing source and not proven connected to the WEF collector. |
| `wef-seaon-people` (actual spelling) `app6uHgUYCCKTVlJE`; `wef-season-entries` `app2d8p8VdUjT8hBn` | Each currently only `meta` `tblwBvigcmqRlSYUb`, Name field. | Season plan candidates, **wef-season-coordinator**. No entity/participation writer established. |
| `wef-season-horses` `appETrjio9Yj9Pv17`; `wef-season-riders` `appkWkm9rFLrkUvaD` | Each currently only `meta` `tblwBvigcmqRlSYUb`, Name field. | Existence is not need or approval to build another directory. Leave unused pending choice. |

CWF's actual standing source is **RingStatus** `apptdhhNzduxm5gjn`:
`ww_horses` `tblliyUZ1ZS88Kfvl`, `ww_riders` `tblI6yrqv0Y4QTvyi`, and
`ww_tenants`. It is read-only to the CWF runner. Source-base/source-record or
unique numeric identity resolves existing helpers. Source refresh preserves
nicknames, aliases, notes, manual names, disabling and existing helper keys;
source inactivity is recorded separately. Current entry observations grow the
same roster even when the main trainer has no current participation.

## Smallest remaining work, in dependency order

1. **Preserve the proven CWF path; no migration required.** Entries remains
   tenant-selected; live covers the actual live day independently and can observe
   horses/riders outside that selection. An unmatched live trip is not permission
   to collect all-show entries or manufacture a binder row. Owner decides broader
   scope separately; none of the current links requires that decision first.
2. **Agree schedule multiplicity — entries + schedules + live owners.** Entries
   currently rejects two schedules for the same class/date before saving, then
   requires one exact schedule-key. Live instead requires one active stage-key
   with the corresponding `show|date|group-id_` prefix per group, and permits
   several schedules for a class. Decide from source-supported group context
   whether participation belongs to one group or all applicable groups. For the
   latter, reuse existing plural native `schedules` links, preserve entry-key and
   the current unique-match path; entries/CWF would need a narrow way to carry
   multiple exact stage keys through the sync. The existing scalar schedule-key
   cannot express that. Do not pick the first schedule or change producer keys.
3. **Choose the shared entity owner only when needed — tenant + season owners.**
   Reuse the prepared tables selected above; verify source/customer/entity
   crosswalk and helper-edit ownership first. Keep CWF local helpers intact.
   If native horse/rider navigation is required in entries/live, add read-only
   references to that chosen owner and local links by validated IDs. Scalar IDs
   already support current participation/trip relationships; no directory rewrite
   is required just to consume them.
4. **Tighten downstream acceptance — live + entries owners.** Live's binder
   currently matches exact entry-key only: it does not additionally reject a
   dropped binder or compare synced horse/rider IDs. It also does not enforce a
   binder freshness age. Agree mismatch/stale/dropped handling before claiming
   validated current entity linkage for every live row. Preserve live's own
   source-supported IDs; no names-only repair. This is a bounded consumer gap,
   not authorization to alter collection now.
5. **Timing/results/messages/publish/waze owners acknowledge their reads.**
   Reuse participation and trip keys plus existing schedule/group links, retaining
   source timestamps and Partial/unresolved state. Tenant/private membership
   stays separate. No universal entity links or downstream output integrations
   are claimed working without their own evidence and owner acceptance.

## Missing, stale, unmatched and error behavior

- Entries: canonical show older than 30 minutes or inconsistent identity fails.
  Full current-show source/local schedule parity includes dropped rows; filtering
  dropped rows out of the entire sync would break validation. Participation
  matching excludes dropped schedules (current code tests `status !== dropped`).
  Missing/ambiguous schedule or required entity ID fails the affected source.
  Previous verified membership is retained on failure; valid zero rows succeed.
  Schedule parity is checked, but a separate schedule heartbeat age is not enforced
  by `schedulesFor`; owner freshness contracts must not claim otherwise.
- CWF: requires current successful entries heartbeat, completed trainer/day
  collections and fresh active synced participation within 30 minutes. Failures
  stop consumption without duplicate source fallback. Inactivity hides display,
  not collection; old entry table and links remain preserved.
- Live: show freshness/actual-day guard is enforced. Missing reference reads
  preserve existing links; a readable reference with zero/ambiguous matches leaves
  the link blank (ambiguity warned). Schedule ambiguity is warned per group.
  Broader live scope naturally produces unmatched tenant participation. Current
  code does not compare all source/synced reference fields or their ages.
- Per-class live identity/fetch errors preserve affected observations/links,
  log unresolved/error and allow **Partial** continuation; they are not valid
  empties and prevent retirement. Archives remain a protection requirement.
  Logging failures are separate from collection failure, with best-effort local
  evidence. No decoder redesign or invented identities.

Live logging correction has normal-run/Airtable proof at 11:32 and 11:37 ET.
The saved 11:52–11:53 receipt inspected for this map has no operational/logging
errors but unresolved class 200030429/trip 200398240 **and** classes
200030208/trip 200398529 and 200030209/trip 200398530. This is a saved receipt,
not a fresh Airtable readback of that run. It does not negate the logging fix or
prove the source disagreements resolved. Remote logging outage/local fallback
remains fixture-tested; those accepted normal runs had no logging outage.

## Acceptance and evidence

Any approved link delta needs two normal scheduled runs with exact local links,
stable IDs, preserved helpers/old records/privacy, source-backed multi-group
behavior where naturally present, and heartbeat/run-log/archive readback.
Unmatched out-of-scope trips must remain explainable; unresolved identities must
not gain guessed links. Nonempty private follows and inactive current-participation
cases remain fixture-only until real owner data naturally exercises them.
No manual run, repair or test count substitutes for that proof.

Inspected implementation: `wef-entries/model.js`, `collector.js`, `settings.js`;
`wef-tenants/model.js`, `entries-consumer.js`, `standing-roster.js`, `collector.js`,
`tenant-slices.js`; `wef-live/model.js`, `collector.js`, `schema.js`, `settings.js`;
`wef-schedules/current-store.js`. Relevant history: commit above; existing
uncommitted approved changes were read, not replaced.

- [Current entries README](../../wef-entries/README.md) and [dated status](../../wef-entries/evidence/readme-status-20261024.json).
- [Owner acknowledgments](wef-records-owner-acknowledgments-20261023.md) and [working contract](wef-data-boundaries-working.md).
- [CWF migration proof](../../wef-tenants/evidence/entries-consumer/README.md), [slice proof](../../wef-tenants/evidence/tenant-slices-20261024/release.md), [horse/rider link proof](wef-rider-horse-links-20261021.md).
- [Live schema](../../wef-live/evidence/schema-configured.json), [schedule diagnosis](../../wef-live/evidence/schedule-link-diagnosis.json), [logging correction proof](../../wef-live/evidence/logging-isolation-release.md).
- [Prepared schema snapshot, 22 October](evidence/prepared-tables-20261022/schemas.json). Bounded current metadata GETs on 24 October refreshed candidate base names and the six candidate schemas above; no record writes or source endpoint pings.
- Season coordinator's existing-key inventory: `C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/wef-season-key-dependency-inventory.md`; planning only, no active season writer claimed.
