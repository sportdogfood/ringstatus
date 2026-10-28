# wef-live workflow handoff

## Operational update - 25 October 2026

Owner changed wef-live-collector to every three minutes (PT3M); hidden launcher retained. Owner also approved one heartbeat row per invocation: new row keyed by run ID, stamped started/checked-at with Running before source work, then the same row receives completed/checked-at and terminal result. Existing legacy heartbeat row is preserved. This supersedes the older five-minute cadence and latest-row-only heartbeat description below. Startup/final heartbeat failures remain isolated from collection. 43 offline tests passed; normal-run evidence is in evidence/heartbeat-per-invocation-20261025.md.

errs-log also now has err-class, err-trip and err-group. Each error gets a separate record/JSONL line; known IDs only, ambiguous groups blank. Prior errors were not repaired. See evidence/error-identifiers-20261025.md.

Prepared 24 October 2026. Operational evidence verified through the 13:12 ET scheduled run, completed 13:13:05 ET. This is a handoff document, not authorization for further changes.

Updated 24 October 2026 with entries/CWF acceptance through 13:15:18 ET and the agreed live-to-timing planning boundaries. This documentation update did not refresh live operational status or run any collectors.

Further updated 24 October 2026 with the saved timing-payload investigation and assigned natural-rollover acceptance gaps. Findings below are retained-evidence findings, not a fresh operational check or implementation approval.

## Status and responsibility

The independent TODAY-only live collector is deployed. The participation-link contract passed 40 offline tests, independent review, and two normal scheduled runs. Existing native links were preserved. No manual collector runs or record repairs were used.

Own only wef-live: live observations, local reference links, archives, collections and operational logs. Shows supplies canonical show/date authority; schedules supplies schedule identity and dated collection health; entries supplies tenant-selected participation identity and eligible schedule references. CWF remains tenant-owned. Do not edit those collectors or the shared decoding/source modules as part of live work.

Timing is a downstream consumer, not implemented here. It must distinguish current validated observations from retained historical data. Live does not infer timing predictions, official result readiness or participation group eligibility.

## Where it runs

- Checkout and deployed task directory: `C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\wef-live`.
- Windows task: `wef-live-collector`, every five minutes, hidden WScript launcher -> `run-hidden.vbs` -> `run.ps1` -> `entry.js`. It is not a Codex heartbeat.
- Task inspection at handoff: Ready, last trigger 13:12:12 ET, last result 0, next trigger 13:17:11 ET. This is a dated observation, not a permanent status claim.
- Source checkout HEAD: `28ddf2815d487dbb28144dde34452fdcb38a72d3`. Deployed working files include changes beyond HEAD; use [deployment file hashes](evidence/participation-deployment.json), not HEAD alone, to identify this release. No commit was made for this work.
- Private environment file: `runtime/collector.env`. Never print or copy its credentials into a handoff.

**Documentation warning:** `README.md` contains superseded task-disabled, halt-file and original test-count statements. Current `entry.js` retains older halt receipts as history but does not use them to stop future scheduled execution. Use this handoff, current code and dated evidence below for present behavior.

## Airtable and ownership

| Base | ID | Use by live |
|---|---|---|
| wef-live | appgzosUcVqA4vZdf | Own destination |
| wef-shows | appCaSmL819KK7UFJ | Read canonical shows |
| wef-schedules | appUq5Qz9kARqFija | Read schedules and show-collection health |
| wef-entries | appSxF4pYMBaxpTZd | Receive restricted native entry sync |
| wef-tenant-cwf | appxKPBtdl2Rdq7CS | Protected; no live writes |

[Open wef-live](https://airtable.com/appgzosUcVqA4vZdf).

Owned tables: `live-rings`, `live-groups`, `live-classes`, `live-entries`, `collections`, `run-logs`, `heartbeat`, `errs-log`. Exact field definitions/native links are in `schema.js`; table and field IDs are in `evidence/schema-configured.json`.

Read-only native reference tables:

- `wef-schedules.schedules` (`tblGPXIUnD0sfTEIX`). Groups/classes link to local synced schedule records.
- `wef-entries.entries` (`tblfj77GmXxuiAoB4`). Live entries link through the native `entry` field to these local synced records.
- Entry source view: `wef-live-reference` (`viwRwdJi7CR0zBMEL`) in source entries table `tblqkWmrsYMs2hyQ6`.
- Explicit safe entry fields: entry-key, show-id, date, entry-id, class-id, horse-id, rider-id, status, schedule-key, schedule-keys, last-seen, observation-status. Source editing remains Off; sync is automatic. No private follows or collection provenance are exported.

```mermaid
flowchart LR
  Shows[wef-shows: canonical show/date] --> Live[Independent wef-live collector]
  Source[Wellington live observations + decoded identities] --> Live
  Schedules[wef-schedules: schedules + dated health] --> Live
  Entries[wef-entries: safe participation reference] --> Live
  Live --> Rings[live-rings]
  Rings --> Groups[live-groups]
  Groups --> Classes[live-classes]
  Classes --> Trips[live-entries]
  Trips --> Reference[Local synced participation via entry link]
  Live --> Logs[collections / run-logs / heartbeat / errs-log]
  Live -. downstream observations .-> Timing[wef-timing]
```

## Collection and continuation

Read the canonical default show and selected date from shows; require fresh authority and a match to the current New York date before TODAY collection. An upcoming selected date is not live activity today. Do not replace this with independent show discovery.

`source.js` defines public ListAjax, LiveScoreWidget and getLiveClassData sources. Exact class/trip identities use `wef-schedules-decoded.gombcg.workers.dev/classes/...` through the existing shared `lib/wef-source-read` module. These are runner inputs, not permission for manual source calls.

Five-minute checks continue. If live status is false or all nonempty TODAY groups are completed, detailed collection is skipped and the skip is logged. Empty collections are valid outcomes, not failures by themselves.

Class/source identity errors retain affected observations and links, log the error and allow other classes to continue. An unresolved class is not current validated data merely because the overall run completed. Logging failures are isolated from collection, with local evidence where writable. Mandatory archive/data-integrity failures may still fail a run; subsequent scheduled invocation is not permanently halted by that receipt.

Prior-state/source archives are compressed, hashed and verified before relevant updates/retirement. Unresolved class/participation errors prevent retirement. Do not bypass archive-before-delete protection.

## Participation-link contract

- Stable participation identity: `show|date|entry|class`; preserve existing schedule stage-keys.
- Exact show/date/entry/class/horse/rider IDs must agree. No name-based matching or historical identity substitution.
- Require active participation, `observation-status=successful`, and source observation `last-seen` within 30 minutes.
- `schedule-keys` is sorted, unique JSON. Singleton scalar `schedule-key` must agree; multiple keys require blank scalar. Scalar fallback applies only when the plural field is absent, not when it is present but empty/malformed.
- Require complete fresh dated `show-collection` health, using completion time rather than schedule changed-at. Compare full active source/local stage-key sets for the date; listed keys must each resolve uniquely to active matching schedules. Dropped rows are historical, not eligible.
- Consume participation schedules through the existing synced entry reference. No redundant live-entry schedules link. Do not infer that every class group is eligible for every trip.
- `participation-link-status`: current, historical, unresolved or unmatched. A failed check preserves a previous native link as historical; a rejected candidate without a prior link is unresolved; out-of-tenant-scope unlinked observations are unmatched.
- Audit fields: `participation-link-reason`, `participation-checked-at`, `participation-verified-at`. A failed check does not erase the prior verified timestamp. Consumers must require current status and appropriate freshness, not timestamp presence alone.

Implementation: `participation.js`, integrated by `collector.js`; error delivery is in `entry.js`. Destination write restrictions/read targets are in `airtable.js`. Metadata setup is `configure.js`; do not rerun setup merely to inspect status.

## Verified scheduled evidence

| Run (24 October, ET) | Run ID | Participation result |
|---|---|---|
| 13:07:13-13:08:03 | d66f6c2e-6dd4-4ea8-b93d-ffb5699e840e | Both TODAY links current; six existing links preserved |
| 13:12:13-13:13:05 | 5257eecc-cbfe-4cb6-93bc-e37e3856b472 | Same result; verification timestamps refreshed |

Each run collected 5 rings, 26 groups, 59 classes and 287 live entries. Both had `ok=true`, no participation errors and no logging errors. Airtable run-logs/heartbeat recorded **Partial**, because the separate class identity error below remained. Do not describe overall collection as error-free.

- [Run 1 evidence](evidence/participation-run-1.json)
- [Run 2 evidence](evidence/participation-run-2.json)
- [Release explanation](evidence/participation-link-release.md)
- [Transition evidence](evidence/participation-transition.json): before the upstream fields populated, links became historical, errors reached errs-log and collection continued.
- [Saved automatic/read-only sync settings](evidence/participation-sync-settings.png)
- Prior continuation baseline: `evidence/logging-isolation-release.md`, `logging-isolation-run-1.json`, `logging-isolation-run-2.json`.

## Upstream entries/CWF verification

The entries/CWF owner's saved release records two successful ordinary scheduled pairs. These are separate upstream acceptance results, not additional live runs. All times below are America/New_York on 24 October 2026.

| Pair | Entries run | CWF consuming run | Verified result |
|---|---|---|---|
| 1 | 13:04:01-13:04:31 | 13:07:02-13:09:45 | 9 current, 1 historical; 9 local links verified; zero duplicate people fetches |
| 2 | 13:09:02-13:09:36 | 13:12:05-13:15:18 | Same identities, links and counts; zero duplicate people fetches |

Repeat comparison confirms stable record IDs, schedule/ring/helper links, statuses and active slices. Nicknames, notes, aliases and disabled flags were preserved; retired CWF entry records were unchanged. No private owner/source provenance was exported. Array/status fields arrived through automatic sync. Task definitions remained unchanged. No manual collector runs or record repairs were used.

Evidence owned by entries/CWF:

- [Release](../wef-entries/evidence/multiple-schedules-20261024/release.md)
- [Pair 1](../wef-entries/evidence/multiple-schedules-20261024/run-pair-1.json)
- [Pair 2](../wef-entries/evidence/multiple-schedules-20261024/run-pair-2.json)
- [Repeat comparison](../wef-entries/evidence/multiple-schedules-20261024/repeat-comparison.json)

This proves plural transport/consumer compatibility with the naturally available singleton records. It does not prove participation-specific multi-group eligibility. Both private follow lists remained empty; populated private-follow behavior is fixture-only.

## Agreed timing-input boundaries - accepted for plan

These boundaries describe existing supply and proposed normalization, not authorization to add fields or implement timing. [model.js](model.js) and [schema.js](schema.js) define current mappings.

| Classification | Supply and limits |
|---|---|
| Current ring fields | `current-group`, `next-group`, `completed-groups`: source-reported group links, not inferred actual start/end times. |
| Current group fields | `status`, `is-live`, `has-json`, `estimated-start-time`, `total`, `gone`, `jo-total`, `jo-gone`, `r2-total`, `r2-gone`. These preserve source status/counts and estimates. |
| Current class fields | `total` is live row count; `gone`, `jo-gone`, `r2-gone` count IDs in the respective source lists; `current-trip-id` is source-reported. Separate class JO/R2 totals are not currently exposed. Do not treat counts from different phases as interchangeable. |
| Current trip fields | Exact source identities and native links; `order-of-go`, `actual-order`, `scratch`, `gone`, `position`. Position maps `ActualPos`, falling back to `Pos`; this is not verified official placing. |
| Raw-only values | `source-json.identity.estimated_go_time`; raw live phase start/done values such as `r1_start`, `r1_done`, `jo_start`, `jo_done`; source score/time/place values. Retention does not validate their business semantics. |
| Proposed normalization | Expose `estimated_go_time` explicitly as a source estimate with raw value and provenance preserved. It is not a collector prediction or actual start. No normalized field name or schema change is approved here. |
| Unknown / unverified | Authoritative current source clock; actual group/class start and end anchors; complete phase timestamp/count semantics, including blanks/zeros; verified official-results readiness. |

Retained [class 200030399 payload](runtime/history/class-200030399/551d561fb987eee4c4f88673638121b9d59e8a056aa7423a6a9e474d2e7f2cb7.json.gz), referenced by the 13:07 live run, actually contains `estimated_go_time` values such as `09:03:00`, GMT-labelled `jo_start` values, blank phase start/done fields, and `curr_updated_at = "24oct2026 13:27:24"` without an explicit timezone. GMT-labelled values can be parsed, but their phase meaning, applicability and blank/zero semantics remain unvalidated. This archive does not establish a reliable current source clock, actual group/class anchors or official-results readiness. Do not convert time-only values into absolute timestamps by guessing a date or timezone.

Keep the three clocks distinct:

- Live `last-seen`: collector observation time. A recent observation is not proof the upstream content changed recently.
- `source-updated-at`: retained upstream update label, potentially old or lacking timezone semantics; not a current clock.
- Timing evaluation clock: owned by timing. Show `selected_date` selects scope; it does not replace that clock or independently mean TODAY.

For a Partial run, timing may consume only validated, fresh observations within the relevant show/date/class scope. Use per-class collection/error evidence; do not promote an unresolved class's retained data to current merely because another class succeeded. Participation-dependent calculations must require `participation-link-status=current`, appropriate freshness and the reason/checked-at/verified-at audit fields. The two live runs above establish actual current-link proof for the two TODAY records, not universal readiness for all classes.

Live supplies observations, raw evidence and validated identity links. Timing owns phase-consistent remaining counts, effective order, entries ahead, waits, pace, projections, countdowns, drift/conflicts, freshness/unavailable reasons, provenance and condition-transition events. Results owns official score/fault/time/placing, readiness, corrections and history. `Completed`, `gone`, raw scores or source `place` alone do not establish results readiness. Messaging owns recipients, consent, templates, delivery deduplication and outcomes. These downstream responsibilities are planning boundaries, not implementation acceptance or approval on those owners' behalf.

## Saved timing findings - read-only investigation

The saved-only review inspected 913 class snapshots across 91 classes. Counts here are occurrences across archived snapshots, not unique entries, and describe that review inventory only. No source calls, collector runs or data changes were used.

| Field | Observed coverage / format | Safe use and limit |
|---|---|---|
| `identity.estimated_go_time` | 4,832 `HH:mm:ss` values; 1,967 were `00:00:00`. Estimates changed across snapshots for 270 exact scoped trip keys. No malformed time formats found in this inventory. | Conditionally usable as a nonzero source wall-clock estimate. Class payload supplies show/date context, not a timezone for this field. `00:00:00` is unavailable/ambiguous, not established midnight. No absolute countdown from this field yet. |
| `live.jo_start` | 527 nonblank GMT-labelled occurrences covering 197 class/trip identities. Parsed dates matched payload calendar dates; nonblank values were stable across retained snapshots for the same scoped trip. | Parseable source-reported timestamp candidate. Date/offset syntax does not certify an actual phase-start meaning. No inferred actual group/class anchors. |
| `live.r1_start`, `r1_done`, `jo_done` | All 4,857 live-row occurrences blank. | Unavailable actual start/end evidence. |
| `live.r1_cd`, `jo_cd` | All 4,857 live-row occurrences blank. No live-row `r2_start`/`r2_done` fields observed. | Unavailable source countdown. |
| `curr_updated_at` / `source-updated-at` | Unzoned date-time labels; some unchanged while decoded estimates changed. | Raw provenance only, not current source clock or update time for every decoded identity field. |
| `time_one/two/three`, faults, scores, placing | Numeric result-like values with many zeros; live `Score` also includes `DNS` and `EL`. | Not clock anchors. Preserve zero/status distinctions; results owns units, semantics and readiness. |

Exact-trip example: show 200000075, class 200030399, trip 200398698. In the [earlier snapshot](runtime/history/class-200030399/17857b63e36a9192d33d8a6b2181974a2a052d79884a3d337d41db935af2ed40.json.gz), Gone became 1 while jo_start remained blank. A [subsequent snapshot](runtime/history/class-200030399/7d71c1ab251f632e9a6c32d747c710bbd8366fff6f53b0771cdc82d4469394ef.json.gz) supplied `Sat, 24 Oct 2026 13:02:40 GMT` and JO-list membership. The estimate later changed from 08:48:00 to 08:45:00 and result-like durations populated in the [later retained snapshot](runtime/history/class-200030399/551d561fb987eee4c4f88673638121b9d59e8a056aa7423a6a9e474d2e7f2cb7.json.gz). Match by exact trip ID and validated enclosing class/show/date, never names or capture date alone. These are separate observations, not one synchronized event.

Phase ambiguity: 10 archived occurrences had jo_start without JO-list membership; 29 had JO-list membership with blank jo_start. JO timestamp, JO counts, Gone, and R2 qualification/upcoming flags cannot substitute for each other. Re-entry/attempt semantics remain unestablished. Twenty-nine live-row occurrences lacked matching decoded trip identity and must be excluded from identity-dependent calculations. These counts do not identify a new production incident or override existing unresolved-class handling.

Additional source evidence: decoded `class.results_verified` is actually present, with both 0 and 1 observed (800 and 113 snapshot occurrences). The [class 200030013 archive](runtime/history/class-200030013/84ba6b18e2166b31bff321fefb04903fb08c4ac2b3319692fd752ab5bd9ee52a.json.gz) contains 1. This is an explicit raw source verification flag available to the results workflow, not a normalized live readiness field or proof of current publication/correction readiness. This supplements the earlier unknown-readiness statement; Completed/Gone still do not imply results-ready.

**Smallest optional normalization recommendation:** one read-time parser owned by timing, using existing `source-json.live`, `source-json.identity`, validated identity links and existing observation/source timestamps. No new Airtable fields, duplicate clock or live collector changes are required by this recommendation. Return a parsed value or null, a quality/unavailable reason and the exact raw-field provenance; preserve raw values. Keep estimates wall-clock-only until date/timezone anchoring is agreed. Explicit-zone phase timestamps remain source-reported candidates with unverified phase meaning. Reject missing, malformed or conflicting inputs without guessing. Actual phase durations, phase-based pace/countdowns and inferred actual class/group starts/ends remain unavailable where this evidence is insufficient.

If separately approved, acceptance must cover blank/zero/malformed values, timezone/date boundaries, changing estimates, phase-list disagreements and unresolved identities, followed by two normal scheduled runs preserving raw values and links. Fixture success is not natural source-semantic proof. No manual runs, seeded records or forced source changes.

## Natural default-show rollover - acceptance still open

Authority: [rollover acceptance assignments](../docs/coordination/wef-rollover-acceptance-assignments-20261024.md). This records responsibilities and known gaps only; it authorizes no implementation. wef-records-coordinator owns cross-consumer acceptance tracking.

Live's accountable scope:

- **TODAY guard:** source-selected date must match the configured America/New_York day. That configuration is not evidence of source timezone or UTC offset; both remain unknown. Missing/stale/ambiguous authority must not be treated as valid empty activity.
- **New-show sync lag:** verify new-show schedule/participation readiness and preserve existing links while references lag. Prove current versus historical labeling on the actual transition, not merely repeated same-show runs.
- **Archive/preservation and retirement:** verify archived prior state and native-link preservation. Current date-only retirement covers earlier dates, not rows for another show on the same date; this is an identified acceptance/implementation gap, not a completed fix. Retirement also requires clean source/identity/participation checks.

Cross-owner dependencies remain separate: shows tracks non-atomic default publication and returned identity checks; schedules owns its same-machine verified archive dependency and upstream retirement effects without a downstream acknowledgment gate; entries/CWF owns current-show filtering of retained prior-show participation, dated health/parity and sync-lag handling. Live must not implement changes in those owners' areas under this handoff.

Required natural proof: exact old/new customer/show keys; producer publication run IDs/times and canonical default cardinality; independent schedules/entries/live run IDs/times; relevant-date source/local sync readiness; scoped complete/Partial/failed outcomes; archived old state and preserved record/helper/private links; actual retirement effects, including same-date other-show rows. Observe two normal repeats where repeat acceptance is required. Do not trigger collectors, seed a default, force sync/convergence or repair records to obtain proof.

**No all-four natural new-default rollover has been established.** Repeated same-show runs, historical ESP Fall II archives and producer rollover fixtures are not that proof. This gap is distinct from class 200030429 and from missing participation-specific multi-group eligibility.

## Remaining issues and next-owner instructions

1. **Existing unresolved source identity:** class `200030429`, trip `200398240`, error `No decoded identity for trip 200398240`. Last verified in both runs above. Existing observations/links preserved; errs-log populated; other classes continue. No fix is authorized by this documentation request. Investigate retained evidence first if requested; never substitute historical decoded identity as current proof.
2. **Natural multi-group proof unavailable:** reader handles valid plural arrays and fixtures pass, but entries has no proven participation-specific multi-group source mapping. Producer currently uses unique singleton eligibility and rejects ambiguity. Do not claim natural multi-group acceptance or guess group membership.
3. **Natural rollover acceptance open:** actual all-four new-default transition, sync-lag preservation and retirement behavior are unproven; same-date other-show retirement is a distinct known gap. Follow the ownership and evidence requirements above, without manufacturing a rollover.
4. **Timing semantics still bounded:** raw timestamp/estimate findings do not establish actual phase anchors, a source clock or results publication readiness. The read-time parser is a recommendation only, not implemented or authorized here.
5. **Next status check:** read task state, normal receipts, heartbeat/run-logs/errs-log and participation audit fields. Compare timestamps and local link IDs against the evidence above. This handoff update is documentation only and does not claim a fresh status check.

Normal receipts: `runtime/last-run.json` and `runtime/<run-id>.json`; source/prior-state archives: `runtime/history/`; local errors: `runtime/errs-log.jsonl`; launcher output: `runtime/collector-YYYY-MM-DD.log`.

Offline test command (does not run the collector): `node --test wef-live/test/collector.test.js wef-live/test/entry.test.js wef-live/test/participation.test.js` from repository root. Forty tests passed for this release; do not treat tests as a replacement for required scheduled proof.

**Operating boundaries:** Codex is not the runner. Never invoke `entry.js`, `run.ps1`, a task trigger or alternative production endpoints to obtain proof. No record repairs. A failed verification does not authorize another patch; obtain fresh edit authorization for corrections. Preserve unrelated working-tree changes and other collectors. A handoff file or thread link does not itself transfer credentials or authorize messaging another chat.
