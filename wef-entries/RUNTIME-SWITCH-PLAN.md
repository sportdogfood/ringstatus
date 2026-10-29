# Smallest Entries runtime switch

Version 1.0 — 29 October 2026, America/New_York
Status: prepared for approval; no runtime, task, schema or record changes made by this plan.

## Outcome

Keep the existing recurring Entries writer. Replace CWF configuration, private selections and direct people/class requests with accepted retained Data observations. Use the approved participation-linker to connect supported facts to schedules. Season references may enrich identity when an accepted contract exists; their absence does not block Data-backed links. Entries stops at Entries. CWF is neither a dependency nor an acceptance gate.

## Existing route, verified read-only

- Task: wef-entries-cwf-collector (legacy label only), repeating PT5M from 2026-10-23T08:19:00-04:00: minutes :04/:09/:14 and so on.
- Action: C:\WINDOWS\System32\wscript.exe, //B //Nologo followed by this repository's wef-entries/run-hidden.vbs.
- Working directory: C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\wef-entries.
- Chain: run-hidden.vbs → run.ps1 → entry.js → collector.runEntries; private runtime/collector.env supplies the existing Entries credential. Four-minute execution limit; ignore overlapping instances.
- This is a direct repository deployment, not Cloudflare or a new scheduler. Keep its cadence, task configuration, hidden launcher, lock and atomic receipt. Renaming its legacy label is unnecessary for this switch and excluded.
- Prior rollout pattern: evidence/multiple-schedules-20261024/release.md describes baseline copies, tested staging, deployed hashes and normal-run readback. Reuse that pattern without its historical CWF scope.

## Files and smallest changes

| File | Change |
| --- | --- |
| collector.js | Retain audit logging, archive-before-write, bounded logs and verified upserts. Replace tenant/feed/private-follow loops with retained-class observations and participation-linker. Preserve valid source facts when only links are uncertain. |
| model.js | Replace tenant scope/owners/selections/feed normalization with the prepared canonical scope and schedule parity functions; delegate relationship normalization to participation-linker.js. |
| settings.js | Remove CWF_BASE_ID, TENANTS_BASE_ID, FAVORITES_TABLE and source-fetch/session settings. Add Data base/table constants; keep existing destination, Shows and Schedules targets. Do not copy or print credentials. |
| airtable.js | Remove tenant read targets. Permit GET-only Data runs/collections and filtered pagination; keep destination-only writes, uncertain-create readback, duplicate-key checks and no DELETE. |
| retained-input reader / replay | Promote the existing staged retained_input.py, fixing its repository-root path when moved. Adapt the established Shows retained-head/part-reading pattern; reuse revision_chain/retained_units rather than build a new store. Extend projection to accepted dated inventory/group context and exact provenance. No source endpoint requests. |
| source.js | Remove it from the runtime import path; preserve historical source evidence. No live fallback to old endpoints. |
| entry.js and shared utility imports | Remove dependency on tenant runtime modules. Relocate only the existing pure lock/archive/equality helpers needed by Entries, preserving tested behavior; do not redesign them or modify tenant code. |
| configure.js / configure-schedule-links.js | Do not run their old CWF/template/favorites configuration. Use an Entries-only additive schema change for the five fields below. Remove tenant references from the active configuration path. |
| tests, README.md, scope.md, MATCHER.md | Test the adapted writer and accepted input with fixtures; publish the new runtime contract and evidence. Historical tenant evidence remains historical. |

No CWF, Schedules, Live, Shows or Season code edits. No new collector, base, task, polling service or repair script.

Relocation details: production model.js imports ./participation-linker, not the staged ../../../ path. A promoted wef-entries/retained-input.py resolves repository root with parents[1], not staged parents[4]. Reuse the existing wef-shows/day-source.js bridge process boundary: fixed wef-data/.venv/Scripts/python.exe, -B, hidden window, restricted environment and bounded output/time. Adapt its reader output to class/inventory observations; compact show-day counts are insufficient. Its currently deployed replay already uses the inert wef-schedules/tests/wef_data_handoff.py revision_chain validator; reuse that exact validated helper without importing test runners or modifying Schedules. The unusual directory location is an existing production dependency, not permission to invoke tests as business collection. Do not invent another Python runtime or a parallel revision validator.

## Inputs and acceptance checks

| Existing source | Required data |
| --- | --- |
| Shows appCaSmL819KK7UFJ / tblJA5CUon5s1sWib | show-key, show_id, customer_id, start_date, end_date, selected_date, is_default, last_checked. Exactly one valid canonical default show; preserve existing authority/freshness checks. |
| Data appdl16HJ5ETm8N69 / runs tblSxcmUcmB0W9Vhu | run-key, status, state-json: accepted terminal phase, no pending mutation, canonical authority fences, ordered observation references and pinned predecessor state hashes. Inspect latest attempt separately; never present an older accepted revision as a new successful observation. |
| Data / collections tbl8fQXTGJmv5esNj | collection-key, status, retained-json, retained-sha256 and exact Airtable record ID from each journal reference. Verify ordered complete parts, hashes, show identity and retained graph/readback maps with existing inert validators. Recheck the chosen head after reading. |
| Frozen class source inside those parts | show_id; class.class_id/customer_id/show_id/schedule_ring_id; class_related_data.date/ring/class_group_id; total_entry_trips and explicit trips; each entry/horse/rider/trainer ID, trip ID/UUID; original request completed time. Group OOG membership is optional for ordinary class links. |
| Frozen dated inventory inside the same accepted revision | Explicit source show/date/ring number/ring ID/group/class/source-occurrence relationships. Establish complete group roster from inventory, never infer it from an incomplete OOG list. Missing group proof affects only group attribution. |
| Schedules appUq5Qz9kARqFija / schedules tblqiTFqqO3zk2KJC | stage-key, show-id, class-id, date, ring, status, changed-at, class-name; group-id and row-json for stronger group/occurrence checks. Use existing dated show-collection health. |
| Entries local schedule sync tblVcbGJHhzLYZRxn | Existing eight source fields and local record IDs. Schema checked live on October 29: no group-id or row-json currently synced. After exact source/local key and field parity, enrich only the in-memory local record with source group/occurrence evidence. Do not change the sync or Schedules. |

Data's entity/show-entry keys are not operational participation keys. Read retained membership, never later mutable tables to reconstruct an older revision. Undated directory entries remain source facts in Data; do not fabricate dated participation, missing trips or whole-day emptiness.

Latest failed/incomplete attempt must not refresh accepted source time, claim a new collection or trigger retirement. Previously accepted facts may remain historical; whether still usable as current depends on the producer's accepted per-scope freshness contract. The existing staged reader does not yet enforce that full policy. The local Season owner has been asked to confirm it before implementation is declared ready. Do not invent a new TTL or make the group contract a gate for ordinary facts.

Producer confirmation received October 29: last accepted head adaptive-2026-10-29T14:11:18.894144+00:00, record recGpfoLlXWmVap8G, sequence 13, state SHA256 c8f92c92f5ea050c2d2e02a50e7c194e50b4bd499410642abbef5eb2c1d82f66, 41 parts. A newer 14:17:37.008934 head is phase Writing and must not be selected. This is a dated producer readback, not a hard-coded deployment head. Select the highest completed sequence matching fresh canonical authority and verify its chain. Replay retained-bootstrap plus unit.graph deltas; class observations use source.classes and metric.requests original completion time. Complete class snapshots permit only class-scoped reconciliation, including explicit zero; never regress newer records or apply stale drops. Directory completeness (841 show entries in that readback) does not establish attendance or participation completeness. The existing helper/Python route is confirmed; newly staged retained_feed.py is not an accepted dependency. A later ordinary acceptance check must read the then-current producer state rather than rely on this snapshot. No universal new freshness threshold was supplied; preserving original timestamps, no regression and withholding stale retirement remain required, not a fabricated TTL.

Season optional references: only source/customer-scoped exact entity identities with accepted provenance and conflict status may enrich the records. The earlier proposed Season tables are not assumed live. If no accepted mapping is available, use Data's explicit IDs and report reference-unavailable; do not merge by name or invent Season record IDs. Native cross-base entity syncs/new helper tables are outside this minimal switch.

Confirmed by the Season owner: its twelve domain tables are approved but not yet created; only logging/meta tables currently exist. Therefore the initial switch has no native Season reference dependency and must not wait for those tables. Do not substitute staged Season consumers for deployed accepted evidence.

## Destination: reuse existing tables

Live schema checked read-only on October 29: Entries base appSxF4pYMBaxpTZd contains entries tblqkWmrsYMs2hyQ6, collections tbltCxLRlhpdHdHSw, run-logs tblCWYgZs49eOuQ7e and heartbeat tbloQWorYyYtTIv8T.

Add only five fields to entries:

| Field | Airtable type | Purpose |
| --- | --- | --- |
| source-trainer-ids | singleLineText | Actual source trainer IDs, serialized consistently. Do not repurpose legacy trainer-ids feed associations. |
| schedule-link-status | singleLineText | linked or unresolved, independent of source entity observation. |
| schedule-link-basis | singleLineText | class-date or observed-group; empty when no current supported link. |
| group-link-status | singleLineText | linked or unresolved; ordinary class association is not group proof. |
| link-issues | multilineText | Readable per-entry reasons for unresolved schedule/group links. |

Reuse source-json for source trip details, revision/part hashes, source observation times, group identity and reference provenance. Avoid adding separate audit tables. Existing observation-status remains the observation state; explicit new link statuses/basis carry the narrower assertions. Consumers must not interpret class-date as proved group eligibility.

Preserve entry-key show|date|entry|class and the existing record ID, source IDs, helper fields, reciprocal/native links and scalar/plural schedule transport. Upsert by existing key. Resolve schedule links only to the local synced IDs. Never rekey using Data's different show-entry or occurrence key.

Keep scalar/plural transport internally consistent: a singleton array agrees with schedule-key; a multiple-key array requires a blank scalar. When retaining prior keys/native links as history, set the current schedule-link-status unresolved and clear the current schedule-link-basis; never stamp an old key as a fresh match. Preserve its prior link evidence/time separately in source-json/archive. Existing producer/source per-scope freshness rules must be resolved before activation; broad switch approval does not waive that check, invent a TTL or make a stale accepted head current.

Collections: new tenant-free keys data|customer|show|date|ring|class; source-kind retained-class, source-id the exact source scope; existing dataset-id/fingerprint/message/archive-file/entries fields carry the accepted revision and membership evidence. Historical tenant collection rows are retained untouched; tenant-key is not authored for new rows. New heartbeat-key wef-entries; retain old heartbeat history without refreshing or deleting it.

No global disappearance sweep. Only an explicitly complete successful observation of the same class/date scope can mark its previously observed participation absent/dropped. Missing, failed, stale, partial or unobserved scopes retain their old rows and timestamps. Group uncertainty never drops a participation. Unresolved current links retain prior native links as history while their status/basis makes the uncertainty explicit; no clearing or manual repair.

Initial switch specifically must not treat old feed-only records as absent simply because they are missing from the new projection. Require established retained scope membership and a later complete accepted observation before retirement; otherwise retain the row as historical/unverified. Historical trainer-feed collections are not evidence that the new Data scope was completely observed.

## Deployment and rollback safety

Prepare all code and schema checks in staging; record current hashes and exact baseline records. No live code is changed during preparation. Under runtime-switch approval, add only the five Entries fields, wait for the existing scheduled process to be idle, acquire the same lock without executing business collection, install the tested files and release the lock before a normal wake. If the safe window cannot be obtained, wait for another normal window; do not kill a process or start a manual run.

No task stop, rename or cadence change is required. Keep baseline files and append-only schema additions. On a failed scheduled verification, stop that verification path and report the exact failure under the runner rule; do not automatically retry, repair records or restore the old CWF/feed writer. Reverting to tenant acquisition would contradict the new scope and requires explicit owner direction. Archived pre-write evidence protects data; no destructive rollback is proposed.

## Offline and scheduled acceptance

Existing narrow matcher has 46 passing Node checks plus four retained-input checks. They are preparation proof, not runtime-switch proof.

Add adapted-writer fixture checks for: zero tenant/source calls; read-only producer access; accepted-chain/hash/authority failure; failed latest attempt and unchanged accepted time; valid empty class; unresolved group with continued supported rows; missing schedule sync with retained facts; partial scope without global drops; repeat input with stable IDs and no duplicates; existing archive/log/upsert readback behavior; actual trainer/feed separation; no writes outside Entries. Test source/local enrichment without altering schedule records.

After approved deployment, verify two DISTINCT ordinary Entries wakes on the existing five-minute task: deployed hashes, terminal task/receipt outcome, module heartbeat and run-log agreement, accepted Data revision/observation provenance, zero direct acquisition and tenant reads, stable participation IDs/keys on repeat, correct local schedule links and readable uncertainty, preserved history and every archive recorded. A normal run may complete with unresolved link issues; valid facts must still update. Missing producer authority or failed writes are not success. A naturally empty run is valid but cannot substitute for nonempty link proof when that proof is required. No CWF or Live consuming run is a gate. No manual run or repair counts as acceptance.

## Approval and current release status - 29 October 2026

“APPROVED TO EDIT. Replace wef-entries' old CWF/feed acquisition with accepted Data inputs and optional accepted Season references, using the existing writer, narrow matcher and five-minute task. Add the five listed Entries fields, preserve keys, records, history and links, deploy and verify two normal Entries runs. Keep CWF, Schedules and Live untouched. No manual runs, task changes, record repairs or deletions.”

Direct owner approval covers implementation, five fields, deployment and two normal Entries runs. The subsequent owner correction removes CWF compatibility and consumption from deployment and acceptance. CWF and Schedules remain untouched.

Accepted observations retain their original source times; currentness is not asserted. No consumer TTL is invented. Automatic absence retirement is disabled pending separate policy, overriding the earlier conditional retirement proposal. No retained class observations means unavailable, not empty success.

Candidate: evidence/data-input-switch-20261029/staged. Tests: 36 passing offline checks. Independent review passed, five fields and runtime deployed. Two normal scheduled runs verified at 13:29 and 13:34 Eastern; receipts and readbacks are in the evidence directory.
