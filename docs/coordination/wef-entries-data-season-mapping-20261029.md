# Entries input contract: local Data and Season mapping — 29 October 2026

## Controlling owner boundary — linking only

Entries consumes accepted collected Data/Season evidence and operational Schedules records. It links participation, classes, rings, groups, horses, riders and trainers; it does not independently collect source entries. CWF is outside all WEF workflows and is not an input, output, compatibility prerequisite or acceptance gate. Earlier references to preserving a CWF reader transition below are historical and superseded. Source collection belongs to wef-data under local wef-season-coordinator; Season retention belongs to wef-season-* under that same coordinator. Runtime decoupling is not yet claimed verified.

## Current resolution: ordinary schedule association and group eligibility

This amendment supersedes the stricter singleton prerequisites and CWF deployment dependencies below for the independent Entries build. Entries, Schedules and Live agree to separate ordinary schedule association from group eligibility. Entries stops at Entries; CWF is outside this workflow and build scope.

Smallest change: reuse the proven show/class/date matcher against active operational schedules. Exactly one matching occurrence supplies its existing stage-key, date and ring; resolve links to destination-local records. Missing or multiple matches leave only that schedule association unresolved. Preserve supported entry, horse, rider and trainer facts; do not stop collection or fan out across groups. Do not manufacture stage-keys from Data occurrence keys.

Group-specific attribution is a separate result. Saved Live run a251f69e-3046-4a04-9305-84d7a665575b demonstrates class 200030245/trip 200398234 declaring group 200029469 while its order-of-go class roster disagrees with that same run's group inventory. Parent-group context alone therefore cannot establish eligibility. This does not invalidate an independently unique class/date association. See wef-entries/evidence/tenant-independent-20261029/saved-mapping-findings.md for the positive historical example and exact counterexample paths; its earlier group-adapter approval language is superseded by this narrower core scope.

Owners: Entries implements and tests the narrow matcher with saved fixtures; Schedules supplies existing active rows and stable operational keys without product changes; Live reviews consumption and uncertainty handling without new gates or product changes. Verification must cover unique, missing, ambiguous and contradictory-group cases while retaining supported identities and existing verified links.

Authorization checked directly in Entries chat: owner message 01a18899-d9ba-7082-a3c8-d5628ccd840a approves local narrow-matcher implementation and saved-evidence tests, keeps CWF out, and forbids manual runs or record repairs. Implementation is in progress, not deployed or accepted. Production activation and any consumer changes require a separately reviewable scope and applicable approval; this document grants none. No schema, task, cadence or record change is part of this resolution.

## Tenant-independent candidate handoff — later October29 scope

Owner removed CWF/tenant configuration as a prerequisite for core Entries linking. Candidate is preparation only at wef-entries/evidence/tenant-independent-20261029/staged, not deployed; owner reports11linking+4frozen-inputtestsPASS. Current stricter candidate requires separate explicit participation eligibility even for a contextual singleton. No live eligibility adapter is claimed. Messages/results/waze remain optional nonblocking consumers. Preserve keys/history/private records; tenant selection is downstream.

Schedules confirmed: operational stage-key = show-id|YYYY-MM-DD|machine-key. Existing `normalizer.js` buildScheduleMachineKey priority is class_group_id_class_number, then class_groupxclasses_id, then class_id; explicit break fallback block:class_group_id. `current-store.js` columns() prefixes show/date. Data occurrence keys are a different namespace. Match source/customer/show/date/ring-number/class/group/occurrence against actual operational fields/row-json, then copy the matched stage-key and resolve destination-local sync records. Never rebuild a portable link from a guessed split string.

Live confirmed: model.js builds explicit source identities; ring ID differs from ring number. collector.js schedules matcher requires a unique active show/date/class plus explicit group-key prefix, but does not independently compare ring. participation.js validates supplied eligible keys, entry/class/horse/rider identity, observation/date health and source/sync parity including ring agreement; it does not derive trip-group eligibility. Existing natural two-link preservation proof and multigroup fixtures do not establish the new Data input matcher. A unique show/date/class/ring candidate does NOT prove attributable participation-to-group or repeated occurrence eligibility. Keep ambiguous or unproved candidates unresolved; no group fanout.

Separate downstream compatibility gate confirmed by direct source inspection: `wef-tenants/entries-consumer.js` load() requires heartbeat-key CWF, per-feed collection-id CWF|show|date|trainer-feed|pid, and defines exported team as rows with nonempty trainer-ids, comparing against local sync tbl7ychn5HELB3HTr. Removing these source heartbeat/collection writes without reader migration breaks the current downstream consumer. Filling trainer-ids with all-show actual trainer IDs can broaden old exports. The candidate must keep actual source trainer identity separate; do not restore tenant/CWF work into Entries core or silently change old access semantics. Downstream owner must receive separately authorized selection/readiness migration before shared writer deployment. No task disabling, consumer repair or tenant-data change is authorized by this planning handoff.

Two remaining deployment prerequisites: source-backed participation eligibility adapter/fixtures, and accepted downstream legacy-reader transition. Count/revision storage and source graph alone do not close either. Existing Data transport failure/recovery approval belongs to its own current owner; this handoff grants no stop clearing or source execution.

Planning only. No reader migration, code/schema/record/task changes, source calls or manual runs. Accountable producer is LOCAL wef-season-coordinator, responsible for distinct Data acquisition and Season stewardship functions. Former cloud Data chat is historical reference only. Shows is sole canonical show/day publisher. Entries retains its current CWF/private selection acquisition and existing links until separately approved and proven replacement.

## Exact existing Data surface

Inspected `wef-data/schema.json`, `README.md`, `data_runner.py`, `data_contract.py`. Base **appdl16HJ5ETm8N69**. This is saved local implementation/schema evidence, not a live Airtable schema audit or complete current data assertion. Native links in the following table belong only to Data's base; Entries resolves portable business identities into destination-local records.

| Need | Existing table and ID | Exact fields / mapping | Limit / required delta |
|---|---|---|---|
| Source customer/show | shows tbl7HtrFIRUlJpVsc | show-key, show-id, customer-id, facility-id, start-date, end-date, selected-date, last-success, last-run | Data local context mirrors canonical Shows; no discovery authority |
| Dated scope | days tbl7WyLyshiM4KNjh | day-key, show, date, status, expected-classes, collected-classes, trip-count, last-success, last-run | Mutable scope row is not immutable evidence or complete daily unique-entry count |
| Show entry | entries tblcKPbT4DPbFPu5b | entry-key, show, entry-id, entry-number, horse, riders, trainers, owners, last-success, trips, results | Entry key show-id\|entry-id lacks customer; retain existing key and carry explicit customer context; not operational participation key |
| Horse | horses tblkGRRMJxnH55hi8 | horse-key, horse-id, horse-name, last-success, entries, trips | Source reference, not approved global master or permission |
| Rider | riders tblGEzjOJzpn3vxX5 | rider-key, rider-id, rider-name, last-success, entries, trips | Source reference, role kept distinct |
| Actual trainer | trainers tbl9ROAViHtbKqpRd | trainer-key, trainer-id, trainer-name, last-success, entries, trips | Trainer source identity does not prove feed selection/team membership; CWF feed association remains separate |
| Dated entry/class and trip | trips tblt0GvQ39bHIu930 | trip-key, class, entry, horse, rider, trainer, owner, trip-id, source-uuid, scratch, record-status, source-json, fingerprint, last-success, last-run | Traverse class.day/show for dated participation; no standalone Data participation table. Repeated trips do not duplicate participation. Association without a trip needs independent source evidence; no invented trip |
| Class/context | classes tbl0IFf067w4wUBj5 | class-key, show, day, ring, group, class-id, class-number, status, entry-count, source-json, fingerprint, last-success, last-change, last-run, schedules, trips | Cross-date class-ID-only lookup risk and participation-specific group eligibility require source validation |
| Group | groups tblQObOvktufkyq54 | group-key, ring, group-id, group-name, sequence, source-json, last-success, classes, schedules | Class-group presence does not prove each entry's eligibility |
| Ring | rings tblhKkfYXppW3TbTq | ring-key, show, day, ring-id, ring-number, status, last-success, last-run | Resolve to existing destination-local ring by approved producer context |
| Schedule occurrence | schedules tbl6KKlYuMIPHEJWe | schedule-key, show, day, ring, group, class, kind, source-position, source-id, source-json, last-success | Data occurrence key differs from operational stage-key; no split-string guess. Exact source-backed crosswalk and attributable eligibility missing |
| Scoped acquisition receipts | collections tbl8fQXTGJmv5esNj | collection-key, run, show, day, ring, class, scope, status, expected-records, collected-records, readback-records, started, completed, fingerprint, error, retained-json, retained-sha256 | Frozen parts and exact membership must be replayed; counts alone not full coverage. Missing/failed scope never valid empty |
| Completed revision / checkpoint | runs tblSxcmUcmB0W9Vhu | run-key, mode, status, started, completed, errors, summary, state-json, collections | Verified completed revision plus hash-verified predecessor/observation chain required; run completion may cover only bounded observations, directory-complete can remain false |

Preserve actual stored key builders: class includes customer/show/date/ring/class; schedule uses source occurrence or position fallback; trip uses class-key/entry/source UUID with existing trip-ID fallback. Empty/ambiguous fallback cannot establish complete identity. Exact entry/class context must survive consumption. Operational Entries participation stays show/date/entry/class with existing IDs; multiple Data trip rows do not warrant duplicate Entries rows. Exact operational plural schedule-keys remain unchanged.

## Season stewardship mapping

Local Season owner confirms four identified existing target bases, but domain table IDs, mapping revision and conflict/provenance fields are not established. No accepted exact Season entity/history destination table mapping or global authority exists in this handoff. Do not activate empty season bases or Tenants candidates merely to fill this gap. Existing proposed Tenants horse/rider tables and duplicate horse138724 remain conditional; source/customer identity, provenance, aliases/manual edits and conflicts require accepted mappings, never name merges.

Season's required output contract is retained source/customer/entity references plus accepted mapping revision, show/entry/participation associations, provenance and unresolved conflict status. These are requirements, **not deployed fields**. The local Season coordinator must name exact targets/fields and sole writer or explicitly keep them unassigned. Entries can continue its proven source IDs/CWF helper links independently; unfinished stewardship does not gate unrelated valid collection. Shared CWF roster/helper/private membership is not overwritten by provider history.

### Exact prepared Season destinations and fields (proposed, no domain IDs)

The owner's corrected October28 specification, independently read from `C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/wef-season-full-plan-review.md` and `wef-season-table-field-export-review.md`, selects four **existing** bases, each with a proposed entity table plus local shows/show-days. Earlier people/entries-season destinations are superseded for this entity-maintenance scope. Their schemas were reported meta-only at that inspection; no fresh schema audit is claimed here.

| Existing destination base | Exact prepared entity table | Proposed entity fields copied from Data, plus local links |
|---|---|---|
| wef-season-trainers appzZsd3UqMRR2CNc | trainers | trainer-key, trainer-id, trainer-name, last-success, shows, show-days |
| wef-season-riders appkWkm9rFLrkUvaD | riders | rider-key, rider-id, rider-name, last-success, shows, show-days |
| wef-season-horses appETrjio9Yj9Pv17 | horses | horse-key, horse-id, horse-name, last-success, shows, show-days |
| wef-season-rings appxMk9wfbBHEvC5e | rings | ring-key, ring-id, ring-number, ring-name, last-success, shows, show-days |

Each proposed local shows table: show-key (primary exact retained key), show-id, customer-id, facility-id, show-name, start-date, end-date, selected-date, last-success, reciprocal show-days and entity reciprocal. Canonical metadata comes from Shows, checked against Data context. Each proposed local show-days table: day-key (primary exact retained key), date, show (single local link), last-success and entity reciprocal. These are retained local association mirrors, **not a second canonical day/count/readiness publisher**. No count projection or acquisition table is added to those bases. Season is sole writer of its local entity/mirror records.

Source key fields are text; provider IDs retain existing numeric types; timestamps are accepted source observation times. Entity shows/show-days links are unions of attributable accepted observations, never reset per show. Ring-key already includes show/date/ring: preserve separate ring-day rows; no inferred physical-ring master. No name merges. Show membership may come from attributable show entry evidence; dated entity membership needs actual accepted class/trip evidence, not a show selected date. Twelve proposed domain tables have no established table/field IDs; accepted mapping-revision/provenance/conflict representation and Entries consumption contract still need specification. This prepared field list is not implementation proof or authorization to create tables.

## Shows and Entries consumer boundary

Shows planning mapping accepted: customer-id, show-id, date, show-day-key; unique-entry-count, count-coverage, count-rules-version; count-observed-at; publication-status; latest-attempt-status/at/reason; data-revision/data-evidence-ref with day/run/collection supporting keys. Proposed show-days field IDs do not exist by virtue of this plan. Entries needs canonical identity/validity first and accepted producer evidence for only the scopes it might skip.

Owner approval directly verified in Shows chat on October29: include scratches in dated observed registrations; exclude verified drops from current counts while retaining history; keep undated entries at show level; zero readiness lease five minutes from source accepted-as-of, not verification/publication/failed-attempt time. Existing consumer wake cadence unchanged. Schedules still collects changes. Unknown schooling/classless evidence cannot produce broad zero readiness. Revised positive evidence, scope changes, correction, invalidity or expiry resume eligible acquisition. These policy decisions are settled; implementation approval is separate and not granted here.

Entries migration must preserve CWF sync, existing participation record IDs, plural schedule/ring/horse/rider links and helpers, inactive-but-collected entities, separate private owner/reason/follow/trainer-sharing controls. Data retained graph alone does not reproduce CWF feed selection or personal follows. Need a retained selection-equivalence/provenance contract before stopping duplicate acquisition. Do not export private selection data into shared source acquisition. Existing 30-minute schedule health is not a new count threshold.

## Gaps, owners and acceptance

| Gap | Accountable owner | Closure evidence |
|---|---|---|
| Source/date/identity coverage, directory reconciliation and explicit empty scopes | Local Season coordinator, Data function | Accepted exact child/source membership, page/identity checks and reconstructible retained revision; schooling attendance source support or explicit unknown |
| Exact source-backed participation → schedule crosswalk / group eligibility | Local Season Data function supplies proof; Entries validates; Schedules owns stage keys | Correct source occurrence/context and attributable participation eligibility; no guessed class-group join; natural multi-group proof still open |
| Selection-equivalent retained input for CWF and private lists | Entries with local Season Data function | Same selected participation/sync membership and privacy, including nonempty private delivery; no shared/private scope mixing |
| Season destination, authority and mapping revision/conflicts | Local Season coordinator, stewardship function | Exact existing targets/field mapping/sole writer and accepted identity rules; unassigned until evidenced |
| Day count-summary encoding and safe publication fence | Local Season Data function + Shows | Reuse collections retained-json/hash and runs.state-json, verify immutable count membership and completion-last; Shows stages/readbacks canonical projection; no parallel evidence tables |
| Shows day projection and Entries/Live gates | Shows publishes; Entries/Live own readers | Exact local links; policy/source-asof/freshness/covered scopes; no partial/error deletion or unknown→zero; skip/resume normal-run proof |

Before replacing an input: separately approved implementation, meaningful offline identity/privacy/fence/failure fixtures, two normal scheduled producer→Shows→Entries/CWF consumer pairs, stable IDs/native links and repeat idempotency, actual populated-private and multi-group proof where claimed, source-asof/expiry/resumption behavior. A successful producer run, schema existence or local tests do not establish migrated end-to-end acceptance. Preserve current readers until exact acceptance is met.

## Acknowledgment record

**Local Season coordinator ACCEPTED** the existing Data fields and retained-json/SHA plus runs.state-json chain as the planning evidence handoff. It confirms no standalone participation table or proven trip-to-schedule/group eligibility crosswalk; Season's domain table IDs/mapping revision/conflict/provenance fields are unestablished. Existing four base targets do not settle those mappings. No count publication or consumer gating claimed.

**Entries ACCEPTED** the proposed table/field mapping after independently matching saved schema, but explicitly does not accept an input switch. Its remaining gates are proven dated participation including relationships without trips, CWF feed8778/50121/80970 selection equivalence, source-backed schedule crosswalk/group eligibility, scratch/drop/failure semantics, exact retained membership replay, dated coverage/day-count projection, Season entity authority, and private-follow delivery. Data owners is not private-follow ownership/sharing permission. Existing CWF collection/native links stay intact.

**Shows ACCEPTED** canonical field mapping and portable retained evidence references; no objection to local Data retained machinery reuse. It does not accept Entries migration on Entries' behalf. Shows confirms policy decisions are approved, leaving technical coverage, publication and lane-specific consumer verification as blockers. These acknowledgments are planning assent, not deployed or scheduled end-to-end proof.
