## Linking-only restoration - v1.7 - 29 October 2026

Owner explicitly approved restoration of linking-only scope, preserving records, links and compact-input/readback/log fixes. Deployed 19:37:05 Eastern using the unchanged wef-entries-cwf-collector route and existing lock. Normal scheduled acceptance PASS:19:39 and19:44Eastern triggers completed task0/businessPartial without errors; both preservation readbacks passed. All513entries/446collections, checked links and source times preserved. See evidence/linking-only-restoration-20261029/acceptance.json.

Only active Entries-local shared schedules select matching accepted Data class/participation evidence. No new Data-only participation outside those scopes. No shared schedules means no Data replay or business updates. Live reads/qualification/enrichment and collector comparison outputs are removed from active execution; the unused live-input helper is retained in release before/ history. Existing stored lane/Live JSON is preserved as historical evidence, not scrubbed, regenerated or used to select records. Current linking still validates identities, deduplicates, reports unresolved/Conflict and preserves supported facts.

Preserved: compact once-per-day inventories below the existing 8MB bridge limit; bounded exact-key Airtable readbacks with lost-response protection; compressed verified archive indexes for oversized logs; all matcher/freshness/identity/group safeguards, existing keys, native/helper links and source times. No absence retirement, schema/task/cadence changes, manual runs or record repairs. Existing downstream Live consumes the unchanged core source-evidence/schedule contract; no consumer code or sync changes.

Evidence: evidence/linking-only-restoration-20261029/ (before/, staged/, current-tests.txt, saved-replay-check.json, review.md, deployment.json, before.json, release.md). 41 current-contract Node checks, compact projection Python check and saved406class replay passed; independent review cleared the selective correction. Historical CWF/feed tests remain historical, with baseline failures documented; they do not restore tenant collection duties.

This supersedes later comparison/enrichment implementation claims and earlier pending comparison proposals. Prior accepted shared-schedule linking behavior and historical evidence below remain protected. Data/Season scope and separately approved Live retirement work are independent.

# wef-entries scope

Version: 1.6
Date: 29 October 2026, America/New_York.
Status: Independent linking module deployed and accepted. Runtime migration deployed 29 October at 17:27:23 UTC; two normal scheduled runs verified at 13:29 and 13:34 Eastern, both business-ok Partial. See MATCHER.md for the implemented contract.

Latest owner boundary relayed by Records on 29 October: CWF is not a WEF input, output, dependency or acceptance gate. Entries consumes collected Data/Season facts and schedules; it does not independently acquire entries. The Season coordinator owns Data collection/retention and Season stewardship. Earlier consumer-migration gating language below is historical and superseded. Subsequent owner approval authorizes five fields, deployment and two normal scheduled verifications. No task changes, deletion or repairs.

## Purpose

wef-entries is the linking module for wef-schedules. It connects schedules, rings, groups, classes, horses, riders and trainers using source-supported identities and relationships.

It supplies common linked data. Tenants consume that data and select their own records downstream. Tenant configuration must not determine which relationships this module builds.

## Responsibilities

- Use only the active schedules already shared into the Entries base for schedule scope and links. Retain canonical Shows context solely to validate accepted source identity.
- Use retained source entry/participation evidence to connect schedules, rings, groups, classes, horses, riders and trainers.
- Preserve existing operational participation and schedule keys; resolve native links within each destination base.
- Normalize and deduplicate evidenced relationships. Do not merge by name, invent IDs or infer every entry's group membership from class membership alone.
- Distinguish source-confirmed trainer identity from a person's feed association.
- Maintain relationship status, successful observation time and provenance; report missing or ambiguous relationships explicitly.
- Run independently on its approved recurring route, logging every run and archive and preserving verified evidence on failed/incomplete inputs.
- Publish a tenant-independent relationship contract for downstream readers.

## Outside this module

No tenant-details or trainer-slot configuration; no main trainer or additional-trainer limits; no user lists, favorites or private follows; no tenant membership, sharing permissions, active-team slices or tenant roster/helper maintenance. No tenant-specific collection keys, heartbeat keys or verification gates.

CWF is only an example downstream consumer. Its collector, scheduler, helpers, private lists and task-completion discrepancy belong to its own owner. They are not responsibilities or acceptance gates of wef-entries. Historical CWF evidence remains evidence, not current module scope.

Live observations, timing calculations, results management, messages, publishing and waze remain independently owned consumers or sources of related facts. They do not expand this module into their business workflows.

Entries includes developing connection points for messages, results and waze. Missing implementations in those systems do not block the core schedule/entity relationships. Entries does not run their business workflows.

## Dependencies and owners

| Dependency | Purpose |
| --- | --- |
| wef-shows | Sole canonical show authority; no independent show discovery by Entries. |
| wef-schedules | Operational schedule keys and dated ring/group/class context, with completeness and freshness evidence. |
| Data, under the local Data/Season owner | Retained show entry/participation and entity facts, source identities, accepted revision membership, observation time, provenance and coverage. Entries does not own a second all-show acquisition process. |
| Season | Optional accepted entity/history references when available; no name-based merges or unapproved global master authority. Unfinished stewardship does not block otherwise valid source relationships. |
| Downstream consumers, including tenants | Read common relationships; own their own selection, privacy, helpers, notifications and presentation. |
| Records coordinator | Reconcile producer/consumer contracts and release dependencies without executing collectors. |

## Recurring workflow

1. Read active local shared schedules; their show/date/class/ring scopes determine what Entries links.
2. Read matching accepted retained source entry/participation evidence.
3. Resolve schedules, rings, groups, classes, horses, riders and trainers using explicit source relationships.
4. Validate identity, deduplication, completeness and freshness. Keep unresolved relationships visible rather than guessing.
5. Update the module's common relationship records using stable keys and destination-local links; archive prior evidence as required.
6. Finish run-logs and heartbeat with counts, errors and evidence references.
7. Downstream tenants and other consumers independently read and select their own data.

A valid complete empty input succeeds. Missing, partial, failed or stale evidence is not an empty collection and must not erase verified relationships. A zero trip count or no competition classes cannot establish an empty show day. No canonical day suppression gate is activated by this scope correction.

## Existing storage and identity contracts

Base: wef-entries appSxF4pYMBaxpTZd.

| Existing table | ID / purpose |
| --- | --- |
| entries | tblqkWmrsYMs2hyQ6; operational participation records |
| collections | tbltCxLRlhpdHdHSw; accepted class membership/evidence; new keys data|customer|show|date|ring|class; historical rows preserved |
| schedules sync | tblVcbGJHhzLYZRxn; read-only local operational schedules |
| run-logs | tblCWYgZs49eOuQ7e; run/archive audit |
| heartbeat | tbloQWorYyYtTIv8T; module run state under wef-entries; historical keys preserved |

Preserve operational entry-key show-id|YYYY-MM-DD|entry-id|class-id and exact schedule stage-key, with explicit source/customer context. Data's show-entry key and schedule occurrence key are different identities; neither may silently replace these keys. A horse, show entry, participation and trip are distinct. Existing plural schedule transport remains useful, but actual multiple-group eligibility requires validated source evidence.

## Deployed implementation

The runtime reads its local shared schedules and matching accepted Data journals. Canonical Shows supplies source identity context. It does not read the source Schedules table or show-collection, replay source/sync parity, or require dated schedule health. Tenant configuration and direct source acquisition are removed. Optional Season references remain unavailable and nonblocking. The existing launcher/task/cadence are unchanged; its historical task name does not define module scope.

Actual trainer identity is published in source-trainer-ids. Historical trainer-ids feed associations are preserved. schedule-link-status, schedule-link-basis, group-link-status and link-issues distinguish accepted mappings from uncertainty. source-json stores original sourceAsOf, accepted revision, evidence, latest applicable attempts and prior link history.

Accepted observations are not asserted current; no producer TTL is invented. Automatic absence retirement is disabled. An explicit complete empty class succeeds; a revision with no retained classes is unavailable. Missing or uncertain schedule links preserve existing native links as history while valid entity facts continue.

## Acceptance and execution boundary

29 October implementation clarification: ordinary unique show/class/source-date schedule association and observed group eligibility are separate. The matcher copies supported class/date schedule links while flagging uncertain group links. Group uncertainty does not discard supported entry/entity facts or fail the entire prepared function. The stronger group test requires full roster equality and accepted same-revision inventory; a partial roster is unresolved, not necessarily erroneous. This supersedes earlier wording that treated group eligibility as a prerequisite for every schedule association.

This module's acceptance is two required normal scheduled runs with valid common relationships, stable keys/native links, correct empty/failure handling and complete run evidence. Tenant task success is not its completion criterion. Consumer compatibility should be checked at the contract boundary without taking ownership of tenant workflows.

Codex inspects and makes authorized repeatable changes; the runner executes collection. No manual collector runs, substitute production requests or record repairs. Preserve existing data and consumers through a concrete migration; do not delete CWF records to remove tenant coupling. Failed workflow verification stops the affected path under the owner's runner rules.

Release accepted on 29 October: 36 offline checks and independent review, five additive fields, then two distinct normal scheduled runs at 13:29 and 13:34 Eastern. Both task exits were zero, business ok true, result Partial for uncertain links, with no processing errors. Record IDs, helpers, links, original source times and task configuration were preserved. Exact receipts and proof are in release.md.

Remaining downstream work is Live reference sync exposure and consumer freshness policy under separate ownership/authorization. It is not an unfinished Entries release gate. Group attribution remains unresolved where evidence is insufficient; no claim of fresh upstream Data or consumer compatibility is made.

## References

- README.md and release.md: current operation and accepted deployment evidence.
- ../docs/coordination/wef-data-boundaries-working.md: working architecture and scope amendments.
- ../docs/coordination/wef-entries-data-season-mapping-20261029.md: prepared source tables and unresolved crosswalk/revision requirements.
- evidence/: dated proof and historical failures; historical consumer failures do not redefine this module's ownership.


## Conflict refinement accepted - 29 October 2026

Ambiguous schedule associations now show exact Conflict; missing associations remain unresolved. Supported group evidence may still disambiguate. No guessed schedule, clearing prior links, source timestamp refresh or absence retirement was added.

Records independently reviewed the two-file change; 30 offline checks passed. Two distinct normal scheduled runs passed with task exit0, business oktrue, Partial for link uncertainty and no errors:
- ad9fb75d-bd5f-41fe-8f0a-23bc09fd9053: 14:09:02-14:09:51 Eastern.
- f7745e75-9005-413d-91ea-3ce0df49e512: 14:14:05-14:14:49 Eastern.

Airtable confirms MASTERFUL record recB75ieQrVXxiVNY is Conflict; 113 supported schedule mappings remain linked. Repeat readback:127records,zero duplicate keys,zero changed entry rows. Existing IDs, helpers, native links and source times preserved. Heartbeat/run-log agree. Schedules code hashes and task XML unchanged. No manual runs or repairs.

Evidence: evidence/conflict-status-20261029/ (deployment.json, tests.txt, normal-run-1/2.json, task-run-1/2.json, after-1/2.json, verification-1/2.json).


## Shared-schedule scope refinement - 29 October 2026

Only active local shared schedule scopes select Data observations. Out-of-scope observations are skipped without changing or retiring prior records. Missing accepted class evidence is a visible Partial link issue, not an empty-data claim or a processing failure. Actual IO and invalid required identity remain reportable errors.

The global Data-failure gate is removed. Accepted group inventory is always evaluated. Unscoped producer failures remain in run audit; only known exact-class attempts appear as latestApplicableAttempts. No source timestamps are renewed and no absence retirement is enabled.

Duplicate shared schedule keys or ambiguous schedule selection produce Conflict only on affected relationships. Healthy entries continue; prior links/history remain.

Group-link-status linked with class-date basis proves SOURCE group membership independently of an ordinary schedule link. It does not prove the particular operational group occurrence; that requires observed-group basis. No group IDs are inferred from stage keys.

Four-file change independently reviewed with34 offline checks. Deployed18:35UTC; normal18:39 and18:44 runs verified. Evidence: evidence/shared-schedules-20261029/.


## Shared-schedules-only release accepted - 29 October 2026

Four reviewed files deployed through the existing idle/lock route; 34 offline checks passed. Two distinct normal scheduled runs passed with task exit0, business oktrue, Partial for relationship uncertainty and no processing errors:
- 5ca0b0c7-fca9-44f3-9f74-143a472a7da1: 14:39:02-14:39:44 Eastern, accepted Data revision15.
- 0b494dd2-560b-499b-9360-ef36f3006369: 14:44:02-14:44:46 Eastern, accepted Data revision16 (18:22:08.409536 UTC).

504 active shared schedule rows define the universe. Each run processed160 matching accepted class scopes and117 participations. 257 shared class scopes lacking accepted observations were reported without pretending they were empty or stopping valid work. Readback:127totalrecords,113schedulelinks,1Conflict,103sourcegroupmemberships,11unresolvedgroups,13historicalrecords. Repeat IDs, schedule links, statuses, identity fields and original source timestamps unchanged; zero duplicate keys. Heartbeat/run-log match both receipts. Deployed hashes match; task XML and Schedules code hashes unchanged.

No external Schedules table/show-collection reads, source/sync parity replay or global Data-failure gate remain in the collector. Accepted Data integrity checks still apply. Actual errors remain reportable. Source group membership with class-date basis does not prove an operational group occurrence. No manual runs, record repairs, task/schema changes or absence retirement.

Proof: evidence/shared-schedules-20261029/ deployment.json, tests.txt, normal-run-1/2.json, task-run-1/2.json, after-1/2.json and verification-1/2.json. Release complete.
