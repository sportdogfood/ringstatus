# wef-entries scope

Version: 1.4
Date: 29 October 2026, America/New_York.
Status: Narrow tenant-independent matcher implemented and tested locally. Runtime migration deployed 29 October at 17:27:23 UTC; two normal scheduled runs verified at 13:29 and 13:34 Eastern, both business-ok Partial. See MATCHER.md for the implemented contract.

Latest owner boundary relayed by Records on 29 October: CWF is not a WEF input, output, dependency or acceptance gate. Entries consumes collected Data/Season facts and schedules; it does not independently acquire entries. The Season coordinator owns Data collection/retention and Season stewardship. Earlier consumer-migration gating language below is historical and superseded. Subsequent owner approval authorizes five fields, deployment and two normal scheduled verifications. No task changes, deletion or repairs.

## Purpose

wef-entries is the linking module for wef-schedules. It connects schedules, rings, groups, classes, horses, riders and trainers using source-supported identities and relationships.

It supplies common linked data. Tenants consume that data and select their own records downstream. Tenant configuration must not determine which relationships this module builds.

## Responsibilities

- Read canonical show context from wef-shows and operational schedules from wef-schedules.
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

## Intended recurring workflow

1. Read the canonical show and valid dated operational schedules.
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
| collections | tbltCxLRlhpdHdHSw; collection membership/evidence; current tenant-specific keys require migration review |
| schedules sync | tblVcbGJHhzLYZRxn; read-only local operational schedules |
| run-logs | tblCWYgZs49eOuQ7e; run/archive audit |
| heartbeat | tbloQWorYyYtTIv8T; module run state; current CWF key requires migration review |

Preserve operational entry-key show-id|YYYY-MM-DD|entry-id|class-id and exact schedule stage-key, with explicit source/customer context. Data's show-entry key and schedule occurrence key are different identities; neither may silently replace these keys. A horse, show entry, participation and trip are distinct. Existing plural schedule transport remains useful, but actual multiple-group eligibility requires validated source evidence.

## Current implementation gap - not removed from runtime yet

The running collector still implements the earlier CWF-specific design. Documentation alone does not remove these dependencies:

| Existing coupling | Required removal from this stage |
| --- | --- |
| settings.js CWF_BASE_ID, TENANTS_BASE_ID and favorites configuration | Replace tenant inputs with accepted schedule/source relationship inputs. |
| model.js CWF scope, fixed trainer IDs, main-user/user-2 selections | Remove tenant and private-follow selection from module normalization. |
| collector.js reads tenant-details/trainers/favorites and fetches selected people feeds | Consume retained tenant-independent evidence instead. |
| CWF-prefixed collection keys, log text and heartbeat key | Migrate audit identity without deleting historical evidence or breaking existing references. |
| CWF-specific tests, verification and runner naming | Define module-only acceptance and reconcile the existing deployment route; no task changes made here. |

The existing Data mapping is a starting point, not a ready reader: prove dated participation including associations without trips, accepted frozen membership/status, and the Data schedule-to-operational key crosswalk. Multi-group relationships remain unresolved where source evidence is ambiguous. Do not replace the proven collector with guessed inputs or declare runtime removal complete.

## Acceptance and execution boundary

29 October implementation clarification: ordinary unique show/class/source-date schedule association and observed group eligibility are separate. The matcher copies supported class/date schedule links while flagging uncertain group links. Group uncertainty does not discard supported entry/entity facts or fail the entire prepared function. The stronger group test requires full roster equality and accepted same-revision inventory; a partial roster is unresolved, not necessarily erroneous. This supersedes earlier wording that treated group eligibility as a prerequisite for every schedule association.

This module's acceptance is two required normal scheduled runs with valid common relationships, stable keys/native links, correct empty/failure handling and complete run evidence. Tenant task success is not its completion criterion. Consumer compatibility should be checked at the contract boundary without taking ownership of tenant workflows.

Codex inspects and makes authorized repeatable changes; the runner executes collection. No manual collector runs, substitute production requests or record repairs. Preserve existing data and consumers through a concrete migration; do not delete CWF records to remove tenant coupling. Failed workflow verification stops the affected path under the owner's runner rules.

The prepared candidate in evidence/tenant-independent-20261029/staged reads no tenant configuration and performs no private-follow selection. Eleven linking tests and four retained-input tests pass offline. The retained-input checks reuse Data's existing immutable revision validator and the Data owner's frozen fixture. These are local checks, not normal-run proof.

The running collector, tasks and Airtable remain unchanged. Deployment still needs a supported source-to-operational schedule mapping and a coordinated downstream reader transition. The current CWF reader explicitly requires the old CWF heartbeat and trainer-feed collections; this dependency belongs to that consumer and must not be reintroduced into the new Entries module. Existing trainer-ids means feed association and cannot silently become all-show actual trainer identity. Earlier tenant-specific scope and CWF verification ownership in version 1.0 are superseded.

## References

- README.md: historical implementation and proof; its tenant-owned scope wording is superseded here.
- ../docs/coordination/wef-data-boundaries-working.md: working architecture and scope amendments.
- ../docs/coordination/wef-entries-data-season-mapping-20261029.md: prepared source tables and unresolved crosswalk/revision requirements.
- evidence/: dated proof and historical failures; historical consumer failures do not redefine this module's ownership.
