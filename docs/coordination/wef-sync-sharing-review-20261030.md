# WEF shared-table review and decision record

October 30, 2026. REVIEW / DECISION PREPARATION ONLY. No source views, sync configuration, fields, records, readers, collectors or tasks are changed by this document. Existing source ownership and approval/failure-stop boundaries remain controlling. Schedules retirement/Data-only migration remains a separate preparation project.

## Owner update - published Data Library sources

The owner reports using **Publish to Data Library**, rather than configuring destination syncs, for the following Season sources. This is recorded as an owner-reported publication action, not a claim that destination imports, fields, filters, permissions or freshness were independently verified. The connector currently available here does not expose Data Library publication settings.

| Existing source base | Published/planned library name | Current status |
| --- | --- | --- |
| wef-season-riders / appkWkm9rFLrkUvaD | w-riders | PUBLISHED - owner reported |
| wef-season-horses / appETrjio9Yj9Pv17 | w-horses | PUBLISHED - owner reported |
| wef-season-trainers / appzZsd3UqMRR2CNc | w-trainers | PUBLISHED - owner reported |
| wef-season-rings / appxMk9wfbBHEvC5e | w-rings | PLANNED - not published yet |

**Owner cancellation, October 30, 2026:** People is removed from managed scope, backlog and this sharing proposal. The former `w-people` proposal is withdrawn; no destination needs it under this plan. Do not implement or revive People management. Existing People bases and dated inspection/preparation evidence remain untouched. Separate riders, trainers, horses and rings sources and workflows continue unchanged.

Publishing a library source and importing it into a destination are tracked separately. This update does not certify which entity/parent tables or fields the three library publications include. Retained shows/show-days remain historical membership context, not canonical current-show authority. Review the actual library source scope before any consumer change.

## What is being decided

Decide which producer tables are suitable to share, which existing consumers need them, and whether a consumer needs a local read-only reference or its existing validated input path. A source table, a published Data Library source or enabled outgoing sync view, a destination table and an operational reader are different facts. An existing publication/sync is not automatically the final design; availability is not a requirement to import it everywhere.

Status labels: **CONFIRMED** is existing owner authority or inspected evidence. **RECOMMENDED** awaits the owner's final sharing decision. **BLOCKED** requires an exact producer/consumer contract before implementation. No recommendation below is a schema/sync/reader-change approval.

## Confirmed ownership / meaning

| Source | Authoritative meaning | Not authoritative for |
| --- | --- | --- |
| wef-shows | Canonical show/default/date context and qualified show-day publication | Every entry, universal coverage, fresh zero readiness from stale counts |
| wef-data | Source-backed collected schedule/registration/trip/entity facts and accepted evidence | Current occurrence membership from all retained rows or last-success alone |
| wef-live | Validated current-day observations and progress | Official final placing, forecasts, whole-class completion or Data acquisition ownership |
| wef-entries | Source-supported participation/schedule/entity relationships and qualification | Source collection, one trip per participation, private tenant choices, Live enrichment |
| four wef-season bases | Retained historical entity identities and source-backed show/day memberships | Canonical current default, live readiness, attendance inferred from directory registration |

Local Season coordinator still owns Data acquisition and separately Season retention. There is no new Data agent handoff. Entries remains linking-only. No collector depends on Entries to continue collection.

## Recommended shareable source catalogue

These are proposed field-limited read-only views on existing owned tables, not permission for broad access or a new table.

| Producer | Table(s) | Recommendation | Consumer purpose / boundary |
| --- | --- | --- | --- |
| Shows | shows | KEEP as the one canonical show reference | Data, Schedules, Live, Entries, Timing/Engine and other consumers that actually need show context; existing direct validated reads can remain instead of adding a sync |
| Shows | show-days | KEEP; import only where day counts/readiness/display are required | Pair with correct parent Shows reference; preserve evidence/coverage/source age; never make it a circular Data count input |
| Live | live-rings | KEEP as observed ring-state reference | Direct ring activity/browsing; not mandatory for every base |
| Live | live-groups | KEEP as observed group-state reference | Timing/Engine group decisions, with separate phase counts |
| Live | live-classes | KEEP as observed class progress | Timing/class decisions; Results only for source-backed candidate selection, not completion certification |
| Live | live-entries | KEEP as observed TRIP reference | Exact trip progress/order/identity; the table name does not make it the registration or participation source |
| Data | schedules | PRIMARY operational-reference candidate | Live/Entries future Data-only schedule references; accepted membership, row-version delivery and exact occurrence mapping are BLOCKED prerequisites |
| Data | rings, groups, classes | CONDITIONAL reference closure only | Add only if exact lookup/native-link needs cannot be met by the accepted schedule view; do not automatically add three more syncs |
| Data | entries, trips | PREPARE separate registration and trip references | Consumers that need source registrations or individual trip facts; never substitute Data show-entry rows for Entries participation rows |
| Data | horses, riders, trainers | OPTIONAL current collected entity references | Import only required identities/labels. Prefer these for current Data fact relationships, Season for retained cross-show history |
| Data | owners | HOLD until an explicit consumer and allowed field scope are agreed | Avoid broad sharing of personal/association fields merely because a table exists |
| Entries | entries | KEEP / expose qualified relationship publication | Current tenant consumer; other consumers only if they need participation/schedule/entity relationships, not as a new universal collection gate |
| Entries | collections | TECHNICAL evidence view only if needed | Narrow membership/coverage proof; not a broadly synced business table or substitute for immutable accepted evidence |
| Season riders | riders, shows, show-days | OPTIONAL retained-history family | Historic roster/entity/membership consumers; preserve original source and parent meaning |
| Season trainers | trainers, shows, show-days | OPTIONAL retained-history family | Historic trainer identity/membership; not tenant permissions or private trainer/team settings |
| Season horses | horses, shows, show-days | OPTIONAL retained-history family | Historic horse identity/show-day memberships |
| Season rings | rings, shows, show-days | OPTIONAL retained-history family | Historic dated ring identity/context |

Do not re-export Data.shows/days or the four Season shows/show-days as competing canonical current show sources. Keep them as their existing local graph/retention records. A historical entity view may need its own historical parents for native-link closure; label them with the source base and historical role.

## Minimum destination map - recommended, not an active migration

### Shows recommendation - minimum context versus daily counts

Owner requested this planning-map clarification in Shows on October 30. It changes documentation only: no source view, destination import, field, reader or existing sync is added or removed. Source table existence is confirmed; outgoing source-view enablement and destination configuration/freshness remain separately unverified. Existing copies of both tables do not prove both are required.

Source: Shows base `appCaSmL819KK7UFJ`, `shows` / `tblJA5CUon5s1sWib`, `show-days` / `tbl14MaCCDw6dAywH`.

| Destination | Minimum Shows information needed | When both shows + show-days are appropriate |
| --- | --- | --- |
| Data | shows-only canonical identity/default/date context through its approved input | Only a confirmed daily-count display or separately approved readiness use case; not acquisition input or a circular count dependency |
| Schedules | shows-only canonical identity/default/date context through its independent approved input | Same conditional use; no Data lane access or collector change implied |
| Live | shows-only canonical identity/default/date context | Confirmed count display or separately approved readiness use only; zero-day suppression is NOT approved |
| Entries | shows-only canonical identity/default/date context | Only a confirmed count display or approved readiness use; Entries remains linking-only |
| Season retention bases | Retained show identity through the existing Data handoff; separate direct Shows sync is optional, not established as necessary | Only an independently confirmed use requiring canonical daily counts; retained Season show/day membership is a different historical contract |
| Any other destination | Need must be established by its owner; existing validated reads can remain | Destination decision PENDING until the exact count/display/readiness contract is confirmed |

No destination is currently declared to require both merely because both tables are available. For destinations choosing show-days, include the corresponding parent Shows references and preserve historical parents. Join by portable `show-key = customer_id|show_id` and `show-day-key = show-key|date`, resolving native links to destination-local records. Never transplant source native IDs or join by name. Whether show-days needs an additional standalone parent show-key lookup remains an owner decision; no new field is approved.

Shows allowlist: `show-key`, `customer_id`, `show_id`, `facility_id`, `show_name`, `start_date`, `end_date`, `selected_date`, `available_dates`, `is_default`, `last_checked`. Show-days allowlist: `show-day-key`, `show`, `date`, `unique-entry-count`, `trip-count`, `count-coverage`, `count-rules-version`, `count-observed-at`, `count-verified-at`, `publication-status`, `accepted-summary`, `data-evidence-ref`, `latest-attempt-at`, `latest-attempt-status`, `latest-attempt-reason`.

`accepted-summary` must preserve completeness, coverage, expected/covered/accepted scope keys and original as-of. Count/readiness validity expires five minutes after original source observation; sync, replay or review time cannot renew it. Missing, partial, failed or stale evidence is not zero. Competition-class coverage does not establish whole-day emptiness; `available_dates` establishes date context, not participation membership. Keep raw payloads, local archive paths and operational logs/heartbeat/errors/reviews out of general consumer views while preserving existing technical handoffs. Shows retains its validated Data reader/protected replay; no inbound whole-Data-table sync is needed.

Remaining decisions: exact destinations needing daily counts; date/history scope; optional parent-key lookup; final allowed fields/views, local link mapping and source-to-destination configuration. These remain proposals requiring agreement before implementation.

| Existing destination | What it should obtain | What it does not need by default |
| --- | --- | --- |
| Shows | Existing verified Data count reader/replay | Broad inbound Data/Live/Entries table syncs |
| Data | Canonical Shows shows-only minimum; show-days only for a confirmed display/approved readiness use | Four raw Live syncs for acquisition; reverse Entries gate. Existing approved Live overlay is separate |
| Live | Canonical Shows context; future accepted Data schedule/local link references | Entries input; whole Season history; indiscriminate Data entity tables |
| Entries | Canonical Shows, accepted Data participation/entities and future eligible Data schedule references | Live enrichment; every raw source/operational log; ownership of producer evidence generation |
| Four Season bases | Existing verified Data head/outgoing archive handoff, retained native entity/show/day records | Blanket syncing mutable Data/Live tables as a replacement for durable retention |
| Timing / Engine | Existing accepted Data/Schedules input path as currently approved, plus validated Live evidence needed for its calculator | A sync purely to replace an already working validated reader; every Entries/Season/entity table |
| Tenant CWF | Its existing qualified Entries reference and local owner/private tables; deliberate current/historic entity references only if needed | Private settings shared upstream; automatic replacement of existing protected Schedules links |
| Results | Source-backed class/trip candidates and its accepted result contract | Treating live position/counts as final official results; every raw Live table by default |
| Messages / Publish / Alerts / Waze | Canonical context and the specific published answers/relationships its workflow requires | Blanket raw-source syncs or recalculating technical evidence from arbitrary mutable rows |

Timing/Engine already have direct source/evidence readers. Results, Messages, Publish, Alerts and Waze have only meta tables in the current inspected bases; these are proposed consumer needs, not claims of deployed readers or configured syncs. Their existing chat contracts must be checked before implementation. No downstream owner has been assigned new work by this document.

## The four retained Season families - explicitly in scope

| Existing base | Retained entity table | Retained parents | Sharing rule |
| --- | --- | --- | --- |
| wef-season-riders / appkWkm9rFLrkUvaD | riders / tblQxTdBoM6hWEZzX | shows / tblqMP5w5rZsLqBTS; show-days / tbl4njCgUqYc2t7g0 | Retained history, optional source family for native historical links |
| wef-season-trainers / appzZsd3UqMRR2CNc | trainers / tbl1vRzTPX4UOoW8u | shows / tblhwDJZDwrsFkpsL; show-days / tbl5L8plumfTMxGSq | Same; trainer identity is not tenant authorization |
| wef-season-horses / appETrjio9Yj9Pv17 | horses / tbl8uYnqzysdiBqiu | shows / tblI7qSWXr7Twtwa7; show-days / tbl2qxWvX4unSHHch | Same; preserve source IDs and recorded memberships |
| wef-season-rings / appxMk9wfbBHEvC5e | rings / tblzQ33F0whmvcPjp | shows / tbll9QcoUzENjI565; show-days / tblVaBn3Q3xjFjWy3 | Same; dated ring identity, ring ID and number remain distinct |

Current Data entities and retained Season entities represent different time scopes. They must not become competing writable masters. No name-based merging, source-time renewal, source-key changes or automatic overwrite of newer facts. If a destination only needs a portable history lookup, it need not import every parent; if it needs native parent links, use the exact local-parent mapping and preserve historical parent rows. Current default filters must not erase required past links.

## Minimum field / evidence contracts

**Shows:** shows exports show-key, customer_id, show_id, facility_id, name, dates, selected_date, available_dates, is_default, last_checked. show-days exports its key/date/parent plus count values, coverage/rules, original count-observed-at, verification/publication status, latest-attempt state and the accepted-summary/evidence needed by any readiness consumer. Verified zero expires five minutes from original observation; a synced status does not self-expire and whole-day coverage is not inferred from competition-class counts.

**Live:** preserve existing customer/show/date keys, scope status, original observation versus raw source-update timestamp, ring/group/class/trip identity and distinct phase metrics. Parent relationships need portable source IDs or verified destination-local links. Class last-seen can be discovery rather than successful detail; the four business tables alone do not yet provide a complete portable revision-consistent accepted-membership/detail-time contract. A six-minute overlay publication rule cannot blindly be applied to every last-seen. Operational consumption of new sync views is BLOCKED until the precise evidence projection is approved; browsing alone must be labeled accordingly.

**Data:** preserve existing schedule/ring/group/class/registration/trip/entity keys, accepted revision, original source times, scope membership and freshness/coverage. Registration rows and their accumulated rider/trainer/owner relationships are not a single dated assignment; exact single-trip role evidence supplies that meaning. A saved accepted generation had 519 retained schedule rows but only 405 current occurrences. All rows and last-success cannot establish current eligibility. Existing source fields also differ from Schedules sequence/type/time/count semantics. Current operational source views require the reviewed membership/delivery contract, not a rename or unqualified filter. The existing archive-backed validated input path stays in place until an approved alternative passes.

**Entries:** participation key remains show|date|entry|class. Expose entity IDs, source-trainer-ids, ring/class labels, schedule-key(s), status/observation status, schedule/group link qualification and original last-seen. One row can contain multiple trips; it is not the source of one arbitrary horse/rider/trainer tuple per trip. Historical unresolved schedule links can remain present and are not valid current links just because synced. Do not export private follows, tenant settings or legacy trainer-ids feed associations as source trainer identity.

**Season:** retain existing entity/show/day keys and local membership/checkpoint semantics. Existing entity fields contain key/ID/name/last-success and native shows/show-days memberships; detailed source revision/run/archive provenance resides in the existing per-show checkpoint and pinned Data evidence, not new entity columns. Last-success does not timestamp every retained membership. Names/scalars are the latest retained facts protected against regression, not a complete immutable historical name/status timeline. Membership proves only the recorded source-backed association, not universal attendance/currentness. No separate public Season exporter or sync family is claimed enabled by table existence.

Exclude raw source-json, local archive paths, full journals, work-items/slots/requests, heartbeat/run/error logs, review records, technical status tables and private tenant data from general-purpose views. Existing technical consumers can retain their individually approved evidence access; that is not a blanket sync contract.

Sync copies are not independent historical archives. Existing Live/operational retirement can remove rows from source tables; destinations must not assume a synced copy preserves those rows or native historical links forever. Keep accepted archive/retention contracts and historical parents; any changed source-view retention/filter needs downstream preservation review. No new archive service is proposed.

## Actual inspected wiring and limits

- Live has existing local wef-schedules.schedules and historical wef-entries.entries reference tables; collector source reads the former, not the latter. Do not delete or retarget historical fields.
- Entries has existing local wef-schedules.schedules. Existing accepted Data evidence is read separately. Data-only switch is not implemented.
- Tenant CWF has wef-schedules.shows/rings/schedules and wef-entries.entries references. Their current schemas/source are inspected; complete deployment/sync-health reacceptance was not performed here.
- Data now has wef-shows.shows / tblAcQWswWfsc1MpS and wef-shows.show-days / tbl2iAfdZagycQkBp. Detailed field metadata confirms these local references link to each other. Existing Data shows/days remain separate graph mirrors. Table/link presence does not prove current source-view filters, sync freshness or a switched runtime.
- Shows/Live outgoing sharing is owner-supplied context. Metadata confirms owned table/field existence. This connector does not establish all outgoing sync-source permissions, exact view filters, every destination source mapping or sync health. Those remain a pre-implementation UI/owner-verification gate.
- Initial metadata calls were rate-limited; successful bounded sequential reads completed the named 17-base inventory. No collector or source requests were substituted.

## Decision log for owner confirmation

| ID | Proposed decision | State |
| --- | --- | --- |
| S1 | Shows is the only canonical current show/default/day publication source | CONFIRMED existing authority |
| S2 | Expose both Shows tables, but require show-days only for genuine count/readiness/display consumers | RECOMMENDED; destination/date scope to confirm |
| S3 | Keep all four Live source families; import only the levels a consumer needs | RECOMMENDED; operational evidence projection BLOCKED |
| S4 | Prepare Data schedule/reference and registration/trip families, entity libraries only by need | RECOMMENDED; accepted current-membership and field parity BLOCKED |
| S5 | Entries publishes qualified relationships; remains linking-only with no reverse collector gates | CONFIRMED scope; field-limited sharing RECOMMENDED |
| S6 | Include all four Season entity + historical parent families as optional history references, never canonical current authority | RECOMMENDED; specific consumers/native-link needs to confirm |
| S7 | No blanket sync of audit/raw/private tables or every producer into every base | RECOMMENDED |
| S8 | Existing validated API/archive readers remain until separately approved migration; synced arrival does not replace evidence | CONFIRMED preservation boundary |
| S9 | Preserve current Schedules references, native IDs/history/archives and collector while future migration is prepared | CONFIRMED current boundary |
| S10 | Exact source-view fields/filters, destination list, retention scope and link targets receive a final recorded owner decision before configuration | PENDING this review |

Final decisions will be appended with the owner's response/date, exact source/destination/view contract and implementation approval boundary. Do not silently turn these recommendations into approved configuration.

Evidence: [current 17-base metadata inventory](evidence/wef-sync-review-20261030/schema-inventory.json); current producer reports in Shows/Live/Entries owner chats; Data/Season [source-to-destination report](<C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/data-season-sync-decision-map-20261030.md>) and its exact proposed field/key allowlists JSON. Current source metadata supplements the owners' saved-schema reviews; outgoing sync-source configuration remains unverified.
