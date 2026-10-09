# Existing system register — sessions, comments and observations

Reviewed: 7 October 2026. Historical implementation/test record: June 2026.

**This system already has implementation and documented historical testing. Reuse this record before commissioning the same paths again. WEC is the dataset used by this implementation; sessions, scoped comments, check-ins and observations are system capabilities, not WEC engine calculations.**

This is a source-backed inventory, not a new deployment or fresh live certification. The CRM/Recognize storage task is paused by the owner. This register does not resume it.

## What exists

| Capability | Existing implementation | Recorded test evidence / limit |
|---|---|---|
| Browse show → day → ring → class → entry | `comment-state.js` builds the nested model; `session-widget.js` renders it. All class entries are included; active-trainer entries receive `cwf-entry`. | June README reports 26 classes, 444 entries, 26 marked entries; a mixed class rendered 24 entries with 2 marked. Historical dataset counts, not current counts. |
| Start a session and save a display name | Widget keeps device/session state in cookie/local storage and calls `start-session`; backend creates/updates `wec_sessions`. | README lists session/name support as done. This is session identity, not proof of authenticated Recognize/CRM identity. |
| Heartbeat and list active sessions | `session-heartbeat` updates `last_seen_at`; `list-sessions` returns active sessions. | Backend handlers exist. The inspected hosted widget has no heartbeat call/timer; continuous heartbeat operation is not established by this review. |
| Check in/out of a ring | Widget calls `ring-checkin`/`ring-checkout`; backend ends the previous ring check-in when switching rings. | README explicitly records live check-in/out writes and UI check-in verification. |
| Submit ring, class or entry comments | `add-comment` writes the corresponding scoped table, then master `wec_comments`; widget reads comments back. | README records a linked live comment and a scoped entry-comment write. Separate live tests for every scope are not individually evidenced there. Two sequential writes do not establish atomicity or retry safety. |
| Answer/dismiss a contextual prompt | Widget builds a bottom sheet from selected context and schedule/order fallback; `add-observation` accepts `yes`, `no`, `unsure`, `dismissed`. | README records prompt rendering and a UI answer writing an Airtable observation. This does not prove every answer/edge case. |
| Mark first-hand context | Matching active session ring check-in adds `source_confidence = first_hand` to comments/observations. | Code and June README agree. The label reflects check-in context, not independently verified physical location or observation accuracy. |
| Configure presets/questions | Schema and seed scripts define operator-managed options/templates. | Runtime use of these configuration tables was not found in the inspected hosted routes. The widget currently builds prompt text in code. |
| Store current Waze user state | Schema/seed work defines `waze_users` and cookie/visit/device/geo fields. | Seed coverage exists; ongoing updates from the inspected hosted routes were not found. Schema fields do not prove tracking works. |
| Keep historical interaction footprints | Schema script defines `waze_session_footprints` and ten event-type choices. | Schema work located; no runtime writer found in the inspected application source. Not included in the ten-table seed script. |

## All eleven tables

Documented base: [wec_schedules — app6XS1RvsPNRT6os](https://airtable.com/app6XS1RvsPNRT6os). Table names are preserved for discovery; this review did not inspect or alter the live base.

| Existing table | Responsibility | Evidence located |
|---|---|---|
| `wec_sessions` | Session start, last-seen heartbeat and active-session listing | Backend + widget + schema + seed; historical session support record |
| `wec_ring_checkins` | Ring presence declarations and check-in/out history | Backend + widget + schema + seed; historical live/UI verification |
| `wec_comments` | Master comment log | Backend + schema + seed; historical linked write |
| `wec_ring_comments` | Ring-scoped comment log | Backend scope branch + schema + seed |
| `wec_class_comments` | Class-scoped comment log | Backend scope branch + schema + seed |
| `wec_entry_comments` | Entry-scoped comment log | Backend + schema + seed; historical scoped write |
| `wec_observations` | Structured prompt answers | Backend + widget + schema + seed; historical UI-to-record verification |
| `wec_comment_presets` | Operator-managed comment choices | Configuration schema + seed; consumer not established |
| `wec_question_templates` | Operator-managed question definitions | Configuration schema + seed; consumer not established |
| `waze_users` | Latest Waze user/display/device/visit/geo state | System/extension schemas + seed; ongoing writer not established |
| `waze_session_footprints` | Historical session/interaction events | Footprint schema only in located implementation; no runtime writer found |

## Implemented paths

```text
WEC schedule/helper data
  → comment-state → hosted session widget → ring/class/entry browsing

Widget session/name actions → edit → wec_sessions
Widget ring check-in/out   → edit → wec_ring_checkins
Widget scoped comment     → edit → scoped comment table + wec_comments
Widget prompt answer      → edit → wec_observations
                                      ↑
                         matching ring check-in adds first_hand
```

The implemented reader uses `focus_show`, `class_start_times`, `class_oog`, `class_hide`, `rings` and `trainers`. Backend comment/observation writes resolve ring/class/entry links. Schema and seed links to shows, focus_show and waze_users must not be mistaken for proof that every runtime action populates all those links.

Historical hosted routes: `/test/wec-schedule/session-widget`, `/test/wec-schedule/comment-state`, `/test/wec-schedule/edit`. These are recorded deployment paths, not endpoints exercised in this review.

## Relationship to Recognize and rs-user-data

- **Recognize/user-data:** shared people, barns, horses, memberships, assignments, opt-ins and identity mappings.
- **This system:** interaction sessions, ring check-ins, comments, observations, question configuration and interaction history.
- **WEC engine/data adapter:** supplies show/day/ring/class/entry context and computed timing. Comments/observations are additional human input; feeding them back into timing is separate work.
- The current `waze_users`/`session_id`/`device_id` fields are not established as the same identities as Recognize `entity_uid`/recognition person IDs. No identity merge or migration was performed.
- WEC-specific table names, source fields and fallback show `14906` remain in code. The concept is reusable, but a portable adapter or WEF implementation has not been proven by the WEC example.

## Work not established as complete

The June README retains unfinished work for the public Webflow embed, browser geo gate, live-confidence prompt selection, observation feedback into timing, alert windows, missing-link auditing and stale-data checks. Their current deployed status is unknown from this document review. The located widget/schema gaps above are also not complete runtime features.

The ten-table [seed script](../seed-airtable-wec-waze-system.js) uses show `14906`, day `2026-06-14`, ring `670`, class `29100`, entry `1856`. It creates/updates linked records; it is not an isolated test and was not run. Its existence and README description are not a fresh execution receipt.

The supplied `ringstatus-data` search result is not a verified alternate project location. This register uses the concrete source paths below; it does not assert that similarly named work cannot exist elsewhere or in remote history.

## Source map and review evidence

| Source | What it establishes |
|---|---|
| [Historical lane README](README.md#what-is-done) | June implementation and reported live/UI test results; version `2026-06-14 v0.2` |
| [Historical unfinished work](README.md#what-still-needs-to-be-built) | Remaining work as recorded in June |
| [Backend actions](../../../webflow-cloud-test/src/pages/wec-schedule/edit.js) | Session, check-in, scoped/master comment and observation handlers |
| [Hosted widget](../../../webflow-cloud-test/src/pages/wec-schedule/session-widget.js) | Actual browsing, name/session, comment, check-in and prompt UI paths |
| [Nested state reader](../../../webflow-cloud-test/src/pages/wec-schedule/comment-state.js) | WEC dataset input and nested response construction |
| [System schema/index script](../ensure-airtable-wec-waze-system-schema.js) | Ten table purposes, record-ID formula and link/index setup |
| [Preset/question schema](../ensure-airtable-wec-comment-config.js) | Operator configuration fields |
| [User/footprint schema](../ensure-airtable-wec-waze-user-footprint-schema.js) | Current-user extension fields, footprint schema, event choices and links |
| [Linked seed script](../seed-airtable-wec-waze-system.js) | Ten-table seed path; footprints excluded |
| [Historical iframe](../webflow-drops/wec-comments-proof-embed.txt) | Intended Webflow embedding route |
| [Ring Waze concept](../../../concepts/ringwaze-concept/README.md) | Broader system concept; not a substitute for implementation evidence |

Review basis: current checkout at Git HEAD `159ff8b4098629abc87a6e0d53dbb75cdf34cfde`. The lane README's latest file commit is `b8350dd5a` (15 June 2026); the latest combined backend/widget history entry inspected is `cec4a190b` (20 June 2026). These files were unchanged locally when reviewed. No deployed-version equivalence is claimed.

Verification performed for this register: read the named sources; inspect the implemented action branches, widget calls and schema/seed coverage; search current application source for the footprint/config/user table names; check document links and eleven-table coverage. Historical tests remain attributed to the June README. No production workflow, schema script, seed, browser submission or deployment was executed.

Reuse rule: consult this register and its sources before creating equivalent tables or features. Preserve implemented work; distinguish source-present, historically tested and currently verified when scoping any future continuation.

## Repository reference index — 8 October 2026

Discovery scope: searchable files in the current RingStatus checkout, using `ringwaze`, `ring waze`, `ring-waze`, `ring_waze`, `waze`, `waze_`, `wec_comments` and `wec_observations`, plus related filenames. This found 20 Markdown documents, including this register and two byte-identical duplicate pairs. It does not establish completeness of remote history, ignored files or other repositories. No application code or production process was changed or executed.

### Product and concept documents

| Reference | Role |
|---|---|
| [Ring Waze concept](../../../concepts/ringwaze-concept/README.md) | Ring-specific human updates, comments, check-ins and operational context; links back to the implemented WEC example. |
| [RingWaze product overview](../../../concepts/ringwaze-concept/ringwaze-overview.md) | Broader observation product: authoritative schedule, witnessed facts, reliability and participant timing; concept statements are not runtime evidence. |
| [Worked examples](../../../concepts/ringwaze-concept/examples.md) | Delays, entry-on-course anchors, ring drag, sparse observations, participant propagation and observer coverage. |
| [Location Layer concept](../../../concepts/location-layer-concept/README.md) | Related location/check-in/movement context; separate project scope. |
| [Publishing API concept](../../../concepts/publishing-api-concept/README.md) | Proposed shared delivery architecture and future Ring Waze views; historical architecture does not override later owner decisions. |

WEF Maps and its named **ChatGPT Maps** display tool belong specifically to **ring-maps**, per the owner's 8 October clarification. That supplied specification is not a RingWaze document and is not reassigned to Locations by this index.

### Patent working documents — separate from implementation evidence

| Reference | Role |
|---|---|
| [Patent package introduction](../../../concepts/ringwaze-concept/ringwaze-patent-readme.md) | Package index and distinction between product scope and proposed patent core. |
| [Invention boundary](../../../concepts/ringwaze-concept/invention-boundary.md) | Proposed information chain, roles and exclusions. |
| [Patentability assessment](../../../concepts/ringwaze-concept/patentability-assessment.md) | Preserved historical assessment; not re-evaluated by this reference search. |
| [Prior-art matrix](../../../concepts/ringwaze-concept/prior-art-matrix.md) | References discussed in the existing assessment; no fresh prior-art verification performed. |
| [Specification support checklist](../../../concepts/ringwaze-concept/specification-support-checklist.md) | Disclosure/support checklist, not a feature-completion checklist. |

### Implementation history, boundaries and integration references

| Reference | Role |
|---|---|
| [WEC Comments and Observations lane](README.md) | June implementation, table definitions, historical tests and unfinished work. |
| [This existing-system register](EXISTING-SYSTEM-REGISTER.md) | Eleven-table inventory, code paths and evidence limits. |
| [WEC systems scope contract](../wec-systems-scope-contract-v0.1-2026-06-15.md) | Historical `wec-waze` ownership/scope alongside other WEC systems. |
| [June 1 platform skeleton](../../../ringstatus_skeleton_documentation_2026-06-01.rev2.md) | Sections 15, 23 and 28.13 describe Ring Waze, SMS/web intake and shared comment handling. These are documented plans, not proof of every integration. |
| [Diary copy of platform skeleton](../../../rs_diary/ringstatus_skeleton_documentation_2026-06-01.rev2.md) | Byte-identical to the root copy on 8 October; retained as a reference, not counted as independent evidence. |
| [June 4 merged infrastructure](../../../rs_diary/ringstatus-merged-infrastructure-readme-2026-06-04.md) | Section 22 covers alerts, SMS and Ring Waze in the broader platform. |
| [Second June 4 infrastructure copy](<../../../rs_diary/ringstatus-merged-infrastructure-readme-2026-06-04 (1).md>) | Byte-identical to the other June 4 copy on 8 October; preserved unchanged. |
| [Content-actions generation procedure](../../rs-content-actions-generation-procedure-2026-07-20.md) | Historical `waze` page/content-table mapping and generation process. |
| [Webflow page-change runner handoff](../../webflow-page-change-runner-handoff-2026-07-20.md) | Historical page-change scope and `waze` page mapping; no current runner authorization implied. |
| [User-data minimum schema review](../../user-data-minimum-schema-review-2026-10-07.md) | Shared stable entities and legacy references; keeps RingWaze interactions separate from entity records. Later Airtable-primary/CRM-deferral decisions control over older CRM plans. |

The two July documents record Webflow page `6a5c23f67752640f03bfb26e` and content table `tblPRZum8a5iStXkf` for `waze`. These identifiers were found in repository documentation, not verified against the live services in this search.

### Additional build and prototype artifacts

The backend, widget, state reader, system/config/user schemas, seed and iframe are linked in the source map above. Also preserve:

- [Initial comment schema](../ensure-airtable-wec-comments.js).
- [Simple comment test page](../wec-comments-simple-test.html).
- [Full comment test page](../wec-comments-full-test.html).
- [Session template](../wec-comments-session-template.html).

These are source artifacts, not new execution receipts. Schema and seed scripts can mutate Airtable and were not run.

The search also found Ring Waze references in shared renderer/template files. Preserve them as integration leads; a name or navigation entry alone does not prove an implemented RingWaze feature:

- `webflow-cloud-test/src/pages/rs-page-render.js`
- `webflow/rs-s/index.html`
- `webflow/rs-skeleton-contract/rs-standalone.html`
- `webflow/rs-template-system/webflow_pages/thispage/thispage-draft.html`
- `webflow/rs-template-system/webflow_component_pages/webflow-draft-page.html`
- `webflow/rs-template-system/webflow_component_pages/webflow-draft-page-embed.html`
- `webflow/rs-template-system/webflow_component_pages/pages/topnav.html`
- `webflow/rs-template-system/master_ks/rs-webflow-embed.html`
- `webflow/rs-template-system/master_ks/rs-standalone.html`
- `webflow/rs-template-system/master_ks/kitchen_sink_template.html`
- `webflow/rs-template-system/master_ks/00_APPROVED_BASE_DO_NOT_REBUILD.html`
- `webflow/rs-template-system/starter_outputs/section-add-demo/index.html`

Paths in this last list are relative to the RingStatus repository root. Historical development log files were located but are not promoted to test evidence by this index.
