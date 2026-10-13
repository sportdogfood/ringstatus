# Publish operating overview — 10 October 2026

LATEST OWNER CLARIFICATION: PRO-SCHEDULE target is /rs-barn-onboarding-schedule-v24. Saved legacy loader/print and current token-library evidence is recorded in the dated addendum below, which supersedes earlier unresolved page designation and initial Webflow-access statements. Application endpoint wiring/live behavior remains unverified.

Publish presents agreed schedule, pro-schedule, results, notifications and RingWaze painter outputs through endpoints and Astro templates. Retain print-pdf as Expected pending final requirements. Astro is shared delivery; Webflow owns static/CMS/native presentation. Messages owns SMS normalization, lookup response, outgoing delivery/logging/feed updates; Recognize owns silent identity/auth/session; Tenants owns onboarding/inputs/inline editing/saves/permissions/options. Engines process data and provide outputs. Do not duplicate their masters or invent endpoints.

CURRENT EVIDENCE: source and live Airtable schema/records only. No business workflow, endpoint substitute, print preview, SMS, publish, deployment or migration performed. Health Needs work because application bindings and live acceptance remain unresolved. Operating base readiness is separate from application readiness. Known running Publish version unresolved; inspected working checkout a5235466eed1b094a26db14ab34b514726abf91d. Historical packing commit 647ea24f8 inspected; current lib latest 41849ac266deba8abcd119048487791e9b17bfec. Shared Recognize/Tenants source remains protected; no implementation edits.

STEP 1 — Scope, identity and authorized source selection
Trigger: Display request in its existing page/session
Expected: Recognize supplies verified identity/session where required; Tenants supplies tenant permissions/options; engines supply outputs. Publish does not duplicate masters or calculate a new engine.
Current: Designations remain unresolved; application assignee unassigned. Identity recognition alone is not permission. No runtime call performed.
Evidence/status: Designation pending; C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/publish-specialist-handoff-20261010.md; Company Home page_6aca71a8c7d881918e6687969a6aad48
Open task: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/reciheBKjXDIg9os5

STEP 2 — Schedule: existing output → reader → Astro page
Trigger: Visitor opens current schedule page; accepted producer cadence supplies current output
Expected: Preserve the established endpoint-to-template-to-display pattern. Root source candidate: docs/schedule/data/latest or sibling ringstatus-data files → GET /schedule/data/latest/watch_schedule.json and watch_trips.json → src/schedule-app/data.js/render.js → src/pages/schedule.astro /schedule. WEC candidate: existing Airtable focus_show,class_start_times,entry_go_times,class_hide,rings → /wec-schedule/state → existing selected template (not yet identified for Publish) → designated display (unresolved).
Current: Source inspected; two different candidates preserved, no cutover selected. Root reader caches last-good data; empty source route can return HTTP200 ok:false with rows:[]; generated_at is request time. WEC default app6XS1RvsPNRT6os and environment table overrides are code references, not fresh live wiring.
Evidence/status: Source confirmed; live unverified; C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/src/pages/schedule.astro; src/schedule-app/data.js; src/pages/schedule/data/latest/watch_schedule.json.js; webflow-cloud-test/src/pages/wec-schedule/state.js. Latest source commits 5af85fe71073de9361e146fc7be0afc7a0adfbd4 and 030091ee653acf02bfae518a274f004b76985b94.
Open task: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recUrvUING4XTKUJ2

STEP 3 — Pro-schedule: scoped output → template → PRO display
Trigger: User opens entitled PRO view or requests scoped lookup
Expected: Existing schedule/trip output → selected existing read endpoint (unresolved) → reusable locked PRO row/rollup skeleton → ring/time/filter/class modal display. Messages handles lookup endpoint response; no automatic SMS from lookup/share. Recognize/Tenants provide identity/permissions separately.
Current: Historical display intent 2026-05-15.v1.0 and preview builder located. Runtime endpoint/page, entitlement, options and identity mapping unresolved; preserve skeleton rather than rebuilding.
Evidence/status: Partial; C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/daily_schedule_app_ui/SCHEDULE_DISPLAY_SCOPE.md; build_visual_identifier_preview.js; visual_identifier_contract.json
Open task: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recaB6SXJKmOT1w9n

STEP 4 — Results: engine output → reader → results display
Trigger: Accepted existing result output becomes available or visitor opens results
Expected: Existing engine result payload/stable IDs → authorized reader endpoint (unresolved) → existing/approved results Astro template (unresolved) → front-facing results display (designation pending). Processing stays with engine; no copied result masters until actual consumer justifies configuration.
Current: Owner-defined capability recorded. Exact producer, endpoint/template/page/freshness not established by bounded source inspection; no absence of all historical implementations claimed.
Evidence/status: Designation pending; C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/publish-specialist-handoff-20261010.md; Company Home current product boundary
Open task: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recRDFUv2IA462TRs

STEP 5 — Notifications: Messages feed → reader → preserved display
Trigger: Messages writes actual processing/delivery outcome; visitor opens feed
Expected: Messages normalization/lookup/outbound/logging → shared notifications-feed writer/source (unresolved) → authorized display reader (unresolved) → preserve normalizeMessage/template baseline → designated notification display. Distinguish accepted/submitted/delivered/failed states and references; exclude OTP/private verification content.
Current: Existing Prototype.tsx uses notificationSamples and browser read state; normalizeMessage formatter exists. This sample UI is a reuse baseline, not a live feed. Shared feed table/writer/endpoint still unresolved in Messages evidence. No delivery store copied to Publish.
Evidence/status: Partial; C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/messages-alerts-feed-references-20261010.md; C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus/webflow-cloud-test/src/components/rs-onboarding/Prototype.tsx; notification-records.ts
Open task: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recJPIYTMhISOrMY8

STEP 6 — RingWaze: observed context → reader → painter
Trigger: Existing accepted human observation or engine context is available
Expected: Preserve implemented WEC context → comment-state → session-widget browse/render; edit handles existing sessions/check-ins/comments/observations. For Publish painter: existing RingWaze output → selected reader (unresolved) → painter template (unresolved) → designated display. Keep observed facts, forecasts, stale state and source identity distinct.
Current: June implementation/testing is historical, register reviewed October 7. Existing /test/wec-schedule/session-widget, comment-state, edit routes are source/historical references; not freshly invoked. Portable painter and Recognize identity relationship remain unverified. Inputs remain developing.
Evidence/status: Partial; C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/horseshowing/wec-comments-observations/EXISTING-SYSTEM-REGISTER.md; webflow-cloud-test/src/pages/wec-schedule/comment-state.js; session-widget.js; edit.js
Open task: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recyc3EsmXZt98vmy

STEP 7 — Print-pdf: same data → print template → print/PDF view
Trigger: User explicitly chooses print/export in approved display
Expected: Preserve known PRO print intent: by ring, 8.5x11, two columns, phone/desktop print-page preview. Reuse pattern from packing stateReport → printReportHtml → GET /wec-packing/print text/html; specialized horse-kits print route also exists. Final Publish endpoint/template/output selection remains undecided.
Current: Print-pdf retained Expected, not a new business table. Historical packing example is not proof Publish prints. Final capability scope/acceptance requires reconciliation with documented PRO details; no owner rediscovery of known requirements.
Evidence/status: Partial; C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/webflow-cloud-test/src/pages/wec-packing/print.js; src/lib/wec-packing.js; webflow/packing-worksheet; docs/wec_packing_comprehensive_overview_2026-06-06.md; historical commit 647ea24f8ddb5ab0f8bb32e53386e30067f5b124
Open task: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recDEuKFif3fuTCsr

STEP 8 — Freshness, errors and permitted refresh
Trigger: Fetch fails, source is empty/stale, permission is missing, or refresh becomes due
Expected: Record producer as-of/version, payload validity, allowed cache/persistence and accepted refresh contract per selected display. Render honest empty/stale/error state. Runner executes approved cadence; Codex does not invoke substitute manual/direct paths or repair data.
Current: Root source/cache behavior inspected; application-wide refresh/entitlement and cadence unresolved. HTTP200/inventory/test count does not prove valid/live display. No refresh interval invented.
Evidence/status: Partial; C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/src/schedule-app/data.js; src/pages/schedule/data/latest/watch_schedule.json.js; AGENTS.md; daily_schedule_app_ui/SCHEDULE_DISPLAY_SCOPE.md
Open task: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/reciheBKjXDIg9os5

STEP 9 — Verify application and leave stable handoff
Trigger: Separately authorized implementation reaches acceptance; each work session ends
Expected: Verify chosen endpoint/template/display end to end and baseline preservation, update existing tasks/evidence/changes/activity/START HERE; cross-base activity linked to Publish and Agents. Every required human step names accountable person/action/input/evidence/resume condition. Stabilize foundational systems before new engine work.
Current: Base setup verification is current assignment; application/live acceptance is separately assigned. No human handoff owner invented, no publication/deployment/pipeline run.
Evidence/status: Designation pending; C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/new-airtable-base-guide.md; context-check-procedure.md
Open task: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recvxgJbw2K4omDtb

START/STOP RULE: read START HERE, workflow and latest activity; take existing scoped task only with appropriate authorization; maintain sources/versions and responsibility. No new engine work before Website/Recognize/Tenants/Messages/Publish are established and stable. Planned rows do not launch workers. If approved runner path fails stop that execution path, retain exact failure, continue independent authorized work; no substitute runs/repair. Use smallest established owner-authorized human handoff if needed, with person/action/input/evidence/resume condition; none has been invented here. No application code or automatic activation under base-setup assignment.

No business configuration tables were justified by an inspected consumer of this Publish base. Existing pack blueprint/config/domain tables belong to their existing system, not boilerplate to copy. Nine physical operating/index tables listed in publish-index, all cross-referenced in Agents lanes; six capabilities remain separate Expected registry entries. Original Table 1 and three blank record IDs retained through index reuse; original optional fields left as unused provenance. Fails remains empty because no runtime failure test was attempted, not because the application is healthy.

## Direct directory

Current full overview: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/publish-workflow-overview-20261010.md
Stable resume checkpoint: https://airtable.com/appZY1hcqGvUvxIuV/tblTFXEyvmO0JTKfN/rec1sXjVK2pmzvxeC
How to use this base: https://airtable.com/appZY1hcqGvUvxIuV/tblTFXEyvmO0JTKfN/recsowb4icjb9brGg
Workflow: https://airtable.com/appZY1hcqGvUvxIuV/tblDMIp9RjKg1mw9V
Tasks: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd
Development: https://airtable.com/appZY1hcqGvUvxIuV/tblkGrX9nvTMmKN5l
Latest actual session: https://airtable.com/appZY1hcqGvUvxIuV/tblgVStAJRZ9oJ6E9/recbILbgz9r9IBMAX
Index: https://airtable.com/appZY1hcqGvUvxIuV/tblAzdr8GmPG1gUaW
Base setup assignment: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/publish-specialist-handoff-20261010.md
Reusable guide: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/new-airtable-base-guide.md
Rules: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/AGENTS.md
Context procedure: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/context-check-procedure.md
Company Home: https://chatgpt.com/space/page_6aca71a8c7d881918e6687969a6aad48
Central assignment: https://airtable.com/appZahVgD156cMAe3/tbluT1yhP1FJMb8Cm/recCnFdl0Ap6TEdmd
Project preserved: https://airtable.com/appZahVgD156cMAe3/tblKjqNiCiqVFSdDL/recmjScmqrxpGjj22
Historical packing proof: commit 647ea24f8ddb5ab0f8bb32e53386e30067f5b124; C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/wec_packing_comprehensive_overview_2026-06-06.md
Historical PRO design intent: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/daily_schedule_app_ui/SCHEDULE_DISPLAY_SCOPE.md version 2026-05-15.v1.0; current owner purpose/handoff controls system ownership, applicability unresolved, no live claim.
Historical RingWaze implementation: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/horseshowing/wec-comments-observations/EXISTING-SYSTEM-REGISTER.md (June tests, October review). Preserve existing sources and reported gaps.
Local paths do not transfer access. Essential overview/workflow also stored fully in Airtable.

## Physical tables

publish-index | tblAzdr8GmPG1gUaW | https://airtable.com/appZY1hcqGvUvxIuV/tblAzdr8GmPG1gUaW | Physical table directory only; no runtime configuration. Original Table 1 ID and three blank records retained; original Notes/Assignee/Status/Attachments/Attachment Summary fields are unused placeholder provenance. Name, Table ID, Purpose, Table link are the directory fields.
system | tblTFXEyvmO0JTKfN | https://airtable.com/appZY1hcqGvUvxIuV/tblTFXEyvmO0JTKfN | Scope, START HERE/resume checkpoint, health, full workflow overview, sources/connections/skills and how-to reference. Operating work links only to Publish record.
tasks | tblnLPLYGLBHm23Sd | https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd | Executable scoped work, acceptance, evidence and remaining dependency; status differentiates setup completion from application work.
agents | tblVtRxqbyY5fhAUH | https://airtable.com/appZY1hcqGvUvxIuV/tblVtRxqbyY5fhAUH | Actual chat responsibility and tasks; no worker/monitor starts from these records. Future application owner remains unassigned.
fails | tblQdbva3cTD0obEO | https://airtable.com/appZY1hcqGvUvxIuV/tblQdbva3cTD0obEO | Observed problems/corrections only. Intentionally empty: no Publish runtime verification performed; not a zero-defects claim.
activity | tblgVStAJRZ9oJ6E9 | https://airtable.com/appZY1hcqGvUvxIuV/tblgVStAJRZ9oJ6E9 | One note per actual setup session, completed evidence/last stop/next action with task/agent links.
development | tblkGrX9nvTMmKN5l | https://airtable.com/appZY1hcqGvUvxIuV/tblkGrX9nvTMmKN5l | Proven reuse references and unresolved capability/interface proposals; no proposal is runtime behavior.
workflow | tblDMIp9RjKg1mw9V | https://airtable.com/appZY1hcqGvUvxIuV/tblDMIp9RjKg1mw9V | Ordered endpoint → template → display expectations for six capabilities, identity/source/freshness and acceptance, with source/current gaps explicit.
changes | tblKHgVpMR9PX7Egb | https://airtable.com/appZY1hcqGvUvxIuV/tblKHgVpMR9PX7Egb | Meaningful applied setup changes and readback evidence; no live/deployed claim.

## Connections and downstream contracts

CONFIRMED current connector: Publish appZY1hcqGvUvxIuV; Agents appZahVgD156cMAe3; selective Recognize app9kOZdIaGyKk5uG and Tenants app7BXcw801n19QKF schema/system reads succeeded 2026-10-10. Publish schema/record writes succeeded; final readback recorded in task/activity. Access is not runtime acceptance.
Company Home read: https://chatgpt.com/space/page_6aca71a8c7d881918e6687969a6aad48 — responsibility reference, not system stability.
Recognize identity/session checkpoint: https://airtable.com/app9kOZdIaGyKk5uG/tblTlw48kcRoEX6yf/recB69Oe39xZ0nsAe — reference IDs/contracts only; gate/page mapping and deployed version not verified by this setup.
Tenants membership/input/permissions/options checkpoint: https://airtable.com/app7BXcw801n19QKF/tblMKWJw8Q6joKc8U/recJzisHwiPPkKG6y — no copied people/horse/tenant masters. Cross-base entitlement unresolved: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recaB6SXJKmOT1w9n, https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/reciheBKjXDIg9os5.
Messages checkpoint supplied/documented: https://airtable.com/apphwDLkVCp8ut2Hd/tblycxFymN5gaPKlT/recjGDLwQ5U0VliiE; owns incoming/outgoing SMS, logs, lookup response and feed updates. Existing sample formatter/display inspected in sibling source; no fresh Messages base read or feed wiring claimed. Shared feed reader/table/writer and outcome mapping unresolved: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recJPIYTMhISOrMY8.
RingWaze source register inspected: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/horseshowing/wec-comments-observations/EXISTING-SYSTEM-REGISTER.md. WEC original app6XS1RvsPNRT6os, historical interaction routes /test/wec-schedule/session-widget, /test/wec-schedule/comment-state, /test/wec-schedule/edit. Historical retained RingWaze copy appPBk3FRrATwpHwd is documented only, not active destination proof. Painter/portable/identity mapping unresolved: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recyc3EsmXZt98vmy.
Webflow site supplied approved ID 6982268b7543ac3c80151266, production MCP named webflow only. No Webflow call/access check/publish here. Native/static/CMS presentation remains Webflow-owned; no Astro implementation ownership inferred.
Existing Astro/Webflow Cloud source project_id d7d97751-20e1-4148-a5cf-ee58671c128a read in C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/webflow-cloud-test/webflow.json. Environment 110f06dd-c1ea-4839-98af-d829cbe77941 and mount /test are historical documented metadata from shared contract; deployed commit 18e6e993aeaff6fd53168dac4d6f1b7b46baf195 is historical, not freshly verified. Current source mapping/host/mount/deployed version unresolved: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recUrvUING4XTKUJ2.
Root Astro source /schedule reads file-backed /schedule/data/latest/watch_schedule.json and watch_trips.json; sibling ringstatus-data candidate paths are code fallbacks, not verified separate checkout access.
WEC source /wec-schedule/state defaults app6XS1RvsPNRT6os and name-based focus_show/class_start_times/entry_go_times/class_hide/rings with documented environment overrides; it does not point to Publish base. Preserve readers/writers; physical table IDs/current consumers not looked up here.
Packing source uses AIRTABLE_TOKEN and AIRTABLE_BASE_ID/AIRTABLE_BASE plus metadata registry AIRTABLE_WEC_META_TABLE; runtime values not read/stored. GET stateReport and HTML printReportHtml patterns inspected. Not reconfigured to Publish.
publisher.js owns existing runner publish_queue and exact configured paths, with PUBLISH_URI/PUBLISHED_BASE source defaults ringstatus-proxy.gombcg.workers.dev. No runner invoked, queue copied or endpoint exercised; default URLs do not prove active connection.
No installed connector implies destination access. Cloudflare/Twilio not needed for setup and not queried. Credentials: names/references only, never values.

## Skills and access

READ for this assignment:
C:/Users/gombc/.agents/skills/request-router/SKILL.md — target/surface routing, no mode/model switch.
C:/Users/gombc/.agents/skills/instruction-loader/SKILL.md — applicable instructions/startup.
C:/Users/gombc/.agents/skills/boundaries/SKILL.md — allowed base/registry writes; protected source.
C:/Users/gombc/.agents/skills/planning/SKILL.md — proven-source reuse and smallest delta.
C:/Users/gombc/.codex/skills/rs-coordinator/SKILL.md and references/skill-routing.md — existing task/thread records; no messaging or dispatch authorization.
C:/Users/gombc/.agents/skills/handoff/SKILL.md — self-contained checkpoint.
C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/airtable/7.0.0/skills/airtable-overview/SKILL.md — schema, typed fields, records and links.
C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/superpowers/6.4.2/skills/verification-before-completion/SKILL.md — fresh full readback before setup completion.
C:/Users/gombc/.agents/skills/webflow-cli-cloud/SKILL.md — conditional source/hosting mapping only, not initialization/deploy approval.
Conditional next work: C:/Users/gombc/.agents/skills/webflow-mcp-custom-code-management/SKILL.md — load before actual native binding inspection; not loaded or invoked for this base setup.
READ for final linked-record lane reconciliation: C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/airtable/7.0.0/skills/airtable-filters/SKILL.md; base-lanes filtered by Publish base record ID, with full result/readback. Recognize/Tenants examples inspected selectively as content references; their domain skills not activated to perform domain work. No standalone Astro or Publish skill claimed installed. Later code/review skills require separate assignment. Model/effort unknown; recommendation not a selected setting.

## Remaining work and first next action

All eight application tasks are Planned and unassigned. First next task: [Confirm existing schedule delivery binding](https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recUrvUING4XTKUJ2). Read the existing Cloud deployment metadata and trace its endpoint/template/page without deployment or pipeline execution. Acceptance: exact current source version, host/mount/page/endpoint, table/key mapping, reader/writer and accepted cadence recorded; live acceptance remains separately assigned.

Read [tasks](https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd) for all open work and [development](https://airtable.com/appZY1hcqGvUvxIuV/tblkGrX9nvTMmKN5l) for unresolved decisions. This overview records base setup and source references, not completed application behavior.

## Verified setup closeout

Authorized base setup complete. Fresh connector readback checked every physical table and all records, guide field types/status choices, reciprocal link pairs, exact index table IDs/URLs and retained three placeholder IDs. Final task and session readbacks confirmed two Done tasks and eight unassigned Planned application tasks. Agents preserves base/project IDs, the actual chat ID, verified Resume here, nine Locked physical table references and six Expected capabilities. No application source files or pre-existing AGENTS.md edits were changed.

Latest local activity: https://airtable.com/appZY1hcqGvUvxIuV/tblgVStAJRZ9oJ6E9/recbILbgz9r9IBMAX
Central cross-base activity: https://airtable.com/appZahVgD156cMAe3/tblITeJfxlIhlDvGk/recZyjTW9F00iR2be
Meaningful changes: https://airtable.com/appZY1hcqGvUvxIuV/tblKHgVpMR9PX7Egb/recsa7zkKCHiysIjN
Setup acceptance task: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recuQAC9ezBGYfdBS
Small completed source-map task: https://airtable.com/appZY1hcqGvUvxIuV/tblnLPLYGLBHm23Sd/recUgEzAqaxRGYcN2

Health remains Needs work for application operation. Last stop is completed setup/source mapping and final record/registry reconciliation. Exact next action and acceptance are retained in START HERE and the first existing schedule-binding task; no owner handoff or application assignment has been fabricated. This checkpoint is a compact resume packet; local path/chat links do not transfer access.

## Owner page, loader and token clarification

OWNER CLARIFICATION + CURRENT READ-ONLY SOURCE MAP — 2026-10-10T20:40:52Z
Owner supplied: rs-barn-onboarding-schedule-v24 is PRO-SCHEDULE and intended to populate from an endpoint; print2 is working print-proxy/print-to-PDF; lite/master/cwf are existing legacy examples refreshing/loading latest files. rs-barn-onboarding-feed and rs-barn-onboarding-alerts are LIKELY Messages-related, tentative page ownership; preserve feed content/events and SMS opt-in versus outbound-delivery distinction.
CONFIRMED Webflow page identities on site 6982268b7543ac3c80151266 (production MCP guide then complete paginated page metadata; no publish):
rs-barn-onboarding-schedule-v24 | 6ac7d2c7cf113c577ef2a6d7 | https://ringstatus.com/rs-barn-onboarding-schedule-v24 | current saved metadata: draft=false, lastUpdated=2026-10-09T02:36:43.664Z
rs-barn-onboarding-feed | 6ac459b4ad3971b205a5ee11 | https://ringstatus.com/rs-barn-onboarding-feed | current saved metadata: draft=false, lastUpdated=2026-10-09T02:34:54.523Z
rs-barn-onboarding-alerts | 6ac459b4506821bd349ed047 | https://ringstatus.com/rs-barn-onboarding-alerts | current saved metadata: draft=false, lastUpdated=2026-10-09T16:36:32.719Z
print2 | 6a91ee1730a21f93cc64cf77 | https://ringstatus.com/print2 | current saved metadata: draft=false, lastUpdated=2026-09-28T02:07:22.253Z
master | 69d951709ba656b0cdca5fbe | https://ringstatus.com/master | current saved metadata: draft=false, lastUpdated=2026-09-28T02:07:19.960Z
lite | 69c2f0da8fda56535e117292 | https://ringstatus.com/lite | current saved metadata: draft=false, lastUpdated=2026-09-28T02:07:18.550Z
cwf | 69bf51a058819121d14281ba | https://ringstatus.com/cwf | current saved metadata: draft=false, lastUpdated=2026-09-28T02:07:18.539Z
Metadata/saved-code verification is not live refresh, endpoint wiring or PDF-generation proof. Prior handoff draft states are historical; current saved metadata above supersedes them.
SAVED SOURCE: lite page embed 833ffd89-b064-f02d-305d-cb632abb2a22; master same element GUID in its own page scope; cwf embeds 9d73ed0a-c7f1-bced-48c2-d11ddaac3a62,224dcb39-9b28-f0b6-e452-b3f40341366c,d1b94aeb-beb7-e937-a311-0fb070405742. Read all saved code settings. lite/cwf resolve https://ringstatus-proxy.gombcg.workers.dev/docs/9999/schedules/schedule.json; master resolves https://ringstatus-proxy.gombcg.workers.dev/docs/schedules/master.json. Existing POLL_MS=6*60*1000; initialLoad followed by pollForUpdates, fetch cache:no-store, compare Last-Modified and response body; redraw only when changed, restore scrollY, keep existing UI on refresh failure. This is observed source cadence for these legacy examples, not newly chosen PRO cadence or freshly observed live refresh.
print2 embeds cf46fdf2-15f3-fcb1-53da-b0f73c04d80b,82aafbb4-0b74-0fa4-7273-33b0c152146e,74ae5587-5e89-034e-3ad7-91513c4289a8. Saved source renderTenant fetches same /docs/9999/schedules/schedule.json; handles embedded wef-schedule-data, uses synchronous XHR on ringstatus.com/pdf=1 otherwise fetch cache:no-store. TRIPS_URL is defined for active tenant but definition alone is not a fetched trips consumer. Letter two-column layout and fitToOnePage preserved. PDF link uses https://ringstatus-pdf.gombcg.workers.dev/?url=<current-page-url>&filename=<built-file-name>. Boot/tenant selection calls rendering; inspected embed has no setInterval refresh. No PDF request/share/print/browser execution performed. Owner's working-proof statement remains attributed to owner.
Proxy source C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/lib/cloudflare/ringstatus-proxy/worker.js: /docs/* -> env.UPSTREAM_BASE/docs/<relative-path>, data JSON bypasses cache per inspected source. Actual deployed UPSTREAM_BASE, writer/cadence/version/file freshness unresolved; no default example URL treated as current runtime configuration. Preserve established files/loaders/publisher; do not run endpoints/pipelines.
PRO native page HtmlEmbed query found zero page-level embeds; that result does not establish absence of all site/page scripts, components or bindings. Intended endpoint/data/native-hook mapping remains existing task recUrvUING4XTKUJ2/recaB6SXJKmOT1w9n; design preserved.
TOKENS: CONFIRMED current schema and 42 records read with no cursor at https://airtable.com/appara5767XpIaJIf/tblVcIhTfG1pNsCyi (Webflow base appara5767XpIaJIf; rs-token-lib). Owner reference ProApp page6a63f491030cc3634eb83d92, /audit/proapp confirmed by page metadata read; existing docs/coordination/schedule-v24-new-draft.md:17 and decisions.md are dynamic Schedule x ProApp evidence, not design-change authority. Historical Agents library IDs are superseded for current mapping.
Existing semantic reference examples, not implemented PRO assignments:
status2=Underway is an observed legacy source value; library recR7fOzw9lHGvJLm meaning Current or underway, aliases Now/Underway/livenow -> prosk-state-now -> background-css rgba(73,209,125,0.16).
status2=Upcoming observed legacy value; library reckxwdooKbnQlwRH Upcoming item, aliases Not Started/Upcoming -> prosk-state-upcoming -> rgba(154,163,180,0.12).
status2=Completed observed legacy value; library rec8yuT0jrq6hlHWl Completed item, aliases Completed/Done -> prosk-state-done -> rgba(154,163,180,0.08).
Library recI7kAu5PMOSN1VA Immediate next item/Next -> prosk-state-next -> transparent; recu12iM0xJmMFSQy Following item/Following -> prosk-state-follow -> rgba(143,184,255,0.07). Exact selected PRO endpoint values/field assignments not yet traced.
Library recMU67QzfQ5mLyvW Over Fences -> prosk-token-teal -> rgba(88,218,199,0.16); recHrsLxBBj1DGgNK Under Saddle/Flat -> prosk-token-violet -> rgba(156,89,255,0.18). Class/sequence fields must be traced in selected actual endpoint before assigning them. Preserve text/border colors and exact background-css alpha, not only hex.
Each library row's verification-scope describes prior stored native main/base CSS/source verification and no rendered-browser claim. This read verifies records, not re-verification of CSS rendering. Some meanings are blank: do not infer canonical semantics from sample labels.
UNRESOLVED: actual selected PRO endpoint fields/value-to-record-to-native-class/style application and unmatched-state fallback. Existing task recaB6SXJKmOT1w9n now carries this mapping acceptance; no arbitrary/default color or assignment chosen. Source v24/native design remains visual authority; no recoloring, new token system or design replacement.
Only remaining first action: trace current PRO page's existing scripts/components/native binding hooks and selected endpoint to these established sources and semantics. Preserve established loader pattern; exact compatibility delta and actual deployed source/writer/as-of remain unresolved. Existing eight tasks retained; no duplicate task or business table.


