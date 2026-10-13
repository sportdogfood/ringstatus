# Tenants specialist — start here

Owner-requested domain skill: read `C:/Users/gombc/.codex/skills/tenants/SKILL.md` for Tenants work and include its path in subordinate handoffs. Current status comes from the live checkpoint, not dated progress paragraphs below. Scheduled monitoring: `docs/coordination/specialist-skill-supervision-20261010.md`.

## Clarification: existing decisions are not new approval questions

The owner-approved `shared-data-behavior-contract.md` already specifies one shared IndexedDB cache/pending-operation layer, exact-operation confirmation, preservation of newer edits and reuse of proven implementations. Do not request approval of that same design again through an optionally selected brainstorming skill. Existing authorization and protected-source boundaries still apply to implementation.

The owner agreed that Astro input tables are logs and Airtable automation applies them to designated records. The workflow overview lines 21–22 and 38 document this intended processor. Its absence is implementation work, not a reason to ask the owner to select a processor again. Prepare the smallest compatible automation and confirmation path within existing authorization; activation and live access grants retain their existing boundaries.

No verified live test person/tenant membership has been established by these decisions. Recover any previously authorized test association from evidence; never guess one or grant access. Use synthetic fixtures for independent implementation and tests while that live-proof dependency remains. Ask only for the concrete person/barn association if it cannot be recovered and is actually needed for the next live step. Do not require the owner to choose technical record IDs or design the implementation. Sensitive membership/consent changes remain outside a basic personal-preferences proof; do not invent policy or let future policy questions block that proof.

Update corresponding task/development records to distinguish already-decided behavior from missing implementation and live-test dependencies. Continue independent authorized work rather than ending at these repeated design questions.

You are the Tenants specialist coordinator for existing Barn Inputs, onboarding/profile and opt-in workflows. Read docs/coordination/specialist-shared-contract-20261010.md, docs/coordination/tenants-schema-completion-20261010.md and docs/coordination/tenants-workflow-overview-20261010.md. Preserve established business rules and existing working code; do not start a new CRM/app implementation.

## Base and current state

Tenants (formerly Entities) app7BXcw801n19QKF; checkpoint https://airtable.com/app7BXcw801n19QKF/tblMKWJw8Q6joKc8U/recJzisHwiPPkKG6y; tasks tblXxPquz9jtdQM3n; index tbl5kWiI8OAfWdzRp; rules tbldkZBRCOMlbYydg. All 39 physical/index names verified matching; no proof of completed runtime cutover.

Business tables: tenants, tenant-level, people, tenant-memberships, roles, trainers, horses, riders, rings, locations, tenant-locations, tenant-trainers, tenant-roster, tenant-assignments, favorites, optins, optin-options, input-submissions, events, sources, onboarding-stages. Read stable IDs through the existing index and schema only for the current task. ___ tables retained as legacy; never delete or repeat migration from their presence alone.

People consolidation and structural relationships completed. /root/tenants_schema_completion finished task recW9Cy6E6JO4eh9e; exact schema evidence is linked above. New trainer/roster/assignment relationship tables were empty by design. No permission grants or runtime enforcement were made. Historical barns/riders add/edit/save/reload and opt-in save/reload/opt-out worked against older destinations, not proof of this base's end-to-end loop.

## Business rules to preserve

Tenant = barn/container. People consolidates owners, staff, contacts, grooms and exercisers; rider and horse identities remain distinct/shared. Staff may also be a rider. Groom access is assigned horses only and owner-managed. Membership is independent of appearing this week/today.

Primary trainer supplies horses and riders; additional/unaffiliated trainers supply horses only. Owner can disable tenant-local horses/riders without disabling globally. Enabled riders can maintain personal favorites separate from tenant roster. Assignments/favorites never create membership. Opt-ins include individual scope/options and consent; owner management never fabricates consent. tenant-level describes benefits, not a second profile master.

Locations are venue main/stalls/turnout/rings and related service places, with tenant/show association; live positions/check-ins/walk times belong to future location/RingWaze work. Trainers/horses/riders/rings are shared across Pack/show engines, not duplicated per app. Show-week/day subsets filter participation, not permitted season membership. Red-line missing horse/rider and whole-list audit forms belong in Barn Inputs and remain future development, not required reinvention of basic editing.

## Code, pages and proven references

Use the existing deployed-source root/Cloud IDs in shared contract. Actual current source files: src/pages/rs-inputs/native-barn.js.ts, native-alerts.js.ts, [operation].js, subscriptions.js; src/lib/rs-inputs.js, rs-inputs-store.js, rs-inputs-airtable.js, rs-inputs-access.js, rs-inputs-recognition.js. rs-inputs-crm.js and rs-inputs-zoho-auth.js exist historically; Zoho is deferred and must not be reactivated. Inspect actual imports before assuming a file runs.

UI: /rs-barn-onboarding-setup-v24 and /rs-barn-onboarding-alerts first. /rs-barn-onboarding-feed and /rs-barn-onboarding-schedule-v24 remain downstream work. Preserve native Webflow divs/styling; Astro supplies data/actions. Do not inject an Astro-styled replacement. Existing WEC Packing proof 647ea24f8 has reusable inline add/edit/save behavior, but its hosted template presentation is not the native binding target. Read relevant existing implementation before choosing a delta.

Recognize owns identity in app9kOZdIaGyKk5uG. Messages apphwDLkVCp8ut2Hd is historically recorded; verify its current registry before delivery wiring. Existing legacy Inputs still use Recognize tables by stable ID; do not assume Tenants cutover or remove that path.

## Exact next work

1. rec2YJdtU25OHZSDn: read-only verify actual setup/alerts page loader -> route -> current deployed commit -> configured base/table/fields. Reuse shared baseline and discover only missing Tenants mappings. Save rollback source and exact minimal gap. This can proceed independently of Recognize implementation.
2. rectHlHXOH2YvPAMk: remaining launch-relevant identity ambiguity only; do not repeat completed imports.
3. recjNCFtq6dJB7sk3: reuse established add/edit/save implementation and map intended Astro input-submissions -> designated records/result. Validate same tenant, authorized actor/role, one target, primary-only rider inclusion, assignments and consent. Stable operation IDs prevent duplicate application. Choose minimal existing processing mechanism; human bridge only if explicitly agreed. Logging intake is not confirmed save.
4. recStkwN8e7t6IJUv: persistent optimistic/dirty UI using proven cache/retry patterns. Immediate display, persisted pending edits, clear dirty only on confirmed target outcome. Preserve later edits during earlier saves, failures and conflicts; clean data may refresh. Server remains authority. Exact 60-second cadence/TTL are proposed, not locked.
5. recwzt7S1umtaMyvj: prove load/add/inline edit/save/reload, pending reload/offline reconnect, duplicate retry/lost acknowledgement, second-device conflict, failed writes, account switch/expiry, cross-tenant isolation and opt-in restrictions. Start with one agreed entity flow; do not call the whole system complete from barns/riders alone.
6. rec8bsbiHuxG0hDMV: downstream references; future engines must not block basic Inputs.

Use one narrow mapping/source-review worker if useful, then one authorized code writer and independent read-only tester. Do not launch three writers for onboarding/inputs/opt-ins that share the same adapter. Load Cloud/Airtable skills for mapping, native custom-code skill for bindings, frontend testing and verification skills for acceptance. Read equestrian-caption-app/SHELL_CONTRACT.md before applicable UI-shell changes. No Twilio/Cloudflare infrastructure skill is needed for ordinary tenant editing.

## Sibling awareness and closeout

Recognize specialist packet: docs/coordination/recognize-specialist-handoff-20261010.md. Consume its verified identity/session contract; do not modify its cookie/OTP/session implementation. Shared access adapters and Cloud deployments require recorded single-writer ownership under shared contract. Keep membership authorization on Tenants side and never infer access from recognition or a show entry. Actual sibling chat ID pending dispatch.

Maintain domain system health/tasks/activity plus linked Agents worker/handoff records. Read live rules/development to separate agreed behavior from proposals. Complete the assigned task with exact evidence and leave one next action; update worker state when it stops. Naming/schema is verified; complete Tenants runtime remains open.
