# Messages specialist — base retrofit and connection map

**Current ownership correction — 14 October 2026:** Messages owns `/rs-barn-onboarding-alerts` and its input/save/retry acceptance. Recognize provides identity; Tenants retains canonical opt-ins, options and permission/data processing. Read [the exact three-area ownership and alerts handoff](recognize-tenants-messages-ownership-20261014.md), including existing source, failed checks and release boundaries. This supersedes earlier Tenants page ownership; product work is not restarted by this documentation change.


Current entry (14 October 2026): use this domain's resume row in [the authoritative-source directory](domain-coordinator-structure-20261013.md). This dated document preserves workflow/source evidence; original setup commands and old dispatch/monitor instructions are historical, not a new assignment. Follow the current owner request and verified checkpoint. Guidance-only updates leave stopped product work stopped.

Current shared structure (13 October): read the relevant sections of [Company, domains and coordinator capabilities](domain-coordinator-structure-20261013.md). It defines responsibility, sibling interfaces, required skill/connection mapping, proven frontend-agent reference and completion/handoff duties. Capabilities are not automatic agent assignments. Current tasks and authorization control; stopped work stays stopped.

## Clarification: finish the retrofit without requiring future runtime decisions

The owner cannot answer technical questions about recipient precedence, reply implementation or engine isolation. Those are not prerequisites for this assignment's base retrofit. Record unresolved future behavior in development, distinguish it from existing proven behavior, and finish the operating records, index, registry readback and usable resume checkpoint. Do not expand this assignment into implementing messaging or claim unknown policies were approved.

Recipient ownership is already established: Tenants owns membership, assignments, favorites, consent and individual options; Messages consumes them, Recognize owns identity. Preserve those boundaries. Map existing supported behavior and document genuine conflicts; do not ask the owner to redesign this contract. For an unresolved business choice, prepare a plain-language recommendation grounded in existing code and rules, with its practical consequence, for later review. Do not silently enforce the recommendation.

Engine isolation is explicitly deferred and code remains reference-only. Missing Twilio Console evidence belongs in the connection register as unverified with the exact smallest human read-only action, if necessary; it does not prevent completing the base retrofit. No browser retry loop or request for secrets. Support ownership and new reply policies may remain open development items without preventing this scoped deliverable.

Before stopping, verify all five retrofit deliverables and their readbacks. If all are met, close the retrofit assignment with evidence and retain the separate future implementation tasks. If any executable retrofit work remains, continue it. Do not present deferred runtime decisions as blockers to already-authorized base work.

You are the Messages specialist coordinator. Retrofit the existing Messages base into the agreed usable operating model; preserve working implementations and document the actual connections. Code is reference-only in this assignment. How messaging will be isolated from the local WEF show-engine is NOT decided. Do not split, move, rewrite, deploy or activate engine/Worker code to fill that gap.

Read AGENTS.md, docs/coordination/specialist-shared-contract-20261010.md and docs/coordination/new-airtable-base-guide.md. Then read only relevant sources below. The Recognize model supplies operating structure, not a claim of complete runtime or a business schema to clone.

## Verified starting point — 10 October

Messages base https://airtable.com/apphwDLkVCp8ut2Hd; Agents bases row recc04e4cPGXJOTRo in appZahVgD156cMAe3/tbltaHwEyBfcuOHIm.
Live schema has only TWO tables:
- rs_sms_requests tblQWqhaSVXdDxQF0: request_uid, person_uid, primary_phone_e164, status, record_mode, requested_at, provider_message_sid, error_code and event backlink.
- rs_sms_events tblr7sWHgoYDgj7uV: event_uid, occurred_at, event_type, provider_message_sid, source, error_code, request link.

The Agents row has THREE linked base-lanes, including two named rs_sms_events. Reconcile by stable physical table ID; do not assume a third table or delete a table because the registry is duplicated. Messages currently lacks the operating tables. Neither these tables' contents nor their live writers were verified in this preparation. Start with read-only inventory and dependency identification before changing existing names/fields. Final naming rule: one word where practical, otherwise lowercase-hyphens; ___ for confirmed legacy/replaced tables. Never delete data. Keep dependent naming changes pending until consumers are accounted for.

## Retrofit deliverable and order

1. Read actual tables, minimum relevant records, Agents base/lanes/connectors/skills and existing code/automation references. Establish purpose, readers/writers, current use and last evidence for each table. No real SMS or workflow execution during inventory.
2. Reuse/create the agreed operating tables: system, tasks, agents, fails, activity, development, workflow, changes, plus messages-index. Include only necessary date, scope/purpose, message, status, ownership, evidence and links following the base guide. Do not create a separate connections/skills/task-management subsystem: system holds verified connection and applicable skill references.
3. Populate Start here/Resume here with overview, actual health, latest completed work, one exact next task, all open tasks/development and downstream dependencies. Attach overview/workflow/code reference documents. Put undecided engine isolation and reply policies in development; put concrete discovery/retrofit actions in tasks. No empty boilerplate or claims that tables alone implement messaging.
4. Reconcile index to every physical table by ID, purpose, category (global/data/runtime/deprecated), status and direct link. Reconcile Agents base-lanes and Resume here; record changes/activity linked to Messages. Read back both base and registry before declaring retrofit complete.
5. Deliver a concise connection/workflow map and remaining implementation plan. Keep code/runtime changes outside this assignment until the owner decides the isolation boundary. Do not let that decision block the base retrofit.

## Responsibilities and input/output model

Messages represents outbound requests, alerts, incoming responses, delivery/processing history and notification-feed. Here feed means notifications, not the broader show data feed. Reuse existing two tables where sufficient; add business tables only for demonstrated missing needs, with purpose and minimum fields established first.

WEF local show-engine supplies show/week/day/ring/class/entry/live/results context and trigger evidence. Its existing runner remains the producer; Messages does not recreate schedules or calculate a parallel show-engine. Record source event IDs, timestamps/freshness and candidate endpoint boundaries as references. Hosting, polling/push ownership and separation remain undecided.

Tenants owns membership, tenant roles, assignments, follows/favorites, consent and individual preferences. Messages consumes these to determine eligible recipients and permitted scope; it must not grant membership, invent a consent grant, infer consent from an inbound text or silently maintain an independent preference master. Recognize owns identity/browser verification and OTP; its working Airtable-native SMS flow remains separate and must not be moved just to consolidate this base.

## Tenant options and precedence to map explicitly

Tenant base app7BXcw801n19QKF: optins tblCX61u6pjSLrP5S, optin-options tblFHcfIFjGuoE2hp, tenant-level tbleGDeOz4XDIMAGd; read rules and relevant memberships/assignments/favorites.

Inventory actual supported options: recipient/phone, consent/revocation, engine_scope, target_type/target_uid, alert_types, Notification scope, Selected favorites, Membership, Managed by, preferences_json and each optin-options Choice/Value. Preserve all 18 existing UI preference keys, numeric lead times, clock times and IANA timezone from the proven native form. Do not discard values because the current request/event tables lack fields.

Record which choices act at tenant, role/assignment, individual, selected horse/rider/trainer, show/week/day or alert level based on actual rules—not guessed inheritance. Tenant-level benefits (Ad-free, Pro Schedule, Website logo) are not SMS consent. Owner-managed groom preferences must resolve an authorized same-tenant manager and recipient assignment; staff/rider overlap must not duplicate sends. Revocation/suppression prevents notification delivery. Unknown or conflicting option precedence becomes a concrete development decision, not an invented default.

Current historical form proof: docs/coordination/sms-optin-connection-20261009.md. It saved preferences and opt-out to Recognize legacy subscriptions tblbpALv7flu3NNEE, not a verified Tenants production cutover; no SMS delivery was activated. Identify which source currently wins before implementing a consumer.

## Two-way SMS and recognizing an incoming text

Trace the existing inbound path before proposing code: Twilio incoming message -> configured webhook/Cloudflare Worker -> sender/context lookup -> intent/data lookup -> response -> request/event history. Confirm actual endpoint, method, deployment/source revision, validation, secrets by NAME only, linked Airtable automation and error behavior. Never expose credential values or capability webhook URLs.

Reference source exists at lib/cloudflare/ringstatus-sms/worker.js, worker-live.js, worker_rollback.js, worker-err.js and openapi.html. These are alternatives/history, not proof of which is deployed. Inspected worker.js reads From, Body and MessageSid and has ring/rider lookup functions (decideRingStrict, parseLookupIntent, tryRiderLane) plus an Airtable webhook call. This is existing domain logic to inspect and preserve where proven, not authorization to repurpose it for OTP. Current Worker URL, active version and bindings remain unverified.

Map phone normalization and sender lookup to known person/aliases, then tenant memberships/allowed scope. A text sender has no browser cookie; distinguish inbound sender association from browser authentication. Ambiguous/shared/reassigned phones, multiple tenants, unknown sender and unrecognized text require an explicit response/human fallback, not guessed identity or fabricated show answers. Use existing deterministic commands/lookup behavior first; no LLM intent service is prescribed.

Inventory STOP/START/HELP/provider opt-out handling before any application reply. Record duplicate MessageSid handling, webhook authenticity, replay/retry behavior, response deadlines and delivery callback correlation. Separate accepted/queued/sent/delivered/failed/unknown using real provider evidence; provider success is not handset receipt. Do not send a second reply on repeated webhook delivery. Define unsupported-request/support handoff with actor, action, recorded result and resume condition; do not invent a human owner.

## Connections to establish, without activating

- Twilio: exact account/subaccount identifier, sender or Messaging Service, incoming webhook and status callback, native Airtable connection, provider suppression behavior. Record nonsecret identifiers only; verify through existing authorized tools.
- Cloudflare: actual Worker name/URL/version, route/custom domain, environment and binding names, upstream endpoints and source relationship. Reuse existing Cloudflare connection; no provisioning/deployment.
- Airtable automations: source base, automation ID/name, trigger, conditions, actions, callback, deployment/enabled state and latest available evidence. Table existence does not prove automation. Recognize automation wfl6oiGMgUPmEkUuS (sms-recovery) is a separate known flow; its callbacks use ringstatus.webflow.io/test/rs-inputs/native-recognition. Do not copy or move it.
- WEF local engine: documented runner/workflow, code paths, current outbound event/endpoint references and scheduler ownership. Read-only inventory, no alternate/manual triggering.
- Astro/Webflow: native opt-in form and existing subscriptions routes are consumer references; preserve the current verified UI path. Messages handoff does not authorize changes to the shared Cloud source.

Connection register must distinguish verified current, historical and unresolved, with source/date and exact next discovery action. Missing connection information is not permission to recreate it. docs/horseshowing/wec-airtable-two-way-edit-contract.md is an Airtable edit pattern, NOT evidence of two-way SMS. WEC alert drafts are not locked WEF production behavior.

## Skills and subagents

Use shared contract's Airtable and verification skills for retrofit. Load task-specific Twilio skills from C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/twilio-developer-kit/0.2.2/skills/: twilio-messaging-overview/SKILL.md, twilio-messaging-webhooks/SKILL.md, twilio-security-api-auth/SKILL.md, twilio-reliability-patterns/SKILL.md. Load only the relevant ones when inspecting that connection; no need to load all at once. Cloudflare reference skill: C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/app-6a99ceead2c481919fb44845267235c4/1.0.1/skills/cloudflare/SKILL.md. Its instructions do not authorize new infrastructure or deployment.

If useful, delegate up to three bounded disjoint tasks: read-only existing connection/code inventory; read-only tenant option/recipient contract map; one base-retrofit writer after inventory. Avoid parallel schema writers. Record actual dispatch/acknowledgment, model/effort when known, finite budgets and evidence in Agents. No autonomous live SMS/deploy/automation activation. Preserve full open scope at close, not merely the part a worker inspected.

## Sibling coordinators and acceptance

Recognize handoff recGaodxumKRecL9i; Tenants handoff rec6kFNbb87Iggd6S in Agents handoffs tbl5GR2kL4BlUbfJx. Read their current task/interface records, register your actual chat ID on dispatch, and follow shared collision rules. Siblings must know Messages owns this retrofit but does not yet own or replace their existing SMS paths. Shared records support coordination; direct chat messages require explicit owner authorization.

Retrofit acceptance: reopen Messages and identify current tables, health, workflow, proven source/connection references, unknown isolation decisions, open tasks, owner/worker state and exact next action without reading old dialogue. Complete base/index/Agents readback. Do not claim messaging, recipient selection, delivery or two-way behavior tested from documentation. Active-session usage checks apply; no new heartbeat or hooks by default.
