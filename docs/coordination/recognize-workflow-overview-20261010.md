# Recognize — overview and expected workflow

Latest owner-requested workflow review: [shared native/API-agent workflow and exact change register, 14 October](recognize-shared-workflow-review-20261014.md). Read it before the dated implementation notes below. It records the staging-only recognition page, silent public entry, shared inputs and Tenants handoff; unresolved credential/retry decisions are explicit. Documentation only: it does not establish a new deployment or authorize edits.

Additional proven source: [Recognize frontend-agent demonstration](recognize-frontend-agent-proof-20261013.md), 13 October 2026, owner-confirmed phone/code to `/rs-member` using the existing native workflow. Its local implementation and exact source turns are indexed there. For later shared release evidence see [October 11 resume checkpoint](resume-recognize-tenants-20261011.md); the earlier checkpoint below does not supersede that later evidence.

## Current runtime checkpoint — 11 October 2026

The dated implementation notes below are historical. Current evidence is in [Recognize completion release](recognize-completion-release-20261011.md): approved source deployed as `785f571c2167c82c214f0e5c81ffcb77208b70e8`; original Edge browser silently redirects to `/rs-member`; Sessions create/reuse and confirmed browser state are now live verified. Three public footer updates are saved and read back, awaiting owner publication. Public-page live checks and desktop/mobile same-browser SMS acceptance remain open. Do not use the older “not implemented” descriptions below as the current runtime state or infer full completion from this checkpoint.

Recorded October 10, 2026 from the current Airtable documentation. This is a specification and recorded checkpoint, not a new runtime test. The owning system record remains the maintained status reference.

[Resume here](https://airtable.com/app9kOZdIaGyKk5uG/tblTlw48kcRoEX6yf/recB69Oe39xZ0nsAe) · [Workflow](https://airtable.com/app9kOZdIaGyKk5uG/tbljuflGbfeEmjcbn) · [Open tasks](https://airtable.com/app9kOZdIaGyKk5uG/tbltfO97Vy1StVVkO)

## Purpose and pages

Recognize silently checks device identity on designated static Webflow pages. Public visits remain uninterrupted. Gated entry qualifies the visitor and routes through Recognize only if necessary, then to /rs-member. Cookie lifespan: 365 days subject to browser clearing. Keep identity recognition distinct from permission to edit/access protected data. Reuse existing proven implementations; do not reinvent or layer fixes without addressing causes. Read workflow for intended/current differences, tasks for authorized scope and acceptance, and activity for the last session. Record meaningful changes in changes with evidence. Never equate code inspection with live verification. Working page list supplied by owner; gate designation and live wiring remain to be confirmed.

WORKING PAGE MAP — owner supplied; not yet live-verified
Public pages for silent footer recognition:
https://ringstatus.com/
https://ringstatus.com/ring-status
https://ringstatus.com/about-ringstatus

Launchpad: https://ringstatus.com/rs-member (replaces /member-home in the intended workflow).
Recognition page: https://ringstatus.com/rs-recognize (possible future name /recognizer; not renamed).

Downstream pages:
https://ringstatus.com/rs-barn-onboarding-setup-v24
https://ringstatus.com/rs-barn-onboarding-schedule-v24
https://ringstatus.com/rs-barn-onboarding-feed
https://ringstatus.com/rs-barn-onboarding-alerts (SMS opt-in)
Confirm which downstream pages require gated entry before enforcing access.

Shorter slugs come after styling, experience and workflow are settled. No page, redirect or code changed by recording this map. Astro endpoint-to-page mapping still needs verification.

CONFIRMED: /, /ring-status and /about-ringstatus are the public static website, NOT gated. Recognition on these pages is silent only: no login requirement, no forced launcher redirect, and unrecognized visitors continue browsing.

## Ordered workflow: expected versus recorded implementation

### 1. Public-page silent check

Trigger: Visit a designated static Webflow page

Expected: Footer checks recognition quietly; no forced login, overlay or redirect.

Recorded implementation: runSilentRecognition exists, but successful recognition navigates to launcher. Do not install it unchanged as the general public footer.

Evidence level: Partial. [Owning workflow record](https://airtable.com/app9kOZdIaGyKk5uG/tbljuflGbfeEmjcbn/recmilE5hLM6LboQf).

### 2. Read and validate device identity

Trigger: Silent check request

Expected: Browser sends cookie automatically; server validates device/person; local session state is only a cache.

Recorded implementation: Server prioritizes HttpOnly cookie, falls back to supplied device token and calls recognizeDevice. Client keeps token/expiry in localStorage.

Evidence level: Source confirmed; live unverified. [Owning workflow record](https://airtable.com/app9kOZdIaGyKk5uG/tbljuflGbfeEmjcbn/recLTroLxW43YLmDc).

### 3. Phone verification for an unknown gated visitor

Trigger: Unrecognized browser enters a designated gated area

Expected: Only gated entry starts phone/code verification; public browsing continues without it.

Recorded implementation: OTP start/check path exists. Complete page-level gating and current desktop/mobile behavior remain unverified.

Evidence level: Partial. [Owning workflow record](https://airtable.com/app9kOZdIaGyKk5uG/tbljuflGbfeEmjcbn/recsptB8JWd9DlIx4).

### 4. Create or refresh recognition cookie

Trigger: Successful OTP or confirmed returning device

Expected: Set/refresh secure HttpOnly SameSite=Lax recognition cookie for 365 days; browser may remove it earlier.

Recorded implementation: Server sets __Host-rs_recognition_device after verified OTP and successful silent recognition; clears it on failed matching.

Evidence level: Source confirmed; live unverified. [Owning workflow record](https://airtable.com/app9kOZdIaGyKk5uG/tbljuflGbfeEmjcbn/recwlb3rDs8HW5R6z).

### 5. Maintain device-side session state

Trigger: Page navigation, refresh and recognition response

Expected: Reuse session ID and retain minimal server-confirmed recognition state; refresh/revalidate and clear stale state.

Recorded implementation: rs_native_recognition_session_v1 is stored in sessionStorage; native silent path does not persist a reusable recognized-state result. Separate tabs can have separate sessions.

Evidence level: Partial. [Owning workflow record](https://airtable.com/app9kOZdIaGyKk5uG/tbljuflGbfeEmjcbn/recbexiJuWJrs0xyP).

### 6. Create or reuse server session

Trigger: Server-confirmed recognition

Expected: Reuse eligible session or create one; link person/device and maintain start, last activity and lifecycle.

Recorded implementation: Sessions schema exists but is not connected to current recognition writer. Existing session UID is currently carried in event rows.

Evidence level: Not implemented. [Owning workflow record](https://airtable.com/app9kOZdIaGyKk5uG/tbljuflGbfeEmjcbn/rec5jvzRjmiWImla0).

### 7. Record session and visit footprints

Trigger: Recognition and designated activity events

Expected: Correlate recognition/visit events with person, device, session, time and page; record supported network/location context.

Recorded implementation: Current silent success writes a recognition event to Footprints. Key derives from session UID + device token, so repeated pages in that session are deduplicated; this does not prove a distinct footprint per page visit.

Evidence level: Partial. [Owning workflow record](https://airtable.com/app9kOZdIaGyKk5uG/tbljuflGbfeEmjcbn/recLpOGCdY96QBJnJ).

### 8. Qualify gated entry and open launcher

Trigger: Recognized visitor requests gated access, or successful gated verification

Expected: Validate qualification on server; navigate to launcher only for gated entry/recognition completion. Launcher buttons are separately designated.

Recorded implementation: Intended launchpad changed from /member-home to /rs-member per owner. Saved native client redirects on success and defaults to /; actual live destination and page configuration remain unverified. No code change made.

Evidence level: Partial. [Owning workflow record](https://airtable.com/app9kOZdIaGyKk5uG/tbljuflGbfeEmjcbn/recGXOpqSaEmYTDPJ).

## Resume checkpoint

RESUME HERE — Recognize
Reviewed: 2026-10-10T13:15:27Z (documentation/record review; not a live application test).

STATE / HEALTH
Needs work. Base documentation is ready to resume from. Runtime completion remains unverified.

WHAT IS ESTABLISHED
Eight supporting tables and their links are created and read back.
Existing source contains recognition-cookie handling, a browser-tab session ID and recognition-event logging.
Public pages /, /ring-status and /about-ringstatus are NOT gated; silent checks must not force login or redirect.
Intended launchpad: /rs-member. Recognition page: /rs-recognize. Existing downstream slugs remain until styling/workflow settle.

WHAT REMAINS UNVERIFIED / UNFINISHED
New Sessions table is not wired. Reusable recognized browser state and public-footer behavior need work. Current deployed version, page-to-service connections, downstream gated designations and full visitor journey require verification. See tasks for exact scope.

WORK STATUS / RESPONSIBILITY
7 open tasks, all Planned; no task is recorded In progress.
No agent is recorded Working in this base; existing setup assignment is Complete. These are registry states, not a live check of other chats.
System owner and open-task assignments are not yet confirmed. Do not treat these tasks as actively running.

OPEN TASKS
Try this base with one small task — Planned
https://airtable.com/app9kOZdIaGyKk5uG/tbltfO97Vy1StVVkO/rec3Idh2NoTnFbmEq

Decide who handles each open item — Planned
https://airtable.com/app9kOZdIaGyKk5uG/tbltfO97Vy1StVVkO/rec4Aqv8CSZ1AiDLH

Remember the visitor and record their session — Planned
https://airtable.com/app9kOZdIaGyKk5uG/tbltfO97Vy1StVVkO/rec7FIzlG6f9Qd9f3

Check the complete visitor journey — Planned
https://airtable.com/app9kOZdIaGyKk5uG/tbltfO97Vy1StVVkO/recEOLQ5QiYvVKQot

Write the fallback and return-to-working steps — Planned
https://airtable.com/app9kOZdIaGyKk5uG/tbltfO97Vy1StVVkO/receQvuf4HM6D1HMs

NEXT — Verify deployed Recognize source and connections — Planned
https://airtable.com/app9kOZdIaGyKk5uG/tbltfO97Vy1StVVkO/recmzhSVu14Ka4ahn

List the pages and set the right entry behavior — Planned
https://airtable.com/app9kOZdIaGyKk5uG/tbltfO97Vy1StVVkO/recpJKchoFBgvqyj9

DEVELOPING / UNDECIDED
Pair Recognize and Entities/Tenants coordinators — Proposed
https://airtable.com/app9kOZdIaGyKk5uG/tblTNh8qOz5CBi1k5/recLUqh2XJivHBuVP

Operating-model review and minimal corrections — Proposed
https://airtable.com/app9kOZdIaGyKk5uG/tblTNh8qOz5CBi1k5/recmydOyjCaF5Ylkm
Recognize/Entities coordinator pairing is undecided; Recognize may remain independent and Tenants may pair downstream.

NEXT ACTION
First: existing task recmzhSVu14Ka4ahn — read-only verification of deployed source, page scripts, Astro routes, table IDs and existing SMS automation configuration. Assignment remains unassigned; do not infer deployment from local source. Then use those results for the existing page/session and complete visitor-journey tasks.

LATEST SESSION NOTE
https://airtable.com/app9kOZdIaGyKk5uG/tblMYsOaJyiHq7qhw/recnh3TuFqmJ32oVN

SESSION-CLOSE RULE
Before ending work, update affected tasks, agent status, development/failure/change records only where relevant, and the session note in activity. Refresh this summary and Health/Last checked from actual evidence. State what stopped, what remains open, who owns it and the exact next action. Do not create extra structure or claim planned work is running.

LAST COMPLETED ACTION
2026-10-10: Handoff content review completed. Exact available skill paths and connection references added, unresolved mappings assigned to the first next task. Reference inventory task marked Done; application gaps remain open. Earlier readiness review checked structure but did not sufficiently check content. No application changes or live tests in this session.

## Code and connections

Repository: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus
Last inspected implementation: C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus/webflow-cloud-test
Client: src/assets/rs-recognition/native-client.js
Entry: src/pages/rs-inputs/native-recognition.js
Backend: src/lib/rs-recognition-native.js
Device matching: src/lib/rs-inputs-recognition.js
Footprint writer: src/lib/rs-recognition-session.js
OTP: src/lib/rs-recognition-otp.js
Table mapping: src/lib/rs-recognition-config.js
This identifies inspected source, not a fresh verification of the deployed version.

Recognize owner base: https://airtable.com/app9kOZdIaGyKk5uG (records read 2026-10-10).
Downstream Tenants: https://airtable.com/app7BXcw801n19QKF/tblMKWJw8Q6joKc8U/recJzisHwiPPkKG6y — owns barn membership, roles, assignments, preferences/profile entities; Recognize owns identity/session. Cross-base runtime mapping not verified.
Agents shared references: https://airtable.com/appZahVgD156cMAe3
Webflow production MCP: webflow; site 6982268b7543ac3c80151266 (configured project reference, not live checked this session).
Public pages: /, /ring-status, /about-ringstatus — silent only.
Recognition /rs-recognize; launchpad /rs-member. Page-to-Astro mapping must be checked in task recmzhSVu14Ka4ahn before changes.
Existing Astro/Webflow Cloud source is listed in Code locations; its deployed version is unresolved, not inferred from folder name.
SMS: existing Airtable-native Twilio connection and SMS verification requests table; configured automation identity/activation and callback mapping require current read-only verification. No credential values in this base.
No connections were activated or changed in this documentation session.

## Supporting documents and precedence

- [October 9 partial checkpoint](recognize-partial-checkpoint-20261009.md): historical saved/deployed evidence, not current live proof. Its temporary `/` destination is superseded by the owner-designated `/rs-member`.
- [Original native Webflow/Astro plan](recognize-native-astro-plan.md): historical architecture/source references; old slugs, invitation-first assumptions and implementation states do not override the current workflow above.
- [Member-home change history](recognize-member-home-20261009.md): redirect history, not current page authority.
- [Shared persistence contract](shared-data-behavior-contract.md): cross-system background; Tenants dirty-state requirements are not additional Recognize responsibilities.
- [Base-building guide](new-airtable-base-guide.md).

Read current owner instructions and system/workflow/task records first. Preserve native Webflow presentation and existing working code. IP/geo alone is not identity verification. Do not reproduce historical source blindly or treat documentation as application completion.

