# Recognize specialist — start here

**Verified current prototypes, 29 October (coordinator browser evidence):** Recognize Only Site `appgprj_6adfb02df0148191926193d78c24ee02` v7 / `9e2cfb85f1cce4e7ee4b2ff34d27e732db68918c` uses sample credentials/local recognized flag only. Profile Widget Site `appgprj_6ad0e6be23848191b16edc09134e5045` v29 / `90022e40c7d925c9e3f7cf9c22fe69552b52164e` uses seeded browser-local profile/Team rules. Separate origins/keys; no shared identity handoff or live product API/cookie connection observed. Hosting sign-in is not product recognition. These are not the historical rec2/Astro backend. Exact assets and evidence attribution are in the [workflow baseline](recognize-workflow-overview-20261010.md). Owner verify-only instruction pauses all form changes; retain drafts separately. No historical-source backend correction or release is implied.

**29 October User/Team design and owner correction:** read the first section of [the updated Recognize workflow](recognize-workflow-overview-20261010.md). User identity/profile is independent; Independent/Preferred levels and Team roles/membership/access remain outside Recognize. Team invitation acceptance is separate from recognition invitation. Owner confirms Recognize does not require active Team membership. Earlier coupling findings came from the historical bindings worktree, not the current `publish-recognize/profile` baseline; claims of a current backend gap/required separation are withdrawn. Stop dependent workflow/logic changes pending the coordinator's actual current-form/request/destination verification. No backend changes or release are authorized. Older route maps, graphics and screenshots are historical; no current form route/graphic was verified.

**14 October ownership clarification:** Recognize owns identity/session and return-to-page behavior. Messages owns `/rs-barn-onboarding-alerts`; Tenants owns its canonical preferences/permissions. Use [the shared three-area contract](recognize-tenants-messages-ownership-20261014.md); no duplicate auth, data migration or product restart.


Current entry (14 October 2026): use this domain's resume row in [the authoritative-source directory](domain-coordinator-structure-20261013.md). This dated document preserves workflow/source evidence; original setup commands and old dispatch/monitor instructions are historical, not a new assignment. Follow the current owner request and verified checkpoint. Guidance-only updates leave stopped product work stopped.

Current shared structure (13 October): read the relevant sections of [Company, domains and coordinator capabilities](domain-coordinator-structure-20261013.md). It defines responsibility, sibling interfaces, required skill/connection mapping, proven frontend-agent reference and completion/handoff duties. Capabilities are not automatic agent assignments. Current tasks and authorization control; stopped work stays stopped.

Owner-requested domain skill: read `C:/Users/gombc/.codex/skills/recognize/SKILL.md` for Recognize work and include its path in subordinate handoffs. Current status comes from the live checkpoint, not dated progress paragraphs below. Scheduled monitoring: `docs/coordination/specialist-skill-supervision-20261010.md`.

You are the Recognize specialist coordinator. Own this workflow through its existing remaining tasks; use the shared contract for subagents, coordination, skills, usage checks and permission boundaries. Do not re-educate the owner or reconstruct the project from the old dialogue.

First read docs/coordination/specialist-shared-contract-20261010.md, then docs/coordination/recognize-current-binding-review-20261010.md. Read docs/coordination/recognize-workflow-overview-20261010.md only for additional behavior detail. Supersede old table spellings and old dispatch claims with current index and this packet.

## Base and implementation

Base app9kOZdIaGyKk5uG; system checkpoint https://airtable.com/app9kOZdIaGyKk5uG/tblTlw48kcRoEX6yf/recB69Oe39xZ0nsAe; tasks tbltfO97Vy1StVVkO; index tblZ6CTQoaqkHJvey. All 29 table/index names read back matching. Naming completion does not prove runtime.

Core tables: people tbly1PM5iFYqVzKSm; recognition-devices tblfkRSJAEMzuzApR; sessions tblBgvP7pYSv8o20j; footprints tblWjbASVMIjFLyW8; sms-verification-requests tblxC4SYtzYhJNcJS; rs-phone-aliases-test tblgDWKi0Bb6OcoqS. Existing SMS outcomes still reference ___rs-sms-events tblF1Hdqi3yoKTZPV. Deprecated prefixes do not prove safe deletion. Six legacy Inputs tables remain runtime-connected by ID; do not migrate them in this assignment.

Under the deployed-source root in the shared contract: src/assets/rs-recognition/native-client.js; src/pages/rs-inputs/native-recognition.js; src/lib/rs-recognition-native.js, rs-recognition-session.js, rs-recognition-config.js, rs-recognition-otp.js, rs-recognition-otp-store.js, rs-recognition-claims.js, rs-recognition-sms.js; shared rs-inputs-recognition.js. Tests under test/; existing Cloudflare test loader test-support/cloudflare-workers-loader.mjs. Original supplied recognition proof is f012c3077, not permission to restore stale base/redirect settings.

## Locked behavior

- Public /, /ring-status, /about-ringstatus: silent identity check; known and unknown stay on the page, no UI or navigation.
- /rs-recognize: known browser silently proceeds to /rs-member. Unknown gated browser uses existing phone/code entry in the originating browser, including desktop. Do not substitute a phone-opened link for desktop verification.
- Secure server-validated person/device cookie, intended 365-day expiry; refresh it and reuse session identity where valid. IP/geo is context, not sufficient authentication. Preserve rejection/revocation/account-switch handling.
- Existing Airtable-native Twilio automation wfl6oiGMgUPmEkUuS and existing D1 binding RS_RECOGNITION_CONTROL_DB. No new auth provider/database. Only approved test number 16318752160; live sending/publication needs applicable authorization.
- Webflow owns native UI; Astro binds data/actions only. Do not alter original /recognize proof, tenant access rules or launcher button destinations.

## Current progress and exact sequence

Internal worker /root/recognize_completion completed read-only discovery. Discovery task recmzhSVu14Ka4ahn Done; no implementation worker running. Agents record recvIfHlAJKjTmbBE and old assignment recrZ0pTDSgER8HqZ Completed. Do not reuse the old historical worker thread as current.

1. recpJKchoFBgvqyj9: public pages still load legacy client. Existing native silent success redirects; implement explicit public/no-navigation mode and replace only those conflicting loader invocations. Preserve working /rs-recognize -> /rs-member.
2. rec7FIzlG6f9Qd9f3: sessionStorage UID and footprints writer exist, but separate sessions table has no writer. Connect existing summary table, person/device/activity and minimal server-confirmed device state; preserve footprint deduplication. No second logging architecture.
3. recEOLQ5QiYvVKQot: qualify published known/unknown public and gated visits, same-browser desktop/mobile OTP, silent return, refresh, invalid/expired/reused codes, revoked cookie, account switch, retries, correct sessions/footprints and unchanged Inputs behavior. Green local checks are not this evidence.

Begin by reading those three live tasks and checking only source changes since verified 18e6e993a. Record smallest authorized delta and existing baseline; do not rerun full discovery. Prepare/use one scoped writer plus independent read-only verification when implementation scope is authorized. Current handoff creation does not authorize production publishing or live test execution. Complete independent preparation without waiting for Tenants.

## Sibling awareness

Tenants specialist packet: docs/coordination/tenants-specialist-handoff-20261010.md. It owns tenant membership, roles, profile inputs and opt-ins. It depends on your verified identity/session contract, not your UI. Register any change to cookie name, native-recognition request/response, person/device/session identifiers, expiry or rejection semantics in Agents before consumer changes. Do not let Tenants rewrite shared recognition logic or release over your unfinished work. Use shared Agents reservations/records; actual sibling chat ID is pending dispatch.

## Completion and resume record

Update existing task evidence, system health/next action and activity at each accepted checkpoint; link Agents handoff/worker. Complete only the acceptance actually proved. Record human publication/test steps precisely if needed. On stopping, name exact next executable action and actual worker status. No new schemas, broad table cleanup or future invitation redesign is required to start these tasks.
