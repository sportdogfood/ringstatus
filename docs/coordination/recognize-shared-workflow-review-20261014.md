# Recognize: shared native and API-agent workflow

**Latest implementation checkpoint:** [Native/API-agent parity](recognize-agent-parity-20261014.md). Owner subsequently approved unique known-phone recognition without mandatory PIN/SMS. Shared local source and fixture verification now exist; no deployment/publication. Earlier proposal/unresolved-policy statements below are historical where superseded by this checkpoint.


14 October 2026. Owner-requested workflow documentation and exact change plan. **No application edits or deployment authorized by this document.** This is the latest workflow review; it is not a claim that the described target is implemented or accepted. Open decisions below must not be silently converted into policy.

## Purpose and scope

Recognize identifies a returning person silently, maintains the device relationship and session history, and supplies verified identity to Tenants. Tenants owns barn membership, roles, permitted options and profile/input data. Recognize is not a second onboarding system.

Keep two interfaces to the same workflow:

- Native Webflow recognition inputs.
- The existing OpenAI API agent as an optional, controlled experiment, using the same actions, inputs and outcomes.

Silent recognition never requires a model call. The agent may guide an interaction but cannot decide identity, validate credentials itself, grant membership, write Airtable directly or invent recovery steps. Preserve existing working implementation and narrow adapters; no replacement authentication provider, framework migration or new database is proposed.

## Page map

| Surface | Purpose and target behavior |
|---|---|
| `/` | Public; footer checks quietly. Known and unknown visitors remain on the page. |
| `/ring-status` | Same public behavior. |
| `/about-ringstatus` | Same public behavior. |
| `/rs-recognize` | Staging/test surface for both interfaces; not a mandatory intermediate page for ordinary navigation. Verified page ID `6ac459b135ad0c4c3b253d97`. |
| `/rs-barn-onboarding-setup-v24` | Tenants entry. Recognize when needed, then load permitted tenant data. Verified page ID `6ac7b2de07b992a509c90029`. |
| `/rs-member` | Existing launcher and historical proof destination. Do not treat reaching it as proof that Tenants loaded. No redirect changes are authorized here. |

The owner supplied the Tenants page ID for both pages; Webflow metadata and published HTML established the distinct IDs above. The owner's priority is silent behavior, not redirect adjustments.

## Complete target sequence

### 1. Page entry and silent check

On a designated public page, run the existing recognition check in the background. Do not open the recognition panel or agent, require a button, or call OpenAI. The browser sends its existing secure cookie; the server validates the device/person and current eligibility. Browser memory is a convenience, not permission.

Return distinct outcomes: recognized, genuinely unknown, revoked/expired, or unable to check. An unavailable service is not evidence that a known person has become unknown. Public browsing remains available for every outcome.

### 2. Known person and session

Reuse the valid session, or establish the next valid session when needed; refresh the intended 365-day device cookie subject to browser retention. Keep session state and device identity distinct. Record session start/reuse/activity/end or revocation and associated footprints according to the existing data model. Do not create a new session record merely because both interfaces read the same state.

If browser storage is unavailable, do not prevent a valid cookie from reaching the server. Preserve the identity result separately from session/logging outcome. Required session tracking must still be accounted for; logging failures must not masquerade as an unknown identity or be labelled successfully logged. The existing tracking/reconciliation capability is the starting point, not another infrastructure project.

### 3. Enter Tenants

When the person enters Tenants, use the existing server-validated identity and resolve their enabled membership and role for the requested tenant. Load only permitted records/options. A known person need not repeat phone entry merely to move into Tenants.

If no eligible membership exists, explain that access cannot be granted for that tenant; do not repeatedly send the person through recognition or silently create membership. Existing authorized tenant-creation behavior must be preserved where applicable. If tenant selection is necessary, it belongs to Tenants and does not create another identity.

### 4. Unknown person at gated entry

Only gated entry, an explicit sign-in request, or deliberate use of the staging surface exposes the recognition inputs. Present the agreed phone/PIN/invitation choices. Preserve the original Tenants destination through the interaction using a validated local destination, not a model-provided arbitrary URL.

- **Phone:** normalize and locate an eligible record; do not automatically launch SMS merely because this field was submitted.
- **PIN:** validate through the existing server-side credential flow once its actual secret-PIN policy is confirmed. A four-digit phone suffix is not an established secret.
- **Invitation:** validate the invitation, expiry, intended identity and unused state on the server; consume through the existing one-use mechanism. Support the input/link format actually approved, not an invented short invitation code.
- **SMS:** retain it as an explicit fallback/recovery capability. No model or interface silently selects it as the default replacement for PIN or invitation.

Credential policy boundary: a phone-number match alone identifies a record but does not prove the person typing owns it. The owner has requested no mandatory SMS; this can be met by a valid remembered cookie, valid secret PIN or valid invitation. Owner subsequently explicitly selected unique known-phone recognition without mandatory PIN/SMS. Tenants permissions remain independently enforced; see the implementation checkpoint above.

After successful verification, bind the current browser through the existing backend, create/reuse the session, refresh recognition, then resume the requested Tenants entry. Both interfaces receive the same authoritative result. A redundant Continue/confirm step is not required merely because the user chose the agent.

### 5. Not you?

Make the same account-switch action available from the recognized-user context on desktop and mobile. It must retire/clear the current browser's identity as intended, end that session, clear that person's local display/draft context from the active view and return to neutral recognition inputs. Do not affect unrelated devices, delete records or automatically start SMS recovery. Do not replay the prior person's pending tenant edits under a new identity.

The account-switch request must be authorized by the current validated identity; it must not depend unnecessarily on a different expired legacy access cookie. Existing server safeguards remain necessary.

### 6. Retry and failure

Use the same deterministic retry rules in both interfaces. Distinguish invalid input, rejected credentials, expired/used invitation, unavailable service and an uncertain submitted outcome. Allow deliberate retry where the operation is retryable, then give a concise failure message and a concrete available next option. Never perform repeated model-directed retries or automatically send another SMS.

A retry of an uncertain mutation must retain its operation ID and check the prior outcome; it must not blindly consume an invitation or repeat a write. The exact general retry ceiling is not supplied by the owner and is not locked here. Existing OTP limits remain in force until separately reviewed. A stopped operational test is not restarted by updating this document.

### 7. Session and invitation history

The requested coverage is all sessions, including unknown visits, with unknown sessions unbound to a person. Associate identity only after verification; preserve the transition so guest history is not misrepresented as already authenticated. Reuse the existing session/footprint model where possible; inspect schema compatibility before any changes. No raw credentials, PINs, SMS codes, private invitation links or cookies belong in ordinary logs.

Invitation history needs distinguishable issued, submitted/sent where confirmed, accepted/used, expired/revoked and failed/uncertain outcomes, with time and relevant identity/reference. Provider submission is not delivery. Reuse current invitation claims, SMS events and audit records; a new table is not presumed necessary. Existing evidence does not establish a complete joined operator-facing history today.

## OpenAI API agent: same behavior, bounded authority

The verified source is `C:/Users/gombc/.codex/recognize-agent-test-20261013/`. Historical owner confirmation of phone → SMS code → `/rs-member` is recorded in [frontend-agent proof](recognize-frontend-agent-proof-20261013.md) and chat `01a134dd-e5c9-71d2-977a-7006af22af7d`. Preserve that proof and code; it does not prove mobile hosting or Tenants completion.

Current reality: Edge/Chrome extension, localhost service on port 43187, model `gpt-5.4-mini`, a fixed test recipient, ephemeral conversation, aggregate usage cap, and no direct production writer. It is not currently a generally hosted mobile feature.

Target responsibilities:

1. Remain inactive during silent recognition; optional assistance must not become another interruption.
2. Receive a small authoritative state summary and permissible actions. Do not infer success from a welcome label or URL.
3. Guide the same phone/PIN/invitation/recovery inputs. Private credentials stay in dedicated controls and go through the shared application controller; they are not included in model messages.
4. Invoke only actions permitted by that state and the same server contract. Preserve the existing adapter approach; no independent agent authentication endpoint, SMS sender or Airtable writer.
5. Observe the same pending, rejection, retry, Not you? and completion outcomes as the native interface.
6. Transfer to Tenants when identity and tenant access allow it. Stop the agent's identity task there; do not quietly turn it into a profile editor or permissions administrator.
7. If OpenAI is unavailable or the experiment reaches its budget, keep native recognition usable. Never make model availability a login prerequisite.

To support ordinary desktop/mobile browsers, adapt the local service into a small authenticated/server-controlled endpoint in the existing Astro application and expose the experiment's optional UI on the approved pages. Keep the API key on the server. Local-extension trust and pairing are not sufficient for a public endpoint: validate allowed actions, origin/session scope and usage limits on the hosted boundary. This is an identified hosting change, not deployment authorization or a claim it has happened.

## Exact change register

Release-source root for the following relative paths:
`C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/tenants-correction-20261011/candidate/webflow-cloud-test/`.

The published `src/assets/rs-recognition/native-client.js` matches this source after line-ending normalization. Backend deployment identity must be rechecked before implementation.

| ID | Existing location | Required delta | Proof required |
|---|---|---|---|
| R1 | `src/assets/rs-recognition/native-client.js`, `runSilentRecognition`, lines 13–57; startup 332–337 | Preserve distinct outcomes instead of one false result. Do not open login for a service/storage failure. Public silent calls remain noninteractive. | Known/unknown/service-failure cases on public, staging and gated entry. |
| R2 | Same silent function and mount initialization | Allow cookie validation when local/session storage is unavailable; handle session replacement without converting it into unknown identity. | Valid cookie with blocked storage; expired/revoked identity; session conflict. |
| R3 | `src/lib/rs-recognition-native.js`, recognize branch; `rs-recognition-session.js` | Separate identity, session and footprint outcomes; retain accountable tracking and safe reconciliation. Extend unknown-session coverage after schema inspection. | Recognized and guest session readbacks, repeat visit, tracking failure and recovery without false identity claims. |
| R4 | Client `verifyPhone`, `otpInput`, click handler; `rs-inputs-recognition.js` credential handling | Replace unconditional OTP dispatch with explicit shared phone/PIN/invite selection under the settled credential policy. Keep SMS available but optional. | Valid and invalid credential paths match both interfaces; phone-only lookup never silently acquires unapproved rights. |
| R5 | Client `acceptInvitation`; `rs-inputs-access.js` redeem; existing claims/history | Connect valid invitation acceptance to browser/session completion without redundant manual confirmation. Expose accurate issued/used outcomes without duplicating the existing one-use mechanism. | Valid/expired/reused invite, lost response, one consumption and matching history. |
| R6 | Client retire handler; native action routing; Inputs access guard | Not you? uses validated current identity, retires this browser and returns to neutral entry rather than forcing recovery; clear actor-scoped UI context. | Current browser changes identity; unrelated device remains valid; no cross-person tenant replay. |
| R7 | Client retry/executePending and feedback | Shared bounded retry and clear final failure; preserve operation IDs; no automatic SMS resend. | Retryable failure, invalid credential and uncertain action remain distinct. |
| T1 | `src/lib/rs-inputs-access.js`; `rs-tenant-barn.js`; native Tenants adapter | Preserve existing remembered-cookie fallback. Resolve person → selected tenant → membership/role consistently; return usable option permissions, not a guessed tenant/user flag. | Owner and restricted member see and can perform only allowed actions; server rejects disallowed requests. |
| T2 | Published staging binding and gated-entry configuration | Preserve requested Tenants destination through both interfaces; remove dependence on a staging detour. Redirect configuration follows the owner’s final destination choice. | Known person and newly verified person reach actual permitted Tenants data. |
| A1 | Agent `policy.mjs`, `server.mjs` instructions/schema/state guards | Expand SMS-only decisions to the approved shared action/state vocabulary, including Not you?, invitation and bounded retry; keep credentials out of the model and preserve budget controls. | Invalid/model-invented actions rejected; same allowed actions as native; no model call on silent visits. |
| A2 | Agent `extension/adapter.js`, `extension/content.js` | Reuse native controller actions/results for all approved paths. Replace hardcoded `/rs-member` success and English-text matching with authoritative identity/access/pending outcomes. Preserve private credential controls. | Same input yields same result as native; navigation alone never marks full completion. |
| A3 | Agent `server.mjs`, `extension/background.js`, `extension/manifest.json` and hosted UI integration | Adapt localhost/extension transport for the existing Astro host and approved mobile/desktop pages; server-held secret, validated access/action boundary, rate/cost limits. Retain local proof as reference. | Real mobile and desktop work without localhost/extension; model outage leaves native flow usable. |
| D1 | This document, existing workflow/agent proof and later task records | Keep desired behavior, deployed version, evidence and unresolved decisions separate. Record actual outcome after implementation. | A new runner can identify which changes are proposed, implemented and verified without assuming all are done. |

These are corrections within the existing workflow, not instructions to patch repeatedly or replace it. Inspect the deployed source and each existing implementation before editing. Changes to schema, credentials, activation and publishing remain separate actions under existing authority.

## Owner input rules — 14 October 2026

These input rules supersede earlier uncertainty about credential lengths. Documented here; not implemented by this update. Apply the same rules to native and agent-assisted interfaces.

- Invitation: exactly **7 digits**, unique, with **active/inactive** status. Inactive invitations cannot qualify entry. Preserve issued/used history. The owner's status rule does not itself settle whether use automatically inactivates an invitation; do not silently change existing consumption behavior.
- PIN: exactly **4 digits**; duplicates across people are allowed. Preserve leading zeroes. Never use a PIN by itself to select the first matching person.
- Phone: exactly **10 digits**, unique after normalization. For every outgoing SMS, convert the canonical ten digits to **+1 followed by those ten digits**, once; do not double-prefix a number already in that format. Equivalent formatted phone inputs must resolve to the same canonical identity.
- Treat invitation and PIN values as digit strings, not numbers. Do not log raw credentials or put them in the model context.
- Owner explicitly asks to consider Geo-IP, device ID and cookie in PIN collision handling. Proposed handling: a server-validated cookie/device association fixes the person first; check the PIN only for that person. Without that association, the unique phone plus PIN can select a single candidate under the agreed access policy. A client-supplied device ID or Geo-IP alone does not establish identity. Shared Wi-Fi and reused PINs must never merge accounts or choose a person automatically.
- If identity remains ambiguous, request the unique phone or another supported verification path. Keep tenant membership/role authorization separate from recognition. No new authentication method or infrastructure is authorized by these documentation notes.

## Decisions still requiring explicit resolution before dependent edits

- **Phone-only decision resolved:** unique eligible known phone identifies the person; no mandatory PIN/SMS. Existing tenant membership/role checks still control access.
- **PIN/invitation implementation:** lengths, uniqueness and invitation active/inactive states are now specified above. Verify compatibility with the existing stored PIN/invitation implementation; do not equate a phone suffix with a secret PIN. Collision handling above is a recommendation until selected; automatic invitation inactivation on use remains unspecified by this latest instruction.
- **Retry ceiling:** choose the common bounded rule; do not silently invent a number. Preserve existing OTP protection meanwhile.

These decisions do not prevent documentation or inspection of independent silent-recognition, session, UI and agent-hosting changes. No final choice is requested or implied by writing this plan.

## Acceptance: one workflow, two interfaces

## Local identity, cadence and AKA lookup — owner follow-up, 14 October

Documentation only; no application or schema changes. Owner requires locally available user context, fewer repeated recognition requests, quick role/access/opt-in display, and associations across known credentials and devices. The numerical cadence below is a recommendation, not an already implemented or owner-approved policy.

### Storage and authority

- Keep the existing server-set HttpOnly, Secure recognition cookie on the browser, with the existing intended 365-day lifetime. Do not copy its credential into localStorage. Browser eviction can shorten retention.
- Cache only a minimal display snapshot locally: stable person reference, display name, selected tenant reference, that tenant's roles/permitted options, opt-in summary and last-confirmed time. Scope it by person and tenant; do not cache an entire person or tenant roster. No PIN, OTP, invitation secret or reusable bearer credential in this snapshot. Remove expired snapshots after 365 days at most; local retention is not authentication lifetime.
- Keep the current tab's non-authorizing session tracking reference in sessionStorage; it must never grant access. The server remains authoritative for identity, access and notification consent. Pending opt-in changes remain visibly pending until accepted; sending systems use the confirmed server state.
- The current client stores a device token and expiry in localStorage and explicitly waits for server revalidation on every visit. It does not implement this user/tenant cache. Its JS-readable device-token fallback requires review because the server accepts it when the HttpOnly cookie is absent; do not simply add another copy of it.

### Recommended event-driven cadence

| Event | Required behavior |
|---|---|
| First visit in a tab or no confirmed tab state | One silent cookie check; create a tracked session if none exists. Cached display context does not establish identity. Public pages stay public. |
| Navigation/reload within 15 minutes of confirmation | Reuse the tab confirmation and display cache; no separate recognition request solely because a page or the second interface loaded. A protected data request still validates identity and access on the server and may refresh the shared context. |
| Next navigation or return to the foreground after 15 minutes | One background refresh of recognition and current tenant roles/options/opt-ins through existing appropriate endpoints. No continuous polling or new scheduler. Consolidate simultaneous callers within the tab. |
| 30 minutes without user activity, or a new tab without session state | Start a new tracked visit on the next interaction, validate the existing cookie silently and retain the same person/device association. Do not ask for credentials just because a session ended. No timer or unload request is required to close a vanished tab; record its last observed activity honestly. |
| Successful recognition or credential verification | Create the device cookie after verified binding, or refresh its existing 365-day expiry on successful recognition; reuse the current active session. An unknown-to-known transition is tracked explicitly, not recorded as authenticated before verification. |
| Tenant switch, accepted role/opt-in edit, or access denial | Refresh the affected tenant snapshot immediately. Every protected read/write still enforces current server permissions; the display cache never bypasses them. |
| Known or genuinely unknown result | Stop that recognition attempt. Unknown public visitors see no form; only gated entry or explicit sign-in exposes inputs. Cache the unknown result for the same 15-minute quiet window unless the user signs in. |
| Service failure/offline | Mark confirmation unavailable/stale, preserve permissible cached display and person-scoped drafts, and stop; one deliberate retry, no loop or automatic SMS. Do not label the person unknown or claim current permission. Refresh on a later relevant event or reconnection. |
| Logout, Not you?, revocation | Invalidate current identity as applicable, clear its active local context and confirmation, end the tracked session, and keep pending edits isolated from the next identity. Propagate account switching to other open tabs. |

The 15-minute display refresh and 30-minute visit boundary are usability defaults proposed here, not security guarantees or numbers dictated by a standard. An already open page is refreshed on relevant activity rather than a permanent timer. No new role authority, polling service or storage product is needed.

### AKA: one person, multiple verified associations

Reuse the existing person, phone-alias, device, invitation and session/footprint records. Do not build a record for every combination or assume a new table is needed.

- Normalize phone input to one canonical number; the inspected code already queries primary phone and phone aliases and rejects ambiguous matches. Associate additional phone numbers only after verification/authorized confirmation; never merge people on a weak match.
- Resolve an invitation through its existing hashed, expiring, single-use record to the intended person; preserve issued/used history. Consuming it binds the current device; the used invitation is not a reusable alias credential.
- PIN remains unresolved as independent authentication: existing member_pin/last-four lookup is not a proven standalone secret verifier. Do not store plaintext PINs in aliases or browser caches, or silently activate that path.
- Each verified desktop/mobile browser has its own device association to the same person. A phone used to receive a code does not automatically authenticate its browser; the originating browser completing verification is the one bound.
- Geo-IP, IP hash, browser/device category and timestamps are footprint context. Shared Wi-Fi, matching location or a phone-plus-IP combination must not authenticate a different device or automatically merge identities. No browser-fingerprinting invention.
- Lookup priority: valid cookie/device -> person; verified invitation or approved credential -> same person -> bind device; phone/alias -> candidate requiring proof. Multiple matches fail clearly. Tenant memberships/roles/opt-ins are then resolved for that person and the selected tenant.
- A useful operator view can show person -> verified phones -> devices -> sessions/footprints -> invitation use. Current record IDs in inspected configuration: people tbly1PM5iFYqVzKSm, devices tblfkRSJAEMzuzApR, phone aliases tblgDWKi0Bb6OcoqS. These are source references, not a fresh live schema/readback or measured lookup-performance claim.

Native UI and API agent share this state and cadence. Silent checks require no model call. Before implementation verify both interfaces reuse one confirmation, blocked storage does not block cookie recognition, role changes are enforced server-side, Not you? clears cross-tab context, and multiple devices resolve to one person without duplicate sessions from duplicate interface calls.

Source inspection: release candidate src/assets/rs-recognition/native-client.js; src/lib/rs-recognition-native.js; rs-recognition-session.js; rs-recognition-config.js; rs-inputs-recognition.js. No fresh live execution performed. Storage boundary reference: [OWASP HTML5 Security: local storage](https://cheatsheetseries.owasp.org/cheatsheets/HTML5_Security_Cheat_Sheet.html#local-storage).

## Acceptance checks

Verify on desktop and mobile for native and agent-assisted entry:

- Public known and unknown visits remain uninterrupted; silent entry uses zero model calls and sends no SMS.
- Known gated entry reaches permitted Tenants data without phone re-entry; service failure is shown as failure, not unknown identity.
- Each approved phone/PIN/invitation path follows the same validation and completes in the originating browser. SMS occurs only when explicitly selected and authorized.
- Not you? switches identity safely; retries stop according to the agreed rule; invalid/expired/reused credentials fail consistently.
- Device/session/footprint and invitation history agree with actual events, including unknown sessions; no duplicate events solely from using two interfaces.
- Tenant membership and role control actual server operations and visible options.
- Agent model outage/budget exhaustion does not block native recognition. Credentials never reach the model or normal logs.
- Revisit and reload preserve the intended identity/session behavior. Arrival at a launcher, a passing build or a local model response alone is not completion.

No live sends, code changes, hosting changes or stopped-work restarts were performed while preparing this review.
