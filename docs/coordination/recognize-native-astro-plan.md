# Recognize — Webflow presentation, Astro behavior, Airtable storage

Plan prepared 8 October 2026. Requested scope: plan Recognize integration and full-system acceptance, then reuse the proven method for barn-input, barn-onboard and user-optin. Planning does not authorize implementation, deployment, publishing, migrations or real-user test writes.

Common sequence: [Prototype → native Webflow → mapping → integration/testing → owner publication → locked baseline](common-delivery-stages.md). This is the Recognize-specific plan; preserve its exact destination and acceptance rather than substituting the generic stages.

Execution update, 8 October 2026: owner requested starting a dedicated Recognize wiring task. [Execution record](recognize-wiring-task.md) supplies current implementation authorization and runner identity. The original plan-only status is historical; publishing/deployment and unrelated schema work retain their separate boundaries.

## Fixed design

**Native Webflow Recognize → small page-specific binding script → Astro JSON endpoints → Airtable Recognize → JSON result → designated native Webflow display.**

Webflow owns markup, layout, fonts, colors, responsive rules and visual state styling. Astro owns validation, permitted actions, recognition decisions, data access and logging. A small binding script reads form values, calls the endpoints, fills existing text/input targets and activates the predefined native states. Astro must not inject a replacement interface, stylesheet, inline visual styles or a React shell. Data values are inserted as text, never executed as HTML. Secrets remain server-side.

Recognition is distinct from authorization, barn membership and SMS consent. A recognized device must not automatically acquire permission to edit unrelated people/barns or opt in to messages.

## Verified starting point

- Webflow site: `6982268b7543ac3c80151266`.
- Current native template: `6ac459b135ad0c4c3b253d97`, **RS Barn Onboarding — Recognize**, `/rs-barn-onboarding-recognize`. Live metadata confirms draft=true; native text query returned Recognize, Recognized and sample-profile copy. No page changes made.
- Airtable Recognize: `app9kOZdIaGyKk5uG`. Current schemas read through the connector; no records read or changed.
- Existing Astro application: `webflow-cloud-test`, server output with Cloudflare adapter. Existing recognition routes: `src/pages/rs-recognition/device.js`, `action.js`, `session.js`, `client.js.ts`. Action/session logic is in `src/lib/rs-recognition-action.js` and `rs-recognition-session.js`.
- Existing `src/assets/rs-recognition/client.js` creates DOM and a style block. It cannot serve unchanged as the new native-template adapter. Preserve existing consumers while adding a page-scoped adapter; switch only Recognize's approved loader after proving it. Do not globally remove the old client.
- Source code defaults to older Airtable base `apptdhhNzduxm5gjn`, overridden by `AIRTABLE_RS_RECOGNITION_BASE_ID`. Actual deployed value is unverified. It must explicitly resolve to Recognize before any test writes; do not conclude production currently uses the wrong base.
- Prior visual acceptance is incomplete: heading/theme colors, narrow logo width and 393×852 mobile height remain documented deviations. Preserve the owner-accepted design; establish its current baseline and resolve those exact acceptance gaps without redesign.
- Recovery code queues a `send_member_link` intention. That alone does not prove a link is delivered or redeemable. Its actual downstream consumer is unverified.
- Current device/action responses include `member_pin`. Field mapping must separate safe display data from credentials; a PIN must not be returned as ordinary display data. Existing phone/PIN matching must be assessed against the actual access it grants before release, not assumed to be secure authentication.

Historical source: `docs/user-data-storage/recognize-build-2026-10-07.json`; native evidence: `.git/ringstatus-control/1911bfd3ae618e9cf6cfb4f276710a87ec3e4107f9b7361da3180a0317e924c8/recognize-verification-summary.json`. Historical tests are not fresh end-to-end proof.

## Storage boundary

Owner-confirmed destination: **Recognize base `app9kOZdIaGyKk5uG` only**, for all recognition reads, writes, aliases, devices and logs, including test runs. The older base is historical diagnostic evidence, never an allowed destination or fallback. Missing or mismatched configuration must stop the operation before data access; it must not silently use a legacy default. Apply this requirement to every route and downstream recovery/log consumer, not just the browser adapter.

Reuse the following tables already verified in that designated base. Their existing names do not authorize reuse of old-base wiring. No new base, blanket schema migration or table cleanup is part of Recognize wiring.

| Purpose | Existing table | ID |
|---|---|---|
| Profile and recognition status | rs_people_test | tbly1PM5iFYqVzKSm |
| Device-to-person relationship | rs_devices_test | tblfkRSJAEMzuzApR |
| Phone aliases | rs_phone_aliases_test | tblgDWKi0Bb6OcoqS |
| Recognition/action event log | rs_recognition_sessions_test | tblWjbASVMIjFLyW8 |

Use current correctly typed fields; preserve legacy fields unchanged. Table names ending in _test do not themselves isolate test records. Maintain an explicit synthetic fixture/record-ID list and isolate these from real identities and message automation.

Core log contract already has session_event_uid, session_uid, event_at, event_type, event_result, person/device/phone_alias links, recognition_status and idempotency_key. Reuse these fields. Record safe error codes and correlation IDs, not PINs, device credentials, raw payloads or unnecessary personal data. Do not expand geo/device collection for this wiring.

## Implementation sequence and gates

### 1. Freeze the exact contract

Capture the current template element IDs/attributes, page code, styles and responsive screenshots. Inventory every visible Recognize control and state. Read effective deployment configuration without exposing secrets; verify routes, mount/origin and table/field types.

Produce one explicit mapping: **native target → response property or form input → action → Airtable field → expected log event**. Map all six existing surfaces: recognized, profile, login, recovery, received and unavailable, including loading and error feedback in their designated areas. Exact runtime selectors are established from the current element tree, not invented from historic IDs.

Example mapping intent:
- Name display ← person_name / first_name from the permitted response.
- Profile inputs ↔ first_name, last_name, person_name, primary_phone_e164, email.
- Person/device UIDs stay application identifiers, not display labels or authorization proof.
- Unknown/rejected/retired device results select the appropriate existing state.
- “Not you” retires the correct device and clears the client state.
- Recovery “received” means only what the backend has actually acknowledged; never imply a message was sent from a queued event.
- Access/consent fields are not inferred from visible success.

Gate: no unmapped control, unresolved write target or undefined success/error meaning before wiring. Record effective base/table IDs for device lookup, actions, sessions and recovery. Verify they all target `app9kOZdIaGyKk5uG`; test missing/mismatched configuration with no data access. End-to-end evidence must identify this base for both stored data and logs. No migration or copying from the older base is implied.

### 2. Wire the existing native elements

Add stable, page-specific data attributes only where needed; preserve native classes and style definitions. Bind one event handler per action. Reuse the existing Astro endpoints and validated logic where correct; inspect root causes before any narrow backend change.

Existing actions to cover: create_profile, update_profile, phone_login, recovery, confirm_device, retire_device; device recognition lookup and session recording. Retain a stable request/event ID across retries. Do not treat session-log deduplication as proof that profile/device mutations are idempotent.

Prevent the prototype script and the new adapter from both owning the same controls. Keep an exact before/after loader record and rollback switch. Preserve other pages and old-client consumers.

Gate: one data-binding owner, no injected styling/replacement interface, exact state/action/data mapping, and no unauthorized cross-person update.

### 3. Test the full deployed path

Use the actual browser interface in an authorized test/staging deployment, synthetic records in Recognize, and the actual Astro runtime. A local mock, direct API call or manually seeded success state cannot replace the full proof.

For each case capture **browser action → request ID → Astro response → Airtable data readback → matching log → displayed result**, plus expected zero-write cases. Never log raw credentials as evidence.

Required cases:
1. New visitor, unknown device and returning recognized device.
2. Create profile, edit profile, refresh and browser revisit; persisted values appear in the designated native fields.
3. Valid and invalid login; ambiguous phone/PIN matches, inactive person, retired device and disallowed access; no unintended disclosure/access.
4. Confirm device, “Not you”, re-entry; cached identity cannot bypass revocation.
5. Recovery from the UI through the configured downstream mechanism, including usable return/redeem behavior if the UI promises a member link. Use authorized test destinations. Queuing a row is not recovery completion.
6. Empty/invalid input, duplicate phone, slow response, timeout, backend/Airtable failure and retry.
7. Double-click, replay and concurrent request: no duplicate profile/device/alias, no duplicate effective action, and correlated logs.
8. Data mutation succeeds but logging fails: preserve the original outcome, show truthful status, retry safely without repeating the mutation. Verify expected audit recovery.
9. Keyboard operation, focus return, close/reopen and navigation; page loading twice must not attach duplicate handlers.
10. Native appearance before/after binding at widths 375,393,479,480,481,767,768,899,900,991,992,1280,1440,1920; white/dark, relevant states, explicit 393×852 mobile. No layout drift from long/empty data or errors.
11. A second clean session and a later returning session independently repeat the critical create/edit/recognize path. Do not repeatedly replay unrelated passing checks.
12. Regression: existing consumers, protected Webflow pages and unrelated application routes remain unchanged.

Gate: every required interaction, runtime, storage/log and visual result has current evidence. If the approved runtime/workflow fails, stop that path and report the exact failure; no manual substitute or production record repair.

### 4. Release and lock Recognize

Owner update, 8 October 2026: the owner will publish the Webflow draft when ready. The implementing runner must provide the exact page/link, passed pre-publish gates and any checks that require publishing, then hand publication to the owner. Do not publish on the owner's behalf. After owner confirmation, verify the deployed version before closing Recognize. This assigns the publication step; it does not start implementation or authorize Astro deployment by itself.

Before releasing, have a reviewable tested change, exact target/origin, rollback artifact and impact list. Webflow publishing and Astro deployment require their own explicit authorization; this planning request does not grant them. If staging cannot execute the necessary page code, select an authorized test deployment before claiming full-system proof.

After the authorized release, repeat the critical browser/data/log checks against the released version. Record template/page identity, code revision, adapter version, deployed configuration identifiers, table IDs, field mapping, complete test matrix and rollback steps together.

**Recognize is locked complete only when all required tests pass, all material deviations are resolved, recovery fulfills the displayed promise, and the released path is verified.** A tested staging build may be labeled ready for release, not fully released/complete. No open required gate may be hidden under “locked.”

Lock means a documented, versioned baseline: later changes require an explicit delta and affected regression tests. It is not a claim that Webflow becomes technically immutable or future failures are impossible.

## Execution discipline

One fresh implementation runner owns Recognize alone; an independent verifier reviews the evidence when implementation is authorized. No new runner is launched by this plan. Keep styling acceptance, data binding and end-to-end verification as explicit gates of the same Recognize outcome; do not hand off an incomplete gate as a complete system.

Use existing cost controls: reconcile prior Recognize elapsed time first; no invented remaining budget or reset on a new chat. Before dispatch, state the remaining authorized time. At most two correction/retest cycles per defect, 20 minutes aggregate corrections within the authorized budget, one supported recovery within five minutes, and reserve the final 15 minutes for verification/checkpoint. A limit produces a saved checkpoint and concrete remaining requirement, never a completion claim.

Rollback restores only the approved page's previous loader/attributes and changed application version after checking for subsequent edits. Preserve audit evidence; never delete real records or reverse other work. Synthetic record cleanup uses the recorded fixture IDs only.

Before implementation map complaints to acceptance: C003/C016/C022 → protected native design and scoped writes; C007/C017 → actual browser/runtime/data/log proof; C011 → exact mapping and source baseline; C039/C061/C062 → executable next action, finite correction budgets and truthful unfinished gates; C059 → accepted handoff with accessible evidence. Use typed visual, interaction, runtime and handoff evidence. These controls document evidence; they do not create correctness.

## Reuse after Recognize

Proceed in the requested order, one bounded project at a time:
1. barn-input
2. barn-onboard
3. user-optin

Each reuses the same Webflow-presentation / Astro-behavior / Airtable-storage boundary and gets its own mapping, tests and locked baseline. Do not assume barn-input and barn-onboard have identical workflows. Confirm their exact page and data responsibilities when each is assigned. Opt-in tests must preserve explicit grant/revoke evidence and must not equate Recognize success with consent. No work on these next projects is started by this plan.

## Technical reference

Astro supports server API endpoints returning data independently of page rendering: [official endpoint documentation](https://docs.astro.build/en/guides/endpoints/), checked 8 October 2026. This plan uses the existing application's endpoints; it requires no framework upgrade.
