# Tenant profile and Users — 15 October 2026

## Current boundary

Owner approved edits and local tests only: recognized phone/PIN/invite -> assigned Barn -> owner profile and User fields, roles and individual opt-in links. Owner clarified that SMS/PIN belong to the recognized owner, while the Barn keeps its own name/logo. No deployment, publishing, automation activation, SMS or live business-record test was performed for this extension.

This extends the earlier local profile-link correction. The previous live two-User proof remains historical, at release `733587411ad328b5535c4549d9b7a81f9a896389`, deployment `f3d7efc7-3e77-4739-934b-b8a997f7530e`. Do not describe the extension as live.

## Existing method retained

Working repository references: `webflow/packing-worksheet/wec-packing.js` (`saveInlineEdit`, line 541; `postAction`, line 898; confirmed record merge, line 1049; failed-action persistence, line 1141) and `webflow/hps/hps.js`. The existing Tenants adapter retains immediate feedback -> direct Astro/Airtable save -> verified returned-record merge, with retained request/retry. No intake automation was added to this path. Historical WEC proof at `647ea24f8` is a reference, not evidence of the current release.

## Implemented

- Barn name/logo remain on the Barn. Owner profile fields are on the existing Tenants Person; PIN stays in Recognize. Editing another User never rebinds the owner's device or session.
- Existing User add/edit now includes name, display name, SMS, PIN, image URL, membership roles and descriptive tags. SMS normalizes to +1; a nonblank PIN is exactly four digits. Blank PIN preserves the existing PIN and is never refilled or returned in User state.
- Unique phone/alias checks use existing Recognize helpers. User identity is linked to Tenants by its stable recognition person UID, with the existing enabled membership and assigned Barn. No name-based identity matching.
- Owner manages Barn/User fields and membership roles. An enabled non-owner can load the assigned Barn and edit their own User; they cannot edit another User/Barn or grant themselves roles. Broader staff/rider/groom assignment workflows remain deferred.
- Opt-ins opens the selected User's own saved records/options. Owner viewing another User does not accidentally open the owner's own alerts preferences. The signed-in User receives the existing Edit my opt-ins link. No consent is granted, revoked or copied by a profile save; on-behalf consent editing was not added.
- Nonunique PINs remain allowed as requested. Existing ambiguous-PIN handling asks for phone/context instead of guessing an identity. Seven-digit invitation handling remains in the existing credential flow.
- Existing failed requests retain the exact payload/request ID when reopened through the expanded form, including intentionally blank PIN controls. Newly added empty fields do not invalidate the old retry.
- Image inputs use HTTPS URLs and existing image fields; no upload service was introduced.

## Schema readback

Three missing fields were added to Tenants `people` (`app7BXcw801n19QKF/tblyTBgrgdhT3Tcg7`) and read back through the connector:

| Field | ID | Type |
| --- | --- | --- |
| display-name | fldMVQzpXUfyJ5hOo | singleLineText |
| profile-img | fldBWGq9WplW0F4cU | url |
| tags | fld4pcenmJhzp1Tzf | singleLineText |

Existing phone, recognition UID, membership Roles, Barn name/logo and opt-in fields were reused. No tables or live business records were created for testing.

## Verified local evidence

Application handlers ran against synthetic Airtable/D1 fixtures, not the live bases:

- Phone, PIN and invite each recognize a User and load the assigned Barn. Owner profile save, two User creates, return/edit/reload, same-operation replay, preserved owner device, membership isolation and selected individual opt-ins passed.
- Edge browser test used the archived native Webflow markup and current generated client, with real application handlers and fixture storage. Visible clicks proved automatic assigned Barn, owner fields, Barn logo, two Users, return, inline edit, immediate feedback before confirmation, injected failed connection, reload, same-request retry, confirmed reload and the selected User's opt-in display. No page errors.
- The browser fixture removes remote scripts/styles for deterministic functional testing. Screenshots are functional evidence, not current published CSS or full mobile visual acceptance. A desktop viewport capture is not a separate complete desktop journey.
- 37 targeted existing/new regressions passed; production build and generated bundle consistency passed. The separate handler journey and visible browser journey passed.
- Independent read-only source re-review found no actionable blocker in this scope. That review did not independently operate the browser or verify live deployment.

Checkout: `C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus/webflow-cloud-test` (HEAD `bf9a93531209212f303d7e8175af5c9fb01ba2bd`, shared uncommitted changes).

Evidence there:
- `test-evidence/user-profile-tests.log`
- `test-evidence/user-profile-browser.log`
- `test-evidence/user-profile-regressions.log`
- `test-evidence/user-profile-build.log`
- `test-evidence/tenants-user-profile-mobile.png`
- `test-evidence/tenants-user-profile-desktop.png`

## Exact continuation

Local authorized implementation/testing is the current scope. Release remains unapproved. After explicit release authorization, isolate only the verified profile/link files from the shared checkout, deploy that package and verify the same workflow with live persistence and current page styling. Do not restart prior agents, repeat inventory, bulk-push this checkout or resume historical automation processors.

Scoped source: `src/lib/rs-tenant-profile-fields.js`, `rs-tenant-barn.js`, `rs-recognition-action.js`, `rs-inputs.js`, `rs-inputs-access.js`; `src/pages/rs-inputs/[operation].js`; `src/components/rs-onboarding/api.ts`; `src/assets/rs-barn-onboarding/barn-native-adapter.mjs`, `barn-native-view.mjs`, generated `native-client.js`. Tests: `rs-tenants-user-profile.test.js`, `tenants-user-profile-browser.test.js`, `tenants-pending-record-recovery.test.js`, updated access expectations/DOM fixture in `rs-tenants-barn-correction.test.js`, and `test-support/tenant-profile-fixture.mjs`.

Follows, show/rider/horse/location/ring links, broader entity workflows and removal of Setup/Add Barn remain outside this assignment. Alerts delivery and on-behalf consent management are not certified by this profile extension.
