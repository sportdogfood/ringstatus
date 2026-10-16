# Recognize and Tenants — owner/User access rules

Owner authorization: edit and test the existing working implementation; no new index or publishing. Latest correction: “users can use pin as well.” This document controls over earlier User PIN prohibition and generic invitation acceptance.

## Owner/User access lock — 15 October 2026

This supersedes older general invitation guidance. RingStatus/admin establishes the Barn and owner. No public signup or new-Barn self-provisioning. Invitations are owner-only and retain the existing expiry/one-use rules. Users may enter with their registered phone OR their explicit four-digit PIN, but must have a phone on file and an active Person and enabled tenant membership. A PIN is not an exception to the phone requirement. Preserve existing collision handling: use an eligible known device or ask for the associated phone; never choose the first match or phone suffix. Existing cookies must obey current eligibility too.

The owner can add or manage prepopulated Users, enable/disable tenant membership and change existing roles. New Users default to the existing `user` role. Nonowners see/edit only their own User profile and cannot edit the Barn, other Users, status or roles. Existing owner role is the current Barn-management permission; do not invent new permission fields. Individual opt-ins remain separate; no consent changes from profile/access edits.

Use existing People, tenant-memberships and roles plus Recognize identity/device tables. No new index, automation, schema, deployment or publishing is authorized for this correction. Source and evidence: `docs/coordination/recognize-tenants-access-rules-20261015.md`. Current source: `C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus/webflow-cloud-test`; do not use the historical deployed-delta worktree as the current correction source.

## Existing implementation preserved

Working source: C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus/webflow-cloud-test, HEAD bf9a93531209212f303d7e8175af5c9fb01ba2bd plus existing uncommitted approved changes. Do not deploy the entire dirty checkout. Previous live two-User release 733587411ad328b5535c4549d9b7a81f9a896389 remains distinct from these local changes.

Preserved WEC/HPS method: immediate visible edit, existing API request, direct Astro-to-Airtable save, returned confirmed record merge, retained same-operation retry. No automation is added to User saves. See tenants-user-profile-verification-20261015.md for the preceding profile-field extension and its evidence.

## Current tables and authority

- Recognize app9kOZdIaGyKk5uG: People tbly1PM5iFYqVzKSm holds existing phone/PIN/invitation and identity; devices tblfkRSJAEMzuzApR retains cookie association. Existing D1 operation/invitation claims are reused.
- Tenants app7BXcw801n19QKF: People tblyTBgrgdhT3Tcg7 holds canonical User and registered phone; tenant-memberships tblFf4aWQYA2RRsWo holds Person → Tenant → Roles and Enabled/Disabled status; roles tblRqRbjmqmNfJtIP supplies existing role IDs.
- Existing People.recognition_person_uid links the two bases. Recognition checks current canonical phone/membership/role eligibility. No new lookup table or combined index.
- Tenant-local status does not delete People, history or opt-ins. Owner can view disabled Users to reactivate them. Existing prepopulated People with stable entity_uid and membership can be edited without requiring a prior form-save event; first save maps the same canonical record.
- SMS/PIN belong to the person; Barn name/logo belong to Barn. Follows, shows, horses, riders, locations and rings extensions remain deferred.

## Correction paths

- rs-tenant-recognition-policy.js reads existing canonical identity/membership/roles; imported by existing recognition and invitation/session adapters.
- rs-inputs-recognition.js retains explicit PIN/phone/device matching with eligibility checks; phone suffix is not a PIN.
- rs-inputs-access.js restricts invitation redemption before consumption, rechecks signed session access and removes general self-create-Barn permission.
- rs-recognition-credential.js rechecks successful same-operation credential replay after deactivation.
- rs-tenant-barn.js reuses direct save, exposes active/inactive Users, defaults new Users to user, validates role/status changes, and rechecks owner-or-self access on saved-result replays after demotion.
- Native Barn adapter/view reuses existing fields and controls for Active/Inactive and roles. Native and API-agent credential interfaces retain the same shared server/controller, including PIN and collision prompts.

## Verification and resume

Local verification completed 15 October 2026. Evidence below is in the active webflow-cloud-test checkout:

- `test-evidence/tenant-access-final-20261015.log`: three integrated checks passed. Assigned Barn, owner profile, two Users, edit/reload; phone and PIN access; owner invite accepted and User invite rejected without consumption; disable/reactivate; phone removal blocks PIN/cookie; self-role escalation denied; default User role; no new Barn signup; replay checks.
- `test-evidence/tenant-access-policy-final-20261015.log`: two final checks passed. Prepopulated canonical User edited without duplication, opt-ins viewed before first save, owner-demotion replay restriction, signed invitation-cookie and credential-replay revocation.
- `test-evidence/tenant-access-regressions-final-20261015.log`: 29 relevant recognition/PIN-collision/invitation/replay, profile-link and direct-save regressions passed. Earlier test fixtures expected no default role and hidden disabled Users; updated to the new authorized rules, not relaxed acceptance.
- `test-evidence/tenant-access-browser-20261015.log`: local Edge actual native controls passed: assigned Barn, owner profile, add two Users, inline edit, immediate feedback before confirmation, retained failure/retry across reload and separate opt-ins.
- `test-evidence/tenant-access-controls-20261015.log`: 22 checks passed, including actual Edge controls enabling a prepopulated User, changing role, saving/reloading and PIN entry.
- `test-evidence/tenant-access-build-final-20261015.log`: final Astro build passed. Native bundle `--check` passed.
- `test-evidence/tenant-access-source-20261015.json`: source SHA-256 inventory; not deployment authority.

Existing independent reviewer `/root/tenants_cause_review` identified a saved-result replay visibility gap after owner demotion and an unmapped prepopulated-User opt-in lookup gap. Both were corrected in their existing paths, tested and rechecked. No remaining blocker in that scoped review; unrelated application code is not certified.

No live business records, automations, index, schema or deployments changed. Tests use real handlers and archived page markup with synthetic Airtable/D1. Native and API-agent retain the same credential controller and updated shared server. A hosted API-agent/model conversation and deployed acceptance remain untested for this revision.

Both bases' system checkpoints were updated and read back. Agents activity: https://airtable.com/appZahVgD156cMAe3/tblITeJfxlIhlDvGk/rec0QCDjiinezW3zx . Both custom skills now include the dated lock.

Exact next action: preserve this tested source. On a later explicit release instruction, isolate the approved package and verify the hosted native/API-agent owner/User journey. Do not restart discovery, create an index, publish or expand follows/other entities. Prior live two-User proof remains historical and does not establish that these new rules are deployed.
