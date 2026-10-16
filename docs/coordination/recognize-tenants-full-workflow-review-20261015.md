# Recognize → Tenants full-workflow source review — 15 October 2026

Correction follow-up: `recognize-tenants-review-corrections-20261015.md` records the subsequent owner-authorized local changes, exact test results, independent re-review and remaining release limits. Findings below preserve the original review snapshot.

## Outcome and review method

Three actionable findings remain before the current implementation can be accepted. No application code was changed, no business endpoints or jobs were executed, and nothing was deployed or published.

The requested dedicated `codex review` command was attempted but did not complete: installed CLI 0.145.0 was rejected with HTTP 400 because its configured gpt-6-astra model requires a newer CLI. The exposed Code Review MCP tool only reads PR CI diagnostics. Neither is recorded as a successful review. An independent read-only reviewer subsequently inspected the full workflow assignment using the Requesting Code Review skill; the parent checked the cited implementation paths. These are source findings, not live exploit or browser-test results.

Assignment and tool evidence: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/recognize-tenants-full-review-20261015/`. Files: `prompt.txt`, `tool-result.txt`, `source-before.json`, `source-verification.json`. Source hash comparison: 157 source files, zero changes during review.

Current implementation: `C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus/webflow-cloud-test`, HEAD `bf9a93531209212f303d7e8175af5c9fb01ba2bd` plus existing dirty changes. This is not the previously deployed two-User release `733587411ad328b5535c4549d9b7a81f9a896389`.

Acceptance sources: `recognize-tenants-access-rules-20261015.md`, `tenants-user-profile-verification-20261015.md`, and the owner's current workflow: owner-only invitations, existing assigned Barn, registered-phone-required Users who may use explicit PIN, active membership/roles, owner/self profile edits, individual opt-in links, immediate UI and safe retained retry using WEC/HPS behavior. Future follows and other entity links are deferred.

## 1. P1 — Reachable legacy login bypasses explicit PIN rules

`src/pages/rs-recognition/action.js:17` directly invokes the legacy action handler. `src/lib/rs-recognition-action.js:157–178` accepts a four-digit identifier matching explicit PIN **or the last four phone digits**, then associates a caller-provided device token. The configuration targets the same Recognize base. The native facade accepts a body device token when there is no device cookie (`src/lib/rs-recognition-native.js:69`) and, after eligibility validation, sets its recognition cookie (`:101`).

Impact: an eligible existing member with a uniquely matching phone suffix can be recognized through the old route without following the agreed explicit PIN rules. Disabled or unlinked identities are still denied by the current tenant policy; this is not a universal membership bypass. The public legacy route also permits unauthenticated Recognize `create_profile` writes, though those identities do not automatically gain Tenants membership.

Smallest correction direction: enforce the existing current credential/access policy at all reachable public entry points, or disable unsupported public actions. Remove phone-suffix authentication from reachable paths. Preserve approved phone-only recognition; do not introduce OTP requirements or a replacement identity system.

Required proof: old public route cannot bypass current rules or create unapproved identities; normal phone, explicit PIN/collision, owner invite, disabled User and cookie-return cases still behave correctly.

## 2. P1 — A server-side save failure can permanently strand Retry

`src/lib/rs-tenant-barn.js:338–348` reserves operation and entity revision before `applyDirect`. `applyDirect` performs another preflight at `:254`, including fresh reads. A transient read failure can therefore occur after reservation but before any Airtable write. The next identical request sees an incomplete operation at `:331` and returns `write_outcome_unknown`; a new request cannot acquire the retained entity revision. This path has no release or reconciliation to complete the failed operation.

Impact: retaining the browser draft does not make actual server failures retryable. Existing tests proving a connection failure before server execution do not cover this case. If failure occurs later during a phone change, Recognize credentials may have changed (`:260`) before canonical Tenants People (`:263`). A nonowner then fails the phone equality policy in `src/lib/rs-tenant-recognition-policy.js:19` and cannot access Retry. Owners are exempt from this equality comparison, so owner lockout is not claimed.

Smallest correction direction: safely release proven zero-effect failures and reconcile uncertain/partial effects within the existing direct-save operation. Preserve records, latest edits, request identity and cross-base phone consistency; do not add another processing architecture.

Required proof: inject failures after reservation, between identity and People writes, and after canonical save; same-operation retry completes without duplication, lost edits or unintended access. Repeat the exact assigned Barn → User 1 → User 2 → return → inline-edit User 1 → save → reload workflow.

## 3. P2 — A new User's personal opt-in link has no first-use path

`src/lib/rs-tenant-barn.js:294` supplies `/rs-barn-onboarding-alerts` as the self User's edit link even with no opt-in records. Its actual handler (`src/lib/rs-input-subscriptions.js:32–42`) routes personal preferences into `rs-tenant-submissions.js`. `targetFor` at `:22–23` requires an existing personal preference record; reads at `:51` reject its absence. Saves additionally require exactly 18 existing option rows and prior audit at `:56–57`. User creation at `rs-tenant-barn.js:263–269` does not provision these records.

Impact: a newly created User can enter Tenants but cannot initialize personal opt-ins through the supplied link. Existing fully prepared preference records are a different case. This finding concerns the new User's destination, not SMS delivery or missing consent activation.

Smallest correction direction: support first use through the existing preference implementation, or define an explicit authorized provisioning handoff before exposing the link. Never grant/copy consent during profile creation. Messages owns the alerts destination; Tenants owns User/relationship context.

Required proof: a newly added User with no preseeded preferences opens their own opt-in page, makes an explicit choice, saves/reloads it, and another User's preferences remain untouched.

## Coverage and acceptance still needed

Inspected: current acceptance docs and dirty source; recognition configuration, public action, native/shared credential flow; invitation/session eligibility; owner/self authorization and roles/status; profile credential writes; direct save/replay; client persistence and confirmed-record merge; selected-User opt-ins and actual destination; API-agent controller adapter; original WEC and HPS save behavior.

Partially inspected: session-summary/footprint implementation, public silent bootstrap integration, generated bundle parity and older queued submission paths. This bounded review is not an exhaustive certification.

Still unverified for the latest revision: actual footer mounting and silent behavior on all three public URLs; native desktop/mobile journey; hosted OpenAI API-agent parity; live schema and persistence; alerts processing and new-User first use; deployed acceptance. Earlier local builds and historical live tests do not establish these gates for this revision.

Next correction order: close public entry-point inconsistency; repair existing save/retry recovery; resolve new-User opt-in first use. Re-review and test those exact paths, then obtain release authorization and verify the agreed hosted workflow. This review authorizes no edits, deployment, publishing, schema changes or operational restart.
