# Recognize / Tenants V1 — authorized completion attempt

Window: 2026-10-15 22:17:42–22:47:42 UTC. One 30-minute total limit, including independent review and deployment. No automatic extension.

## Result

PARTIAL. Corrections were built, reviewed and deployed. Complete V1 and the requested fresh nonowner end-to-end live workflow are not yet verified. This document does not convert local fixture results into live acceptance.

Baseline: `0dec58fb39ab35cab5c2a4fd830783dab0257f00`.
Released commit: `fef5ea561305d3b6634260ef4fcef7b24bcb214e`.
Webflow Cloud deployment: `836dc9a8-3ac7-45a2-a81a-67a8d0e70fd7`, success at 22:40:44.573Z, serving under `/test`.
Release contains 16 explicitly selected files; it was not a deployment of the entire dirty correction checkout.

## Corrections delivered

- Tenant saves persist the local operation and update visible data before background actor validation/network persistence. Editing can continue while a save is pending.
- Per-record sequencing preserves independent interaction, rebases a later save onto the confirmed revision, and prevents an older response from replacing newer edits. Pending adds retain their canonical identity on confirmation.
- Alert controls remain usable during background persistence; a subsequent explicit Save queues the newer draft without falsely confirming pending consent.
- Active Barn eligibility applies to recognition and returning access. An active User with empty Roles can access their own profile and alerts without gaining tenant-management authority.
- Native profile editing routes through Tenants. Legacy native profile writes and SMS/OTP/recovery actions are rejected in this native recognition flow.
- Existing actor isolation, retry identity and legacy save callbacks were preserved.

## Verification

| Gate | Evidence and limit |
|---|---|
| Build and release | PASS: local Astro build; deployment status success for the exact commit above. |
| Access and correction tests | PASS: 12 focused tests covering native restrictions, inactive Barn, own-only access and existing correction cases. |
| Persistence and recovery tests | PASS: 21 focused tests covering retained operations, actor boundary and previous save defects. |
| Proactive profile interaction | PASS locally at mobile width 390: second edit/save while first response was held, revision rebasing, newer unsaved text preservation, confirmed reload. Not live proof. |
| Alerts interaction | PASS locally: new User with empty options; mobile save/opt-out/history/retry/isolation; desktop isolated rerun. Earlier concurrent desktop run timed out; rerun passed. |
| Independent review | No blocking new regression identified against the baseline. Reviewer independently confirmed delayed alert saves, pending-add edits, actor isolation, empty-role own-only access and inactive-Barn checks. Complete V1 remains partial. |
| Protected two-User proof | Preserved and not repeated. Existing evidence remains in the earlier handoff/evidence directory. |
| Fresh nonowner live phone recognition → own profile → own opt-ins | NOT VERIFIED. Owner entered the test phone and Continue, but could not provide the resulting visible state. The private Edge window is not exposed by the browser connection. The attempted in-app browser opened an already-recognized owner context, so it was not used as nonowner proof. Available browser capabilities did not expose an isolated context; no owner-session retirement or direct API substitute was attempted. |
| Live proactive interaction, replacement profile credentials, own-user isolation and opt-in persistence | NOT VERIFIED on the new deployment. Local tests are not substitutes. |
| Live PIN / owner invite | Deferred by owner. |

## Remaining implementation gap

Independent review found personal opt-ins still lack saved tenant/membership context; actor identity alone does not satisfy section 2's explicit person-and-tenant binding. This is not claimed complete.

Follow-up source verification within the original window confirms this is **missing implementation, not merely missing live evidence**:

- `webflow-cloud-test/src/lib/rs-input-preferences.js:28` derives the preference record ID from the actor alone; its record construction at line 62 stores Person/owner identity without Barn or membership.
- `webflow-cloud-test/src/lib/rs-input-subscription-store.js:14` explicitly rejects personal records carrying `barn_uid` or `user_uid`. Its immutable-scope check at line 67 also prevents simply attaching a different scope to an existing row.
- `webflow-cloud-test/src/lib/rs-tenant-submissions.js:22–35` derives the same actor-only target and rejects populated Barn/User/Membership scope, including in staged submissions and receipt validation.
- `webflow-cloud-test/src/lib/rs-input-subscriptions.js:44–60` routes existing preference records between these direct and staged writers. Updating only one writer would leave existing records incompatible.
- `webflow-cloud-test/src/assets/rs-input-subscriptions/native-client.js:140` scopes device persistence with `tenantId: 'personal'`, not a resolved Barn/membership.

Existing fields can express tenant context; a new schema is not the issue. A safe correction must consistently resolve the eligible membership and adapt both existing write/read/receipt paths and pending-device scope while preserving existing consent/history and retry identity. A single added field would be rejected or leave inconsistent behavior. That correction and its compatibility/retry verification cannot safely finish in the remaining original window. No speculative patch or additional deployment was made.

Pending tenant changes are retained with manual Retry. Automatic replay after load/online remains absent; retained retry is verified locally, automatic recovery is not delivered.

## Preserved boundaries and handoff

- API-agent untouched; no static Webflow publication; no SMS sending; no repeated two-User proof.
- Existing protected Users, memberships and unrelated drafts were not cleared or repaired.
- The owner separately authorized activation of test Barn `recsJkP55Xps5nSL8`; the coordinating chat reported its checkbox update and readback. This agent did not repeat it.
- Single live proof target remains existing test User phone `6313757752`, Person `recfl24ktSvsf6R0q`, membership `recnPR1cvvKp9eqGR`. Do not create a replacement User or repeat discovery.
- Correction checkout: `C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus`; it retains preexisting unrelated changes. Do not reset or deploy the whole checkout.
- Release manifest: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/recognize-tenants-full-review-20261015/v1-release-2239.json`.
- Owner cannot support further manual browser troubleshooting; do not ask them to repeat sign-in or inspect developer tools. The unresolved prerequisite is a controllable isolated browser session. Once available under renewed authorization, verify only the existing User's recognition, own-profile save/reload and explicit own-alert preference save. New work beyond the fixed window requires a new owner instruction.
