# Recognize native / API-agent parity — 14 October 2026

## Owner decision and delivered scope
A unique eligible known phone alone identifies the person on a new browser. No mandatory PIN or SMS. This is owner-approved identification by a known number, not proof of phone possession. Tenants still enforces existing membership and permissions. Both interfaces use the same server/controller actions; the model does not decide identity or access.

Phone: ten digits; existing 11-digit/+1 representations resolve identically. SMS adds +1 once. PIN: four digits, leading zeroes preserved, nonunique; ambiguous matches require the phone or an existing validated device association. Never use a phone suffix or first matching record. Invitation: seven digits, existing hash/expiry/one-use validation; legacy 43-character links retained. Existing invitation lifecycle is preserved, not redesigned. Explicit optional SMS remains available. Five failed attempts per ten-minute IP bucket; successful recognitions do not consume the failed-guess budget.

## Sources and changes
Isolated checkout: C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus
App: webflow-cloud-test
Baseline: bf9a93531209212f303d7e8175af5c9fb01ba2bd.
Original API experiment: C:/Users/gombc/.codex/recognize-agent-test-20261013, unchanged.
Existing experiment source was copied without credentials/configuration into webflow-cloud-test/recognize-agent for this release source. No replacement auth service, infrastructure or framework.

- src/lib/rs-recognition-credential.js reuses existing Airtable recognition/access and D1 claims for shared phone/PIN/invitation actions and replay.
- rs-inputs-recognition.js adds explicit candidate matching and safe retired-device replay lookup; rs-inputs-access.js accepts seven-digit invitations while preserving existing link handling.
- rs-recognition-native.js routes credentials, preserves intended 365-day secure cookie, clears the previous Inputs actor cookie on account change, and retains existing session/footprint paths.
- src/assets/rs-recognition/native-client.js provides shared state/actions, silent known-cookie entry despite unavailable browser storage, optional explicit SMS, identity-to-Tenants actor consistency, bounded access wait and retry, and a validated local destination.
- recognize-agent policy/server/extension use that controller rather than label/URL guesses. The panel starts collapsed. Silent recognition makes no model call. Credentials stay out of model context. Guidance cannot submit credentials, send SMS, grant membership or navigate independently; user actions perform existing shared commands.
- Recognize skill updated to the latest owner rule; old SMS-first guidance superseded.

## Verification evidence
47 focused credential, regression, cadence and existing recognition checks passed (agent-unit-tests.log).
18 browser integration checks passed after the access timeout/retry fixes (agent-all-interfaces-tests.log): phone/PIN/invite/explicit SMS across native and API-agent adapter, desktop/mobile viewport; through existing Tenants access, preference processor, save/reload, consent preservation, rejected actor mismatch and silent revisit. Additional tests cover uncertain dirty writes and access timeout followed by adapter retry.
Astro build passed (agent-parity-build.log); existing dynamic [slug].astro getStaticPaths warning remains.
Independent read-only review identified account-switch, retirement replay, retry parity, busy state and access timeout issues; fixes were included and regression checked. Final independent source recheck confirmed both remaining timeout/retry findings resolved; reviewer did not rerun tests.

## Proof boundary and exact next action
These are local executable fixtures with synthetic Webflow markup and simulated provider/storage data; the actual handlers and processor run. They are not current published-page acceptance, hosted mobile agent proof, paid OpenAI model execution, live Airtable writes or SMS delivery. Existing local extension/localhost hosting remains the experiment delivery method; mobile viewport coverage does not prove an extension runs on a phone.
No deployment, publication, automation activation, real SMS, environment/secret changes or business record edits in this parity batch. Tracking records are separate.
Keep no-publish boundary. At authorized release, use this exact manifest with prior Tenants changes, validate current runtime schema and active script compatibility, release source/agent together as applicable, and verify actual published native and agent journeys, role-scoped Tenants load/save/reload, known silent return, unknown entry, account switching and optional SMS. Do not restart discovery or rewrite the workflow. Broader cache/refresh cadence and all-session lifecycle proposals are not newly claimed implemented here.

Previous binding batch and evidence: recognize-tenants-bindings-20261014.md. Shared workflow review: recognize-shared-workflow-review-20261014.md.
