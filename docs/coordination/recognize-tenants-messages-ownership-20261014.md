# Recognize, Tenants and Messages — owner-confirmed responsibilities

**Latest authorization:** owner subsequently approved remaining corrections and restarting scoped work; [current assignments and write boundaries](three-area-corrections-resume-20261014.md) supersede historical stopped-work statements below. No publishing or deployment.

Owner decision: 14 October 2026. This document supersedes earlier assignment of the alerts page to Tenants. Ownership changes do not move data, rewrite code or authorize release.

| Area | Owns | Connection |
| --- | --- | --- |
| Recognize | Silent identity, phone/PIN/invite entry, cookies, sessions, footprints, account switching; optional SMS recognition journey | Supplies the validated person/device identity and returns to the requested permitted page. Does not grant tenant membership or notification consent. |
| Tenants | Barn container, people, roles, membership, records, input forms, assignments, favorites; canonical opt-ins and individual options | Supplies permitted tenant/person context and saved preferences. Preserves consent history and existing submission/confirmation processing. |
| Messages | `/rs-barn-onboarding-alerts`, its preference-input experience and completion; alert processing, outbound SMS, inbound two-way replies, logs and notifications feed | Uses Recognize identity and Tenants permissions/saved opt-ins. Page ownership does not relocate the preferences tables or make a pending save confirmed. |

## Alerts flow

Known visitor → silent Recognize/access → stay on alerts → load own permitted preferences → immediate edited display → Save → existing Astro intake/processor → confirmed Tenants target → Saved → reload same values.

Unknown visitor → existing Recognize entry → identify → return to alerts. Failed save → retain draft and original operation → Retry. Account switch hides the previous person's draft and prevents replay under another person. Opt-out preserves prior consent events. No extra consent prompts. Saving preferences does not send SMS or prove delivery.

Page ID `6ac459b4506821bd349ed047`; slug `/rs-barn-onboarding-alerts`; Webflow site `6982268b7543ac3c80151266`. Metadata verified earlier this session. Native Webflow remains presentation; Astro remains the data/action delivery method.

## Existing owners and handoff

- Messages completion owner: `rs-messages`, chat `01a12690-e713-7fa1-8787-87b45eceb1a3`; base `apphwDLkVCp8ut2Hd`, system `tblycxFymN5gaPKlT/recjGDLwQ5U0VliiE`.
- Tenants data/processor dependency owner: `rs-tenants`, chat `01a12690-0e9d-71f0-bb85-000d02307e8d`; base `app7BXcw801n19QKF`, system `tblMKWJw8Q6joKc8U/recJzisHwiPPkKG6y`.
- Recognize identity dependency owner: `rs-recognize`, chat `01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5`; base `app9kOZdIaGyKk5uG`, system `tblTlw48kcRoEX6yf/recB69Oe39xZ0nsAe`.

Messages owns alerts acceptance and coordinates any necessary existing shared-source correction with Tenants/Recognize. One writer per overlapping source/release. Tenants must not independently continue the alerts implementation after this transfer. No new agent, base or duplicate preference store is required.

## Exact checkpoint retained

Source: `C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus/webflow-cloud-test`; baseline `bf9a93531209212f303d7e8175af5c9fb01ba2bd`; preserve all prior unpublished corrections.

Read `tenants-immediate-feedback-20261014.md`, `recognize-agent-parity-20261014.md`, and `recognize-tenants-bindings-20261014.md` only as needed for the shared boundaries. The current alerts findings are in `.git/ringstatus-control/tenants-immediate-release-20261014/alerts-checkpoint.json`; exact release actions in the adjacent `RELEASE-ACTION.md`.

Latest alerts result: build passed; local suite 11/34 passed, 23 failed. Review identified missing Saved feedback and two fixture contract errors. Unknown recognition/return, exact-page canonical save/reload/opt-out/history, failure/reload/same-ID retry, actor isolation and desktop/mobile presentation remain unverified. Existing preferences processor `wflUVrvlg5XZVzcz6` was read as undeployed/OFF; unchanged by this ownership update. The previous verified Barn/Rider package is retained as `deployment-source.pre-alerts.zip`; it is not alerts proof.

This documentation assignment does not clear those findings or restart product work. Existing recorded correction/release approval boundaries remain. Next authorized implementation belongs to Messages and must reuse the prepared source/tests, not repeat discovery or rebuild the form. Messages presents the concrete correction/release action after reviewing that checkpoint; no deployment, publishing, activation, live record changes or SMS authorized here.

## Skills and connections

All owners read their current applicable instructions and the shared ownership map. Messages reads `C:/Users/gombc/.codex/skills/rs-coordinator/SKILL.md` and its existing Messages handoff; load the relevant sections of `recognize/SKILL.md` and `tenants/SKILL.md` from that same skill directory for these interfaces. Frontend testing/code review applies to actual changes; Webflow/Astro guidance only for the corresponding surface. Use Airtable connector for operating records, existing Webflow MCP for the page, existing Astro runtime/processor for saves. Twilio/Cloudflare delivery work is not required to test preference input. Do not claim a skill loaded merely because it is listed.

## Verified ownership receipts

All three system records updated and exact changed field values read back. Agents task `reccAPPd1hMxt22z9` in `tbluT1yhP1FJMb8Cm` assigns alerts completion to rs-messages and preserves the existing implementation blocker; it is not marked product-complete.

- Messages receipt turn `01a13d26-e1b7-7180-b2a1-7ee8d52cee72`: accepts alerts completion and Recognize/Tenants dependencies; preserves source/failures and stopped work.
- Tenants receipt turn `01a13d28-03c0-7943-8b5a-7cabb57be578`: read ownership packet, handoff and skill boundary; retains Barn/Rider and data processor; will not independently continue alerts edits.
- Recognize receipt turn `01a13d28-80cb-7aa3-9334-2159d348b7ef`: read packet/handoff; retains identity/session/return; acknowledges single-writer shared-source rule.

These are actual guidance acknowledgments, not code execution or workflow proof. No operational work restarted.
