# Recognize / Tenants binding correction — 14 October 2026

**Latest implementation checkpoint:** [Native/API-agent parity](recognize-agent-parity-20261014.md). Owner subsequently approved unique known-phone recognition without mandatory PIN/SMS. Shared local source and fixture verification now exist; no deployment/publication. Earlier proposal/unresolved-policy statements below are historical where superseded by this checkpoint.


## Approved scope and result

Owner approved edits using existing working code, preservation of records and consent history, and testing both interfaces through Tenants. No publishing. This batch addresses the two named table-wiring gaps and the related hardcoded owner-role lookup. It does not implement the separate proposed recognition redesign, phone/PIN policy, hosted API-agent UI, or proposed cache cadence.

Implemented and locally verified. No application deployment, Webflow change/publication, automation activation, real SMS, credential/environment change or live business-record write occurred. The only live schema change was renaming the existing active SMS event table, retaining its ID and records. Tracking records were updated separately.

## Exact sources

- Isolated attached checkout: `C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus`.
- App: `webflow-cloud-test` inside that checkout.
- Exact deployed-source baseline: `bf9a93531209212f303d7e8175af5c9fb01ba2bd`, from the October 11 release evidence. Main checkout and other workers' changes were not overlaid.
- Reused preferences implementation: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/tenants-connections-20261010/release/source/`, plus its existing `customScript.js`. This was a prepared source-only package, not a previously verified live preferences deployment.
- Existing agent adapter: `C:/Users/gombc/.codex/recognize-agent-test-20261013/extension/adapter.js`, unchanged.
- Exact changed/new source hashes: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/recognize-tenants-bindings-20261014-manifest.json`.

## Changes made

1. Recognize `app9kOZdIaGyKk5uG`, table `tblF1Hdqi3yoKTZPV`: `___rs-sms-events` renamed to `sms-events`. Runtime and live automation use the stable table-ID/endpoint path; no name-based dependency was found in the affected implementation. Schema rename and index read back. No event rows moved/deleted.
2. Subscription destination uses Tenants `app7BXcw801n19QKF`: `optins` (`tblCX61u6pjSLrP5S`), `events` (`tbluecPFIorOytASP`), `optin-options` (`tblFHcfIFjGuoE2hp`) and existing `input-submissions` (`tblldMTDEC697hPwE`). Personal preferences reuse the prepared intake -> existing Airtable processor -> authenticated completion/readback flow. The native client retains Pending until confirmed. The existing engine-subscription operation remains its existing operation with the corrected destination; no alert dispatch or engine semantics were added.
3. Barn bridge and existing Barn/Rider processor resolve the unique `owner` row in `roles` (`tblRqRbjmqmNfJtIP`) and an Enabled person/tenant membership (`tblFf4aWQYA2RRsWo`), replacing the hardcoded role record ID. Global descriptive roles do not grant tenant rights. No new staff/admin/trainer permission policy was invented.
4. Integrated the staged server check for `X-RS-Actor-Scope` so cached changes cannot cross accounts. Independent review also caught a rejected-save lifecycle issue: an explicitly retried, pre-admission revision rejection now refreshes the revision and preserves the draft under a new operation; uncertain/admitted operations keep their exact request. Completed polling state is cleared. This is covered by the actual native-client regression test.

## Records and consent preserved

Read-only comparison before implementation: old Recognize subscription `recbLNY3M6un3KEHH` and existing Tenants opt-in `recGOGga4HDUSIZXL` have identical common fields, including Revoked consent/status, revision 2, timestamps and preferences. No duplicate/migration was needed.

Both historical subscription events already exist in Tenants with identical fields:
- `recheJznjJDU8qEVy` -> `rece3rHEgdScUjPo6` (grant).
- `reccQODuah23uVe7v` -> `recnWtlfhxfX0Z2kN` (revoke).

All 18 existing option rows are uniquely keyed and link only to the existing target. These observations do not prove future live writes. Existing Recognize copies remain untouched and must not be deleted during release. A future release must recheck for new source writes since this snapshot before switching the writer.

## Verification

Final scoped suite: **97 passed, 0 failed**. Final Astro build succeeded. Build warning about unchanged `[slug].astro` getStaticPaths remains unrelated.

Evidence in the isolated app directory: `correction-tests.log`, `correction-build.log`, `native-persistence-tests.log`.

- Four real headless Edge journeys: native and existing agent adapter, each at desktop/mobile viewport. Actual native client, OTP/device handlers, cookie issuance, session/footprint writers and Tenants access/read handlers execute against local fixture storage. Known return redirects silently; the same browser identity reads Tenants.
- Actual subscription HTTP handler -> actual saved Airtable processor script in a VM -> actual claim/completion handler -> readback. Grant/save/reload/revoke/replay preserves the prior event and produces no duplicate operation.
- Actual native preferences client VM tests cover durable drafts, pending confirmation, account change, uncertain replies and explicit recovery from a revision rejection after an earlier successful async save.
- Existing native Barn/Rider integration executes the actual processor and tests add/edit/readback/replay plus dynamic owner role, disabled membership, staff denial, another tenant and ambiguous role rejection.
- Independent reviewer `/root/bindings_review` found the actor-scope and save lifecycle issues, then verified the corrections and independently reran the 8 native-client tests. No remaining actionable finding in that focused review.

These are **local integration proofs**, not a live site/model/provider result. Browser tests use synthetic page controls; Airtable, D1 and SMS delivery are local fixtures. The existing API-agent adapter is exercised, not a paid OpenAI model call or a newly hosted/mobile API-agent product. No new SMS was sent. Initial harness failures (missing bundled Chromium, invalid enabled-with-no-alert fixture, wrong expected error label) were corrected in tests; no live workflow was run or retried.

## Resume / release boundary

Implementation and local verification for this correction batch are complete; unpublished runtime acceptance remains open by owner instruction.

When release is authorized:
1. Revalidate deployed baseline and current destination/source consent records; preserve any new source consent or queued operation. No blind replay/migration.
2. Release only the manifest's app files from this isolated checkout, coordinating the matching existing Barn/Rider processor source and the existing preferences automation `wflUVrvlg5XZVzcz6` (currently OFF). Do not activate it or alter the deployed Barn/Rider automation `wflEeG5BOUO3vbaW0` under this no-publish instruction. Ensure the processor trigger selects only its personal-preferences submissions.
3. Verify normal live native and agent-assisted identity -> tenant read/edit/save/reload and opt-out/history through the intended page/automation path. Local fixtures are not this acceptance.
4. If a release must be reverted, restore the exact prior app/processor versions under authorization; preserve consent/events. Never restore an older consent snapshot over later user decisions. The active `sms-events` name does not require rollback because runtime uses its stable ID.

No stopped operational worker or scheduler was restarted. Relevant skills read: Recognize, Tenants and Verification Before Completion. Context checkpoint performed using this chat's telemetry; high context was observed, so this file plus manifest is the bounded continuation record.

## Registry readback

Existing Recognize/Tenants system and affected tasks updated with this scope, evidence and release boundary; broader acceptance remains open. Activity read back after creation:
- Recognize: https://airtable.com/app9kOZdIaGyKk5uG/tblMYsOaJyiHq7qhw/reciGEhvMfMAszPGX
- Tenants: https://airtable.com/app7BXcw801n19QKF/tblWYivXHL6TWNjOc/rec5vRNOzoagnetHn
- Agents: https://airtable.com/appZahVgD156cMAe3/tblITeJfxlIhlDvGk/recBQylIT8o7zhcmW

Recognize index row `recA3v7j18KvYU5yc` now names `sms-events`, category `runtime`, with the deprecation flag cleared and dependency explanation. Original index history fields retained.
