# RingStatus Recognize and User Inputs implementation

This is an **isolated implementation and storage trial**, not a deployed production system. It implements the accepted barn onboarding model in the existing Astro application. WEF is the first context; no entity identifier or input rule is tied to WEF/WEC. The infrastructure inventory base remains read-only except the explicitly authorized ownership/complaint records.

**Current direction (owner instruction, October 5):** the owner supplied `app9kOZdIaGyKk5uG` (`recognize`) as the owned target. Build only the fields and tables required by the existing Recognize and accepted input flow. The earlier interim test used `apptdhhNzduxm5gjn`; it remains historical evidence, not the new target. Zoho remains deferred. No runtime binding has been switched yet.

## What is built

- `/onboarding` hosts the existing React onboarding model, including its inline and nested editors, White/Dark presentation, and mobile runtime. `/test/onboarding` is its intended Webflow Cloud mount. The original Sites project was not changed.
- `/rs-inputs/state`, `/record`, `/profile-link`, and `/recognition` share server-side request handling. Prefix these with the application's mount in Webflow.
- Barn, user, rider, horse and location creation/editing preserve the observed fields and relationships. Server validation handles required names, duplicate names within the relevant visible set, optional email, cross-barn relationship rejection and stale revisions. The UI displays confirmed saves; failed writes retain drafts.
- Profile linking is explicit. A recognized person's identity is separate from their user/roster record in a barn. Repeated linking reuses the existing record.
- Recognition reuses the existing action implementation through a configurable wrapper. An explicit `RS_INPUTS_RECOGNITION_BASE_ID` selects the existing identity base independently of new operational input storage. Device lookup is read-only; ambiguous device and identity matches fail closed. Public self-registration and recovery delivery remain disabled. The owner selected invited or approved access on October 6; operator-issued access reuses an existing canonical person and permits that person to complete their profile in RingStatus.
- A new-base-only Airtable provisioning command and schema, plus a bounded CRM Barn/Location adapter, are implemented. No new base was provisioned. Owned-base fixtures and schema repairs are documented below; CRM has not been written.

The existing Schedule, SMS alerts and Feed reference screens remain unchanged. They are not connected to new mutation events. Audit rows are not a delivery queue and do not imply that outputs have refreshed or that messages were sent.

The API prefix reads Astro's build-provided `BASE_URL`; manual image paths preserve the visible mount. This follows Webflow's [mount-path configuration](https://developers.webflow.com/webflow-cloud/environment/configuration), inspected during implementation. No mount or deployment setting was changed.

## Source and ownership

| Responsibility | Owner / source |
| --- | --- |
| Static site and entry links | Existing Webflow site `6982268b7543ac3c80151266` |
| UI and request orchestration | This existing Astro application in the RingStatus repository |
| Accepted UI reference | Sites project `appgprj_6ac27216ef888191b7fe22568801d0b8`, v23, source `02de1f51581f24ab44de9de071e0ba767d63024e` |
| Input validation, permissions, relationships and request outcomes | `src/lib/rs-inputs.js` |
| Trial persistence | `src/lib/rs-inputs-airtable.js`; owned target `RS_INPUTS_BASE_ID=app9kOZdIaGyKk5uG` |
| Trial schema | `config/rs-inputs-schema.json`; provisioning in `scripts/rs-inputs-storage.mjs` |
| Existing recognition logic | `rs-recognition-action.js` / `rs-recognition-session.js`, reused by `rs-inputs-recognition.js` |
| CRM replacement qualification | `src/lib/rs-inputs-crm.js`; supplied mappings and RingStatus organization only |
| Durable production owner | Not selected by mocked tests. Catalyst remains the established API/output direction; CRM is a candidate for bounded entities |
| Project/system mapping | Infrastructure base `appZahVgD156cMAe3`, unchanged |

Operational entities use immutable `entity_uid`; relationships use these IDs, never names or Airtable record IDs. Airtable's record IDs remain provider identifiers. Identity records retain the established `person_uid` and the recognition provider's internal links. This scaffold does not migrate legacy `ww_profiles` / `active_iams` or invent replacement membership rules.

## Authentication boundary

The public route now calls `handleAuthenticatedInputRoute`, which verifies an HMAC-signed, HttpOnly/Secure/SameSite=Strict cookie and checks the current canonical person in Airtable on every request. It ignores browser headers and supplied locals as identity. Only active people explicitly marked `input_access=invited` or `approved` receive input read/write and barn-create capabilities. Barn access is restricted to records they own; no shared-barn membership is inferred.

Recognition and authorization stay separate. A new browser sees its authenticated invited profile, then explicitly confirms the device. The server reuses the existing recognition action/logger and preserves the person UID. Profile editing follows confirmation. Legacy `access_level` is not elevated: the successful live invited fixture remained Guest. Account switching clears local recognition tokens, without retiring another person's stored device.

Four fields on the existing `rs_people_test` table hold access state: `input_access`, `input_invite_hash`, `input_invite_expires_at`, `input_session_version`. No auth table was added. Raw invitation tokens and signed cookies are never stored in Airtable or application logs. Approved operators remain responsible for correctly identifying the recipient and delivering their private link; no email or SMS is sent by this implementation.

The operator-only `scripts/rs-inputs-access.mjs` issues/reissues a 24-hour link or revokes access. It requires an existing unique canonical person and explicit invited/approved/revoked decision. For a genuinely new invited person, the operator first creates the minimum canonical person record (immutable `person_uid`, name, Active status; no implicit legacy membership), then issues the link. This is assisted invitation onboarding, not public self-registration or a new general entity-deduplication rule.

The invitation is a URL fragment, removed before requests; acceptance is explicit. The server consumes the hash before issuing an eight-hour session. Reissuing rotates the version and invalidates older links/sessions; revocation also works for inactive people. An uncertain consumption response may require reissue. Logout clears the browser cookie; a separately copied cookie remains valid until expiry/revocation. Because Airtable cannot atomically compare-and-consume invitations, this implementation refuses operation outside `isolated-trial`. Production concurrency is not qualified.

```sh
# Required environment: AIRTABLE_TOKEN, RS_INPUTS_BASE_ID, RS_INPUTS_RECOGNITION_BASE_ID,
# RS_INPUTS_WRITE_MODE=isolated-trial, RS_INPUTS_ONBOARDING_URL=https://ringstatus.webflow.io/test/onboarding
node scripts/rs-inputs-access.mjs invited PERSON_UID NEW_PRIVATE_OUTPUT_FILE
node scripts/rs-inputs-access.mjs approved PERSON_UID NEW_PRIVATE_OUTPUT_FILE
node scripts/rs-inputs-access.mjs revoked PERSON_UID NEW_PRIVATE_OUTPUT_FILE
```

The command creates a new mode-0600 output file, never prints the bearer link, never sends it, and refuses overwrite. Runtime also requires secret `RS_INPUTS_SESSION_SECRET`, a cryptographically random 32-byte value encoded as 64 hexadecimal characters. Keep it in the runtime secret environment, not repository files. Rotation invalidates all signed sessions. Implementation uses the Workers [Web Crypto API](https://developers.cloudflare.com/workers/runtime-apis/web-crypto/).

## Clean Airtable trial

**Current owned-base state, October 5, 2026 (America/New_York):** six input tables are created and fresh schema reads match every field name/type in the operational schema. One labeled synthetic barn, user, rider, location and horse were created through the Airtable connector. Fresh reads verified all canonical references; the horse edit persisted at revision 2, and an identical upsert replay retained its record ID with exactly one horse row. One evidence event records this scope. This is connector storage evidence, not application authentication, UI/API end-to-end, atomic concurrency, or production validation.

| Table | Verified table ID |
| --- | --- |
| rs_input_barns | tblRvTwo3HYPUkZou |
| rs_input_users | tblYgoeLEey05xgw9 |
| rs_input_riders | tblnd2ToLs7dTzLAM |
| rs_input_locations | tblEuwOr1rUKnj1j3 |
| rs_input_horses | tblpyyaOMgjLzLvkP |
| rs_input_events | tblwts3huk3w1ACjh |

Fixture run: `rs_storage_20261005_2145`. The user row deliberately has no invented recognition identity. Original imported recognition records were cleared under owner authorization; the later labeled test fixtures described below are now present. Sixteen incompatible recognition fields have now been replaced and verified as described below. Six quick-reference records were retained.

Webflow Cloud metadata inspection confirmed the RingStatus app `d7d97751-20e1-4148-a5cf-ee58671c128a`, main environment `110f06dd-c1ea-4839-98af-d829cbe77941`, mount `/test`, and successful last deployment on October 2. A secret `AIRTABLE_TOKEN` exists there; its value and scope were not exposed or verified. The environment has none of `RS_INPUTS_BASE_ID`, `RS_INPUTS_RECOGNITION_BASE_ID`, or `RS_INPUTS_WRITE_MODE`. The local execution environment has no `AIRTABLE_TOKEN`. No environment or deployment was changed.

The following provisioner is retained as historical implementation, **not the command to run for this supplied base**:

The provisioner defaults to dry-run. It accepts only an explicit workspace and credential-variable name. It has no existing-base alteration mode. Use credentials supplied through the normal secret manager/environment, never command arguments or committed files.

```sh
node scripts/rs-inputs-storage.mjs --workspace wspVERIFIED_WORKSPACE --token-env RS_TRIAL_PAT
node scripts/rs-inputs-storage.mjs --workspace wspVERIFIED_WORKSPACE --token-env RS_TRIAL_PAT --apply
```

Only use `--apply` with a verified accessible workspace and an authorized isolated trial. The command creates ten tables and then the existing recognition link fields. It reports the newly created base ID and partial progress. Do not blindly rerun a timed-out creation: inspect the workspace and any reported base first.

Runtime bindings:

- `RS_INPUTS_BASE_ID=app9kOZdIaGyKk5uG`: the owner's supplied Recognize base. Its six operational input tables were created and their names/types verified on October 5, 2026 (America/New_York). Do not create another base. The infrastructure and legacy operational bases are rejected.
- `RS_INPUTS_RECOGNITION_BASE_ID=app9kOZdIaGyKk5uG`: intended owned recognition target; replacement fields have passed schema readback, while runtime credential scope remains unverified. The earlier interim test explicitly selected `apptdhhNzduxm5gjn`. If omitted, recognition falls back to the clean input base; an implicit legacy-base fallback remains rejected. Recognition routes do not require operational input storage to be configured.
- `AIRTABLE_TOKEN`: server-only token scoped to the selected recognition/input bases. Connector access does not itself install this runtime credential.
- `RS_INPUTS_WRITE_MODE=isolated-trial`: explicit acknowledgement of the trial's single-writer scope. Missing/other values reject writes.
- `RS_RECOGNITION_SIGNAL_SECRET`: existing recognition signal hashing configuration, when required by that logger.

The adapter paginates reads, uses canonical-ID upserts, paces sequential requests, checks revisions, and records actor/request/entity identifiers. Request retries reuse deterministic entity IDs and committed audit results. If a record commits but its audit fails, the response is uncertain; an unchanged retry repairs the audit without applying the edit again.

**Airtable qualification limit:** revision read followed by write is not an atomic compare-and-swap. Upsert plus audit is not a transaction. Concurrent requests from different workers can still race; in-process checks do not establish a production guarantee. The adapter explicitly reports both capabilities as false. Production writes remain blocked until the selected store/path demonstrates the required concurrency and failure behavior. Existing recognition mutations also remain sequential and do not have operation-wide idempotency.

## CRM trial

The adapter preflights the OAuth target organization (`zgid=941333935`) and supplied `RS_Trial_*` module/field mappings before record access. It requires a unique mapped canonical ID, handles paginated reads, disables requested workflow/cadence triggers on writes, and sends `If-Unmodified-Since` for edits. Only Barn and Location candidates are allowed.

There is no runtime switch that makes CRM canonical and no automatic module provisioning or bulk migration. Actual RingStatus module metadata and correctly scoped credentials are prerequisites. The presently inspected connector targets Sport Dog Food, so it was not used for trial writes. The existence of a RingStatus organization in the organization list does not switch the connector's OAuth target.

## Diagnosis and maintenance

Change UI behavior in the copied onboarding components; change domain validation once in `rs-inputs.js`; change provider serialization only in its adapter. Update the versioned schema and affected contract tests together. Keep the original Site as the unchanged reference, not a second production writer.

Inspect Webflow Cloud runtime logs for structured `rs_input_request` outcomes and the returned `X-Request-Id`. Events carry operation, status, actor/request/entity IDs and a bounded error code; they exclude names, phone/PIN, device tokens and draft contents. Successful domain changes are recorded in `rs_input_events`; recognition uses its existing session event table. Provider errors do not expose upstream bodies or secrets to the browser.

For a reported save failure: correlate request ID, inspect the canonical record and matching audit, determine whether a commit occurred, then retry the unchanged request or reopen the latest saved record. Never treat an unavailable lookup as 'no match' or seed sample data after an API failure. No sync into existing Airtable/CRM/Catalyst datasets is scheduled by this implementation.

## Verification and remaining gates

### Live handler/storage integration: rs_live_20261005_2215

**PASS — 26 checks, local request handler with live Airtable through the authorized MCP connector.** The actual `handleInputRoute`, domain service, Airtable input adapter, recognition adapter, and original recognition action/session logger ran against owned base `app9kOZdIaGyKk5uG`. The test used the existing 19-phase API journey plus seven additional recognition/access/audit checks. It made 151 connector operations. It did not open a browser, deploy an application, or use the blocked preview target.

Verified: known/unknown/retired/ambiguous device handling; no implicit roster creation; explicit profile linking without duplicate rows; barn/user/rider/location/horse creation; saved edit at revision 2; stale-edit rejection; unchanged-request replay; relationship reload; wrong-person, read-only and other-barn rejection; and a linked recognition audit with the new page path. Fresh connector reads independently verified all eight successful domain writes have exactly one correlated audit, the edited horse remains at revision 2, and the recognition event links the correct person/device.

**Scope limits:** the actor was injected by test support. Missing-actor guards were tested, but authenticated session issuance and verification were not. Native REST credential/transport behavior, production concurrency, browser interactions and new-person registration remain unverified or unimplemented. No no-match-to-registration success is claimed. The fixture person is now Inactive/Guest, its alias Inactive, all four devices Retired, and the recognition test event marked processed; these states were freshly read back. Operational QA records and audit evidence remain for inspection.

Code/evidence: `test-support/rs-inputs-live-connector.mjs`, `test-support/rs-inputs-mcp-controller.mjs`, and `test-support/rs-inputs-live-connector-evidence.json`. The controller is Work-only test transport using explicitly authorized tools, exact-equality filter translation, native connector upsert and a fixed test namespace/base. It is not a second production storage adapter. The original `scripts/rs-inputs-e2e.mjs` remains the deployed API test runner once an actual session and deployment exist. The test-support driver has bounded waits and reports unknown fixture writes separately; it never automatically retries unknown mutations.

Independent review closed after resolving timeout handling, unknown fixture-creation reporting, and protection against upserts matching pre-existing rows. The last guard was added after the successful live run and independently verified with three focused tests (all passed); the live run was not repeated or relabeled as verification of that later guard. This remains an isolated sequential test, not an atomicity guarantee. One initial relay bootstrap failed before any connector write because the Work JavaScript isolate has no URL constructor; the subsequent successful run used explicit URL parsing.

**Resolved business decision (October 6):** the owner explicitly chose invited or approved access. Earlier inspected sources had not established that policy. The accepted prototype README explicitly excludes production authentication, invitations and member authorization; its local Add barn UI does not establish that policy. Legacy client copy offers demo accounts by contacting RingStatus, which supports assisted onboarding without proving an invitation-only requirement. Do not silently enable the legacy `create_profile` automatic Active/member grant or substitute device recognition for verified identity.

The unchanged Site baseline was exercised before modification: profile cancel, explicit profile link reuse, nested rider/user/location creation, horse creation, inline cancel/save, and browser reload. Its mobile runtime check (28 checks), build, and packaging checks passed.

Focused backend tests cover create/link/edit/reload, permissions, barn isolation, stale revisions, retry identity, uncertain audit repair, schema/provider mapping, wrong CRM organization, CRM conditional updates, clean-base provisioning isolation, and recognition behavior. Existing recognition and horse contract tests were also run.

The pre-access combined result was **115 tests passed**; the interim-source changes add split-base, route-isolation and complete API-runner tests. Four tests exercise the actual request handler and Airtable adapter together against a schema-aware fake REST provider, including writes that commit before the response/audit fails. Recognition tests also cover principal-bound lookup and same-owner retirement retries after audit failure. These automated provider fixtures are separate from the live connector test below. Existing dependency versions were preserved; the additional UI dependencies match the accepted Site model.

Connected browser QA was attempted through the managed preview with an isolated fixture actor/store outside the repository. The preview started, but browser navigation was explicitly blocked by browser security policy; further attempts/workarounds were prohibited and stopped. Therefore the changed connected UI has **no browser PASS** or new screenshot claim. Its strict TypeScript check and production bundle compilation passed; the earlier unchanged baseline browser evidence remains separate.

```sh
node --loader ./test-support/cloudflare-workers-loader.mjs --test test/rs-inputs*.test.js test/rs-recognition*.test.js test/horse-entity-ui-contract.test.js
npm run build
```

The native build initially hit this workspace's `os.networkInterfaces` error in Cloudflare's debugger setup. The same Cloudflare build passed using a scratch-only config with `inspectorPort: false`; the production adapter configuration was not changed for this environment issue. The runtime also warned that its local `Request.cf` metadata fetch timed out and used a placeholder. Neither result validates deployed edge metadata.

Outstanding release gates are concrete:

1. Verify the existing Cloud credential's access to owned base `app9kOZdIaGyKk5uG` and configure the intended runtime bindings through the normal secret environment. The field repair is complete: the owner authorized replacement fields, and fresh connector reads verified their names, types, select choices and link targets. Browser access is no longer needed for this repair.
2. A reachable, authorized test deployment for browser verification. The invited/approved identity integration is now implemented and locally exercised with signed cookies against live owned Airtable storage; deployed transport and browser handling are not yet verified. Public `create_profile` remains blocked intentionally; assisted invitation onboarding reuses the canonical person.
3. CRM is deferred. Its currently wrong-organization connection is not a blocker for the Airtable test path. No additional Catalyst work is required merely to inspect or test the existing recognition records.
4. Live provider failure/concurrency tests, broad canonical entity matching rules where they exceed the accepted prototype, and production storage allocation. The present five-entity input model is not evidence for unresolved trainer/merge/archive rules.
5. Separately authorized deployment and post-deployment verification. No publish, deploy, production data migration or external message was performed.

## Owned Recognize base: October 5 inspection and bounded repairs

Confirmed initial contents: 3 people, 2 devices, 1,142 recognition events. The device import was mislabeled `rs_phone_aliases_test`. Renamed that table to `rs_devices_test` without changing its ID (`tblfkRSJAEMzuzApR`) or records. Removed leading BOM characters from `device_uid`, `person_uid`, and `session_event_uid` field names. Created only the missing five-field alias table (`tblgDWKi0Bb6OcoqS`), with a real link to the existing people table. Fresh schema readback verified all changes. No imported record values, credentials, statuses, or production bindings were changed.

The code requires these four recognition tables. Existing profile updates upsert aliases, and phone lookup checks aliases even when a direct person matches. No profile/membership/show-engine tables are needed for this repair. Do not run the ten-table new-base provisioner against this supplied base or create a second replacement base.

Resolved on October 5 after the owner authorized creating new fields: renamed sixteen incompatible imported fields with `legacy_` prefixes and created correctly typed replacements under the original runtime names. Replacements: people `primary_phone_e164` (text); device `person` and event `person`/`device`/`phone_alias` (record links); event `event_type`, `event_result`, `matched_by`, `automation_status` (schema choices); event `browser_family`, `os_family`, `device_class`, `client_timezone`, `viewport_bucket`, `page_path`, `referrer_host` (text). Fresh schema/config reads verified all sixteen replacements and preserved original fields. Table IDs and existing runtime field names are unchanged. No records were present in these four tables at replacement; nothing required data migration.

The connector cannot convert field types, but supports the rename/create operations used above. Browser repair was not completed and is no longer necessary. Legacy-prefixed columns are retained, unused import fields; no runtime reads or writes should target them. Airtable returned `prefersSingleRecordLink: false` despite the single-record preference request; application validation must enforce cardinality. The existing alias-to-person link was already correct and was preserved. Compatible text fields and existing complete recognition-status choices were also preserved. No historical alias was recreated or inferred.

This is schema inspection/repair evidence only. Runtime credentials, trusted actor integration, application-level end-to-end verification, and release gates above remain outstanding.

## Live existing-base test: rs_e2e_20261005_21c6a9af03

**PASS — Airtable connector data-contract scope only.** Created one labelled synthetic person, one temporary phone alias, one device and one manual test-audit event in the four existing Recognize tables. Fresh reads verified forward/inverse links, canonical person ID retention, and a persisted name edit. No existing user record was changed. The synthetic phone uses the fictional `202-555-0199` range; no email/PIN was supplied and no message action was requested.

| Record | ID | Final status verified by fresh read |
| --- | --- | --- |
| Person | `rec64nlw6G87zVZI4` | Inactive / Guest |
| Phone alias | `recuf6OBU7aCdfmHI` | Inactive |
| Device | `recRu3FZZIRmi4RA0` | Retired |
| Manual test event | `recIllZQbfOh0N4Wn` | completed; all three record links correct |

The event's detail explicitly records `application_e2e_verified: false`, `browser_verified: false`, and `automation_action: none`. It was written as completed, not queued. This proves actual Airtable create/link/edit/readback and final fixture state; it does not prove that a Webflow browser request travelled through the deployed application.

Observed new-base requirements: keep the immutable person UID distinct from barn user IDs; preserve source record mappings during any eventual migration; preserve people/devices/aliases/session relationships and actual select choices; keep audit replay IDs and revision checks. Schema v2 removes unrelated fields from each operational table: users get email/person link, riders get user link, horses get rider/location links, locations get address, and barns retain only common identity/ownership/revision fields. The existing identity base remains authoritative until an explicit migration is tested; no dual writer is enabled by the draft schema.

## Repeatable application test

`scripts/rs-inputs-e2e.mjs` implements the 19-phase **API-only** journey: unauthenticated read/write rejection; same-origin enforcement; recognition without implicit roster creation; known/unknown/retired device reads; create barn; explicit profile link twice; create user/rider/location/horse; edit; reject stale save; replay the identical request; and reload all IDs/relationships.

Default execution is a dry run with prerequisites and the test sequence. `--run` additionally requires a verified test base URL/mount, exact expected origin, `isolated-trial` environment, unique run ID, server-issued session cookie, fixture device token and expected person ID. Secrets are read through named environment variables only. The runner never creates authentication, updates identities, sends messages or performs cleanup of existing records. It stops at the first failure and retains returned record/request IDs. A missing retired-device fixture yields PARTIAL, not PASS; CLI exit codes are 0 for PASS/DRY_RUN, 1 for FAIL/BLOCKED, 2 for PARTIAL.

```sh
node scripts/rs-inputs-e2e.mjs --dry-run
```

The runner's nine local tests execute the real domain request handler through an in-process fixture transport, including fail-first and secret-redaction behavior. Live application execution remains BLOCKED by the test deployment/session and input storage prerequisites above. Browser cancel, nested-editor rendering and mobile interaction remain separate required UI checks; the API runner does not claim to cover them.


## Invited/approved access implementation and live verification — October 6

Owner decision implemented in the existing application. Four access fields were created in owned base `app9kOZdIaGyKk5uG` and freshly verified. The existing recognition action received a narrow server-only verified-principal argument; no browser payload can enable it. Original callers retain their original behavior. No legacy member grant was enabled.

**PASS: 131 combined automated tests**, strict TypeScript through the connected App entry, and the Cloudflare production build (same scratch-only debugger workaround documented above). An additional client contract check verified invitation acceptance/logout clear prior local recognition state before requests. Independent review closed three identified issues: stale local device on account switching, inactive-person revocation, and invited Guest phone lookup. No remaining blocking code finding within isolated-trial scope.

**PASS: 16 local signed-session handler / live Airtable checks**, run `rs_auth_20261006_a02`, 100 connector operations. The real public wrapper issued/verified a signed cookie, rejected missing sessions/replayed invitations, exposed the invited canonical profile, explicitly associated a new device, created a barn, linked the profile, created/edited/reloaded a horse, reused an identical save without duplication, rejected a stale save, denied a revoked session and cleared the logout cookie. Transport was the authorized MCP relay; no actor was injected. This proves local cookie handling against live storage, not a deployed HTTP/browser journey or native REST credential scope.

Fresh independent reads verified person `recznOJ6f3ODj4EvX` Inactive/Guest with access revoked and invitation cleared, device `recf8Ik5lGlCmFCpE` Retired, horse `recbd2v0h95urxnQS` at revision 2, and four distinct write audits. Recognition event `recog1AR4y1OvXgXe` was manually marked processed to remove it from the queue; no automation execution is claimed. The first driver launch lacked open stdin after one fixture create; that separate person `recsaMyRThcKNZj02` was revoked/inactivated and freshly verified before the successful run. Evidence: `test-support/rs-inputs-live-access-evidence.json`.

### Concrete remaining deployment gate

Target existing Webflow Cloud app `d7d97751-20e1-4148-a5cf-ee58671c128a`, environment `110f06dd-c1ea-4839-98af-d829cbe77941`, mount `/test`, onboarding `/test/onboarding`. Source work is local branch `work/recognize-inputs-20261005`, not committed/pushed/deployed. The separately authorized release would publish only reviewed changes from this work, retaining other existing app routes.

Required runtime configuration: `RS_INPUTS_BASE_ID=app9kOZdIaGyKk5uG`, `RS_INPUTS_RECOGNITION_BASE_ID=app9kOZdIaGyKk5uG`, `RS_INPUTS_WRITE_MODE=isolated-trial`, a new secret `RS_INPUTS_SESSION_SECRET`, and the existing `AIRTABLE_TOKEN` verified for this base. No browser identity override is deployed. The Webflow connector exposes metadata/deployment operations but explicitly cannot write variable values; its instructions require the Webflow CLI for that. This workspace has no Webflow CLI credential or Airtable runtime token. Connector credentials must not be extracted or reused as runtime secrets.

Release therefore needs explicit deployment approval under repository AGENTS.md and authorized Webflow Cloud configuration access. No deployment or secret change has occurred. Once access is available, configure the isolated target, issue a private synthetic invitation, verify the deployed browser journey and failures, revoke the fixture, and record fresh storage evidence. The previously denied local browser target must not be retried or bypassed. Production qualification still requires atomic storage/access decisions; Zoho remains deferred.

Maintenance: access logic lives in `rs-inputs-access.js`, approval issuance in the operator script, business validation in `rs-inputs.js`, schema in the one JSON contract. Inspect Cloud runtime `rs_input_access`/`rs_input_request` logs by trace/actor/request ID, domain audit rows in `rs_input_events`, and recognition events in the existing sessions table. Never log invitation/session values. Update ownership evidence in the existing `rs-inputs-agent` binder; do not create another process table.
