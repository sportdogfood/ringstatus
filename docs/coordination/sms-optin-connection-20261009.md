# SMS opt-in connection — owner revision305

Target: published /rs-barn-onboarding-alerts, Webflow page 6ac459b4506821bd349ed047. Owner approved editing after the live test proved that Save only wrote browser localStorage. Existing source baseline: 2f9d0e41da1d186123a1e2176d882a0c837b0d1f in C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus.

Authorized delta: connect the existing native form to existing Astro/Airtable storage and preserve every selection, numeric timing and time zone. Reuse native presentation/event code, recognized-cookie access, subscription storage, existing write reservation and audit events. No SMS sending, engine activation, new infrastructure, changes to Recognize/Barn presentation or unrelated working code. Webflow publication remains owner-controlled.

The page has no barn/engine selector. Its preferences belong to the recognized person; do not invent a barn, engine or engine-trigger mapping. Store a Person preference record in the existing subscriptions table, with one added preferences_json field for the exact native state. Keep existing engine-specific subscriptions unchanged. Preference records remain outside delivery activation. Phone is a preference destination, not authentication.

Acceptance / complaint mapping: C004 requires reuse of the observed footer and existing storage/access implementation; C025 and recRW0nqJFLyQ04mt require actual published UI save/reload and matching Airtable readback, not local tests alone. Verify on/off, numeric and clock-time values, time zone, ownership, repeat saves and stale revision rejection. Test only the owner's authorized number. No direct write endpoint or manual record repair substitutes for UI proof.

Initial evidence: live page states prototype only; saving a reminder and reloading retained it in the browser; subscriptions readback for this_sms_test returned no records. Source native-reader.js has no network/mounting; subscriptions.js has no configured engine mappings and rejects parameterized alerts. Reuse the existing UI rather than redesign it. Retained reliability-controls task remains unchanged.

Pending: implement scoped connection, local regression/build, deployment via existing source branch, owner publication of the page loader, actual UI/Airtable proof. Reverse by restoring the saved page footer and reverting only this scoped source commit, preserving later work and saved records.

## Revision305 — implementation and local verification

Commit 1c8239c51a6ba7bb259157d4a236ced0856697c0 contains the native footer behavior reused as native-client.js, an Astro script route, a personal-preferences operation on the existing subscriptions handler, and the existing recognized-cookie access extended to subscriptions. No new authentication, database, table, SMS action or engine mapping was created. Personal preferences are keyed deterministically by server-verified person, never a body-supplied owner. No automatic migration of browser-local preferences or consent.

Added only preferences_json (fldXUxFnjBwpUpBYA) to rs_input_subscriptions (tblbpALv7flu3NNEE), base app9kOZdIaGyKk5uG. Updated descriptions to distinguish Person preferences from engine subscriptions. Stored preferences preserve all 18 keys, including 8 parameterized choices and IANA time zone. Explicit Save captures on/off; existing phone formats normalize to E.164. Records remain Test and Draft/Revoked, so this wiring does not activate delivery. Existing subscription audit events and durable write reservations are reused.

Regression reproduced invalid_payload before implementation. Final selected suite: 55/55 pass, covering existing subscription/access/runtime behavior plus exact timing round-trip, opt-out and re-enable, same-request retry, lost-response reconciliation, stale revision, invalid inputs, recognized-cookie lookup and impersonation rejection. One new test fixture initially used an invalid short synthetic Airtable record ID; corrected the fixture to the actual record-ID/GET shape, with no corresponding product workaround. Local tests use synthetic Airtable and real local SQLite; they are not live workflow proof.

Astro build passed. Evidence logs in deployed-source checkout: webflow-cloud-test/.sms-optin-305-tests.txt and .sms-optin-305-build.txt. Saved original footer: docs/coordination/sms-optin-footer-before-20261009.html. Native styling functions were retained from that footer; no CSS changes. Saved footer now contains only /test/rs-inputs/native-alerts.js module loader; readback matches and head code is byte-identical. Two unsupported Webflow action names failed schema validation without writes before using the documented set_page_freeform_code action; this repeats the existing tool-schema complaint and is not an access failure.

Pushed normally to the existing work/inputs-crm-20261006 branch after verifying its prior head matched 2f9d0e41d. Deployment 6492c7a2-8987-4e29-8838-597a4a1f1403 was starting at the first check. Owner publication and live native save/reload with Airtable readback remain required. No opt-in record was manually created and no SMS sent.

Deployment succeeded at 2026-10-09T16:38:08.251Z (12:38 p.m. New York), exact commit 1c8239c51a6ba7bb259157d4a236ced0856697c0. The saved page loader still requires owner publication. Backend deployment and 55 local checks do not prove the published form save: live save/reload and matching Airtable records remain unverified until publication.

## Revision306 — published UI proof

Owner published. Actual Edge page /rs-barn-onboarding-alerts loaded the new behavior using the existing recognized browser, without a new login. Through the native UI only: enabled SMS, entered the authorized test number, selected Class reminder 1 at 30 minutes and Task reminder 1 at 10:15, then saved. The page confirmed profile save. Reload restored the enabled flag, both selections, exact times and America/New_York.

Airtable independent readback: recbLNY3M6un3KEHH, entity_uid rs_876181ac66a07a225f9a7b9967dd6be3, owner/target this_sms_test, revision1, consent Granted, status Draft; preferences_json matched UI including all 18 keys. Next disabled SMS via the page and saved; readback of the same record showed revision2, consent/status Revoked and revoked_at 2026-10-09T16:44:16.668Z. Both selected timings remained intact. A second reload visibly restored SMS off, selected 30-minute reminder, 10:15 task time and time zone; no error or pending-save message remained. Test record retained with SMS off. No SMS sent or engine delivery activated.

This satisfies this scoped form-to-Astro-to-Airtable save/reload and opt-out proof. It does not complete engine message delivery, Recognize's separate partial scope, the CSS audit or Schedule. Two browser CDP input timeouts occurred before any Save; fresh state showed those clicks had not applied. Supported native accessibility actions completed the same UI steps without application changes or duplicate saves.

Independent event readback in tblwts3huk3w1ACjh: recheJznjJDU8qEVy records committed consent_granted at 2026-10-09T16:43:52.582Z (revision1); reccQODuah23uVe7v records committed consent_revoked at 2026-10-09T16:44:17.277Z (revision2). Both link the same subscription UID and actor this_sms_test.
