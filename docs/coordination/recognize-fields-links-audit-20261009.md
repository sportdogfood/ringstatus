# Recognize fields and relationships — October 9, 2026

Scope: the seven tables assigned to recognize in base app9kOZdIaGyKk5uG. Read-only business-schema review; no business fields, records, links, code, automations or views changed. Findings are recorded in recognize-index.

## Findings

121 existing fields across seven tables. 33 selected cleanup candidates were empty across every returned row: devices 11, people 8, session events 52, aliases 1, SMS requests 9, SMS events 24. No pagination remained for these population checks. Of the candidates, 16 are legacy_* fields. Counts are a snapshot; the live application can continue writing.

Empty does not mean safe to delete. The 33 are review/hide candidates, not deletion approvals. The live schema proves most required links already exist. The extra people/alias relationship is separate from the canonical one. Automatic reciprocal links ending in ' 2' must not be deleted merely because their names look duplicated.

## Minimum ordinary views and retained dependencies

### rs_devices_test — 10 fields

- Ordinary view: `device_uid`, `person`, `status`, `last_seen_at`.
- Retain/dependencies: Keep device_token for runtime matching, hidden from ordinary views; keep recognition_source and the real session backlink. first_seen_at is not written by the inspected device upsert: optional historical field, not a reason to invent another writer. Do not confuse the empty text session column with the live linked column ending in ' 2'.
- Relationships: Each device -> one person through person. Many session events -> one device through sessions.device; rs_recognition_sessions_test 2 is its real reciprocal field. Many devices per person are expected.
- Empty cleanup candidates (2): `legacy_person`, `rs_recognition_sessions_test`
- Other existing fields remain in place. A minimal view is not a minimal runtime schema.

### rs_recognition_sessions_test — 48 fields

- Ordinary view: `session_uid`, `event_at`, `person`, `device`, `event_type`, `event_result`.
- Retain/dependencies: Keep session_event_uid and idempotency_key for event identity/retry protection; session_uid groups events. Keep matched_by, recognition_status, event_detail and optional phone_alias. Existing writer sends automation_status/automation_attempt_count and available location/browser signals, so do not delete those because they are hidden or sometimes blank. automation_processed_at/automation_error have no writer in the inspected session module; retain pending consumer review. This is an event table, not one mutable row per session.
- Relationships: Each recognized event -> person and device; phone_alias only when actually used. Unknown-browser events may have no person/device. These links already exist. Preserve both person and device links for historical context; do not add a direct SMS-event link when the request relationship already supplies context.
- Empty cleanup candidates (14): `legacy_event_type`, `legacy_event_result`, `legacy_person`, `legacy_device`, `legacy_phone_alias`, `legacy_matched_by`, `legacy_automation_status`, `legacy_browser_family`, `legacy_os_family`, `legacy_device_class`, `legacy_client_timezone`, `legacy_viewport_bucket`, `legacy_page_path`, `legacy_referrer_host`
- Other existing fields remain in place. A minimal view is not a minimal runtime schema.

### rs_people_test — 29 fields

- Ordinary view: `person_uid`, `person_name`, `primary_phone_e164`, `status`.
- Retain/dependencies: Keep first_name, last_name, email, member_pin, access_level and all four input_* fields: existing profile, recognition, invitation or access code still uses them. Hide technical/auth fields from the ordinary operator view. Retain the canonical reciprocal links to devices, sessions, SMS requests and IAM. The similarly named phone-alias links are two separate relationships.
- Relationships: Person is the existing identity parent. Devices.person, aliases.person, sessions.person, SMS requests.person_uid and active_iams.person already link here. Canonical alias backlink is rs_phone_aliases_tes (fldPGT9t8kdlNaXXu); rs_phone_aliases_test (fld5ZWj94cOthMdzj) is the extra empty relationship.
- Empty cleanup candidates (11): `legacy_primary_phone_e164`, `Phone`, `rs_devices_test`, `rs_recognition_sessions_test`, `profile_name (from ww_profiles)`, `ww_profiles`, `sms (from ww_profiles)`, `active_iams`, `ww_iams (from ww_profiles)`, `profile_rid (from ww_profiles)`, `rs_phone_aliases_test`
- Other existing fields remain in place. A minimal view is not a minimal runtime schema.

### rs_phone_aliases_test — 7 fields

- Ordinary view: `alias_uid`, `alias_phone_e164`, `person`, `status`.
- Retain/dependencies: Keep alias_type and the real session backlink. One older alias exists and is Inactive; that does not make the conditional fallback code unnecessary. Extra rs_people_test link is empty and is not the canonical person field.
- Relationships: Canonical aliases.person -> one person (inverse people.rs_phone_aliases_tes). Sessions.phone_alias -> alias when applicable. Extra aliases.rs_people_test <-> people.rs_phone_aliases_test is a second, empty relationship; consolidate only after dependency review.
- Empty cleanup candidates (1): `rs_people_test`
- Other existing fields remain in place. A minimal view is not a minimal runtime schema.

### rs_sms_requests — 13 fields

- Ordinary view: `request_uid`, `person_uid`, `primary_phone_e164 (from person_uid)`, `status`, `requested_at`.
- Retain/dependencies: Keep record_mode, provider_message_sid, error_code and SMS-event backlink. The phone lookup is actively validated by the sender and must remain. Current code derives destination and expiry rather than reading the four empty candidate fields. Empty provider SID is not grounds for removal: code supports actual provider results.
- Relationships: Each request.person_uid -> one person; despite its name this is a real linked-record field. Phone is a lookup of people.primary_phone_e164. Many SMS events -> one request. source_event_uid is text, not a working link; do not convert it without an actual matching event contract.
- Empty cleanup candidates (4): `source_event_uid`, `to_e164`, `template_key`, `expires_at`
- Other existing fields remain in place. A minimal view is not a minimal runtime schema.

### rs_sms_events — 8 fields

- Ordinary view: `event_uid`, `request`, `occurred_at`, `event_type`, `error_code`.
- Retain/dependencies: Keep source and provider_message_sid for provenance/provider outcomes. detail is empty and not written by the inspected SMS module. A request's current status and its historical events serve different purposes; they are not duplicate tables.
- Relationships: Each event.request -> one SMS request. Person and phone are available through that request; no second person or phone copy is needed here.
- Empty cleanup candidates (1): `detail`
- Other existing fields remain in place. A minimal view is not a minimal runtime schema.

### active_iams — 6 fields

- Ordinary view: `iam_id`, `person`, `phone`, `active`, `last_visited`.
- Retain/dependencies: Keep its six-field schema as the prepared slim register. phone is a lookup, not another stored phone. ips_related is optional associated-network context; IP is not identity proof. Table is empty and unwired: do not add independent devices/sessions/phone copies or pretend its active flag controls access.
- Relationships: One intended IAM row -> one existing person through person; phone looks up primary_phone_e164. This link already exists. Airtable currently permits multiple links and does not enforce one IAM row per person; runtime enforcement is still unimplemented.
- Empty cleanup candidates (0): None proposed.
- Other existing fields remain in place. A minimal view is not a minimal runtime schema.

## Safe simplification order

1. Use concise operator views; keep runtime IDs, hashes, technical telemetry and reciprocal links available in technical views. No view edits have been made by this audit.
2. Review the 33 empty candidates against all consuming automations, interfaces/forms, formulas, exports and every deployed caller before any deletion/type conversion. The inspected source and one automation are evidence, not a complete inventory of external clients.
3. Preserve canonical links and existing primary/foreign identifiers. Do not rename live API fields merely to improve display labels. Removing a linked field can also remove its reciprocal field.
4. Keep the seven accepted table purposes. Do not add new tables or relationships for this cleanup. IAM runtime wiring is separate unfinished work, not completed by documenting the schema.

## Evidence

- Airtable live table/field schemas, linked-field inverse IDs and phone lookup targets; all seven tables read October 9.
- Complete population checks for the named candidate fields; no values or credentials copied into this report.
- Airtable automation sms-recovery (wfl6oiGMgUPmEkUuS), draft and deployed metadata read. No candidate field name/ID references found in the returned serialized configuration. Scripts use server endpoints, so this does not eliminate application dependencies.
- Source checkout: C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus, HEAD 1c8239c51a6ba7bb259157d4a236ced0856697c0. Inspected webflow-cloud-test/src/lib/rs-recognition-{action,session,sms,config,otp}.js, rs-inputs-{recognition,access}.js and src/pages/rs-recognition/device.js.
- rs-recognition-session.js buildAirtableFields writes IDs, event details, automation bookkeeping and available geo/browser fields. rs-recognition-sms.js validates the linked phone lookup, creates request/event rows and supports provider/error outcomes. rs-recognition-action.js still reads/writes profile fields and uses member_pin; field deletion would alter existing paths even if the newer UI does not show them.

These recommendations reduce clutter without claiming that all remaining fields are intrinsically necessary to a future redesign. No new login, SMS, code test or business workflow was run.

