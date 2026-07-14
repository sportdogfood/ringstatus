# RingStatus Recognition Session Test Design

## Goal

Extend the existing RingStatus recognition test without replacing its wired Airtable identity tables. Add an append-only session-event endpoint and a local browser test that prove the request contract while keeping all verification mocked and repeatable.

## Existing contract

- `rs_people_test` is the person record and links to many devices and phone aliases.
- `rs_devices_test` is one record per persistent browser/device token.
- `rs_phone_aliases_test` is one record per alternate phone linked to a person.
- `GET /test/rs-recognition/device` remains the read-only token lookup.
- `rs_recognition_sessions_test` is the new append-only session-event ledger. Every event is a new record; related events share `session_uid`.

## Test increment

Add `POST /rs-recognition/session` in the Webflow Cloud Astro app. It accepts a complete event, enriches it with server-observed network and coarse GeoIP context, checks `idempotency_key`, and creates one Airtable record with `automation_status = Pending`. It never writes directly from browser JavaScript to Airtable.

The existing recognition endpoint gains only backward-compatible behavior: normalize device status casing and return `device_record_id` so session events can link to the exact device record.

Add a repository-tracked HTML test embed based on the existing recognition snippet. It performs the current device lookup and emits one session event per lookup result. It does not implement profile creation, profile editing, phone login, or recovery because the live person table does not yet contain those UI fields.

## Privacy and recognition signals

- Never persist a raw IP address.
- HMAC the exact IP, network prefix, and user agent with `RS_RECOGNITION_SIGNAL_SECRET`.
- Store only coarse edge GeoIP values supplied by Cloudflare.
- IP, GeoIP, and browser data are supporting signals and never authenticate a visitor by themselves.

## Verification boundary

Automated tests mock every Airtable request and verify the exact table name and payload. No live record is created, no Airtable automation is fired, and no direct endpoint call counts as workflow proof.

