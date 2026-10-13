# Messages: all owner-confirmed lanes — 10 October 2026

Scope: bounded documentation/source reconciliation of every lane in messages-lanes-20261010.md. Existing implementation preserved, code reference-only, engine separation deferred. Required workflow is recorded separately from observed implementation. No new table, infrastructure, SMS, deployment, activation or runner execution.

## OTP — Recognize owns verification

Required: retain existing OTP verification; log observed outcomes and update shared notifications-feed without codes/private links. Identity verification never grants tenant membership.

Observed: shared deployed-source checkout C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus at HEAD 18e6e993aeaff6fd53168dac4d6f1b7b46baf195. webflow-cloud-test/src/lib/rs-recognition-otp.js lines 6–30 normalize US phone, verify candidate, start existing D1 OTP store and queue through createRecoverySms; otp_check consumes code before binding the initiating browser. Native route /test/rs-inputs/native-recognition, src/pages/rs-inputs/native-recognition.js and src/lib/rs-recognition-native.js 77–80 support authenticated sms_prepare/sms_outcome callbacks. Existing configured owner-test restriction stays in place.

Queue and log: Recognize app9kOZdIaGyKk5uG SMS verification requests tblxC4SYtzYhJNcJS and existing SMS event table tblF1Hdqi3yoKTZPV. src/lib/rs-recognition-sms.js 6,36–44,65–73,97–99,120 onward uses stable request/event IDs and existing claim controls. Deployed/valid sms-recovery automation wfl6oiGMgUPmEkUuS freshly read: request conditions → authenticated prepare → native Airtable Twilio send action → outcome. Sensitive body exists only at authenticated automation handoff, not ordinary queue/history. Outcome supports submitted/failed/unknown and requires actual provider SID for submitted; prior unknown/native_twilio_no_provider_reference remains unconfirmed delivery.

Concrete gap: shared notifications-feed update/correlation is not established by the OTP logger. Later implementation should project safe lane/request/status metadata from existing observed events, without moving OTP or exposing secrets. No fresh OTP/send/handset test performed.

## SMS/alerts — own logic and queue

Required: its own messaging logic/queue receives show-engine and RingWaze inputs, consumes Tenants consent/options/permitted scope, sends eligible alerts and logs/updates notifications-feed. Messages does not recreate show calculations.

Source/queue/feed reconciliation is recorded in messages-alerts-feed-references-20261010.md and the workflow record. Existing Messages rs_sms_requests tblQWqhaSVXdDxQF0 and rs_sms_events tblr7sWHgoYDgj7uV are prepared copies with nine requests/24 events, not proof of an alert queue writer or sending automation. Do not repurpose the Recognize OTP queue by inference.

Existing WEF baseline: schedules_calculatorv2.js reads active_alerts and profile/tenant/show-qualified timing configuration and creates durable thread_logs directly (source1838–1871); thread_static_id/idempotency context is established in the existing implementation. Live physical IDs in original apptdhhNzduxm5gjn: active_alerts tblgkCC4vcXIjSEn7, thread_logs tblGAFRLhh0ulElVG. Treat these as alert configuration/producer events; a downstream SMS consumer is not yet established. WEC physical IDs in app6XS1RvsPNRT6os: time_engine_triggers tbl5uH3rBkSmKieGZ and operational wec-alerts tblqkxLPy9zZ2FI6z. Existing sync-airtable-controls.js145–161 writes local wec-alerts.jsonl and attempts operational-alert Airtable logging. Historical Time Engine/queue drafts and endpoint response notifications_sent fields are not proof of current delivered messages.

Concrete SMS/alerts gaps: identify current downstream sender/queue consumer from the existing WEF event/alert implementation; recover exact external WEC engine source/version where referenced; connect the owner-required RingWaze event producer; map consent/options to this existing event flow; correlate actual provider outcomes and feed projection. Preserve proven event calculations and tables; do not recreate a queue or claim Messages copied tables already implement one.

Current ownership/context: Windows ringstatus-heartbeat repeats PT3M and invokes the approved repository's run_tagger_heartbeat_lane.ps1; natural inspected logs show no_active_show_scope. This verifies recorded cadence, not active alert production. Current personal preference writer remains Recognize subscriptions; Tenants owns eventual membership/assignment/favorite/consent/options consumption. All 18 copied options match JSON, revoked/Test stays revoked/Test; no eligible recipient inferred. RingWaze input is an agreed required producer, not proof of an implemented connection.

## Two-way — receive, normalize, identify show/intent, query, reply

Required: receive → normalize → identify show and ring/rider intent → query endpoint → send response → log and update notifications-feed. Unresolved show/intent must not be guessed.

Observed current Worker: ringstatus-sms deployment 2317eff4-b7d2-4ab9-b038-cb1ff12e8404, version d18b0954-59bf-4ab5-a595-fd1861120cb0; downloaded-source correspondence with lib/cloudflare/ringstatus-sms/worker-err.js, touching commit 19e9754b41006fd1b5005549f4bf93eec7046588. Public worker address derived from enabled subdomain: https://ringstatus-sms.gombcg.workers.dev. Twilio incoming routing remains unverified; endpoint was not invoked.

Body/From/MessageSid are trimmed at lines 63–74; deterministic ring aliases first, then rider/horse command/name parsing (585 onward,954 onward,1665 onward). Existing endpoints: https://sgl.wellingtoninternational.com/iphone.php/esp/webservice/LiveScoreWidget; https://ringstatus-proxy.gombcg.workers.dev/rider and /horse; feed fallback https://ringstatus-proxy.gombcg.workers.dev/docs/9999/schedules/trips.json then public sportdogfood/ringstatus-data main trips.json. Proxy reads original apptdhhNzduxm5gjn/watch_trips tblCtNpzJtrI19VKq/view viwpJEf8bGL31ge90; SGL_API service broad-tooth-b8ed production.

Output/log: XML-escaped TwiML HTTP200 at1563–1579; original-base genericWebhookReceived automation two-way wfleDEPvPZQjrHJ8E creates twoway_logs tblfQDcePLpX2qkHW. MessageSid→run_id, phone→from, body/title/first/next→body/title/now/next. Capability URL omitted. Latest five returned historical automation runs succeeded, latest2026-10-08T14:14:58Z. Log creation is not response delivery or shared-feed proof.

Concrete gaps: selectLiveScoreShow334–342 tries configured SHOW_ID then returns first record; this does not meet no-guess show selection. Source has no sender/person/tenant association, persistent MessageSid replay suppression, signature validation, provider-status callback or dedicated STOP/START integration. Provider suppression/config stays unknown. Sequential upstream waits and untimed/unchecked log POST leave response/error behavior unresolved. Shared-feed projection not established by twoway_logs alone. Recommendation for later authorized delta: preserve deterministic lookup; use explicit/configured show context or return clarification when unresolved; reuse correlated logs, prevent duplicate replies and add safe feed projection. No runtime changes made.

## Shared notifications-feed and logging

Required: every lane updates notifications-feed alongside logging using actual processing/provider states; safe OTP metadata only. A successful lookup/log is not delivered SMS.

Existing sources differ: OTP request/event history in Recognize; two-way twoway_logs in original base; alert sources identified in the dedicated source review. src/components/rs-onboarding/notification-records.ts is a message formatter and by itself establishes no persistent feed writer. Do not create another feed table merely because the writer is unverified. The concrete destination, current reader and missing per-lane updates must be distinguished in the source review/workflow.

Existing feed UI is Prototype.tsx: fixed notificationSamples269, browser read-state key307, sample filtering313, normalizeMessage caller317 and page=notification-feed screen321. It is explicitly a sample/no-live-SMS connection baseline. No persistent live feed table, writer, endpoint or all-lane projection was established in the bounded sources and physical Messages schema. Concrete gap: connect this existing UI to safe correlated per-lane history through an identified data source/reader after separately authorized implementation. Reuse formatter/presentation rather than rebuild it. OTP must expose only safe status metadata. No feed table created for this map.

Remaining implementation order: reuse identified alert implementation/queue; map show-engine/RingWaze event contracts and permitted Tenants options; connect safe per-lane logging/feed projections; verify actual provider correlation/delivery only under separately authorized implementation/testing. Missing Twilio Console evidence is retained follow-up, not a reason to reopen architecture or ask the owner to explain it. Engine separation remains deferred.

Proof boundaries and linked documents: messages-source-inventory-20261010.md; messages-recipient-contract-20261010.md; messages-alerts-feed-references-20261010.md; messages-lanes-20261010.md. Source-only inspection and prior automation logs do not establish end-to-end messaging acceptance.
