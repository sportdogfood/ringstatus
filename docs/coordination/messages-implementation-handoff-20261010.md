# Messages implementation handoff — 10 October 2026

**14 October ownership addition:** Messages now owns the alerts page and completion, using Recognize identity and Tenants canonical preferences. The [current alerts handoff](recognize-tenants-messages-ownership-20261014.md) preserves the prepared implementation, failed verification and approval boundaries; this older two-way assignment does not replace it or authorize its execution.

Prepared only. No runtime files changed, SMS, automation activation or deployment. Owner-confirmed architecture is in messages-lanes-20261010.md; verified source map is messages-all-lanes-map-20261010.md. Engine isolation remains deferred. Preserve existing OTP, producer calculations, alert events, deterministic two-way lookup and notifications UI. This handoff does not select a replacement architecture.

CURRENT STATUS: first local selector/test scope was subsequently owner-approved and completed on10October.46/46offline regressions pass; independent review also reproduced19baseline failures and passed9edge probes; all bytes outside selector unchanged. Exact artifact/rollback/full-lane readiness are in messages-local-release-readiness-20261010.md and .git/ringstatus-control/messages-show-selection-20261010/. The original Prepared text above remains historical; only first scope implemented. Other queued code, deployment/SMS/activation remain unapproved.

## First concrete implementation scope ready for approval

**Local two-way show-selection correction, with regression proof; no deployment.**

Baseline: lib/cloudflare/ringstatus-sms/worker-err.js at repository a5235466eed1b094a26db14ab34b514726abf91d; last touching commit19e9754b41006fd1b5005549f4bf93eec7046588. Current downloaded Worker source corresponds to this file, active versiond18b0954-59bf-4ab5-a595-fd1861120cb0. Do not edit worker.js/worker-live.js/worker_rollback.js or assume they are deployment sources.

Smallest delta: selectLiveScoreShow330–342 currently tries configured SHOW_ID then selects records[0]. Require one unambiguous identified show: with SHOW_ID, exactly one matching record; without SHOW_ID, exactly one valid show record. Missing/duplicate/ambiguous/unmatched show returns null. Existing buildLiveScoreWidgetRingReply222–231 already turns null into its existing “ring data unavailable right now. Try again shortly.” reply. Preserve that copy, ring aliases, rider/horse parsing, clocks, endpoint calls, TwiML escaping and logging.

Allowed runtime file for that future approval: only lib/cloudflare/ringstatus-sms/worker-err.js selector. Proposed new local test file, not an existing file: lib/cloudflare/ringstatus-sms/worker-err.show-selection.test.mjs. Use a local test fixture/extraction without importing a live endpoint or exposing the embedded capability URL; do not refactor/export production helpers solely for testing. No config/table/secret edits. One writer and independent read-only reviewer when implementation is separately authorized.

Acceptance: reproduce wrong-first-show fallback before change; configured unique match still returns that show regardless of order; unmatched configured show, duplicate configured match, multiple unconfigured shows, empty/malformed payload and missing usable show identity return null; one unconfigured valid show remains supported. Existing null reply is used without invented show answers. Representative ring/rider/horse parser and known-show reply outputs remain unchanged. Run local tests only; record exact source diff and rollback of only this selector. Deployment, publishing and live webhook/SMS testing are separate permissions, not included.

**Only input needed for this first scope:** approval to make this local correction and its tests. No architecture explanation, credentials, recipient decision or engine separation is needed.

## Ordered remaining scopes

### 2. OTP safe feed projection — bounded local contract first

Keep Recognize queue tblxC4SYtzYhJNcJS/history tblF1Hdqi3yoKTZPV and sms-recovery wfl6oiGMgUPmEkUuS unchanged. Baseline shared Cloud source18e6e993a at C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus/webflow-cloud-test; this checkout contains sibling unfinished changes, so never deploy it wholesale. Current Recognize corrected isolated package is sibling-owned and must be reconciled by that coordinator before future shared edits.

Known source hooks: src/lib/rs-recognition-sms.js event36–44/outcome120 onward; src/lib/rs-recognition-native.js77–80; existing native-recognition route. Smallest executable local scope: proposed pure projection helper src/lib/rs-notifications-feed.js plus proposed test src/lib/rs-notifications-feed.test.mjs, both new files requiring scoped approval. Map existing observed OTP event to allowlisted lane/source-event/request/time/status metadata. Never copy message body, phone, OTP code, invitation URL, cookie, access token or arbitrary payload. Unknown stays unknown; submitted requires actual provider reference and does not become delivered. Repeated source event produces the same feed identity.

Acceptance: requested/attempt_started/submitted/failed/unknown mapping fixtures; secret-bearing input fields absent from output; stable source identity; malformed/uncorrelated events rejected; no send/OTP/queue mutation. This establishes a tested projection contract, not live feed integration. Wire an event hook only after the feed destination/read model is accepted and the sibling reserves exact shared files; preserve OTP send and event claim behavior.

### 3. Live notifications-feed — reuse the sample UI and existing history

Existing source files: src/components/rs-onboarding/Prototype.tsx269,307,313,317,321 and notification-records.ts. Preserve visual layout, message formatter, filters and read-state behavior. Smallest recommendation: a recipient-scoped read model over existing lane history, so committed log events automatically become feed items without a second independently maintained history table. This is a proposal, not an existing implementation or selected endpoint.

After that approach and sources are approved, proposed route src/pages/rs-inputs/notifications.js and the helper above can serve authenticated safe items to the existing UI; exact proposed route URL /test/rs-inputs/notifications is not an existing verified endpoint. Reuse existing recognition/access helpers after inspecting their contract; never expose broad cross-base log reads to the browser. Do not invent an API binding or grant access from identity alone.

Dependencies: no persistent live feed writer/reader/destination was established; OTP events link person through requests, while two-way logs currently carry sender phone and lack verified person/tenant association; alerts events require verified recipient/scope. Tenants owns membership/options/assignments and cutover; copied records currently establish no eligible recipients. Resolve these source-to-recipient associations through existing owner systems before live reads. A source-only projection/UI fixture can proceed independently.

Acceptance after separately authorized integration: authenticated recipient sees only their permitted items; no cross-person/tenant leakage; each committed lane event appears once; feed refresh includes new events; read state and existing filters survive normal reload; queued/submitted/unknown/failed labels stay truthful; no OTP secrets; unavailable source shows an honest partial/error state without fabricated items. Approved known-good Inputs/recognition regression remains intact. Desktop/mobile live acceptance waits for deployment/publication authorization.

### 4. Alerts queue/sender and RingWaze — preserve existing event producer

Exact existing producer scope: schedules_calculatorv2.js1393–1467,1838–1871 creates WEF durable thread_logs from active_alerts/profile/tenant/show timing. Physical original base apptdhhNzduxm5gjn active_alerts tblgkCC4vcXIjSEn7/thread_logs tblGAFRLhh0ulElVG. WEC references time_engine_triggers tbl5uH3rBkSmKieGZ and operational wec-alerts tblqkxLPy9zZ2FI6z in app6XS1RvsPNRT6os; sync-airtable-controls.js145–161 is operational logging, not a verified user sender. Never replace calculations or call a direct runner endpoint as cadence proof.

First executable action is a narrowly scoped read-only trace from these existing event records to the actual sender/queue consumer and its source revision. Current map did not establish that consumer, so no exact sending-file edit or new queue API can honestly be named yet. Do not build a parallel sender to fill this gap. Recover external WEC handler source referenced in existing inventory if that lane is selected for integration; do not reconstruct from drafts.

RingWaze producer interface is owner-required but not found as an implemented Messages connection. Resolve existing event ID/time/show/scope/recipient evidence and actual emission point before naming an adapter file/endpoint. If no producer exists, report the smallest proposed adapter against the recovered RingWaze source for separate approval. No endpoint is invented here.

After consumer/interface discovery: adapt only its verified ingress and use stable source event IDs; consume Tenants consent/options/assignments/favorites without recreating preference ownership; preserve18 keys, entry counts, clock times and IANA timezone. Required own alert logic/queue remains distinct from OTP. Add safe event/feed projection at the identified commit point.

Acceptance: repeated producer event creates no duplicate request/send; revoked/suppressed/Test/ineligible records do not send; overlapping roles do not duplicate recipient notice; exact show/target/lead-time/count/timezone scope; real provider SID/outcome correlated to source/request; failed/unknown outcome does not blind retry; every observed event reaches the safe feed/read model. Natural approved runner proof only, no manual substitute. Sending requires separate authorization and verified provider configuration.

### 5. Two-way reply/log/feed integration beyond the show guard

Known files: worker-err.js63–74,585 onward,954 onward,1563–1630,1665 onward; ringstatus-proxy/worker.js rider/horse endpoints; original two-way automation wfleDEPvPZQjrHJ8E → twoway_logs tblfQDcePLpX2qkHW. Preserve commands/endpoints and existing log action.

Known implementable local contracts: allowlisted feed projection from correlated log metadata; response timeout/error fixtures; signature/replay tests with synthetic requests. Actual production authenticity/replay/STOP-START changes require existing Twilio inbound URL/method/suppression configuration and a verified persistent replay claim mechanism. Neither a new binding/store nor a new reply/support policy is invented. Sender/person/tenant mapping must use Recognize/Tenants permitted references; unresolved mapping must not widen feed visibility. Logging failures must not falsely report delivery.

Acceptance: exact known-show commands remain; ambiguous show never first-record guesses; invalid signature rejected once configured; repeat MessageSid produces at most one application reply/log/feed item; STOP/START/HELP does not conflict with verified provider handling; source failure yields existing honest response; safe reply/processing history correlates to MessageSid; TwiML success is response generation, not handset delivery. Live provider tests remain separately authorized.

## Dependencies and approval boundaries

Ready for scoped local approval: task1 selector correction, then task2 pure safe projection fixtures. Both can be proved locally without a feed table, provider connection or engine separation.

Independent read-only dependency work: existing alerts downstream sender and RingWaze source interface trace. Unverified Twilio config remains existing task recnPiu0W8nJu6gX7; no failed browser retries or credential request. Shared Cloud file/release reservation and exact corrected sibling artifact are necessary before shared writes/releases, not reasons to deploy mixed changes.

Later plain-language recommendation: show one safe history drawn from existing lane logs, preserve existing UI and keep OTP secrets out. This avoids maintaining two conflicting histories. If a persistent feed destination is later chosen instead, name its actual table/endpoint and approval before wiring producers. No such destination is claimed to exist today.

Prepared scope is complete when ordered tasks, exact local first change, source/version/protected files, per-lane acceptance, dependencies and this link are saved/read back in Messages and Agents. Runtime remains untouched.
