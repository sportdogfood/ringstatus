# Messages source and connection inventory — 10 October 2026

Read-only inventory at repository HEAD a5235466eed1b094a26db14ab34b514726abf91d. No SMS, Airtable writes, runner execution, deployment, provisioning or activation. Only this document was written. Instructions: AGENTS.md; messages-specialist-handoff-20261010.md; specialist-shared-contract-20261010.md. Twilio messaging-webhooks, security-api-auth, reliability-patterns and Cloudflare skills were read for this inspection. Connection values and capability webhook URLs are omitted.

## Verified current Cloudflare metadata

Control-plane GETs via existing Cloudflare connector succeeded on 10 October; these do not invoke the Workers.

- Account: ringstatus, dce50dddf8a92d48664f0089f8f5da22.
- ringstatus-sms: latest active deployment 2317eff4-b7d2-4ab9-b038-cb1ff12e8404, created 2026-05-10T12:01:17.960972Z, source quick_editor; version d18b0954-59bf-4ab5-a595-fd1861120cb0 carries 100% traffic. Script etag 1815bcd3eccf5250a8a04d8609c14ace73e060fa4723ec6173fc1b8e10caade4; compatibility date 2026-03-05.
- SMS secret binding names: AIRTABLE_WEBHOOK_URL, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN. Plain-text binding names: CUSTOMER_ID, DEBUG_SMS, FUZZY_THRESHOLD, RING_API_BASE, SHOW_TIME_ZONE_OFFSET, SHOW_TZ, UNKNOWN_REPLY. Values were not emitted.
- ringstatus-proxy: latest active deployment 0263a897-f45e-4d89-8503-0d8953ea151c, created 2026-08-21T13:07:29.618142Z, source dash; version de38179f-b130-41bb-9f78-f14ca179bf11 carries 100% traffic. Compatibility date 2026-03-03.
- Proxy binding names: AIRTABLE_BASE_ID, AIRTABLE_RIDER_TABLE, AIRTABLE_RIDER_VIEW, AIRTABLE_TOKEN (secret), CACHE_TTL, GITHUB_BRANCH, GITHUB_REPO, GITHUB_TOKEN (secret), SGL_API (service), UPSTREAM_BASE. Actual table/view/base values and service target remain unreported/unresolved in this bounded inspection.

Read-only SMS script content download reports WORKER_VERSION ringstatus-sms-livescorewidget-ringlane-v4-tz-2026-05-10. Function positions match local worker-err.js after the four multipart envelope lines. Normalized content length 51,852 and noncryptographic rolling31 fingerprint 4001915660 both match worker-err.js. This supports source correspondence, not a cryptographic equality proof or runtime acceptance. No deployment is inferred from filename.

## Repository alternatives and history

All paths below are under lib/cloudflare/ringstatus-sms; no tracked changes were present before inspection.

| File | Latest touching commit/date | Source behavior |
|---|---|---|
| worker.js | 0907783dafef2f9fd66f2f36dbadfd10ab4b50d6 / 2026-05-03 | Ring lane uses authenticated SGL ring API; clock and target-date fetch (140–191); required SGL_TOKEN/SGL_COOKIE names (12–14,1225–1234). |
| worker_rollback.js | 4cc00b5ec45a15a569af95d9c137d63853dd7b05 / 2026-04-28 | Earlier same ring-first plus rider/horse and legacy watch-trips design; distinct historical alternative. |
| worker-live.js | b265b8ddd405f95c732a1bb63c1684b6672c5b79 / 2026-05-09 | Ring lane uses LiveScoreWidget (150–165,182–225), version v2 (28). |
| worker-err.js | 19e9754b41006fd1b5005549f4bf93eec7046588 / 2026-05-10 | LiveScoreWidget version v4 timezone (29); explicit offset and timezone helper (484–495). Current downloaded source corresponds as described above. |
| openapi.html | 7753f452c4e0952cef44d9cf6aa3c8875c2b72d1 / 2026-05-05 | About-page HTML (1–88), not an OpenAPI contract. |

## Existing inbound path and limits

The deployed-corresponding worker-err.js parses inbound POST form Body, MessageSid and From (63–74), trims only; it does not normalize the phone or associate sender with a person, alias, tenant, consent or permitted scope. GET returns health/config information; non-POST returns empty TwiML. All paths use the same handler rather than a distinct authenticated status callback.

Ring-first deterministic recognition (1665 onwards) uses six named ring aliases. Multiple recognized rings get a one-ring-at-a-time response. No recognized ring enters rider/horse name lookup (954 onwards); explicit horse/h and rider/r prefixes; trailing class number, all/al, next/ne, result/results/res/re select lookup mode. HELP is excluded from name lookup and falls through generic ring instructions. STOP/START have no dedicated application handling. No inferred sender authentication or consent grant is supported.

For ring lookup, worker-err.js uses LiveScoreWidget; rider and horse requests use proxy endpoints (601–602). Proxy failures can fall back to watch-trips feed (741 onwards). Upstream env names include LIVE_SCORE_WIDGET_URL, SGL_COOKIE, SHOW_ID, SHOW_TIME_ZONE_OFFSET, TIME_ZONE_OFFSET, SHOW_TZ, SHOW_TIME_ZONE, HORSE_LOOKUP_URL, RIDER_LOOKUP_URL, WATCH_TRIPS_URL. Several are absent in observed deployed settings; defaults may therefore apply. Existing AIRTABLE_WEBHOOK_URL and RING_API_BASE bindings are not read by downloaded source.

Responses are escaped TwiML HTTP 200 (worker-err.js 1563–1579). Upstream timeout helper (1581–1602) returns status 0 on abort/fetch error; individual waits are 6500ms and multiple sequential fallback calls may accumulate. Airtable webhook POST is awaited (1616–1630), lacks a timeout/status check, and swallows thrown errors. Payload fields: twoway_id (inbound MessageSid), body, phone, title, first, next. This establishes a logging attempt, not a successful automation, table writer or delivery event. Hard-coded capability webhook remains redacted.

No X-Twilio-Signature validation, MessageSid replay deduplication, provider status/error callback handling, outbound REST send, or dedicated STOP/START/HELP suppression integration was found in the four alternatives or downloaded source. Existing TWILIO_AUTH_TOKEN binding alone is not validation evidence. Repeated inbound POSTs run lookup/log/reply again; dedupeRings only deduplicates ring matches. Provider suppression behavior and actual Twilio webhook/fallback/status-callback configuration remain unverified.

Source generic unsupported response directs the sender to win@ringstatus.com (worker.js 45–49; worker-live.js 49–53). This is an existing support destination, not evidence of an assigned human owner, completed handoff, support record or resume condition. Those decisions remain open.

## Upstream and runner map

- lib/cloudflare/ringstatus-proxy/worker.js: GET /rider and /horse both query same configured Airtable table/view (311–318,440–475,725–768,946–953), authenticate server-side with AIRTABLE_TOKEN. Actual tenant membership/consent scope is not enforced by this source lookup path.
- Proxy SGL connection prefers SGL_API service binding (94–96); otherwise SGL_TOKEN/SGL_COOKIE headers (106–108). Service target not inspected.
- heartbeat_slot_orchestrator.js: runner log/lock defaults C:\actions-runner\ringstatus (45–47); determines cadence from heartbeat slots (1894–1916); executes schedules_dailyv2, trips_dailyv2, trips_tagger and downstream lanes (1919–1951); publisher blocked after upstream failure (2034–2037). This is producer source ownership evidence, not current scheduled-run proof.
- .github/workflows/ringstatus-pipeline.yml and tagger.yml expose workflow_dispatch only (each 3–4) and ubuntu-latest jobs (11/13). These definitions do not establish Windows Task Scheduler registration, natural cadence, current runner checkout or operational health.
- docs/ringstatus_runner_options_overview.md 269–302 calls for establishing current Airtable/Cloud/Worker ownership before further Worker implementation. Messages must not recreate show schedules or producer calculations. WEF isolation remains an owner decision.

## Unresolved next discovery

1. Read Twilio account/subaccount, sender/Messaging Service and configured inbound method/webhook/fallback/status callback through an existing authorized read-only console/API; no test message. Identify provider suppression settings and recorded latest evidence.
2. Read Worker route/custom-domain/subdomain and exact deployed-version source through control plane; use cryptographic comparison if exact source identity is required. Inspect proxy binding values privately to resolve stable base/table/view IDs and service target; record only nonsecret identifiers and names.
3. Identify the hard-coded Airtable webhook's automation in its owning base without emitting URL. Read automation ID/name, enabled state, trigger/actions/output table and latest run result. Current Worker payload must not be assumed to write Messages rs_sms_requests/rs_sms_events.
4. Read actual Windows scheduled-task registration and latest approved runner logs without triggering jobs; verify runner checkout/source revision and show freshness. No manual substitute workflow proof.
5. Resolve sender/shared-phone/multiple-tenant policy, STOP/START/HELP ownership, replay policy, support actor/record/resume condition and engine isolation as development decisions. Preserve proven lookup behavior until an authorized delta is agreed.

Verification boundary: code and current Cloudflare configuration/source markers inspected; SMS delivery, sender eligibility, Airtable automation writing, natural cadence and live replies remain untested.

## Resumed provider/inbound discovery — 10 October, 12:16 p.m. New York onward

Read-only control-plane progress for task recnPiu0W8nJu6gX7:

- Workers account subdomain gombcg; ringstatus-sms workers.dev enabled, previews disabled. Derived public address https://ringstatus-sms.gombcg.workers.dev; not invoked. Account custom-domain listing has only tack.ringstatus.com → ringstatus-proxy, production. ringstatus.com zone 4d27c67a534de5d2ccf6795ccb26d871 has no Worker routes. This is not an exhaustive assertion about other zones.
- Proxy configured AIRTABLE_BASE_ID apptdhhNzduxm5gjn; AIRTABLE_RIDER_TABLE tblCtNpzJtrI19VKq, verified physical name watch_trips; AIRTABLE_RIDER_VIEW viwpJEf8bGL31ge90. SGL_API targets broad-tooth-b8ed production. GitHub source data binding sportdogfood/ringstatus-data branch main. No secret values recorded.
- Original base deployed/valid automation two-way wfleDEPvPZQjrHJ8E: genericWebhookReceived → createRecord in twoway_logs tblfQDcePLpX2qkHW. Fields map twoway_id → run_id, phone → from, body → body, title → title, first → now, next → next. No Messages-table destination in this action.
- Automation capability URL matches repository worker-err.js by length 108 and rolling31 fingerprint 3173203854, compared privately without displaying the URL. This is noncryptographic correspondence. Latest five returned automation runs succeeded; latest wfxtxUdSZ83icwGG5 at 2026-10-08T14:14:58Z, finished one second later, deployment wfdXtSHYCnhAtXk3g. Historical run success is not a new SMS/reply/delivery test.
- Recognize sms-recovery remains deployed/valid in its own base; no run or send triggered.
- Twilio plugin search returned no results; native Airtable external account remains the only observed provider integration. Browser-surface inventory timed out after 30 seconds and reset its kernel; no Twilio console was reached. Provider SID, sender/service, inbound method/webhook, callback and suppression settings remain unverified. No credential discovery or authentication flow attempted.

Remaining exact dependency: owner supplies read-only Twilio Console access or nonsecret configuration readback (account/subaccount SID, sender/service SID, inbound method/URL, callback, opt-out settings). Record result in task recnPiu0W8nJu6gX7; resume provider verification only when that evidence/access is available. Owner is the requested access actor, not an invented support operator. No provider configuration changes or SMS are authorized by this handoff.

## Independent scope completion after execution correction

The updated shared contract was read. The Twilio access blocker was isolated; remaining recipient and inbound mapping continued without runtime edits.

Read-only Windows scheduled-task inspection on 10 October found ringstatus-heartbeat Ready, last run 12:45:01 p.m. New York/result 0, repeating PT3M. Its action is PowerShell -File against the approved repository's run_tagger_heartbeat_lane.ps1; working directory is the approved repository. The latest inspected runner summary records pipeline_completed at 12:45:40 p.m. with zero deferred failures. heartbeat_slot_orchestrator.log records orchestrator_noop/no_active_show_scope at 16:45:40.830Z, heartbeat recQwF2uRQxUHpnLc, slot C, cadence 180, wec_ran false. Logs reside in C:/actions-runner/ringstatus, while action/summary identify execution code in the approved repository. No independent runner checkout commit is inferred from log location. Repository HEAD remains a5235466eed1b094a26db14ab34b514726abf91d. No scheduled job was started, changed, repaired or retried. This proves recorded natural heartbeat execution, not active WEF schedules/trips or message production.

Proxy view viwpJEf8bGL31ge90 was independently verified as AIRTABLE_RIDER_VIEW in physical watch_trips. The list-views tool does not expose its filtering configuration; membership/tenant scope cannot be inferred from the name.

Inbound safeguard map, verified against deployed-corresponding source: From is trimmed only; browser identity cookies are unavailable to this sender path. Signature authentication, persistent MessageSid deduplication, sender/tenant association, status callbacks, and dedicated STOP/START handling are absent from inspected source. Ring-match deduplication is not webhook deduplication. HELP does not implement provider help/suppression integration. XML escaping is implemented; sequential upstream waits plus untimed/unchecked Airtable webhook logging leave response-budget/error handling unresolved. The createRecord automation has no prior-message lookup, so repeat logs are not prevented by that action. Provider suppression may exist elsewhere and remains unknown. No forged/replay webhook or SMS test was sent.

Remaining policy decisions must precede an authorized runtime delta: unknown/shared/reassigned phone and multiple tenants; sender context selection without browser authentication; provider/application STOP/START/HELP ownership; unsupported request response and support actor; result record and resume condition. Preserve deterministic lookup behavior and existing support destination. Do not invent an operator, consent grant, default tenant or live delivery state.
