# wef-schedules-decoded

Updated **11 November 2026, America/New_York**. README version **2026.11.11.1**. Operating lock version **2026.11.08.1**: [locked baseline and change boundary](VERIFIED-BASELINE.md).

## Current operating state

### November 11 returned-token compatibility and Fast contract

Cloudflare version `ecf2c1de-32c3-40fe-9eee-e140873bb615` added valid returned-anonymous-token adoption and one identical GET replay. The existing local runner now uses the same recovery: validate the returned `_t`, update its existing in-memory token cache, wait 1,000 ms and replay once using the new bearer/decryption key. Invalid, expired or account tokens fail; a second token response stops without a third request. Headers, URLs, credential files, shared queue and pause are unchanged. The runner was restarted once at `2026-11-11T20:53:55.689Z`; TCP-only listener verification passed. Eight offline checks and Code Review passed. This does not establish recovery of every empty source response.

Exact pre-change runner files, hashes and restoration instructions are retained in [backup manifest](evidence/contract-release-20261111/backup.json) and [restore instructions](evidence/contract-release-20261111/RESTORE.md). [Review and executed checks](evidence/contract-release-20261111/review.md). Fast owns the approved class exclusions/order and existing alert-path corrections; its deployment and next normal scheduled acceptance are recorded separately in [Fast's operating README](../../wef-data/README.md). No manual collector is acceptance proof.

### November 10 approved fallback profile release

Deployed version `1c176106-d141-487c-a762-fd3ea765f2a6` retains original, header1, and header2, then adds `windows-clean` and `android-clean`. Stop on the first usable response. After all five fail, renew once and repeat once. Token renewal adds only the tested `Sec-Fetch-Site: same-site` header. Protected hashes confirm runner files, agent URLs/configuration, queue pause, transport, and caller files are unchanged.

All 39 focused offline checks passed. Cloudflare remote preview exercised the complete shared recovery function with earlier failures injected only inside preview. Both added profiles obtained nine rings for November 14/show `200000078` and November 25/show `200000081`, plus class `200031724` with nine trips. Code Review and its receipt verifier passed before deployment. Live ordinary schedule returned nine rings through the runner. After the queue rollout, forced live fallback selected `windows-clean` and returned nine rings for both dates. The sampled live class failed with HTTP 502 after all five profiles and one renewed sweep. Stored Cloudflare logs report upstream HTTP 200 and `Empty source payload`. Class recovery remains open; no correction or repeated class diagnostic was made. [Test runbook](FALLBACK-RUNBOOK.md) · [Release evidence](evidence/fallback-profile-release-20261110/)

Scheduled health at 5:27 p.m. Eastern independently verified runner recovery after the Windows profile installation. It returned HTTP 200, `ok=true`, and nine rings for default show `200000078`, using the runner without fallback. [Recovery notice](https://airtable.com/appCaSmL819KK7UFJ/tblki6YL1Fe3nnfT0/rec4zbNvVRCVIjNaH)

### Airtable decoder logs

**November 10 show-time link correction.** The clock producer preserves its actual source timestamp and source-date record key. When the clock date has no day in the default show, it uses that show's existing selected day for the native show-day link and records the difference in notes. It does not save an observation without a valid show-day. The normal 6:00 p.m. Eastern run linked show `200000078` and selected day November 11 while retaining the actual November 10 clock timestamp. Thirteen focused tests and Code Review passed. Historical records were not manually repaired. [Scheduled readback](evidence/show-time-links-20261110/scheduled-readback.json) · [Review](evidence/show-time-links-20261110/review-report.md)

The owner requested three dedicated tables in WEF Shows. Verified creation and readback found 12 change records, 17 run records, and three alert records, each with a unique primary identity. Four run records preserve the owner's original tunnel-console timestamps and reconnect details.

- [decode-change-log](https://airtable.com/appCaSmL819KK7UFJ/tblhL9n1COVPvr851) records verified corrections, symptoms, evidence, and whether a profile is active, historical, superseded, or preview-only.
- [decoder-run-log](https://airtable.com/appCaSmL819KK7UFJ/tbluyUciGZCt3xnsB) records saved health outcomes and authorized diagnostics with original timestamps, environment, route, lane, profile, counts, details, and available native show/day links. Imported preview rows use the captured batch-completion time and are labeled preview.
- [decoder-fail-alerts](https://airtable.com/appCaSmL819KK7UFJ/tblJZga4wx3JV3LCX) records outages, recovery, and the unresolved class diagnostic. Health notices link to the original warning and matching run. Delivery status is a lookup from the existing warning. The exact delivery timestamp is copied from that warning because the connector's date lookup truncated the time. The warning remains the existing notification authority.

The existing hourly heartbeat was updated to append one deduplicated run per health check and mirror warning events into the alert table. It also reads the named tunnel's connection count once from the Cloudflare control plane and logs that result. Confirmed zero-connection and restored-connection transitions are logged and reported once. An individual QUIC reconnect is not treated as proof that the entire tunnel is down. It still calls health once and sends outage/recovery notices through the existing chat. Its cadence and SGL source requests are unchanged. The first normal append under these new instructions is pending. Continuous console capture is not installed; the tunnel task does not save a logfile, so intermediate console warnings are not automatically ingested. No new Worker logging deployment or timer was added for these tables. [Schema and creation receipt](evidence/fallback-profile-release-20261110/airtable-tables.json)

### November 10 decoder outage and runner change

- **Transport outage, 1:22 p.m. Eastern.** The Worker received HTTP 530 from the runner route. Cloudflare showed the tunnel down with zero connections. The existing Windows relay and tunnel tasks were started. The relay accepted connections on port `18467` and Cloudflare showed four active connections. The cause of the task interruption remains unknown. [Outage warning](https://airtable.com/appCaSmL819KK7UFJ/tblki6YL1Fe3nnfT0/reccBEvCnBy1CLOGx)
- **Source failure after transport recovery, 2:29 p.m. Eastern.** Scheduled `/health` reached the runner, which returned HTTP 200 with unusable schedule data. All three fallback attempts stopped before sending a schedule request. SGL's `/utilities/get-config` returned HTTP 200 without `_t`, including on the final renewal. This does not establish credential expiry or explain why the earlier requests worked. [Health record](https://airtable.com/appCaSmL819KK7UFJ/tblcYgKFwKtILSdU8/recBHZPxfVwugzO6s) · [First failed renewal](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recLROhie4df9pluL)
- **Worker logging change.** Version `5d1e7779-fa86-4c46-912c-b48a8106b785` was deployed after local Worker preview. The existing health log now saves the runner HTTP status, why it switched to fallback, and whether an attempt stopped at token renewal or reached the schedule request. The renewal log records the exact redacted failure and status. The 2:29 p.m. scheduled check verified those fields were saved.
- **Runner schedule headers, 2:59 p.m. Eastern.** The runner switched to the Android Edge User-Agent and omitted the ordinary `path` header for `/schedule` only. The paired change returned nine rings for show `200000078` and source date November 11 in local preview with the supplied test bearer and the installed cookie. Either change alone returned `{}`. With the working PowerShell request, both show IDs (`0` and `200000078`) and the runner's `00/00/00` date resolved to the same nine rings. The 3:31 p.m. scheduled check failed after the relay stopped listening.
- **Runner schedule headers, 4:31 p.m. Eastern.** The cleaned Windows Edge User-Agent and client hints replaced the Android profile for `/schedule` only. With a token obtained by the runner's own renewal flow, preview returned nine decoded rings for November 14 and November 25. A controlled Windows request returned `{}` when any one of `authority`, `method`, `path`, or `scheme` was sent as an ordinary header. All four remain absent for runner schedule requests. The relay and tunnel were started after the tunnel disconnected at 4:03 p.m. Eastern. Port `18467` accepted connections and Cloudflare showed four active connections. The reason the tunnel stopped is unverified. The next scheduled health result is pending. Class headers, fallback, and credentials stayed unchanged. All 18 offline fallback and health checks passed.

### Repeat incident checks

1. **Find the first failed step.** Read the existing [health record](https://airtable.com/appCaSmL819KK7UFJ/tblcYgKFwKtILSdU8/recBHZPxfVwugzO6s). Compare its check time, runner HTTP status, lane, profile, stage, and reason with the last successful scheduled check. The initial runner request already checks reachability before fallback.
2. **Runner HTTP 530 or connection failure.** On the runner, inspect `Get-ScheduledTask -TaskName wef-decoder-runner-relay,wef-decoder-named-tunnel` and `Test-NetConnection 127.0.0.1 -Port 18467`. Check the named tunnel `wef-decoder-runner` in Cloudflare for active connections. With approval, start only the stopped task. A connected tunnel and open port confirm transport. Schedule data still needs verification.
3. **Fallback fails before schedule.** If the stage is `token-before-schedule`, read the matching entry in [token-refresh-log](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP). Compare the status and exact token error. HTTP 200 without `_t` means no token was issued in that response, so changing schedule headers cannot fix that attempt. Keep tokens and cookie values out of logs.
4. **Schedule reached but returned unusable data.** HTTP 200 with `{}` is a failure. A usable response has decode proof, the expected show ID, a source date, and a rings array. A valid empty rings array is accepted. Compare the failing route, show ID, date, and headers with the last successful request on the same lane. Check Git and compare the installed runner file directly with the dated runner snapshot because the installed copy is Git-ignored. If a live diagnostic is authorized, keep the request inputs fixed, change one suspected field at a time, and retain the redacted before/after result.
5. **Confirm recovery on the next normal check.** Read the health record after the runner restart or deployment. Confirm a new timestamp, Healthy status, an attempt with `lane=runner` and `usable=true`, and the actual ring count. A fallback pass, manual request, or preview cannot verify the scheduled runner.

| Date | Observed pattern |
| --- | --- |
| Oct 16 | The Schedules code already classified HTTP 200 with an empty JSON object as `soft_payload_empty` and logged the endpoint and body length. An empty default-date probe did not stand in for the authoritative dated request. Git `1e357b531`. [Guard](../../wef-schedules/lib/soft_payload_guard.js) · [Regression check](../../wef-schedules/tests/schedules_daily_empty_ping.test.js) |
| Oct 21 | The isolated decoder fixed a separate redirect-handling failure. It blocked upstream redirects explicitly, then verified usable decoded responses and normal scheduled collection. The October 22 empty show day was recorded separately as a warning. [Release and scheduled evidence](../../docs/coordination/wef-isolated-decoder-redirect-fix-20261021.md) |
| Oct 29–31 | Data distinguished HTTP failures, timeouts, decode failures, and malformed payloads. The reader retained status, selected diagnostic headers, body length, and hash even for a failed response. This is earlier evidence for separating transport from payload validation, with no isolated header cause. Git `4ab1af07f` and `708b0ac92`. [Data source recovery](../../wef-data/README.md) |
| Nov 6 | Adding only `CF-Worker` changed a seven-ring response to `{}`. [Isolation evidence](evidence/cloudflare-direct-preview-20261106/cf-worker-header-finding.md) |
| Nov 7 | Ordinary `authority`, `method`, and `scheme` headers each reproduced `{}` in isolation. Removing all three restored seven rings. Ordinary `path` alone still worked in that test. [Isolation evidence](evidence/request-header-isolation-20261107/findings.md) |
| Nov 8 | Fallback `header2` removed `path`, `priority`, and client hints and changed fetch-site metadata. Schedule and class detail worked with the full profile. The test did not isolate one header. [Preview evidence](evidence/fast-header-fallback-release-20261108/preview.json) |
| Nov 10 | Runner schedule preview needed both the Android Edge User-Agent and removal of ordinary `path`. It returned nine rings with the original show ID and date parameters. Separately, fallback token renewal returned HTTP 200 without `_t` before sending any schedule request. [Health record](https://airtable.com/appCaSmL819KK7UFJ/tblcYgKFwKtILSdU8/recBHZPxfVwugzO6s) |

These tests identify request differences that changed the result at the time. The trigger for SGL's change in behavior is still unknown. There is no separate tunnel preflight or new automatic restart.

### Recovery coverage and 60-day history

Reviewed window September 11 through November 10, 2026. Sources include tracked decoder history, dated release evidence, the earlier Schedules guard, and the installed runner comparison. No September decoder change was found in those paths. The earliest retained Schedules snapshot inspected is October 16. This inventory cannot establish unrecorded changes or earlier deployed versions with no retained evidence.

| Change or tested behavior | Current recovery coverage |
| --- | --- |
| Oct 16 empty-payload guard | Earlier Schedules implementation. Hosted decoder and shared proxy reject unusable private payloads. The installed runner still marks an empty object decoded, so the shared proxy must reject it. |
| Oct 21 redirect correction | Redirects are explicitly blocked in the decoder. This is a permanent correction, not a retry profile. |
| Oct 29 entries route and Data recovery | Existing route support and bounded Data recovery. Separate from source header selection. Git `873b36e80`, `b0b0e7fa3`, and `4ab1af07f`. |
| Oct 31 failed-response evidence | Data reader captures body bytes/hash before rejecting failed responses. Git `708b0ac92`. |
| Nov 4 source diagnostics and credential-only release | Decoder logs fetch/decode stage and redacted cause. The earlier credential-only release is historical evidence, not a new credential-change instruction. Git `e2f8cf9c0`; see existing credential release records. |
| Nov 6 CF-Worker isolation, relay/tunnel, direct TLS fallback | Local runner is primary. Existing direct TLS fallback avoids the tested injected-header path. [Fallback release](AUTOMATIC-FALLBACK.md) |
| Nov 6 empty guard, shared pause, caller timeout alignment | Payload validation, one-second queue, and timeout alignment are permanent behavior. They are not alternate header profiles. [Pacing](SHARED-PACING.md) · [Timeout evidence](evidence/data-timeout-alignment-20261106/review-report.md) |
| Nov 7 three ordinary-header removal | `authority`, `method`, and `scheme` are omitted from runner/fallback decoder construction. This was applied as a correction rather than retained as a failing profile. [Release](evidence/header-fix-release-20261107/release.md) |
| Nov 7 health recovery and Nov 8 actual request recovery | Original headers, header1, header2; after all fail, one token renewal and one repeated sweep. Stop on first usable response. [Health release](evidence/schedule-health-release-20261107/release.md) · [Request recovery](evidence/fast-header-fallback-release-20261108/release.md) |
| Nov 10 detailed failure logging | Existing health/token tables record runner status, fallback reason, token-before-schedule failures, and redacted token-validation reason. Active. |
| Nov 10 Android User-Agent plus no ordinary path | Tested locally, installed temporarily for runner schedules, then superseded by the approved cleaned Windows profile. It is not an automatic fallback profile. |
| Nov 10 cleaned Windows Edge request | Runner schedule requests only. Local preview with the runner's own fresh token returned nine rings for both November 14 and 25. It is not installed as a Cloudflare fallback profile. Scheduled health after installation is pending. |
| Relay/tunnel restart | Starting the existing Windows tasks restores the transport when stopped. There is no new automatic process supervisor or tunnel preflight. The cause of the repeated stops remains unverified. |

The active groups are transport selection, token acquisition, and schedule header attempts. Transport selection tries the runner and then Cloudflare fallback. Fallback token acquisition has one header configuration, shared by all three schedule profiles. It must succeed before a schedule profile can be exercised. Therefore three failed attempts can all represent the same missing-token failure without three different schedule requests being sent.

Before the approved November 10 fallback release, `header1` used Windows identification with document/navigation and cross-site metadata and retained ordinary `path`. `header2` removed `path`, priority, and client hints and used the original iPhone User-Agent with cross-site metadata. Neither matched the tested cleaned Windows request. The new release closes that profile gap while preserving both existing profiles.

**November 10 initial Cloudflare preview follow-up.** Before approval to deploy, an isolated remote preview added the two proven profiles, `windows-clean` and `android-clean`, to a temporary copy of the existing fallback decoder. The temporary fallback token request added only `Sec-Fetch-Site: same-site`. It obtained a fresh anonymous token, then each profile returned HTTP 200 and nine decoded rings for November 14/show `200000078` and November 25/show `200000081`. This exercised the existing direct TLS path through Cloudflare without the runner or tunnel. The later approved deployment is recorded above. The preview process was stopped and its temporary private cookie file removed. [Redacted preview results](evidence/fallback-profiles-preview-20261110.json)

- **8 November — actual Fast request recovery:** existing `/schedule` and `/classes/{id}` requests now use the same existing header1/header2 fallback after the original fallback fails. Stop on first usable decoded response; if exhausted, renew once and repeat once. Local first, independent fallback, existing one-second queue, URLs, credentials and task schedules are unchanged. Health retains its own recovery owner without nested sweeps. No public-feed changes or new infrastructure. Worker `5367f7f7-73ca-4009-9341-1c4665609c41`. [Evidence](evidence/fast-header-fallback-release-20261108/release.md).
- **Pre-deployment verification:** 47 focused checks passed; actual dated schedule preview returned seven rings and class 200031205 returned 27 trips, both via header2. Two normal scheduled collections completed at 09:39 and 09:45 Eastern on 8 November: 1,898 saves each, zero errors, including schedule and class detail. No manual collectors.

- **7 November monitoring proof, 7:19 p.m. Eastern:** HTTP 200, `ok=true`, default show `200000077`, seven rings, original headers through runner. No pending warnings. One health request; no collectors.
- **7 November verification:** 44 focused checks passed; all three schedule profiles returned seven rings locally and in Cloudflare preview. Live health saved correct show/show-day links; schedule returned seven rings via runner and sampled class `200031233` returned eight trips via fallback. Runner/fallback renewal logs verified. Warning transitions were simulated for that release.

- **7 November 2026 — approved schedule health/logging release:** `/health` reads the sole customer-15 default show from WEF Shows `shows.is_default`, then requests that show's default-date schedule. No hardcoded health show remains. It tries original headers, header1, header2, stopping on usable decoded data; after exhaustion it renews the fallback token once and repeats the three. Existing runner-first original request and independent fallback stay in place; alternate health profiles use the existing fallback transport. New recovery attempts pause at least one second. Superseded on 8 November for schedule/class requests by the approved Fast recovery release above.
- Health state/attempts are saved in `wef-shows.health-table`, linked to current show and matching source-date show-day. Renewal attempts from the existing runner and fallback are recorded without credentials in `token-refresh-log`. New outage/recovery notices are recorded once per transition in `warning-log`; the existing hourly monitoring chat submits pending notices and records submission/error state. Submission is not owner-read acknowledgment. No extra timer or collector was added.
- Release evidence and exact live/scheduled proof limits: [schedule health release](evidence/schedule-health-release-20261107/release.md). Worker version `4ac8cf13-9283-47ce-b8b6-ddb749cfa4d3`; runner logging uses the existing relay task and connection.

- **7 November 2026 — clock-only change:** the show-time task no longer calls `/health` or triggers `/schedule`, and no longer updates decoder health records. Its existing cadence, clock observation/save and clock-health reporting remain. Separate decoder health monitoring and the proxy are unchanged.
- **7 November 2026 — version response:** show-time sends `Sec-Fetch-Site: cross-site` on its clock request. The returned version is recorded in its receipt and existing clock notes; no `/health` request is made.

- Existing agents use the shared proxy. Data/Fast and Live were moved to the hosted shared URL; legacy tagger change was undone. This does not claim every historical script uses the proxy.
- Working route: shared Worker -> permanent tunnel -> existing runner -> SGL -> existing decoder. Local first; independent Cloudflare TLS fallback remains available on existing supported routes. Agents receive decoded data without source credentials.
- One shared queue spaces proxy operations by at least one second after the preceding response completes. Internal token renewal is not a separate agent queue item. No queue-wait maximum is configured.
- Runner and fallback obtain anonymous tokens through /utilities/get-config using existing private credentials and headers. Tokens are cached and renewed when absent or less than 24 hours remain; no token values belong here.
- Prior recorded shared Worker deployment: 2b71ed7b-f3f4-44a9-89df-4854a10503c6; superseded by the approved releases above. See [fallback](AUTOMATIC-FALLBACK.md), [pacing](SHARED-PACING.md), and [migration evidence](evidence/agent-proxy-migration-20261106/FINAL-SCOPE-VERIFICATION.md). Their earlier dated snapshots must not override this current summary.
- Data timeout alignment is installed: removed the default 18-second request cutoff and 28-second outer subprocess cutoff. Existing proxy failure deadlines, network/platform limits and optional explicit diagnostic timeouts remain. No invented queue deadline or delivery guarantee. A 72-second simulated Python-to-Node wait and 36 focused checks passed before installation; 36 checks passed afterward. No collectors ran. [Review](evidence/data-timeout-alignment-20261106/review-report.md), [installation](evidence/data-timeout-alignment-20261106/installation.json).

## Open work — do not resume without owner direction

1. **Class detail: PAUSED / OPEN.** Owner identifies this as the remaining paused class-detail task. Successful isolated class-detail pings (class 200031233, eight trips) do not close the paused work or prove all classes collect successfully. No broader class-detail implementation or retry is authorized by this documentation update.
2. **Show-time freshness: OPEN.** Normal scheduled saves are verified, including 07:54 and 07:57 Eastern on 7 November, updating the same show/day Airtable record recBo37q4NJZPPYCh. The saved freshness remains Unverified. The current implementation deliberately labels Date-header observations Unverified unless a positive Age header marks them Stale; it has no independently proven Fresh state. Successful save is not verified cache freshness. No freshness correction was made.
3. **Public Live routing remains separate.** Live currently targets the shared proxy. Direct runner tests of four public feeds returned `{}` with the three supplied headers; adding only `Sec-Fetch-Site: cross-site` returned usable class details, rings/groups and status `true`. Tested headers and fallback boundaries were sent to Live as information only. No Live migration or public-header failover was installed by this release.
4. **Shows browser discovery is not installed.** Two fresh-session browser checks verified discovery. Recurring Shows still uses proxied `/ring` and `/schedule`; browser tests are not deployment proof. This release did not change Shows discovery.


## Known trouble and acceptance limits

- **Direct Cloudflare fetch:** CF-Worker header injection was isolated as a sufficient cause of SGL returning HTTP200 with an empty object. Deleting it in Worker code did not keep it removed. The working runner path and independent TLS fallback avoid that tested path. Do not claim all403/502 failures are caused by credentials, IPs or throttling; those general causes were not proven.
- **Historical403 and502:** schedule403s and class502s were reported. Current sample success does not establish a cause or recovery for every historical failed request/class. Only the approved health recovery cycle above is enabled; broader retries or credential changes are not authorized.
- **Empty success bug:** installed runner decoder previously accepted an empty object as decoded. Hosted private-route validation rejects missing decode proof or empty/invalid top-level objects and invokes fallback. The fallback source includes the empty guard; installed runner source was not replaced as part of migration. A200 or decoded marker alone is not proof of usable domain data.
- **Runner interruption:** relay previously stopped with0xC000013A while the tunnel remained running; cause unknown. Restoration was verified, not a guarantee of unattended reliability. A tunnel alone does not prove a healthy runner.
- **Waiting:** queue wait has no configured maximum. Data's premature cutoffs are removed, but platform/network limits still apply. Other consumers retain their own existing timeouts; this change did not align every consumer. Scheduled throughput under sustained load has not been established by diagnostic pings.
- **Monitoring scope:** /health checks the current WEF Shows default show schedule and accepts the actual source ring count. Use the dated monitoring evidence above for current status. It does not certify every route, each class, today's full inventory, Airtable writes or every collector. No exact-eight-rings gate is permitted.
- **Show-time:** cadence is07:00–18:57 Eastern every three minutes, then19:00,22:00,01:00,04:00. One latest observation per show/source-date is updated. A task exit0 can mean Skipped or a successfully recorded outage; inspect `clock` and `clockVersion`; show-time no longer requests decoder health. Freshness remains open above.
- **Airtable422:** owner reported Data classes PATCH rejection of source-json. The supplied log omitted the class/value; cause and resolution were not verified in this proxy task. Keep it as an external reported issue, not a proven proxy defect or resolved item.
- **Existing test failure:** earlier broad run had80/81 passing; the Live future-selected-date test at wef-live/test/collector.test.js:365 also failed with frozen baseline adapters. It was not repaired or hidden. The36 focused checks exclude that unrelated test. [Baseline comparison](evidence/agent-proxy-migration-20261106/baseline-test-failure.json).
- **Scope correction:** unnecessary /classsignup, /classsignup/{id}, /entries/{id} additions and legacy adapter rewrite were withdrawn and never deployed. Legacy tagger restored exactly. Do not revive old-code migrations.
- **Proof boundary:** source diagnostics prove the tested payloads and lanes only. No manual collector run was used. Timeout alignment does not itself supply normal scheduled collection acceptance.

## Preservation lock

Keep connection, runner/tunnel, URLs, one-second pause, fallback, source headers/token renewal, existing routes and schedules unchanged. No extra routes, infrastructure, credential edits, manual collectors or record repairs. Any further code/deployment change needs applicable explicit owner approval; failed verification requires fresh approval before a correction. This is a documented operating lock, not a filesystem restriction.

## Historical README — superseded where inconsistent

**Current verified setup and change boundary:** [6 November 2026 permanent runner baseline](VERIFIED-BASELINE.md). The dated description below is historical; use that baseline for the current hosted request path, token renewal, live evidence and known limits.

Current addition: [automatic schedule and class-detail fallback](AUTOMATIC-FALLBACK.md), independent of the local runner and tunnel. Normal traffic remains local-first.

Current pacing: [shared one-second queue](SHARED-PACING.md) across proxy requests, with local-first and fallback unchanged.

Separate Cloudflare Worker for decoded WEF source reads. Existing legacy routes
are not routed through this Worker. It has no scheduled trigger and performs no
Airtable writes. SGL_TOKEN and SGL_COOKIE are server-side secret bindings copied
from the existing authorized proxy configuration; values are not in this project.

Supported GET routes: /health, /ring, /schedule, /people/:id and /classes[/id].
Only explicit source query parameters are accepted; customer_id must be15.
Encrypted fields use Wellington's public AES-GCM decoding method. Schedule `_`
evidence is retained for existing source-monitor columns and composite keys.

Status 2026-10-21: redirect handling corrected under new owner approval.
Uses manual redirects and rejects3xx without forwarding credentials. Pre-switch
live ring/schedule/people tests passed; both independent collectors now use this
lane. Two normal runs of each collector passed:435 schedule class IDs and four
linked tenant entries with stable record IDs and no duplicates. Existing empty
Oct22 show-day warning remains; no failed collection days. Original helper values
and five-minute task definitions are preserved. No manual runs or record repairs.

Offline tests, cached schedule proof and normal-run receipts are in evidence/.
See docs/coordination/wef-isolated-decoder-redirect-fix-20261021.md for release
evidence. Legacy proxy routes and task definitions remain unchanged.

## Returned-token repair — November 11, 2026

Version ecf2c1de-32c3-40fe-9eee-e140873bb615 deployed at 18:31:37 UTC. The decoder saves a valid returned anonymous token in the existing fallback memory cache and replays the identical GET once after the existing one-second gap. Invalid tokens, a second token response and replay failures remain failures. All 48 focused checks and actual Cloudflare preview passed (class 200031333, two populated trips). The release uses the active deployed baseline and excludes unrelated staged work. URLs, profiles, queue/pause, installed runner, credentials and Fast code are unchanged. Normal scheduled Fast save verification FAILED: new-version SglPacer calls for class 200031471 received upstream HTTP 200 with Empty source payload across the recovery attempts; the proxy returned 502. Its native class record recc0ebGwU1BoZTD3 has no full detail or last-success. The active normal Fast attempt began at17:18UTC, before deployment; post-deployment class requests used the new decoder. The passing preview used cgid=-1; Fast's normal request omitted cgid. That difference is untested and is not established as the cause. No further code edits or source calls followed the failed verification. [Repair evidence](evidence/returned-token-20261111/)

## Fast invocation and later recovery — November 11 candidate

Decoder version409af439-426f-4934-be19-a543b96e9825 is deployed; Fast integration and scheduled native saves remain unverified. Fast passes use the existing URLs with `X-RS-Recovery: defer`; this makes one invocation through the endpoint's proven lane/profile, or a family hint before that endpoint has succeeded. Valid matching responses establish a lane in the existing SglPacer storage; schedule dates do not reset it. No payload is cached.

The independent trouble pass uses `X-RS-Recovery: trouble`. Initial unknown endpoints retain established five profiles, one renewal, five profiles. Later trouble passes try one existing alternative, retain the proven lane until a matching success, and advance to the next alternative on failure. No repeated ten-attempt discovery, permanent failure gate, new cadence threshold or user option. Default callers and health recovery are unchanged. Existing pause, queue, routes and credentials are unchanged.

Diagnostic headers distinguish lane, profile, recovery state and redacted failure reason; upstream status appears only when actually observed. A proxy502 does not mean the source returned502. WEF Shows decoder-request-failures records actual captures, show/day links and separate request/recovery/save evidence.

### Test and release checks
Run the component collection-lane, fallback and pacing tests; test identity mismatches, zero rings, remembered lanes across dates, isolated errors, initial discovery and later one-alternative recovery. Test actual Cloudflare preview source responses before deployment. Verify the existing DO storage/pacing wrapper separately if the preview tool cannot bind SQLite DOs. Code Review must inspect exact frozen bundle against the active baseline and Fast's shared-day/invoke/save/trouble consumers. Deploy reviewed artifacts only, then observe normal scheduled invocation, capture, native save/readback and failure-log delivery. Never run a manual collector as proof. Evidence: `evidence/fast-trouble-20261111/`.
