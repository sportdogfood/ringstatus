# WEF acquisition lanes, access, recovery and alerts

Reviewed November 11, 2026 after the remaining read tests. These are source recommendations, not deployed changes. No production changes or collectors ran. Access labels refer to account/token requirements, not pricing. Diagnostic success does not prove normal scheduled collection. Identity-field acceptance remains for owner review.

## The nine lanes

Reuse data already fetched by another lane before adding a request. All source reads below are GET unless noted. API full protected data needs the anonymous-token replay and decoder described below; the public iphone reads do not need a bearer or cookies.

| Lane | Best primary from the evidence | Best fallback / scope limit | Actual payoff |
| --- | --- | --- | --- |
| **Show detail / default** | API `/shows?customer_id=15`; take the returned `default_show`, then its matching show record. | Existing browser default-show discovery for authority; browser-session GetShowData for selected-show inventory. GetShowData does not establish the default. | Full response: 802 shows, default 200000078 at capture. Session GetShowData: 429 classes and 82 divisions for show 200000078. |
| **Show-days** | Reuse the selected show's `show_days_list` from schedule. Keep the existing accepted-date publishing step. | Same-show `/ring` or `/classsignup` date list; GetShowData class dates can corroborate scheduled dates. Do not invent dates from an unrelated show or silently substitute the selected day. | Current show has 5 dates; completed show 200000077 has 4. No extra upstream call when schedule is already available. |
| **Show-time** | `version.json` HTTP Date with the existing working header. | An already-required successful API response's HTTP Date / ring `time_zone_date_time` is a demonstrated conditional clock source. No schedule request solely to obtain time. | Fresh version 0.1.45; HTTP Date advanced to 18:02:25 UTC. `builtAt` stayed a build timestamp. Ring body supplied local date/time and offset. Clock fallback is a recommendation, not installed. |
| **Rings** | Reuse rings/groups/basic classes from schedule. | API `/ring` for active/upcoming ring views; public ListAjax/widget for live activity only. | Schedule supplies all rings for its date; `/ring` returned 9 with group/order/timing data. `/ring/{id}` correctly identified rings but returned empty class/result arrays in both tested cases. |
| **Schedule** | Existing shared-proxy `/schedule` with its runner/profile/token recovery; retain its working URLs/pause. | Official Excel export, then rendered browser/manual handling. The export is verified; an import workflow is not built or tested. | Nov 8 response: 7 rings / 67 classes. Excel: 95 used rows, 3 columns, all 67 class numbers match that API schedule. Current Nov 11 response: 9 rings. |
| **Classes** | Reuse schedule for basic inventory; full `/classes/{class_id}` for class metadata/verification and detail when needed. | Public getLiveClassData for covered trip/score fields; paged `/classes`, session GetShowData and Detail HTML for their narrower inventory/view scopes. | Completed hunter detail: 8 trips; jumper: 4. Both decoded classes have `results_verified=1`. Directory paging/search worked; it is 10 rows per tested page, not a whole-show response. |
| **Trips** | Public `iphonev2/.../getLiveClassData?show_id&cid&t` for frequent covered trip/activity updates. Reuse a full class response if already fetched. | Full API class response through the existing decoder for complete metadata/unsupported legacy classes. | Completed hunter 8/8 and jumper 4/4 trip IDs, entry numbers and names match API; hunter scores/placings and jumper times/placings match. Earlier upcoming comparison matched 23/23. Legacy class 200031333 returned 404 while API detail worked. |
| **Class-signups / order-of-go** | Reuse full class order fields; `/classsignup` plus `/classsignup/{group_id}` for actual signup/group records when needed. | Public live-class OOG/actual-order fields where populated; full class response for its own scope. Legacy signup Index returned DoLogin. | Completed signup group returned 6 entry/class records across two classes with planned order; upcoming group returned 2. Completed jumper legacy OOG matched API. Hunter planned OOG was blank on legacy and unset on API, while actual order was present. |
| **Results** | Reuse decoded full class result/verification fields; use `showChamps` for champions and division/circuit readers only for requested standings. | Public live-class scores/placings as provisional data; rendered Detail view as manual backup. Legacy scores alone do not establish final verification. | Daily: 67 classes; showChamps: 32 divisions; division detail: 3 entries / 3 classes; circuit: 47 divisions, one division's 6 standings and 21 point-detail records. Both completed class examples are verified in API. |

## Access decisions

**Public without bearer/cookies in the controlled requests:** version.json, ListAjax, widget GET and JSON POST, ClassStatus, Detail HTML, supported getLiveClassData classes, and the official Excel export. ListAjax/widget were valid empty wrappers at test time; ClassStatus false did not prevent the completed class returning scores. They are not substitutes for a real class-data check.

**Anonymous API access is a two-step flow, not proof that a token-free body is complete.** A token-free response can issue `_t` while leaving protected values null. Replaying the exact GET with that anonymous token returned protected blocks; the existing decoder then filled trip/entry/horse/person IDs, class verification, scores, times and orders. Completed hunter and jumper controls reproduced this difference. Other API list/summary routes were readable without those protected blocks. No account login was used for successful API schedule, classes, rings, entries/people detail, signups or results tests.

The tested flow follows SGL's client: adopt returned `_t`, remove it from the data, replay the original GET once with the new bearer, then decode and validate the useful fields. Do not accept a matching row count with null required values as full collection success. Our deployed decoder's returned-token rejection remains an existing unimplemented gap; these diagnostics did not change it or establish a new cause for a production 502.

**Session-assisted, not anonymous:** GetShowData, SearchHorse and SearchPerson returned full data in the existing browser session. Matching no-cookie and fresh-anonymous controls returned DoLogin; the API GetShowData/horse-search/person-search siblings returned 401 with the working anonymous token. Legacy search returned 5 horse and 4 person matches. They are useful attended lookups/inventory backups, not anonymous refresh methods. No browser cookie/account token was copied into production.

**Transport limits:** successful local diagnostic reads do not establish ordinary Cloudflare fetch compatibility. The installed direct-TLS fallback and route allowlists are separate facts. Related API token issuers share the same service; they are not an independent backend outage fallback.

## What is actually deployed in the proxy

The existing connected Cloudflare app was used for a passive script/deployment read at **12:27 p.m. Eastern, November 11**. Active Worker version: `1c176106-d141-487c-a762-fd3ea765f2a6`, 100%. This did not invoke SGL.

| Current deployed source routes | Scope |
| --- | --- |
| `/schedule`, `/classes/{id}` | Existing protected-data routes with five-profile request recovery. |
| `/classes`, `/ring`, `/ring/{id}`, `/people/{id}`, `/entries` | Existing additional protected-reader routes. They do not all receive the schedule/class-detail five-profile sequence. Parameter restrictions are route-specific. |
| `/homepage/getLiveClassStatus`, legacy ListAjax, legacy LiveScoreWidget, legacy getLiveClassData | Existing public-feed routes. Live's current source reader calls its public endpoints directly instead. |
| `/health` | Schedule recovery and monitoring. Deployed script does NOT include the candidate class-health check. |

**Not deployed:** `/shows`, GetShowData, `/classsignup`, `/classsignup/{id}`, `/results` and its related result routes, `/entries/{id}`, ClassStatus, Detail, Excel export and version.json. Existing `/entries` does not imply `/entries/{id}`. Existing `/classes/{id}` does not imply a signup/result path. No new routes were added during this research, and no similar-looking family was automatically exposed.

## Reusing success across related endpoints

Use a proven family profile/token/decoder as the starting point, then verify the exact method, query, ID meaning and complete useful body. Similar paths are not access proof.

- Class detail needs a class ID; signup detail uses class-group ID in both path and `eid`; division results use show-division ID; circuit results use circuit-division ID plus circuit ID. IDs in the tests came from actual source responses.
- Paged classes/entries/people returned 10 rows per sampled page; page 2 was checked. Their full saved responses are not claims that all pages were collected.
- API class detail and iphone live data agree on the tested trip identities and populated scores/times, but blank versus zero and unset-order representations differ. Preserve those source differences. API verified-final flags are not present as equivalent confirmation in the legacy row feed.
- Signup group 200028543 covers two classes; its six records are not six final result rows for one class.
- GetShowData gives broad basic inventory, not ring-by-ring timing or full trip/results data. Browser success does not prove its API sibling or anonymous access.
- Widget GET and source-backed JSON POST both returned empty live_data. Do not add a new preflight that merely duplicates an actual data request.
- A successfully issued anonymous token worked on its replayed data reads; it did not authenticate the account-protected search/GetShowData siblings.

## Fallback: installed behavior and the required plan

**Installed for schedule and class-detail requests:**

1. Existing local runner through the permanent tunnel first.
2. On failure/unusable data, existing independent Cloudflare direct-TLS fallback tries `original-header`, `profile-fb-header1`, `profile-fb-header2`, `windows-clean`, `android-clean`; stop at the first usable decoded body.
3. After all five fail, renew once through `/utilities/get-config`, then repeat the five once. A failed renewal stops that cycle. Token bootstrap can occur before data attempts if no usable cached token exists.
4. Return useful data or a real failure. Shared proxy operations are serialized with the existing one-second gap; profile retry pauses exist. This does not pace Live's direct calls. Fast additionally retries a final failed schedule once; it does not add a class retry.

**Other lanes currently differ:** Shows has browser discovery then existing proxy recovery; public Live has its direct reader with a transient retry after 500 ms, not the proxy's profile sweep; the clock has no installed alternate source (an already-required API response clock was tested as a conditional backup); publishing accepted show-days does not require an upstream fallback.

**Required lane procedure for review, not installed across all lanes:** use the first real due request as the lane check and reuse its data. If recovery exhausts because the common source/transport is unavailable, pause that affected stage for that normal run, keep unaffected saves and the last good data with its real timestamp, and emit one incident. Retry on the next existing scheduled run. A class-specific missing/unsupported response is recorded for that class and does not automatically establish a whole-source outage. Resume only after usable same-show/date data saves successfully. Do not add a separate schedule/health ping before every data fetch.

The current Fast loop can continue into other class requests after failed recovery; a common class outage can therefore repeat expensive failed work. There is no proven shared stage-wide pause/alert contract yet. Separate schedule/class stages, sequential direct requests, class gating, returned-token handling and alert coverage are the concrete outstanding work; none was silently changed by this review.

**Fallback data sources:** supported iphone fields can be reused before fetching full detail again, subject to the owner's field review. GetShowData is a possible browser-assisted inventory backup. Browser capture/official export/manual upload are recovery options, but no general CSV importer or class/results browser recovery pipeline is installed. Do not call them automatic fallbacks. Manual recovery needs an accepted responsible person, exact file/input, target and verification; none is assigned here. All can still share an SGL outage.

## Failure alerts

| Lane / failure | Existing behavior | Gap that must remain visible |
| --- | --- | --- |
| Decoder schedule health | Recovery exhaustion creates/retains Outage in existing health/warning logs; one Recovery transition after success. Hourly chat monitor submits pending owner notices, records submission, mirrors fail alerts and appends run evidence. | Hourly, not continuous. Deployed health does not test class detail. |
| Runner/tunnel | Hourly passive tunnel lookup records connection count; confirmed zero-connection transition/recovery is notified once. | A QUIC reconnect is not a full outage. Status between hourly checks can be missed. No continuous log watcher. |
| Fast schedule | Collection errors and existing desktop/Records notification attempt; failed delivery is retried on later normal runs. | Submission/sent is not owner/coordinator acknowledgment. |
| Fast class requests | Failures are in collection receipts. Existing alert filter includes class source-json write rejection but excludes general class endpoint request failures. | Class fetch outage notification is incomplete. Schedule success does not cover it. |
| Live | Existing per-lane logs and deduplicated errs-log records. | No verified automatic owner/all-coordinator notification contract. Direct four-worker dispatch and 500 ms retry differ from the requested pacing. |
| Shows/show-days | Existing source/run/error logs and heartbeat; last verified show authority is retained on discovery failure. | Automatic collector-to-chat delivery is not installed; documented changes/handovers use established manual chat receipts. |
| Show-time | Existing clock health record tracks success/outage separately. | No verified separate owner-warning delivery or replacement clock source. Date-link mismatch and freshness limitation remain open. |
| Signups/results as new readers | Source opportunity evidence only. Existing Data derives results from class responses. | New independent endpoint collectors/alerts were not built. |

Required incident content: affected lane/endpoint, show/show-day, original failure time, actual HTTP/decode/transport cause, attempts and selected source, saved/unaffected work, pause state, and recovery condition. Notify once on outage and once on verified recovery. Keep warning-log as delivery authority; failed delivery retries on the next normal check. Owner chat submission is not acknowledgment. Records, Timing, Messages and Waze handoffs and acknowledgments must be recorded individually; automatic delivery to all four is NOT established. No new alert table/timer is proposed.

## Failed, empty, and untested are separate outcomes

| Observed outcome | Meaning |
| --- | --- |
| Token-free full-class bodies had null IDs/results; bearer replay decoded complete values. | Incomplete protected content, even with HTTP 200 and the correct row count. Validate the fields required by the lane. |
| Legacy GetShowData/SearchHorse/SearchPerson without cookies or with anonymous bearer returned DoLogin; existing session succeeded. | Session-assisted opportunities, not anonymous acquisition/refresh fallbacks. |
| API GetShowData/horse-search/person-search returned 401 with anonymous access. | That anonymous method failed. No account-token API access is claimed. |
| Legacy signup Index returned DoLogin; anonymous API signup reads succeeded. | Prefer the demonstrated JSON read contract for signup data. Reserve/Save/Cancel were not invoked. |
| Legacy getLiveClassData for class 200031333 returned 404; other classes worked. | A class-specific unsupported response, not evidence that the whole public family is dead. |
| Earlier browser daily-results request returned 401; the new local anonymous-token request returned 67 classes. | Earlier request failure was not a permanent route failure. |
| Ring detail returned matching metadata but empty class_groups/result_classes twice. | Valid limited/empty scope; not a proven complete order/schedule fallback. |
| Championship lists for three source-known completed shows returned empty arrays, before and after anonymous replay. | No published championship/chip ID to positively test the conditional detail route. No ordinary class/division ID was substituted. |
| Account-associated horses/people lookup timed out in earlier diagnostics and its browser account list was empty. | Not whole-show inventory or an independent anonymous source. It is outside the current primary acquisition plan. |

Empty live wrappers, zero standings, false ClassStatus and a person with zero trips are valid states. OTP/password/bank/account mutation actions remain outside scope. The prior normal proxy class failure remains unverified as a recovery; source diagnostics are not scheduled workflow proof.

## Original source coverage and the remaining proof boundaries

The captured 5.47-million-character main.js and legacy Detail inline scripts were inspected for route literals, concatenated getters, callers/parameters, response handling and update events. Inventory remains 229 distinct non-video endpoint records; 53 video templates were excluded. Four dynamic expressions were dispositioned as video/subscription/retry handling. This is captured-source coverage, not every endpoint on the server. Owner-deferred account, billing, edit/save, supplies and registration actions were not invoked.

The remaining read pass made 68 sequential diagnostics, including browser-session controls, and reused earlier successful captures. Thirty-six endpoint records now have full primary payloads and 15 matched supplementary payloads, independently checked against captured files and Airtable readback. Secrets alone are redacted. Large responses use payload-1/2/3 with reconstruction instructions in response-1; payload-example holds complete named alternative responses, not a repeated class excerpt. The official Excel original bytes and every used cell value are retained.

**Concrete proof still unavailable:** championship detail needs a championship/chip ID which the three tested lists did not publish. Live ListAjax/widget were empty, so active on-course updates, event timing and Pusher/frame-event coverage are not proven replacements for polling. Completed hunter/jumper scores and order were proven; they do not simulate an active class. Browser-session unattended renewal and a business-file importer are not established.

**Implementation remains separate and paused:** returned-token adoption; class health/failure alerts; separate Fast stages and sequential direct pacing; source-role acceptance; exact show-time show-day linking/freshness. No source switch, proxy route, cadence, credential or business-record repair was made.

## Decisions supported by this review

1. Reuse one successful schedule response for rings/groups/basic classes/dates, and existing Live/full-class responses for compatible trip/OOG/result fields. Fetch champion/division/circuit summaries only for the distinct requested scope.
2. Keep direct public readers where proven, using the owner's agreed sequential pacing. No new proxy merely for a public URL.
3. Keep the shared proxy for current Fast schedule/class recovery. Correct its returned-token handling and class-failure/pause/alert gaps through the existing paused work, after the agreed implementation approval.
4. Keep GetShowData and legacy searches as browser-session inventory/lookup backups. Keep the tested official Excel file as a manual schedule backup. Neither constitutes an installed automatic importer or anonymous refresh.
5. Add no proxy route because a sibling exists. Prove exact read/parameters/data payoff first. Existing omissions above are documented, not quietly filled.

## Evidence

- [Original opportunity document](sgl-application-endpoint-opportunities-20261111.md) and [complete source inventory](evidence/sgl-stage1-20261111/catalog.json).
- [Original auth diagnostics](evidence/sgl-auth-20261111/findings.json), [decoded/token proof](evidence/sgl-auth-20261111/token-handling-proof.json).
- [Full GetShowData payload and controls](evidence/getshowdata-20261111/README.md).
- [Same-class comparison](evidence/wef-source-strategy-20261111/same-class-comparison-current.json).
- [Corrected payload provenance](evidence/payload-audit-20261111/independent-source-verification.json): 13 incorrect copies corrected from actual captures; old readback alone was insufficient.
- [Passive deployed Worker check](evidence/wef-lane-map-20261111/cloudflare-passive-check.json); actual current source readers and route validation inspected without invoking them.
- [Deployed recovery release](../../workers/wef-schedules-decoded/evidence/fallback-profile-release-20261110/release.md); [unreleased dual-health failure](../../workers/wef-schedules-decoded/evidence/dual-health-20261111/task.md).

Source diagnostics and research-table writes were authorized for this test pass. No collectors, production code/configuration, proxy routes, credentials, cadences or business-record repairs were changed.
