# WEF acquisition lanes, access, recovery and alerts

Reviewed November 11, 2026. This is the source map and proposed operating plan; no production changes or collectors were run. Access labels mean account/token requirements, not pricing. Diagnostic success is separate from normal scheduled collection proof. Identity coverage remains subject to owner review.

## The nine lanes

| Lane | Sources and actual payoff | Access and direct/proxy position | Formats and remaining checks |
| --- | --- | --- | --- |
| **Show detail / default show** | API `/shows?customer_id=15`: 802 shows plus `default_show`. Legacy `GetShowData?show_id=…&company_id=15`: one selected show's metadata, 429 classes and 82 divisions. | `/shows` returned data and anonymous `_t` without cookies/bearer in the local test. Shows currently uses browser discovery, with existing proxy ring/schedule recovery. `GetShowData` worked with the existing browser session; cookie omission and fresh anonymous bearer failed. Neither source needs a new proxy merely because it exists. | API JSON; legacy JSON; browser-rendered application. GetShowData requires an input show ID and does not choose the default. Its API sibling `/add-entry/getShowData` rejected anonymous access. |
| **Show-days** | Existing Shows publishes dates from its accepted show's `available_dates` and links each day to that show. Schedule `show_days_list` supplies dated source evidence. GetShowData supplies show bounds and individual class dates. | Publishing accepted dates needs no additional SGL request. Access to the originating source is inherited from the Show/Schedule lane. | Source date list versus show bounds versus class dates. Keep them distinct: 20 GetShowData classes have `0000-00-00`; they must remain undated. Current publisher uses accepted dates, not an invented new endpoint or automatic range replacement. |
| **Show-time** | Direct `https://sglapp.showgroundslive.com/version.json`; record its HTTP Date as source time and log the version. Read the default show from WEF Shows and link the time record to show/show-day. | No login, token or shared proxy in the tested request. Existing clock reader uses iPhone User-Agent and `Sec-Fetch-Site: cross-site`. | JSON version/build plus HTTP Date/Age. The clock does not call `/health` or `/schedule`. Cache freshness is not independently established. The reported wrong-day link is real: code can fall back to the selected show-day when a source-date day is absent; correction is still paused. |
| **Rings** | API `/ring`: local test returned nine rings. Schedule also supplies rings, so reuse it for inventory. `LiveScoreWidget` supplies current live-ring observations. `ClassStatus` provides class/ring status. | `/ring` returned data and anonymous `_t` without account credentials. Widget/ClassStatus direct local requests worked without cookies/bearer; a legitimate empty live feed is not a failure. Proxy `/ring` and `/ring/{id}` are deployed; direct public Live readers already exist. | API JSON; widget GET and source POST; ClassStatus JSON. Ring inventory and live occupancy are different facts. `/ring/{id}` is source-backed and proxy-supported, but no retained successful test establishes that detail variant. |
| **Schedule** | API `/schedule?date=…&show_id=…&customer_id=15`: rings, groups, basic classes, status and trip totals in one response. Nine rings were decoded. Legacy official ExportToExcel returned a binary file. | API anonymous access worked locally, both data+issued-token and bearer/protected response. Normal Fast uses the shared proxy for token/decoder/pacing/recovery. This is an installed design, not proof every API call intrinsically requires a proxy. | JSON raw/decoded; rendered browser; official Excel. Excel contents/import are unverified. Do not equate a basic class/date directory with a full ring schedule. Keep `show_id` and the exact source date fixed during recovery. |
| **Classes** | API `/classes` returned a paged directory and a real test class ID; `/classes/{class_id}` returned full class detail and two trips in one test, 23 in the same-class comparison. GetShowData supplies whole-show basic class metadata. Legacy `Detail` HTML supplies another class/group/ring metadata view. | API anonymous-token flow succeeded locally. Full class-detail collection through the normal proxy remains failing/unverified as a recovery. GetShowData needs the working browser session in our controls. Detail HTML was readable anonymously. | Paged directory JSON, detailed/protected JSON, bulk legacy JSON, HTML. These are different scopes. Proxy has `/classes` and `/classes/{id}`, but its directory query allowlist currently omits the tested page/sort/search parameters. |
| **Trips** | Reuse full class response or direct `getLiveClassData?show_id=…&cid=…&t=…`. Same-class comparison: 23 versus 23 rows, all trip IDs matched; names and entry numbers agreed. Live already saves class, entry and day-0 trip observations from one response. | Legacy live data worked without cookies/bearer for class 200031716. Class 200031333 returned 404 on that legacy feed while its API detail returned two trips. Existing direct reader therefore has useful but incomplete demonstrated coverage. | Full API trip objects versus smaller legacy rows, plus class Detail HTML. API body 65,766 bytes; legacy 19,257 (about 71% smaller body, not a measured speed/call-count saving). No decision that identity is lacking. Active/hunter/completed field coverage remains unverified. No separate new `/trips` route is justified by these findings. |
| **Class-signups / order-of-go** | API `/classsignup` returned nine rings. `/classsignup/{group_id}` returned class/group metadata and two `entry_x_classes` records. Full class response also has order information; the legacy live schema includes OOG/activity fields. | Both API reads returned data and anonymous `_t` with no account credentials in local tests. Legacy `/iphonev2/classsignupv2/Index` returned `DoLogin`. Signups routes are NOT deployed in the shared proxy. | API list/detail JSON, legacy HTML, full class OOG and live OOG fields. `{group_id}` and `eid` used the actual class-group ID, not a class/entry ID. In the 23-trip comparison, legacy OOG was blank while API planned order was populated. Reserve/Save/Cancel signup actions are outside this read review. |
| **Results** | API `/results?div_type=jumper&…`: 24 divisions and 20 loose classes. `/results/{division_id}?channel=1&…`: matching division, three classes and zero standings entries. Full class detail includes result/verification data; legacy live rows have scoring/timer fields. | The two API result reads worked without account credentials and issued anonymous `_t`. Direct `/results/daily` browser request returned 401. No results routes are deployed in our proxy. | Division/standings JSON, class detail JSON, live provisional fields, daily/championship/circuit families. `{division_id}` is not `{class_id}`. Zero standings is valid empty data, not proof of populated-result coverage. Final verification cannot be inferred from live activity. |

## Access decisions

**Direct requests demonstrated without token/cookies:** version.json, ListAjax, LiveScoreWidget, ClassStatus, Detail HTML and the supported getLiveClassData class. The official Excel route also returned a file without credentials; contents remain unchecked. The Live sample for ListAjax/widget was valid but empty, so successful transport is not proof of complete live data.

**No account login needed in our API tests:** shows, ring, schedule, classes/list/detail, classsignup/list/detail, results/list/division and users/getUser returned anonymous `_t`. Several returned readable data in the same response. Bearer calls to schedule/class returned encrypted blocks that the decoder read. These endpoints are not account-login routes simply because they can use a bearer token. Local success does not prove direct ordinary Cloudflare fetch; the existing direct-TLS fallback is a separately tested transport.

**Working session required in the controlled GetShowData pair:** same browser/URL/headers with cookies returned full data; omitting cookies returned DoLogin. Fresh anonymous bearer with iPhone and Windows profiles also returned DoLogin. The individual determining cookie and an unattended session renewal are unverified. Do not copy a browser's private session into a new production configuration.

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

Reuse the proven request and response-handling logic as the starting point, not as a passing result for another URL. Require its actual method, parameter names/ID meaning, same show/date, expected body shape and useful fields. Keep data interpretation separate from whether transport succeeded.

| Proven family / related opportunity | Safe reuse and limit |
| --- | --- |
| Anonymous API shows/ring/schedule/classes/signups/results | The same anonymous token was used successfully for schedule/class. Shared token storage, returned-`_t` adoption and protected decoding are the reusable mechanism. Do not assume every issued token was separately replayed or that every API path accepts anonymous access. |
| API list/detail pairs | Reuse headers/token/decoder, with the actual ID from the list. Class detail needs class ID; signup detail used class-group ID; result detail used division ID. Each body and pagination contract still needs checking. |
| Legacy `/iphonev2/addentryv2` and API siblings | GetShowData versus `/add-entry/getShowData`, SearchHorse versus `/horses/search`, SearchPerson versus `/people/search` are source-backed pairs. Similar caller parameters do not prove equal access: legacy GetShowData session worked while API anonymous requests returned 401. Search sibling access/coverage has not been exhausted. |
| Public `/iphonev2/…/liveclassv2` reads | Reuse the successful public profile for the exact read. A successful class does not prove all class IDs: one class returned 404. ListAjax, ClassStatus, Detail and getLiveClassData are different response formats, not interchangeable paths. |
| LiveScoreWidget GET/POST | Both forms exist in source. POST includes datetimestamp/customer_id/from_wp_api; GET is the existing Live reader. Empty wrappers require a live data sample before coverage equivalence. |

The SGL application saves a returned `_t`, removes it from the response and retries a GET once with the newly stored token. Our deployed decoder still rejects such a data response. That difference was reproduced offline and by authorized replay, but its production correction has NOT been implemented. Additional token-issuing paths are on the same API service; they are not independent outage protection.

## Fallback: installed behavior and the required plan

**Installed for schedule and class-detail requests:**

1. Existing local runner through the permanent tunnel first.
2. On failure/unusable data, existing independent Cloudflare direct-TLS fallback tries `original-header`, `profile-fb-header1`, `profile-fb-header2`, `windows-clean`, `android-clean`; stop at the first usable decoded body.
3. After all five fail, renew once through `/utilities/get-config`, then repeat the five once. A failed renewal stops that cycle. Token bootstrap can occur before data attempts if no usable cached token exists.
4. Return useful data or a real failure. Shared proxy operations are serialized with the existing one-second gap; profile retry pauses exist. This does not pace Live's direct calls. Fast additionally retries a final failed schedule once; it does not add a class retry.

**Other lanes currently differ:** Shows has browser discovery then existing proxy recovery; public Live has its direct reader with a transient retry after 500 ms, not the proxy's profile sweep; the clock has no separately proven alternate source; publishing accepted show-days does not require an upstream fallback.

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

| Actual failed check | What it establishes |
| --- | --- |
| GetShowData with no cookies or fresh anonymous bearer | DoLogin; this anonymous access method failed. Existing browser-session access succeeded. |
| API `/add-entry/getShowData` and `/users/getLoggedInUser` with anonymous access | HTTP 401. They are not demonstrated anonymous acquisition/recovery methods. |
| Legacy classsignup Index with tested group/team inputs | DoLogin. Not a demonstrated anonymous signup reader. |
| AuthenticateUserViaJWT with the working anonymous token | Invalid Token / success=false. Not a working anonymous session bridge in our controls. |
| Legacy getLiveClassData for class 200031333 | 404 `{}`; that class/profile failed. Class 200031716 succeeded, so the endpoint is not universally dead. |
| Normal/live proxy class-detail attempt | HTTP 502 after recovery; prior preview success did not resolve it. Production class recovery remains unverified. |
| Direct browser `/results/daily` request | 401 for that request. Fresh-token/form coverage is unexhausted; do not declare the route permanently unusable. |
| GetAssocaitedHorsesAndPeople | Diagnostic timeout with no HTTP response. Availability/authentication remain unknown. |

Valid empty/status responses are not outright failures: empty live wrappers, zero standings, false ClassStatus and empty account-associated horses. An empty account list also does not meet a whole-show directory need. OTP/password/bank refresh/consent actions are outside scope, not failed data reads.

## Original document and remaining source-backed opportunities

The loaded 5.47-million-character main.js and legacy Detail inline scripts were inspected for route literals, concatenated service getters, callers/parameters, response handling and update events. Inventory: **229 distinct non-video endpoint records**, expanded from the original 38; 53 video route templates were excluded. Four unresolved dynamic call expressions were dispositioned as video downloads, an expanded subscription wrapper and an existing retry, not four undiscovered acquisition endpoints. This is coverage of the captured loaded source, not every server endpoint or a new fresh bundle today.

The anonymous-auth pass covered **29 runtime route patterns and 11 source-only account/action contracts**. Later GetShowData and same-class controls deepened existing route evidence. The 229 endpoints have NOT all been live-tested.

Still relevant and unexhausted: ring detail; paged people/entries detail and linked people; legacy SearchHorse/SearchPerson and their API counterparts; populated live/hunter/completed scores/OOG; daily/championship/showChamps/circuit result families; Excel contents/import. These were retained from the original source rather than invented from similar URLs. Pusher updateavailable/rowupdate and parent-frame messages were identified, but their complete event payloads/coverage were not captured; they are not ready replacements for polling.

## Decisions supported by this review

1. Reuse one successful schedule response for ring/group/basic-class inventory and one existing Live/full-class response for compatible downstream trip/OOG/result fields. This is the clearest path to fewer calls; no global savings are claimed yet.
2. Keep direct public readers where proven, using the owner's agreed sequential pacing. No new proxy merely for a public URL.
3. Keep the shared proxy for current Fast schedule/class recovery. Correct its returned-token handling and class-failure/pause/alert gaps through the existing paused work, after the agreed implementation approval.
4. Keep GetShowData as a browser/session-assisted bulk inventory opportunity. Do not treat it as anonymous auth or a replacement for full rings/timing/trip/results coverage.
5. Add no proxy route because a sibling exists. Prove exact read/parameters/data payoff first. Existing omissions above are documented, not quietly filled.

## Evidence

- [Original opportunity document](sgl-application-endpoint-opportunities-20261111.md) and [complete source inventory](evidence/sgl-stage1-20261111/catalog.json).
- [Original auth diagnostics](evidence/sgl-auth-20261111/findings.json), [decoded/token proof](evidence/sgl-auth-20261111/token-handling-proof.json).
- [Full GetShowData payload and controls](evidence/getshowdata-20261111/README.md).
- [Same-class comparison](evidence/wef-source-strategy-20261111/same-class-comparison-current.json).
- [Corrected payload provenance](evidence/payload-audit-20261111/independent-source-verification.json): 13 incorrect copies corrected from actual captures; old readback alone was insufficient.
- [Passive deployed Worker check](evidence/wef-lane-map-20261111/cloudflare-passive-check.json); actual current source readers and route validation inspected without invoking them.
- [Deployed recovery release](../../workers/wef-schedules-decoded/evidence/fallback-profile-release-20261110/release.md); [unreleased dual-health failure](../../workers/wef-schedules-decoded/evidence/dual-health-20261111/task.md).

No source tests, collectors, production edits, new proxy routes, credentials, schedules or business-record repairs were performed for this map.
