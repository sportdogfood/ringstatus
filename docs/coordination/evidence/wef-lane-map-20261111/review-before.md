# WEF source decisions — 11 November 2026

Recommendation for review: reuse the existing smaller iphone responses where they meet the required fields, keeping the full API for schedule inventory, uncovered classes and authoritative verification. Identity coverage is unconfirmed pending the owner's review; no identity limitation or replacement decision is accepted here. Do not build a separate fetch for each downstream table. Production remains unchanged.

## Concrete findings

- Same show and class, captured 30 seconds apart: full `/classes/200031716` and iphone `getLiveClassData` each contained 23 trips. Every iphone `id` matched a full-API `trip_id`; entry number and horse/rider/trainer/owner names agreed for all 23.
- Response bodies were 65,766 versus 19,257 bytes: the iphone body was about 71% smaller. This measures body size, not compressed network traffic or collection speed.
- Identity coverage is awaiting the owner's review; the captured payloads and exact trip matches are available for that review. In this upcoming class its OOG fields were blank while API planned order was populated. Scores were not populated. Those observations alone do not settle overall replacement suitability.
- Another actual class, 200031333, returned 404 from the iphone detail feed while its full API returned two trips. Adding XHR or an anonymous page session did not restore that iphone class. Coverage is demonstrably incomplete.
- Anonymous config and data-response tokens successfully decoded schedules and class detail in isolated diagnostics. The existing decoder rejects a data response containing `_t`; the source app adopts it and retries. That is a demonstrated handling difference, not proof of the cause of every production failure.
- Live already reuses one class response to save class, entry and trip observations. Its live trip projection currently retains position in `results-json`, leaving other available scoring fields unused there. Mapping their meaning needs active/completed samples before changing consumers.

Evidence: [same-class comparison](evidence/wef-source-strategy-20261111/same-class-comparison-current.json), [auth findings](evidence/sgl-auth-20261111/findings.json), [token handling](evidence/sgl-auth-20261111/token-handling-proof.json). All are diagnostics, not normal collection proof.

## Seven-lane source allocation

| App need | Existing source / useful return | Recommended primary and backup | Remaining limit |
| --- | --- | --- | --- |
| Shows and default show | Shows browser discovery reads the rendered app's show and schedule responses; shared proxy fallback exists. Legacy GetShowData browser evidence supplies basic classes/divisions. | Preserve current browser discovery and exact default-show/date checks. Treat GetShowData as a possible bulk inventory supplement only. | Browser discovery had recorded timeouts. GetShowData is not a proven replacement for rings, dated schedules or trips. Browser discovery still makes endpoint requests. |
| Live | ListAjax supplies group/class references; LiveScoreWidget supplies ring activity; getLiveClassData supplies supported class rows. | Preserve these specialized public readers. Reuse each successful detail response across its existing consumers. Full class API covers missing fields/classes. | Direct local public access does not prove direct Cloudflare access. Active, completed and hunter scoring coverage remains unverified. |
| Schedule | Full `/schedule` supplies rings, groups, basic classes, status and trip totals together. | One useful schedule fetch per due day; save its inventory before class work. Existing runner-first shared proxy and independent hosted fallback remain the transport. | Schedule health passing does not prove class detail works. Valid zero differs from `{}` or malformed data. |
| Class detail | Full `/classes/{id}` supplies class state, verification and linked trip/participant identities. | Retain the existing reader while reviewing iphone coverage. Consider full API for uncovered classes and authoritative final verification. | Normal Fast class collection is failing; isolated successful requests do not close it. Identity coverage and replacement suitability await owner review. |
| Trips | Both full class and supported iphone responses contain trip rows; 23 exact ID matches were verified. | Review reuse of the same Live response for existing trip consumers, preserving established keys. | Identity mapping awaits owner review. Do not change mappings or clear existing fields. No consumer change is implemented. |
| Order-of-go | API planned order plus Live order/activity observations; calculator consumes saved data. | Reuse those saved observations. Keep planned order, actual order and observed current/next separate. No new upstream calculator fetch. | The comparison iphone sample had blank OOG; it is not a proven planned-order source. Incomplete queues must not produce invented ETA or order. |
| Results | Full class response includes result values and verification; iphone schema includes score/time/fault fields; results service reads saved records. | Reuse class/Live responses for compatible provisional updates. Use authoritative verification before declaring final results. | Populated active/completed scores were not compared. Activity/status is not verification. Division-result routes are not automatically class-detail replacements. |

## Request payoff and pacing

Use the existing stages in this order: discover the correct show/day; save a due day's schedule; fetch only eligible class detail; let saved data feed trip, OOG and result consumers. Preserve Fast's existing exclusion of Dropped and Completed+verified classes. The separate schedule/class stage correction remains pending; this report does not deploy it.

Both Fast and Live currently hardcode four class workers. Live's direct calls bypass the hosted queue, and its transient retry waits 500 ms. Fast's proxy callers pass through the shared queue but can still occupy four workers waiting. There is no measured basis for four or ten. The owner's requested sequential calls and agreed one-second gap should be the correction, using the existing readers rather than another queue.

Do not add `/health` before every collection request: health itself fetches a schedule. Passive tunnel/port evidence diagnoses transport; a useful data request proves source availability. Separate clock logging stays separate.

## Recovery options and their actual readiness

| Option | Current evidence | Use / boundary |
| --- | --- | --- |
| Existing local runner through permanent tunnel | Built; latest saved scheduled health at 16:28:59.438Z returned nine rings through runner, original-header. | Keep first. A connected tunnel does not prove runner listener, token or class response is healthy. |
| Existing direct Cloudflare TLS fallback and profiles | Built in shared proxy; independent of local runner/tunnel. | Retain bounded profile sweep, one renewal and one repeat sweep. Profiles address request context; they are not independent SGL data sources. |
| Specialized iphone feeds | Actual anonymous local responses and exact trip-ID comparison above. | Reduce repeated full detail only for proven fields and supported classes. Fall back to full API for gaps. Same SGL service may still fail. |
| Existing browser method / rendered capture | Shows browser method is built; class-page diagnostic data was observed. | Possible human-assisted recovery when API readers fail, after exact show/day/class verification. Browser-wide acquisition/import is not an installed fallback. |
| Official Excel export / CSV import | Export returned a binary response; workbook contents were not retained or parsed. No current importer was found in inspected active acquisition code. | Not operational. A human download/upload can be proposed only with verified columns, identity keys, source date and an agreed import target. Do not infer a ready CSV pipeline. |
| Manual input or last good saved data | Existing Airtable is available; no new repair was performed. | A proposed human handoff must name the responsible person, exact input and record location. Saved data can be displayed with its actual last-success time; it cannot be labelled fresh. |

## Failure decision and restoration

1. **Transport failure:** distinguish tunnel zero connections, runner listener unavailable and request timeout from source HTTP errors. Use existing passive task/tunnel/log evidence. An individual QUIC reconnect is not total tunnel failure. Existing hosted fallback can continue independently; this review does not authorize a restart.
2. **Source/auth failure:** retain actual status, route, selected profile and redacted cause. Use the existing bounded recovery rather than an endless retry. The documented returned-token handling difference needs its own approved correction and preview proof.
3. **Empty or partial data:** validate expected identity and shape. Accept legitimate zero. Do not call `{}` successful, mark missing classes deleted, overwrite rich fields with omissions or switch to a different show/date to obtain a passing check.
4. **Class-only failure:** save successful schedules and unaffected work; record the class stage as failed. Schedule success must not hide it. Restore only when normal scheduled class requests save useful data.
5. **Write/notification failure:** distinguish a successful fetch from a successful save and delivered notice. Keep warning-log as delivery authority; submission is not owner acknowledgment. Preserve the existing once-per-outage/recovery behavior; retry undelivered notices on the next normal check.

## Bounded next decision

The immediate work is the existing Fast class failure, sequential pacing and missing class-request alerts, not more endpoint discovery. Keep the existing URLs, sources and infrastructure. The existing OPP-004/006/007/008 tasks contain these concerns; implementation remains paused pending an agreed narrow change.

Before adopting iphone updates more broadly, first obtain the owner's review of identity coverage. Remaining coverage evidence also needs a small agreed matrix: upcoming, active, completed/verified, hunter and a known uncovered class. Compare populated OOG/scores and partial/blank handling. No further endpoint tests are part of this review. Official-file/manual recovery remains a separate unimplemented option, not an assurance of coverage.

## Airtable evidence delivery

The 40 evaluated opportunity records now have separate `attempt-1`, `response-1`, `payload-1`, `observation-1` fields. Each shows one representative retained request. `payload-1` contains only actual JSON, with secrets redacted and array excerpts disclosed in the observation. No successful JSON is invented for failures, HTML, binary exports or source-only contracts. Existing detailed attempt history is retained separately. Forty-record readback matched all four submitted fields; no new SGL requests were needed for this layout.

The 229-route inventory remains a reference, not a requirement to invoke all routes. Account/action and owner-deferred families stay outside current focus.
