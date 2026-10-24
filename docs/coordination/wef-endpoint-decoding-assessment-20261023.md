# WEF endpoint decoding and consumer assessment

23 October 2026. Documentation only: inspected existing source, historical endpoint contracts, saved owner captures and release evidence. No endpoint requests, collectors, tests, manual business runs, deployments, code/configuration or Airtable changes. This supplements [the Worker reference](wef-cloudflare-proxies-and-decoding-20261023.md).

## Findings

Only routes already supported by the isolated decoder have demonstrated protected-field recovery in the inspected release evidence. The proposed additional routes are not all proven to require decoding. In particular, the saved entries-list page exposes useful source IDs without underscore blobs. Keep source access, decoding need and business consumer need as separate decisions.

Labels below: CONFIRMED = inspected source/capture/release evidence; UNVERIFIED = not established by inspected evidence. Historical captures do not prove current endpoint behavior.

## Additional routes under consideration

| Route | Decoding determination | Consumer need and current implementation boundary |
|---|---|---|
| /entries | NOT DEMONSTRATED for inspected capture: ten entries, all ten entry_id/horse_id/trainer_id values readable, riders[].rider_id present, zero underscore or token-refresh fields. No decoder expansion justified by this capture alone. | Proposed slower season/show entry and horse identity collection; shared entries module could consume needed associations. Current wef-entries collects selected person/class participation instead; it does not need a global entries crawl to continue. No current new-records caller identified. |
| /entries/:id | UNVERIFIED: historical contract lists entry/horse/trainer/owner/rider/class/entryxclasses_uuid fields, but no attributable raw protected-detail payload was inspected. Potentially useful; decoding must be established against raw evidence. | Best targeted candidate for entries linking enrichment, especially source-backed trainer/rider/horse and entry-to-class associations. Legacy endpoint contract documents this purpose. New entries code has no entry-detail caller; current private follows use class detail. Results may benefit from UUID/placing context, but separate results consumption is not established. |
| /people list | UNVERIFIED: historical discussion records an owner capture of 758 people, ten per page and people_id, but its raw list capture was not located through those references. Detail-route encryption cannot be generalized to the list route. | Proposed slower people directory/season reference collection. Tenant and entries selection may later use the directory; the current person-detail feed already serves selected participation. No current records-module list consumer identified. |
| /shows | UNVERIFIED: saved proxy baseline exposes this route, but no attributed raw response proving protected fields was inspected. | Show/season inventory could use it. Current wef-shows intentionally discovers the default through /ring plus default /schedule; /shows is not required for that working implementation. A listing route must not become a second default-show authority. |
| /ring/:id | UNVERIFIED for this exact route. May's endpoint contract describes a small readable ring-detail object; no exact-route raw capture was inspected. A separate six-ring capture contains protected fields, but has no embedded request provenance and cannot prove /ring/:id needs decoding. | Existing legacy ring_info.js and rings.js use ring detail for helper/status/alerts. New schedules already derives dated rings; new live uses direct live feeds. A future season ring/helper consumer needs an explicit delta, not duplication. |
| /classsignup (day list) | UNVERIFIED: historical contract describes readable rings/upcoming classes/order-readiness fields. No attributable raw response was inspected for protected blobs. | Potential order-readiness/group source for schedules/live if needed. Neither current new module uses it. Existing historical contract is reference, not authorization to add a second live collector. |
| /classsignup/:id | UNVERIFIED: legacy enrichment code consumes group/signup payloads and explicitly detects unusable/null entry keys; that symptom alone does not prove encryption. No attributable raw protected sample was inspected. | Legacy trips_tagger group/order enrichment reference. Could support entries/live if their existing class/live sources lack required facts; currently not established as necessary. Must not substitute for failed live detail or manufacture entry/trip links. |
| /schedule/my | UNVERIFIED: route exists in saved baseline; no raw response or active records-module consumer identified. | Only consider for an explicitly defined authenticated user/team schedule feature. Shared schedules currently collects show scope, not a personal account view. Identity, authorization and visibility contract absent. |
| /entries/my | UNVERIFIED: route exists in saved baseline; no raw response or active records-module consumer identified. | Potential account/team entries, distinct from our own tenant favorites. No evidence that upstream account scope equals RingStatus tenant/user scope. Not required for existing private follows. |

Module assignments in this table are business-use candidates unless an existing source caller is explicitly named. They do not claim a collector, permission or downstream integration already exists.

## Existing routes: useful baseline, not an expansion proposal

| Supported route | Decoding/source evidence | Current consumer |
|---|---|---|
| /schedule | CONFIRMED protected class IDs recovered, underscore source evidence and schedule keys retained; repeated scheduled proof on 21 October. | wef-shows default context and wef-schedules dated reads. |
| /people/:id | CONFIRMED protected participation fields recovered in 21 October release evidence. Older saved May samples also contain 15, 21 and 96 underscore fields respectively; counts establish protected fields, not current health. | wef-entries selected person feeds; CWF consumes the resulting sync rather than duplicate people fetching. |
| /classes/:id | CONFIRMED current consumer requirement: entries private-follow class data and live's source-backed trip-to-entry mapping. Existing model rejects missing decoded identity rather than guessing. | wef-entries and wef-live. Exact fields still require class/show/date validation. |
| /classes list | CONFIRMED current schedules class-list enrichment caller; isolated Worker supports route. Separate raw list-only encryption need was not established here. | wef-schedules/collector-core.js builds /classes for enrichment. Route use does not itself prove encrypted fields. |
| /ring | CONFIRMED decoder route/default-show discovery through retained release proof. Do not infer every ring field is encrypted just because the response passes decoding. | wef-shows discovery. |

## Inspected captures and what they actually establish

1. Entries list: C:/Users/gombc/.codex/attachments/0c811fa5-14f3-41ba-a56a-13cae349167d/Pasted text.txt. Parsed object keys entries,total_entries,records_per_page,page,sort,term,show_id,show_name. Ten rows; all ten entry_id, horse_id and trainer_id populated; rider arrays retain source rider IDs. Zero underscore/_t fields. The associated 15 October reference reports total 666/page size ten, not today's count or an API maximum. This capture may already have passed through an upstream/browser layer; absence of blobs proves no additional decryption is needed for the captured representation, not every future response.
2. Person detail: manual_sgl_payloads/people_19676_show_200000061_1778280002.json; people_19676_show_id_200000061_1778333583.json; people_8778_show_200000061_1778280002.json. Parsed object envelopes include people,total_trips,trips,show_id. Recursive underscore counts 15/21/96; no token-refresh markers. Existing October release evidence, rather than these old counts alone, demonstrates useful recovered IDs.
3. Ring-family capture: C:/Users/gombc/.codex/attachments/6114a40c-145d-47cd-b4df-d09806297dbd/Pasted text.txt. Top-level ARRAY of six rings, 27 underscore fields, no _t. Includes ring identity and nested upcoming groups with null times/counts. Original exact request URL and auth/decode context are not embedded in this body. Do not attribute it to /ring/:id or assume which hidden fields it contains.

The six-ring ARRAY is also a compatibility caution: current decodeSglPayload explicitly rejects top-level arrays. Adding a route alone would not support that captured envelope. Only an approved source-backed decoder adaptation should change accepted shapes; no alternate decoder was built here.

## Proxy compatibility gaps independent of decoding

Source baseline: wef-tenants/evidence/proxy-decode-20261021/broad-tooth-before.js, restored deployment documented in the Worker reference. Do not substitute the different tracked utils/workers copies or worker2 all-pages helper as production evidence.

- Saved deployed baseline /entries and /people list allow only BASIC/BASIC_WITH_DATE fields. They omit page, search_text and sort parameters used in the documented list capture/proposal. A route's existence does not prove paginated forwarding works through that deployed lane.
- Baseline /classsignup permits show_id/customer_id/company_id/eid/class_number, but omits show_date used by the historical day contract. That is a request-compatibility gap to verify before adopting the route.
- Baseline /entries/:id inherits date/basic parameters and does not include the historical eid/team_id variants. The path ID may be sufficient, but equivalence is unverified; do not silently drop required request context.
- The isolated decoder currently has its own strict route/query allowlists. New routes need exact parameter and envelope contracts, not blanket forwarding. Do not add private /my routes to a shared read service without defining account/tenant access.
- Historical docs/sgl_endpoint_contract.md contains May unauthenticated-success claims and a sequential fetch-order suggestion. Those are historical observations, not today's authentication contract or a replacement for the independent recurring WEF architecture. Its examples are not current tests.
- Never treat HTTP 200, missing IDs or null values as proof of encryption or a successful empty collection. Validate error objects, authentication, show/date/scope and expected arrays first.

## Smallest supported next work

No additional route is currently proven both necessary to a new records consumer and in need of protected-field decoding. Preserve the functioning shared decoder and existing consumers.

Priority for further evidence: (1) entry detail for source-backed linking relationships; (2) entries/people list forwarding and pagination for the separately proposed season directories; (3) exact provenance and array shape of the saved protected ring payload. Classsignup and private /my routes remain conditional on a demonstrated missing use case.

Before implementation, obtain an attributable saved raw response for each needed route with exact URL parameters, capture date, auth mode/decoder context and expected identity fields. Inspect protected locations and classify plain, protected, mixed, token-refresh or soft failure. Use saved data first; any new source diagnostics need explicit scope and never count as scheduled workflow proof. No endpoint calls were made during this assessment.

## Source references

- workers/wef-schedules-decoded/worker.js and tests/worker.test.js: allowlists, recursive AES decoder, object-only root, failure/header contract.
- wef-entries/source.js; wef-live/source.js and model.js; wef-shows/collector.js; wef-schedules/collector-core.js: current consumers.
- lib/watch_trips_enrichment.js and trips_tagger.js: legacy classsignup endpoint and unusable-entry handling.
- lib/airtable/ring_info.js and rings.js: existing legacy ring detail callers; execution state not established by their source files.
- docs/sgl_endpoint_contract.md; docs/ringstatus_pipeline_scope_2026-05-08.md: historical shapes and lane limits.
- docs/coordination/wef-slower-collection-resume-plan-20261015.md and wef-engine-discussion-overview-20261010.md: captured list IDs, pagination and unimplemented slower-directory proposal.
- docs/coordination/wef-isolated-decoder-redirect-fix-20261021.md and workers/wef-schedules-decoded/evidence/final-verification.json: retained successful decoding/scheduled proof, distinct from initial failures.
