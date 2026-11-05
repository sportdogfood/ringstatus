# Profile GeoIP coordinate ping — 5 November 2026

Baseline309ac5f8. Owner approved finishing the ping through existing Astro, preserving Recognize/Profile loading, testing and deploying. Allowed delta: optional authenticated coordinate metadata on the existing Profile endpoint and person/device-scoped device retention. Protected: cookies, IAM, Airtable schema/records, Schedule, styling, package priority and refresh cadence. No dependency changes.

Implementation: existing GET /test/rs-profile/data?view=geoip, protected by the same remembered identity and matching actor scope. Cloudflare coordinates first; existing server-side GeoJS fallback,1.5-second timeout. No raw IP is returned/stored. The client starts without awaiting after the minimal Profile response, never holds the render/save path, deduplicates within the document, validates values and fences account/logout changes. Metadata is device-held only; no Airtable reads/writes in this path.

Checks:49 targeted tests passed (27 server/GeoIP/loading and22 service/V3); build passed. A deliberately unresolved GeoIP promise did not hold Profile. Provider failure/invalid coordinates, duplicate starts, stale account responses, unauthenticated actor and actor mismatch were exercised. Existing saved/pending/priority/refresh tests passed. The pre-existing modified service test file was not included in the release.

Initial live verification caught a runtime compatibility failure: the new lookup specified redirect:error, which Cloudflare Workers rejects before sending a request. The existing regional implementation does not use that option. Removed that added option; the regression test now rejects it. Source: https://developers.cloudflare.com/workers/runtime-apis/request/ . Corrected release82d2213986843221115a7cb3d3385c6229d6f3cb deployed successfully as dfa02e94-6a40-48c2-8ecd-7cea51ef944d at2026-11-05T13:00:11.710Z.

Live acceptance PASS through the existing recognized browser/app flow:
- Actual GeoIP request returned200 with valid numeric coordinates, source geoip and observation time. No coordinate values, raw IP, person UID, device token or cookie are included in this evidence.
- Device metadata matched the recognized person and device association. Initial observation13:00:27.625Z; a subsequent actual Home overlay reload updated it to13:01:00.802Z while the standalone app stayed mounted, proving the website path also refreshed the shared device metadata.
- Resource timings in the live app: minimal Profile completed2704.6ms after navigation; GeoIP started2722.4ms and completed3154.4ms. Roster started2721.5ms and continued alongside it. The Profile control opened normally. GeoIP was not awaited by Profile or the background package queue.
- Reload with only view=geoip requests temporarily blocked retained the prior coordinate metadata and opened Profile normally. Blocks were cleared and owned test tabs closed afterward.
- Slow lookup, provider failure, invalid coordinates, duplicate starts, account change/stale response and server authorization were tested offline. There were no Airtable reads or writes in the new endpoint path. No cookies, Recognize code, styling, Schedule engine or eight-minute timer changed.
- The runtime-compatible correction reran the six GeoIP/fast-display/priority/refresh tests successfully; the initial broader49 checks and build also passed. The initial live null-result failure was not counted as success.

The optional precise-location permission action and Airtable coordinate persistence were not added. The ping is once per recognized person/device per document, without an added recurring timer.

Rollback: revert GeoIP commits82d22139 and24d1ce17 as an explicitly authorized rollback; preserve subsequent owner changes. No schema migration or Airtable record repair is required.
