# Profile GeoIP coordinate ping — 5 November 2026

Baseline309ac5f8. Owner approved finishing the ping through existing Astro, preserving Recognize/Profile loading, testing and deploying. Allowed delta: optional authenticated coordinate metadata on the existing Profile endpoint and person/device-scoped device retention. Protected: cookies, IAM, Airtable schema/records, Schedule, styling, package priority and refresh cadence. No dependency changes.

Implementation: existing GET /test/rs-profile/data?view=geoip, protected by the same remembered identity and matching actor scope. Cloudflare coordinates first; existing server-side GeoJS fallback,1.5-second timeout. No raw IP is returned/stored. The client starts without awaiting after the minimal Profile response, never holds the render/save path, deduplicates within the document, validates values and fences account/logout changes. Metadata is device-held only; no Airtable reads/writes in this path.

Checks:49 targeted tests passed (27 server/GeoIP/loading and22 service/V3); build passed. A deliberately unresolved GeoIP promise did not hold Profile. Provider failure/invalid coordinates, duplicate starts, stale account responses, unauthenticated actor and actor mismatch were exercised. Existing saved/pending/priority/refresh tests passed. The pre-existing modified service test file was not included in the release.

Initial live verification caught a runtime compatibility failure: the new lookup specified redirect:error, which Cloudflare Workers rejects before sending a request. The existing regional implementation does not use that option. Removed that added option; the regression test now rejects it. Source: https://developers.cloudflare.com/workers/runtime-apis/request/ . Live coordinate acceptance remains pending the corrected deployment.

Rollback: revert only the GeoIP correction commit; preserve subsequent owner changes. No schema migration or Airtable record repair is required.
