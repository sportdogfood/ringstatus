# Independent Cloudflare fallback preview

Reviewer: wef-decoder / 01a1a753-dce7-7951-a147-1b096e1538ba. No second reviewer claimed. Recorded 2026-11-06T22:53:05.080Z.

## Findings and boundaries

Confirmed useful independent preview path: Cloudflare TLS socket -> SGL -> existing decoder. Uses the existing account, Wrangler and decoder. No local runner, tunnel, service binding or external relay. Secure transport validates TLS; serialized HTTP headers contain no CF-Worker. The decoder copy differs from installed production source only by replacing the fetch transport. No dependencies or production configuration changed.

Initial explicit renewal succeeded, but next preview request503 because token held in module memory was absent. Owner separately approved preview-only correction: when missing, invoke the existing renewal handler within the same request, wait1second, then request source data. Subsequent six data checks passed. This is preview token behavior, not a deployed renewal cadence.

The separate tls.peet.ws echo probe failed incomplete-body validation and remains a known diagnostic gap. It was not patched or counted as passed. Actual SGL renewal, schedule and class response parsing succeeded. No destination echo proof is claimed for the socket path; mock execution confirms exact serialized headers lack CF-Worker.

## Actual acceptance

Three schedule reads returned HTTP200, 7rings, show200000077/date2026-11-06 and53resolved class IDs. Three class-detail reads returned class200031233 and8trips/8entry IDs. All decoded markers were1 and outputs nonempty. Requests were sequential with at least20seconds after prior completion. All renewal happened inside Cloudflare; only an existing cookie was supplied as a secret binding. Credentials were never returned or saved to evidence.

Proof: data-checks-corrected.json, renewal.json, tcp-token-probe.json. Earlier failure: data-checks.json. Wire gap: wire-check.json. Source scope: source-manifest.json. Offline command and actual output: execution-log.json. HTTP framing mocks tested fixed-length/chunked success, truncation rejection, allowlisted destinations, header construction and socket closure.

## Limitations / disposition

Preview acceptance PASS for independent schedule/class reads and token acquisition. No failover was wired, production deployed, permanent URLs altered, collectors run or local runner stopped. This survives lack of the local runner by construction; no real outage was induced on the working path. It still shares Cloudflare and SGL with the main path; it is not a Cloudflare-outage backup. No claim of production hardening or long-term unattended operation. Preview parser intentionally limits hosts, response size5MiB and timeout20seconds; unexpected compression fails. It is diagnostic code, not approved production infrastructure.

The separately approved empty-object correction remains a local candidate,12offline tests passed but preview/deployment unfinished. This fallback used the unchanged installed decoder (which still accepts empty objects); acceptance explicitly required populated identities/rows instead of relying on200 or marker. New production activation requires owner approval. Preview stopped after testing.

Official API used: https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/
