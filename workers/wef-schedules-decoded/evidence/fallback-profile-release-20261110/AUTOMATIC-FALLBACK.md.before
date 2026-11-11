# Automatic schedule and class-detail fallback — 6 November 2026

Owner authorized deployment after the independent preview passed. Main connection settings and agent URLs are unchanged. Changes remain subject to explicit owner approval and preview/live verification.

Current pacing addition: [shared one-second proxy queue](SHARED-PACING.md) now serializes requests across agents and both paths, with separate owner approval and preview/live proof.

## Request path

1. Normal schedule and `/classes/{id}` requests use the existing permanent tunnel and local runner first.
2. A primary HTTP error, connection error, missing decoded marker, invalid JSON or empty top-level object triggers one direct Cloudflare TLS attempt, using the existing decoder.
3. Cloudflare renews its own anonymous SGL token through `/utilities/get-config`; fallback does not call the local runner or tunnel. It reuses a valid token within an instance, renews within 24 hours of expiry, shares pending renewal within that instance, and waits one second after renewal before requesting data. New instances renew independently; module memory is not persistent storage.
4. If fallback fails, return an error. There is no retry loop or permanent switch away from local.

Other source routes retain their previous behavior. This fallback protects against loss of the local runner/tunnel; it still shares Cloudflare and SGL with the primary path and does not protect against every provider/source outage.

## Implementation and bounds

- `shared-worker.mjs`: local-first decision and fallback activation.
- `fallback.mjs`: independent anonymous token renewal and existing decoder invocation.
- `tcp-fetch.mjs`: tested direct TLS transport to the SGL host only. It writes HTTP without `CF-Worker`; normal Workers fetch re-adds that header. The transport requests uncompressed responses, validates framing, allows at most 5 MiB and uses a 20-second socket timeout.
- `worker.js`: existing decoder, earlier-approved empty-object guard and an internal transport override. Runner-installed files are unchanged; the local runner was not restarted.
- The existing primary timeout remains 30 seconds. Fallback can add a renewal and a data request; clients with shorter timeouts may disconnect before a slow-primary fallback completes. Fast source/connection failure was exercised; a full 30-second stalled-primary/client acceptance case was not run.
- The subsequent [shared queue release](SHARED-PACING.md) adds production-wide pacing for requests through this proxy. Twenty-second spacing in this fallback release's evidence describes its earlier verification only.

## Verification without stopping local

The existing `RUNNER_KEY` authorizes a private `X-RS-Fallback-Test` request header on eligible routes. A correct key bypasses primary for that request only. An incorrect key does not force fallback. The header is not forwarded to SGL and must never appear in evidence or agent instructions. No public diagnostic route is deployed.

Responses identify the selected path in `X-RS-Source-Lane`: `runner` or `cloudflare-fallback`. Fallback activation logs a redacted path and reason. A marker or HTTP 200 alone is not data acceptance; live checks inspect rings, class identity and trip entry IDs.

Preview deliberately substituted an unavailable runner origin only within preview: both routes automatically switched and returned usable data. Mock tests cover error/empty primary and failed backup, with no collectors. Live tests use the protected header to prove direct fallback while the runner remains running, then ordinary requests to confirm local remains first. Broad-tooth's existing forwarding drops the verification header, so direct forced-live proof uses the shared Worker; ordinary broad-tooth URLs are checked separately.

## Release evidence

Directory: [automatic-fallback-release-20261106](evidence/automatic-fallback-release-20261106/).

- [Deployment](evidence/automatic-fallback-release-20261106/deployment.json)
- [Preview checks](evidence/automatic-fallback-release-20261106/preview-live-checks.json)
- [Empty-object preview checks](evidence/automatic-fallback-release-20261106/preview-empty-checks.json)
- [Live checks](evidence/automatic-fallback-release-20261106/live-checks.json)
- [Regression/deployment output](evidence/automatic-fallback-release-20261106/execution-log.json)

The separate runner-installed empty-object guard has not been activated: preserving the running primary excluded replacing/restarting that process. Hosted schedule/class requests now reject empty primary responses and use the guarded decoder on fallback. Do not describe all other direct/local consumers as repaired. Preserve the earlier evidence rather than rewriting it as current proof.
