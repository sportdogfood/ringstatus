# Automatic fallback release review

Reviewer: wef-decoder / 01a1a753-dce7-7951-a147-1b096e1538ba. 2026-11-06T23:07:14.019Z. One-agent review; no independent second reviewer claimed.

## Findings and acceptance

PASS for approved schedule and class-detail fallback. Deployed wef-schedules-decoded version 156aec1d-4c5a-45c8-a396-5c0b3f21e767. Existing agent URLs and primary runner/tunnel settings preserved. No collector, record repair, dependency upgrade, credentials write, task change or runner stop/restart. Protected file hashes checked before and after deployment.

Normal valid local payloads pass through with X-RS-Source-Lane:runner. Primary HTTP error, exception, absent decoded marker, malformed/empty/nonobject JSON invokes one alternate direct-TLS attempt. Only schedule and classes/{id} enter fallback; unrelated routes preserve behavior. Identified responses with empty arrays remain valid. Both-failed path returns an error; no decoded marker on failure and no retry loop. Fallback invokes existing decoder with internal SGL_FETCH override, retaining the previously approved empty-object guard. Installed local decoder remains unchanged.

The verification header requires existing RUNNER_KEY and does not reach SGL. Invalid key does not force fallback. A primary outage was simulated only in preview by replacing the preview runner origin with an unavailable domain; both routes returned populated fallback data. No public debug route was deployed.

## Evidence

Actual regression command log: execution-log.json,18tests passed. Covers primary success, HTTP401/403/429/502, connection failure, undecoded/invalid/empty primary, failed fallback, unrelated/invalid requests, correct/wrong verification key, renewal reuse/failure and existing decoder regression cases. Logs redact source bodies and credentials. Preview synthetic empty object returned502 without decoded marker; identified empty list remained200. See preview-empty-checks.json.

Preview source checks: preview-live-checks.json,2normal local routes and2automatic fallback routes all usable. Live checks: live-checks.json,2secret-authorized direct fallback routes returned7rings and8class trips;2ordinary shared routes and2ordinary broad-tooth routes remained on runner with useful content. Requests at least20seconds apart. Production runner task last start was unchanged at17:29Eastern; scheduled-task readback said Running. Socket-listener query returned0 despite actual normal live runner responses; do not use that observation as proof of process failure or invent its cause.

Deployment output and actual version: execution-log.json and deployment.json. Source authority/baseline: scope.md, shared-worker.mjs.before, worker.js.before and protected-hashes.json. Full live command output exists across original tool session72926; live-checks.json preserves request start/end, status, lane and acceptance. A complete process-level structured stdout/stderr/start log was not captured for that live diagnostic command; this review inspects the preserved request results and original tool exit0, rather than claiming an independently re-executed live test.

## Scope/limits

No automatic fallback for other route families is claimed. No collector workflow or long-term reliability proof. Actual primary was not stopped to simulate outage. The Cloudflare/SGL providers are shared; this is independence from the local runner/tunnel, not all-provider redundancy. Primary timeout remains30seconds; fallback may add token and data sockets of up to20seconds each, so callers with shorter deadlines can disconnect first. A stalled30second-primary/client acceptance case was not exercised. Native TLS transport requests identity compression and fails explicitly on unexpected encoding or framing; cap5MiB. Prior TCP framing mock evidence is inspected from the independent preview, not independently rerun here.

No global/cross-agent pacing was added. The1second renewal gap and20second test spacing are not a production-wide queue. Original empty-object fix now protects hosted schedule/class primary acceptance and the direct fallback decoder; activation in the installed local decoder and other direct/local consumers remains undone to preserve the running path.

Read the operational reference ../../AUTOMATIC-FALLBACK.md. No secret values are in this packet.
