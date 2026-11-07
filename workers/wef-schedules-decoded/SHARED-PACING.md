# Shared proxy pacing — 6 November 2026

Owner explicitly approved one shared queue after requesting a one-second pause across agents. The proxy now processes one GET request at a time and waits at least 1,000 ms after its response body completes before starting the next. The same queue covers local and fallback paths and health monitoring.

Local remains first. The tested automatic fallback and agent URLs are unchanged. Installed runner/tunnel files, task definitions, source headers, token logic and fallback transport were not changed. No collectors ran.

## Implementation

- One `SGL_PACER` binding in the existing `wef-schedules-decoded` Worker, class `SglPacer`, SQLite migration `sgl-pacing-v1`.
- All Worker instances select the same object name, `sgl-source`. Broad-tooth's existing decoder binding reaches this same queue.
- `sgl-pacing.mjs` supplies the FIFO and one-second completion gap. Only the next-allowed timestamp is persisted; no payloads or secrets are stored. A request failure releases its queue position while retaining the gap. Persistence failure blocks further source work rather than silently disabling pacing.
- The existing local-first/fallback implementation is preserved as `sourceHandler`. The object consumes each response body before releasing the queue. Health's internal schedule read remains inside its current queue position, avoiding recursive queue deadlock.
- `X-RS-Queue-Start` and `X-RS-Queue-End` report server-side request-processing timestamps. Preview and live checks sort simultaneous requests by these values and compare each next start against the preceding completion.
- Non-GET requests retain their previous rejection behavior. Missing/broken queue configuration returns HTTP 503; it does not send unpaced traffic.

## Scope and limits

This spaces **agent requests through the hosted proxy**, not every internal request within one operation. Token renewal remains part of the existing local/fallback implementation. Direct SGL callers that bypass the hosted proxy are not controlled by this queue; the previously inspected WEF Data source-reader contains such a path and was not migrated by this change.

Queued bursts add waiting time. This does not guarantee that a large burst fits every consumer's existing timeout. Existing primary and fallback deadlines were not changed. No automatic retry, alarm, collector, separate Worker URL or dependency upgrade was added to production.

## Verification and deployment

- 23 offline tests passed: concurrency, real timer, early timer wake, persisted timestamp, request-error continuation, storage failure, same-name routing, existing decoder and fallback behavior.
- Authenticated isolated Cloudflare preview: three simultaneous requests across local and fallback returned usable data, with measured gaps of **1,000 ms and 1,000 ms**. Preview health returned `ok=true`.
- Production: three simultaneous requests through shared and broad-tooth URLs—including forced fallback—returned 7 schedule rings and 8 class trips, with measured gaps of **1,000 ms and 1,000 ms**. Live health returned `ok=true`.
- Deployed version: `a43ff3d7-2838-4acf-92b2-7fe9567961da`.
- The isolated preview had its own namespace and temporary copied secrets; production used its own binding. Preview Worker removal was confirmed by API 404. Wrangler's cleanup reported an unrelated KV-list permission error after the Worker had been removed; that failure is preserved in evidence. Production settings readback confirmed the live queue binding remained present. No authentication reset or extra permission was requested.

Evidence: [live concurrent checks](evidence/shared-pacing-release-20261106/live-concurrent-checks.json), [preview concurrent checks](evidence/shared-pacing-release-20261106/preview-concurrent-checks.json), [live health](evidence/shared-pacing-release-20261106/live-health.json), [command log](evidence/shared-pacing-release-20261106/execution-log.json), [deployment](evidence/shared-pacing-release-20261106/deployment.json), [preview cleanup](evidence/shared-pacing-release-20261106/preview-cleanup.json).

Preserve this baseline. Further implementation or configuration changes require owner approval, preview verification and live checks.
